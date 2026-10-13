"""Run the official cleaned LongMemEval-S set against three isolated memory systems."""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
from time import perf_counter

os.environ["MEM0_TELEMETRY"] = "false"
os.environ["GRAPHITI_TELEMETRY_ENABLED"] = "false"


from agent_memory import MemoryScope
from agent_memory.evaluation.comparison import (
    AgentMemoryComparisonAdapter,
    GraphitiComparisonAdapter,
    Mem0ComparisonAdapter,
    RawInteraction,
)
from agent_memory.capture.policy import CaptureSanitizer
from agent_memory.composition import build_local_kernel
from agent_memory.providers import GeneratedTrajectoryClaimExtractor
from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory

# Keep importing this contract module dependency-free. Live clients are loaded
# only inside run(); offline tests exercise the real core, never mock its store.
MODEL = "qwen3.5:9b"
EMBED_MODEL = "nomic-embed-text"
OLLAMA_URL = "http://localhost:11434"
PROTOCOL_VERSION = "longmemeval-legacy-claims-v2"
MAX_CLAIMS = 8
MINIMUM_CONFIDENCE = 0.8
NUM_CTX = 32768
EXTRACTION_PROMPT = (
    "Extract at most eight concise facts useful to answer future questions from this conversation "
    "segment. Include user or assistant facts, named entities, dates, preferences, plans, and explicit "
    "corrections. Skip general advice and filler. Use stable keys for facts that may be corrected later. "
    "Each fact needs key, value, one short factual text, and numeric confidence between 0 and 1. "
    "Confidence is your uncertain extraction score, not verified truth; do not automatically use 1. "
    "Return an empty claims array if there are no useful facts. Do not invent anything."
    "\n\nConversation segment:\n"
)


CLAIM_SCHEMA = {
    "type": "object",
    "properties": {
        "claims": {
            "type": "array",
            "maxItems": MAX_CLAIMS,
            "items": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                    "text": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                },
                "required": ["key", "value", "text", "confidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["claims"],
    "additionalProperties": False,
}


class BenchmarkClaimGenerator:
    """Bound extraction output so long benchmark sessions produce complete JSON."""

    def __init__(self, client: ollama.AsyncClient, calls: dict[str, int]):
        self.client = client
        self.calls = calls
        self.generated_claims = 0
        self.receipts = []

    async def generate_claims(self, event):
        self.calls["agent-memory"] += 1
        prompt = EXTRACTION_PROMPT + event.content
        response = await self.client.chat(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            format=CLAIM_SCHEMA,
            think=False,
            options={"temperature": 0, "num_predict": 1536, "num_ctx": NUM_CTX},
        )
        self.receipts.append(_usage_receipt(response, "extraction"))
        claims = json.loads(response.message.content)["claims"]
        if not isinstance(claims, list):
            raise ValueError("Qwen claim output must contain a claims array")
        # Fail visibly on a broken legacy generation contract, rather than
        # recording a quality score for a run where all facts silently vanished.
        if len(claims) > MAX_CLAIMS:
            raise ValueError("Qwen claim output exceeds the frozen claim limit")
        for claim in claims:
            if (not isinstance(claim, dict)
                or set(claim) != {"key", "value", "text", "confidence"}
                or any(not isinstance(claim[key], str) or not claim[key].strip()
                       for key in ("key", "value", "text"))
                or type(claim["confidence"]) not in (int, float)
                or not 0 <= claim["confidence"] <= 1):
                raise ValueError("Qwen claim output violates the legacy scored-claim schema")
        self.generated_claims += len(claims)
        return claims


class BenchmarkClaimExtractor(GeneratedTrajectoryClaimExtractor):
    """Count real legacy validation outcomes separately from model proposals."""

    def __init__(self, generator, **kwargs):
        super().__init__(generator, **kwargs)
        self.validated_claims = 0

    async def extract(self, event):
        claims = await super().extract(event)
        self.validated_claims += len(claims)
        return claims


def _usage_receipt(response, phase):
    # Missing provider counts are unknown, never inferred as zero or priced free.
    return {"phase": phase, **{
        key: getattr(response, key, None)
        for key in ("prompt_eval_count", "eval_count", "total_duration")
    }}


def _implementation_identity():
    import agent_memory

    root = Path(agent_memory.__file__).parent
    source = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        source.update(path.relative_to(root).as_posix().encode() + b"\0")
        source.update(path.read_bytes() + b"\0")
    versions = {}
    for name in ("agent-memory", "ollama", "graphiti-core", "mem0ai", "redislite",
                 "qdrant-client", "falkordb"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return {"core_source_sha256": source.hexdigest(), "installed_versions": versions}


def _freeze_run(data_path, selected, output_dir, offset, limit):
    manifest = {
        "protocol_version": PROTOCOL_VERSION,
        "implementation": _implementation_identity(),
        "dataset_sha256": _file_sha256(data_path),
        "question_ids": [item["question_id"] for item in selected],
        "offset": offset, "limit": limit,
        "model": MODEL, "embedding_model": EMBED_MODEL,
        "ollama_url": OLLAMA_URL, "num_ctx": NUM_CTX,
        "claim_schema": CLAIM_SCHEMA, "extraction_prompt": EXTRACTION_PROMPT,
        "minimum_confidence": MINIMUM_CONFIDENCE,
        "chunk_utf8_bytes": 9000, "retrieval_limit": 8,
        "agent_memory_token_budget": 2048,
        "graphiti_previous_episode_context": 2,
        "graphiti_max_extraction_items": MAX_CLAIMS,
        "input_control": "identical timestamped chunks; backend extraction policies differ",
        "runner_sha256": _file_sha256(Path(__file__)),
        "client_adapter_sha256": _file_sha256(Path(__file__).with_name("ollama_memory_comparison.py")),
    }
    digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    frozen = {"run_digest": digest, "manifest": manifest}
    path = output_dir / "run-manifest.json"
    if path.exists():
        if json.loads(path.read_text(encoding="utf-8")) != frozen:
            raise ValueError("run configuration changed; use a fresh output directory")
    else:
        if any(p.stat().st_size for p in output_dir.glob("*.jsonl")):
            raise ValueError("unversioned results cannot be resumed; use a fresh output directory")
        with path.open("x", encoding="utf-8") as target:
            target.write(json.dumps(frozen, ensure_ascii=False, indent=2) + "\n")
    return digest


def _judge_prompt(question_type: str, question: str, answer: str, hypothesis: str, abstention: bool) -> str:
    if abstention:
        return (
            "I will give you an unanswerable question, an explanation, and a response from a model. "
            "Please answer yes if the model correctly identifies the question as unanswerable. The model "
            "could say that the information is incomplete, or some other information is given but the "
            "asked information is not.\n\nQuestion: {}\n\nExplanation: {}\n\nModel Response: {}\n\n"
            "Does the model correctly identify the question as unanswerable? Answer yes or no only."
        ).format(question, answer, hypothesis)
    if question_type == "single-session-preference":
        template = (
            "I will give you a question, a rubric for desired personalized response, and a response from a model. "
            "Please answer yes if the response satisfies the desired response. Otherwise, answer no. The model "
            "does not need to reflect all the points in the rubric. The response is correct as long as it recalls "
            "and utilizes the user's personal information correctly.\n\nQuestion: {}\n\nRubric: {}\n\n"
            "Model Response: {}\n\nIs the model response correct? Answer yes or no only."
        )
    else:
        instruction = (
            "I will give you a question, a correct answer, and a response from a model. Please answer yes if the "
            "response contains the correct answer. Otherwise, answer no. If the response is equivalent to the "
            "correct answer or contains all the intermediate steps to get the correct answer, you should also "
            "answer yes. If the response only contains a subset of the information required by the answer, "
            "answer no."
        )
        if question_type == "temporal-reasoning":
            instruction += (
                " In addition, do not penalize off-by-one errors for the number of days. If the question asks for "
                "the number of days/weeks/months, etc., and the model makes an off-by-one error, the response "
                "is still correct."
            )
        elif question_type == "knowledge-update":
            instruction += (
                " If the response contains some previous information along with an updated answer, it is still "
                "correct as long as the updated answer is the required answer."
            )
        template = instruction + (
            "\n\nQuestion: {}\n\nCorrect Answer: {}\n\nModel Response: {}\n\n"
            "Is the model response correct? Answer yes or no only."
        )
    return template.format(question, answer, hypothesis)


def _benchmark_id(question_id: str, prefix: str) -> str:
    digest = hashlib.sha256(question_id.encode("utf-8")).hexdigest()[:28]
    return f"{prefix}-{digest}"


def _parse_time(value: str) -> datetime:
    cleaned = re.sub(r"\s*\([^)]*\)", "", value).strip()
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
        for pattern in (
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(cleaned, pattern)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(f"unrecognized LongMemEval session timestamp: {value!r}")
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)


def _session_event(question_id: str, session_index: int, session: list[dict], timestamp: str):
    # Deliberately use array order and synthetic IDs. Never feed has_answer,
    # answer_session_ids, question_type, or gold answers into a memory backend.
    body = "\n".join(
        f"{turn['role']}: {turn['content']}"
        for turn in session
        if turn.get("role") in ("user", "assistant") and isinstance(turn.get("content"), str)
    )
    occurred_at = _parse_time(timestamp)
    max_bytes = 9000  # RawInteraction caps UTF-8 input at 12 KB.
    chunks = []
    current = []
    current_bytes = 0
    for char in body:
        char_bytes = len(char.encode("utf-8"))
        if current and current_bytes + char_bytes > max_bytes:
            chunks.append("".join(current))
            current, current_bytes = [], 0
        current.append(char)
        current_bytes += char_bytes
    if current or not chunks:
        chunks.append("".join(current))
    events = []
    for chunk_index, content in enumerate(chunks):
        events.append(
            RawInteraction(
                event_id=f"session-{session_index:03d}-part-{chunk_index:02d}",
                role="user",
                content=content,
                occurred_at=occurred_at,
            )
        )
    return events


def _read_completed(path: Path, run_digest: str | None = None) -> dict[str, dict]:
    if not path.exists():
        return {}
    good_lines = []
    complete = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict) and isinstance(item.get("question_id"), str):
            if run_digest is not None and item.get("run_digest") != run_digest:
                raise ValueError("result row belongs to another or unversioned run")
            complete[item["question_id"]] = item
            good_lines.append(json.dumps(item, ensure_ascii=False))
    if path.read_text(encoding="utf-8").splitlines() != good_lines:
        path.write_text("".join(line + "\n" for line in good_lines), encoding="utf-8")
    return complete


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


async def _answer(client: ollama.AsyncClient, question: str, question_date: str, memories: list[dict]):
    context = "\n".join(f"- {item['text']}" for item in memories) or "(no memory retrieved)"
    prompt = (
        "Answer the question using only the retrieved memory. Do not use outside knowledge. "
        "If the memory does not provide enough information, say you do not know. Answer concisely.\n"
        f"Question date: {question_date}\nQuestion: {question}\nRetrieved memory:\n{context}"
    )
    start = perf_counter()
    response = await client.chat(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        think=False,
        options={"temperature": 0, "num_predict": 384, "num_ctx": 32768},
    )
    return response.message.content.strip(), perf_counter() - start, _usage_receipt(response, "answer")


async def _judge(client: ollama.AsyncClient, question_type, question, gold, hypothesis, abstention):
    prompt = _judge_prompt(question_type, question, gold, hypothesis, abstention)
    response = await client.chat(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        options={"temperature": 0, "num_predict": 16, "num_ctx": 32768},
        think=False,
    )
    label = re.search(r"\b(yes|no)\b", response.message.content.strip(), re.IGNORECASE)
    if not label:
        return None, response.message.content.strip(), _usage_receipt(response, "judge")
    return label.group(1).lower() == "yes", response.message.content.strip(), _usage_receipt(response, "judge")


async def run(data_path: Path, output_dir: Path, work_dir: Path, offset: int = 0, limit: int | None = None):
    if type(offset) is not int or offset < 0 or (limit is not None and (type(limit) is not int or limit < 1)):
        raise ValueError("offset must be nonnegative and limit must be positive")
    dataset = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(dataset, list) or not dataset:
        raise ValueError("LongMemEval input must be a non-empty JSON array")
    selected = dataset[offset : offset + limit if limit is not None else None]
    if not selected:
        raise ValueError("the requested offset/limit selects no questions")

    question_ids = [item["question_id"] for item in selected]
    if any(not isinstance(qid, str) or not qid for qid in question_ids) or len(set(question_ids)) != len(question_ids):
        raise ValueError("selected question IDs must be unique nonempty strings")
    output_dir.mkdir(parents=True, exist_ok=True)
    run_digest = _freeze_run(data_path, selected, output_dir, offset, limit)
    completed_by_arm = {
        arm: _read_completed(output_dir / f"{arm}.jsonl", run_digest)
        for arm in ("agent-memory", "mem0-oss", "graphiti")
    }
    import ollama
    from ollama_memory_comparison import (
        OllamaEmbeddings, _make_arm_clients, _stop_falkordblite,
    )
    work_dir.mkdir(parents=True, exist_ok=True)
    call_counts = {"agent-memory": 0, "mem0-oss": 0, "graphiti": 0}
    client = ollama.AsyncClient(host=OLLAMA_URL)
    mem0 = graphiti = lite_db = None
    try:
        mem0, graphiti, lite_db = await _make_arm_clients(
            work_dir,
            client,
            call_counts,
            mem0_collection_name="longmemeval_s_cleaned_qwen35_9b",
            graphiti_max_extraction_items=8,
        )
        summaries = {}
        for arm_name in ("agent-memory", "mem0-oss", "graphiti"):
            result_path = output_dir / f"{arm_name}.jsonl"
            complete = completed_by_arm[arm_name]
            summaries[arm_name] = {"path": str(result_path), "completed": len(complete)}
            with result_path.open("a", encoding="utf-8") as output:
                for item_index, item in enumerate(selected, start=offset):
                    question_id = item["question_id"]
                    if question_id in complete:
                        continue
                    qhash = _benchmark_id(run_digest + ":" + question_id, "lme")
                    generator = None
                    if arm_name == "agent-memory":
                        scope = MemoryScope("longmemeval-s", user_id=qhash, session_id="official-s")
                        local_path = work_dir / "agent-memory.sqlite3"
                        generator = BenchmarkClaimGenerator(client, call_counts)
                        extractor = BenchmarkClaimExtractor(
                            generator, provider="ollama", model=MODEL,
                            minimum_confidence=MINIMUM_CONFIDENCE, max_claims=MAX_CLAIMS,
                            prompt_version=PROTOCOL_VERSION, fail_open=False,
                        )
                        local = UnifiedMemory(
                            build_local_kernel(
                                local_path,
                                extractor=extractor,
                                embedding_provider=OllamaEmbeddings(client),
                            ),
                            scope,
                            journal=SQLiteDeletionJournal(str(local_path) + ".deletions.db"),
                            sanitizer=CaptureSanitizer(redact_builtin=False),
                        )
                        await local.initialize()
                        adapter = AgentMemoryComparisonAdapter(local)
                    elif arm_name == "mem0-oss":
                        adapter = Mem0ComparisonAdapter(mem0, user_id=qhash)
                    else:
                        adapter = GraphitiComparisonAdapter(
                            graphiti,
                            group_id=qhash,
                            previous_episode_context=2,
                            extraction_instructions=(
                                "Extract only facts explicitly stated in this conversation segment. "
                                "Prefer details about the user, other named people, plans, dates, and corrections. "
                                "Include assistant statements only when they are needed to preserve the conversation. "
                                "Do not extract general world knowledge or advice. Return at most eight entities "
                                "and eight factual relationships."
                            ),
                        )

                    start = perf_counter()
                    calls_before = call_counts[arm_name]
                    ingested_events = 0
                    try:
                        # A stable, per-question namespace makes retries idempotent at the
                        # benchmark level. Clear a partial prior attempt before replaying it.
                        await adapter.clear()
                        input_digest = hashlib.sha256()
                        for session_index, (session, timestamp) in enumerate(
                            zip(item["haystack_sessions"], item["haystack_dates"], strict=True)
                        ):
                            for event in _session_event(question_id, session_index, session, timestamp):
                                input_digest.update((event.body() + "\0").encode("utf-8"))
                                await adapter.add(event)
                                ingested_events += 1
                            if (session_index + 1) % 5 == 0:
                                print(
                                    f"{arm_name}: {question_id} sessions="
                                    f"{session_index + 1}/{len(item['haystack_sessions'])} "
                                    f"events={ingested_events}",
                                    flush=True,
                                )
                        stored_current_claims = (
                            len(await local.provider.get_state(local.scope))
                            if generator is not None else None
                        )
                        query = item["question"]
                        memories = await adapter.search(query, limit=8)
                        hypothesis, answer_seconds, answer_receipt = await _answer(
                            client, query, item.get("question_date", "unknown"), memories
                        )
                        correct, judge_text, judge_receipt = await _judge(
                            client,
                            item["question_type"],
                            query,
                            item["answer"],
                            hypothesis,
                            item["question_id"].endswith("_abs"),
                        )
                        result = {
                            "question_id": question_id,
                            "run_digest": run_digest,
                            "input_digest": input_digest.hexdigest(),
                            "ingested_events": ingested_events,
                            "extraction_diagnostics": ({
                                "generated_claims": generator.generated_claims,
                                "validated_claims": extractor.validated_claims,
                                "stored_current_claims": stored_current_claims,
                                "zero_current_claims": stored_current_claims == 0,
                                "note": "legacy scored claims, not verified Atom admission",
                            } if generator is not None else None),
                            "model_usage_receipts": (
                                (generator.receipts if generator is not None else [])
                                + [answer_receipt, judge_receipt]
                            ),
                            "cost_coverage": "incomplete; external backend calls, embeddings and tariffs not measured",

                            "question_type": item["question_type"],
                            "hypothesis": hypothesis,
                            "judge_label": correct,
                            "judge_output": judge_text,
                            "retrieved_count": len(memories),
                            "answer_seconds": answer_seconds,
                            "total_question_seconds": perf_counter() - start,
                            "memory_model_calls": call_counts[arm_name] - calls_before,
                            "retrieved_memories": memories,
                        }
                        output.write(json.dumps(result, ensure_ascii=False) + "\n")
                        output.flush()
                        complete[question_id] = result
                        summaries[arm_name]["completed"] += 1
                        print(
                            f"{arm_name}: {item_index + 1}/{offset + len(selected)} "
                            f"{question_id} judge={correct}",
                            flush=True,
                        )
                    except Exception as error:
                        # Keep a replayable failure marker, but never persist provider
                        # exception strings (they can include credentials/raw inputs).
                        with (output_dir / "failures.jsonl").open("a", encoding="utf-8") as failures:
                            failures.write(json.dumps({
                                "run_digest": run_digest, "arm": arm_name,
                                "question_id": question_id, "error_type": type(error).__name__,
                                "ingested_events": ingested_events,
                                "generated_claims": generator.generated_claims if generator else None,
                                "validated_claims": extractor.validated_claims if generator else None,
                            }) + "\n")
                        raise
                    finally:
                        try:
                            await adapter.clear()
                        finally:
                            if arm_name == "agent-memory":
                                await local.close()

        metrics = {}
        for arm_name in summaries:
            rows = _read_completed(Path(summaries[arm_name]["path"]), run_digest)
            rows = [rows[item["question_id"]] for item in selected if item["question_id"] in rows]
            labels = [row["judge_label"] for row in rows if isinstance(row["judge_label"], bool)]
            by_type = {}
            for row in rows:
                if isinstance(row["judge_label"], bool):
                    by_type.setdefault(row["question_type"], []).append(row["judge_label"])
            metrics[arm_name] = {
                "completed": len(rows),
                "scored": len(labels),
                "accuracy": sum(labels) / len(labels) if labels else None,
                "by_question_type": {
                    kind: {"accuracy": sum(values) / len(values), "count": len(values)}
                    for kind, values in sorted(by_type.items())
                },
            }
        summary = {
            "benchmark": (
                "LongMemEval-S cleaned"
                if len(dataset) == 500 and data_path.name == "longmemeval_s_cleaned.json"
                else "custom input; not a benchmark score"
            ),
            "run_digest": run_digest,
            "protocol_version": PROTOCOL_VERSION,
            "dataset_path": str(data_path.resolve()),
            "dataset_sha256": _file_sha256(data_path),
            "dataset_questions": len(dataset),
            "selected_questions": len(selected),
            "evaluation_scope": "full" if len(selected) == len(dataset) else "subset",
            "official_score_equivalent": False,
            "offset": offset,
            "answer_model": MODEL,
            "memory_extraction_model": MODEL,
            "embedding_model": EMBED_MODEL,
            "judge_model": MODEL,
            "ollama_num_ctx": 32768,
            "graphiti_max_extraction_items_per_event": 8,
            "graphiti_previous_episode_context": 2,
            "judge_protocol": "LongMemEval official yes/no prompts adapted to Ollama; not official GPT-4o scores",
            "metrics": metrics,
            "results": summaries,
        }
        (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        return summary
    finally:
        if graphiti is not None:
            graph_pid = getattr(getattr(lite_db, "client", None), "pid", None)
            try:
                await graphiti.close()
            finally:
                try:
                    await lite_db.close()
                finally:
                    await _stop_falkordblite(graph_pid)
        if mem0 is not None:
            qdrant = getattr(getattr(mem0, "vector_store", None), "client", None)
            if qdrant is not None:
                qdrant.close()
        await client._client.aclose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True, help="Official longmemeval_s_cleaned.json")
    parser.add_argument("--output-dir", type=Path, required=True, help="Per-arm JSONL and summary output")
    parser.add_argument("--work-dir", type=Path, required=True, help="Persistent sandbox database files")
    parser.add_argument("--offset", type=int, default=0, help="Question offset in the official array")
    parser.add_argument("--limit", type=int, help="Number of questions; omit for the remaining full set")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.data, args.output_dir, args.work_dir, args.offset, args.limit)), indent=2))


if __name__ == "__main__":
    main()
