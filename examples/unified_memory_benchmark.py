"""Repeat a controlled, real local-model comparison; not a benchmark leaderboard."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import inspect
import json
import platform
import statistics
import subprocess
import tempfile
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import ollama
import ollama_memory_comparison as native
from longmemeval_retrieval_diagnostics import retrieval_export
from memory_benchmark_protocol import (
    BenchmarkTimeout,
    alias_hit,
    bounded_evidence,
    canonical_digest,
    checked_call,
    grade_answer,
    require_model_accounting,
    summarize_cases,
)
from memory_benchmark_support import ModelCallLedger, OllamaAccountingProxy

from agent_memory import MemoryScope
from agent_memory.composition import build_local_kernel
from agent_memory.evaluation.comparison import (
    AgentMemoryComparisonAdapter,
    GraphitiComparisonAdapter,
    Mem0ComparisonAdapter,
    RawInteraction,
)
from agent_memory.providers import GeneratedTrajectoryClaimExtractor
from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory

ARMS = ("agent-memory", "mem0-oss", "graphiti")
SETTINGS = {"temperature": 0, "num_ctx": 32768, "seed": 42}
ANSWER_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["answered", "unknown"]},
        "answer": {"type": "string"},
    },
    "required": ["status", "answer"],
    "additionalProperties": False,
}
CLAIM_SCHEMA = {
    "type": "object",
    "properties": {
        "claims": {
            "type": "array",
            "maxItems": 8,
            "items": {
                "type": "object",
                "properties": {key: {"type": "string"} for key in ("key", "value", "text")},
                "required": ["key", "value", "text"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["claims"],
    "additionalProperties": False,
}
EXTRACTION_PROMPT = (
    "Extract at most eight concise user facts, preferences, project settings, plans and explicit "
    "corrections from this timestamped message. Use stable keys for the same fact "
    "across corrections. Preserve names, dates and times. Skip advice and filler. "
    "Do not invent facts. Return JSON claims "
    "with key, value and factual text; return an empty claims array if no useful fact is stated.\n"
)
ANSWER_PROMPT = (
    "只根据给定记忆回答问题，不使用外部知识。用中文简短回答。"
    "信息不足时 status 必须是 unknown，answer 必须是‘未知’；"
    "信息充分时 status 是 answered。问题要求只回答当前值时不要附加旧值。\n"
)


class ControlledClient(ollama.AsyncClient):
    def __init__(self, *args, allow_thinking=False, **kwargs):
        if type(allow_thinking) is not bool:
            raise ValueError("allow_thinking must be a boolean")
        self._allow_thinking = allow_thinking
        super().__init__(*args, **kwargs)

    async def chat(self, **kwargs):
        if kwargs.get("model") != native.MODEL:
            raise ValueError("only the pinned local generation model is allowed")
        requested_thinking = kwargs.get("think", False)
        if type(requested_thinking) is not bool:
            raise ValueError("think must be a boolean")
        if requested_thinking and not self._allow_thinking:
            raise ValueError("thinking requires an explicitly enabled client")
        kwargs["think"] = requested_thinking
        kwargs["options"] = {**kwargs.get("options", {}), **SETTINGS}
        return await super().chat(**kwargs)


class ClaimGenerator:
    def __init__(self, client):
        self.client = client

    async def generate_claims(self, event):
        response = await self.client.chat(
            model=native.MODEL,
            messages=[{"role": "user", "content": EXTRACTION_PROMPT + event.content}],
            format=CLAIM_SCHEMA,
            options={"num_predict": 1536},
        )
        claims = json.loads(response.message.content)["claims"]
        # Host-fixed scope/confidence; no question or evaluator rubric enters this boundary.
        return [{**claim, "scope": "session", "confidence": 0.95} for claim in claims]


class CommonAgentAdapter(AgentMemoryComparisonAdapter):
    @staticmethod
    def packing_token_budget(limit=8):
        # Native estimates bound UTF-8 text by four bytes per estimated token.
        # Reserve bullet separators inside the shared 8192-byte evidence budget.
        return (8192 - 3 * limit) // 4

    async def search(self, query, limit=8):
        from agent_memory.retrieval.packing import ordered_bundle_evidence

        # The shared final byte guard still applies to every benchmark arm.
        bundle = await self.memory.recall(
            query,
            limit=limit,
            token_budget=self.packing_token_budget(limit),
            include_current_state=False,
        )
        ranked = ordered_bundle_evidence(bundle)
        # Serialize only governed results and bounded, body-free kernel diagnostics.
        result, self.last_retrieval_diagnostics = retrieval_export(bundle, ranked[:limit])
        return result


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def events_from_fixture(fixture, key):
    return tuple(
        RawInteraction(
            event["event_id"],
            event["role"],
            event["content"],
            datetime.fromisoformat(event["occurred_at"]),
        )
        for event in fixture[key]
    )


async def check_case(client, adapter, ledger, row, spec, timeout):
    memories = await checked_call(
        ledger, row, "search." + spec["id"], adapter.search(spec["query"], limit=8), timeout
    )
    evidence = bounded_evidence(memories)
    # Pass only the question and bounded retrieved evidence, never the rubric.
    prompt = ANSWER_PROMPT + f"问题：{spec['query']}\n记忆：\n{evidence or '(没有检索到记忆)'}"
    response = await checked_call(
        ledger,
        row,
        "answer." + spec["id"],
        client.chat(
            model=native.MODEL,
            messages=[{"role": "user", "content": prompt}],
            format=ANSWER_SCHEMA,
            options={"num_predict": 256},
        ),
        timeout,
    )
    answer = json.loads(response.message.content)
    row["cases"].append(
        {
            "id": spec["id"],
            "phase": spec["phase"],
            "query": spec["query"],
            "retrieved": memories,
            "answer_evidence": evidence,
            "evidence_bytes": len(evidence.encode()),
            "answer": answer,
            "retrieval_hit": (
                alias_hit(evidence, spec["required_aliases"])
                if not spec.get("unanswerable")
                else None
            ),
            "grade": grade_answer(spec, answer),
        }
    )


async def run_arm(client, adapter, ledger, fixture, trial, output, timeout):
    row = {
        "trial": trial,
        "name": adapter.name,
        "status": "running",
        "operations": [],
        "cases": [],
        "deletion_checks": [],
        "cleanup_error_types": [],
        "deletion_pass": False,
    }
    started = perf_counter()
    output["arms"].append(row)
    try:
        for phase, key in (("initial", "initial"), ("updated", "updates")):
            for event in events_from_fixture(fixture, key):
                print(f"trial={trial} arm={adapter.name} add={event.event_id}", flush=True)
                await checked_call(
                    ledger, row, "add." + event.event_id, adapter.add(event), timeout
                )
            for spec in fixture["cases"]:
                if spec["phase"] == phase:
                    await check_case(client, adapter, ledger, row, spec, timeout)
                    print(
                        f"trial={trial} arm={adapter.name} case={spec['id']} "
                        f"passed={row['cases'][-1]['grade']['passed']}",
                        flush=True,
                    )
        await checked_call(ledger, row, "delete.run", adapter.clear(), timeout)
        queries = list(dict.fromkeys(spec["query"] for spec in fixture["cases"]))
        for index, query in enumerate(queries):
            results = await checked_call(
                ledger, row, f"search.deleted.{index}", adapter.search(query, limit=8), timeout
            )
            row["deletion_checks"].append({"query": query, "results": results})
        row["deletion_pass"] = len(row["deletion_checks"]) == len(queries) and all(
            not item["results"] for item in row["deletion_checks"]
        )
        row["status"] = "completed"
    except (BenchmarkTimeout, asyncio.CancelledError) as error:
        row["status"] = "aborted"
        row["error_type"] = type(error).__name__
        raise
    except Exception as error:
        row["status"] = "failed"
        row["error_type"] = type(error).__name__
        row["deletion_pass"] = False
    finally:
        try:
            await checked_call(ledger, row, "cleanup", adapter.clear(), timeout)
        except (Exception, asyncio.CancelledError) as error:
            row["cleanup_error_types"].append(type(error).__name__)
            if isinstance(error, (BenchmarkTimeout, asyncio.CancelledError)):
                row["status"] = "aborted"
                row["error_type"] = type(error).__name__
                raise
            if row["status"] == "completed":
                row["status"] = "failed"
                row["error_type"] = type(error).__name__
        finally:
            row["total_seconds_including_cleanup"] = perf_counter() - started
            row["quality"] = summarize_cases(row["cases"], planned=len(fixture["cases"]))
    return row


def aggregate(rows, repeats, planned_per_arm):
    result = []
    for arm in ARMS:
        selected = [row for row in rows if row["name"] == arm]
        cases = [case for row in selected for case in row["cases"]]
        timings = [row["total_seconds_including_cleanup"] for row in selected]
        result.append(
            {
                "name": arm,
                "planned_trials": repeats,
                "completed_trials": sum(row["status"] == "completed" for row in selected),
                "quality": summarize_cases(cases, planned=repeats * planned_per_arm),
                "deletion_passes": sum(row.get("deletion_pass", False) for row in selected),
                "mean_seconds": statistics.mean(timings) if timings else None,
                "sample_stdev_seconds": statistics.stdev(timings) if len(timings) > 1 else None,
            }
        )
    return result


async def close_trial_resources(local, mem0, graph, lite, pid):
    errors = []
    actions = []

    def add_close(label, owner):
        if owner is not None:
            actions.append((label, owner.close))

    add_close("agent", local)
    add_close("graphiti", graph)
    add_close("falkor", lite)
    if pid is not None:
        actions.append(("falkor-process", lambda: native._stop_falkordblite(pid)))
    add_close("mem0-history", mem0)
    if mem0 is not None:
        add_close("qdrant", getattr(getattr(mem0, "vector_store", None), "client", None))
        add_close(
            "mem0-llm",
            getattr(getattr(getattr(mem0, "llm", None), "client", None), "_client", None),
        )
        add_close(
            "mem0-embedding",
            getattr(
                getattr(getattr(mem0, "embedding_model", None), "client", None), "_client", None
            ),
        )
    if graph is not None:
        add_close("graphiti-embedding", getattr(getattr(graph, "embedder", None), "client", None))
    for label, action in actions:
        try:
            result = action()
            if inspect.isawaitable(result):
                await result
        except Exception as error:
            errors.append({"resource": label, "error_type": type(error).__name__})
    return errors


async def run(args):
    fixture = json.loads(args.fixture.read_text())
    args.output_dir.mkdir(parents=True, exist_ok=False)
    fixture_path = args.output_dir / "fixture.json"
    write_json(fixture_path, fixture)
    source_root = Path(__file__).resolve().parents[1]
    hashes = {
        path.name: sha256(path.read_bytes()).hexdigest()
        for path in (
            Path(__file__),
            Path(__file__).with_name("memory_benchmark_protocol.py"),
            Path(__file__).with_name("memory_benchmark_support.py"),
            Path(__file__).with_name("longmemeval_retrieval_diagnostics.py"),
            Path(__file__).with_name("ollama_memory_comparison.py"),
        )
    }
    report = {
        "schema_version": 2,
        "status": "running",
        "arms": [],
        "backend_setup": [],
        "created_at": datetime.now(UTC).isoformat(),
        "run_kind": "repeated-engineering-diagnostic",
        "manifest": {
            "dataset_id": fixture["dataset_id"],
            "fixture_sha256": canonical_digest(fixture),
            "input_sha256": canonical_digest({key: fixture[key] for key in ("initial", "updates")}),
            "source_commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=source_root, text=True
            ).strip(),
            "runner_sha256": hashes,
            "generation_model": native.MODEL,
            "embedding_model": native.EMBED_MODEL,
            "settings": {**SETTINGS, "thinking": False},
            "top_k": 8,
            "shared_answer_evidence_utf8_bytes": 8192,
            "agent_internal_estimated_token_budget": CommonAgentAdapter.packing_token_budget(),
            "agent_embedding_input_contract": {
                "version": native.OllamaEmbeddings.retrieval_prefix_version,
                "roles": ["query", "document"],
                "truncate": False,
            },
            "generation_limits": {
                "agent_extract": 1536,
                "mem0_extract": 2048,
                "graphiti_extract": 8192,
                "shared_answer": 256,
            },
            "graphiti_max_extraction_items": 8,
            "graphiti_previous_episodes": 2,
            "prompt_sha256": canonical_digest(
                {
                    "extraction": EXTRACTION_PROMPT,
                    "answer": ANSWER_PROMPT,
                    "claim_schema": CLAIM_SCHEMA,
                    "answer_schema": ANSWER_SCHEMA,
                }
            ),
            "repeats": args.repeats,
            "operation_timeout_seconds": args.operation_timeout,
            "per_arm_timing_includes_setup": False,
            "platform": platform.platform(),
            "python": platform.python_version(),
            "dependency_versions": {
                dist.metadata["Name"]: dist.version for dist in importlib.metadata.distributions()
            },
        },
        "excluded": [
            {
                "name": "hindsight",
                "status": "not_deployed",
                "reason": "requires an isolated service, PostgreSQL vector backend and adapter",
            }
        ],
        "limitations": [
            "Authored small diagnostic, not LongMemEval or a held-out leaderboard.",
            "Native extraction prompts and output limits differ; shared answering is controlled.",
            "Tests generic UnifiedMemory capture/recall, not V7.2 MemoryHost authority "
            "or L2/L3 acceptance.",
            "Substring rubrics need answer review; three repetitions do not establish "
            "general superiority.",
            "Delete probes verify API-visible emptiness, not physical erasure "
            "or per-source parity.",
            "Token counts are server-reported; missing usage remains unknown. "
            "No dollar/energy total is measured.",
        ],
    }
    manifest = report["manifest"]
    ledger = ModelCallLedger(args.output_dir / "model_calls.jsonl")
    started = perf_counter()
    with OllamaAccountingProxy(args.upstream, ledger, timeout=args.operation_timeout) as proxy:
        native.OLLAMA_URL = proxy.url
        client = ControlledClient(host=proxy.url, timeout=args.operation_timeout)
        try:
            ledger.start_phase(0, "shared", "manifest")
            tags = await client.list()
            manifest["ollama_version"] = (await client._client.get("/api/version")).json()
            manifest["models"] = [
                item.model_dump(mode="json")
                for item in tags.models
                if item.model in (native.MODEL, native.EMBED_MODEL + ":latest")
            ]
            if not any(item["model"] == native.MODEL for item in manifest["models"]):
                raise ValueError("pinned local generation model is not installed")
            ledger.start_phase(0, "shared", "warmup")
            await client.chat(
                model=native.MODEL,
                messages=[{"role": "user", "content": "Reply ready."}],
                options={"num_predict": 8},
            )
            await client.embed(model=native.EMBED_MODEL, input=["warmup"])
            manifest["protocol_sha256"] = canonical_digest(manifest)
            write_json(args.output_dir / "report.json", report)
            for trial in range(1, args.repeats + 1):
                shift = (trial - 1) % len(ARMS)
                order = ARMS[shift:] + ARMS[:shift]
                print(f"trial={trial} order={','.join(order)}", flush=True)
                with tempfile.TemporaryDirectory(prefix="unified-memory-benchmark-") as sandbox:
                    root = Path(sandbox)
                    ledger.start_phase(trial, "shared", "backend_setup")
                    calls = {arm: 0 for arm in ARMS}
                    setup_started = perf_counter()
                    mem0, graph, lite = await native._make_arm_clients(
                        root,
                        client,
                        calls,
                        graphiti_max_extraction_items=8,
                    )
                    report["backend_setup"].append(
                        {"trial": trial, "seconds": perf_counter() - setup_started}
                    )
                    original_chat = mem0.llm.client.chat

                    def controlled_mem0_chat(_chat=original_chat, **kwargs):
                        kwargs["options"] = {**kwargs.get("options", {}), **SETTINGS}
                        return _chat(**kwargs)

                    mem0.llm.client.chat = controlled_mem0_chat
                    mem0.llm.client._client.timeout = args.operation_timeout
                    mem0.embedding_model.client._client.timeout = args.operation_timeout
                    graph.embedder.client.timeout = args.operation_timeout
                    path = root / "agent.sqlite3"
                    local = UnifiedMemory(
                        build_local_kernel(
                            path,
                            extractor=GeneratedTrajectoryClaimExtractor(
                                ClaimGenerator(client),
                                provider="ollama",
                                model=native.MODEL,
                                event_types=(
                                    "user.message",
                                    "agent.model.completed",
                                    "agent.tool.completed",
                                ),
                                fail_open=False,
                            ),
                            embedding_provider=native.OllamaEmbeddings(client),
                        ),
                        MemoryScope(
                            "unified-benchmark", user_id=uuid4().hex, session_id=uuid4().hex
                        ),
                        journal=SQLiteDeletionJournal(str(path) + ".deletions.db"),
                    )
                    adapters = {
                        "agent-memory": CommonAgentAdapter(local),
                        "mem0-oss": Mem0ComparisonAdapter(mem0),
                        "graphiti": GraphitiComparisonAdapter(graph, previous_episode_context=2),
                    }
                    pid = getattr(getattr(lite, "client", None), "pid", None)
                    try:
                        await local.initialize()
                        for name in order:
                            row = await run_arm(
                                client,
                                adapters[name],
                                ledger,
                                fixture,
                                trial,
                                report,
                                args.operation_timeout,
                            )
                            await require_model_accounting(proxy, row, args.operation_timeout)
                            row["model_usage"] = ledger.summary(trial=trial, arm=name)
                            write_json(args.output_dir / "report.json", report)
                    finally:
                        resource_errors = await close_trial_resources(local, mem0, graph, lite, pid)
                        report.setdefault("resource_cleanup_errors", []).extend(resource_errors)
                        if resource_errors:
                            # A failed close can leave a model client or database
                            # alive. Do not start another isolated trial or report
                            # a completed run while resource ownership is unknown.
                            raise RuntimeError("resource cleanup failed; stop before the next trial")
            report["status"] = (
                "completed"
                if all(row["status"] == "completed" for row in report["arms"])
                else "completed_with_failures"
            )
        except (Exception, asyncio.CancelledError) as error:
            report["status"] = "aborted" if isinstance(error, asyncio.CancelledError) else "failed"
            report["error_type"] = type(error).__name__
            raise
        finally:
            await client._client.aclose()
            drain_error = None
            try:
                await require_model_accounting(proxy, report, args.operation_timeout)
            except BenchmarkTimeout as error:
                drain_error = error
            for row in report["arms"]:
                row["model_usage"] = ledger.summary(trial=row["trial"], arm=row["name"])
            report["total_seconds"] = perf_counter() - started
            report["summary"] = aggregate(report["arms"], args.repeats, len(fixture["cases"]))
            report["model_usage"] = ledger.summary()
            write_json(args.output_dir / "report.json", report)
            if drain_error is not None:
                raise drain_error
    print(
        json.dumps({"status": report["status"], "summary": report["summary"]}, ensure_ascii=False)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path(__file__).with_name("fixtures") / "unified_memory_pilot_v2.json",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repeats", type=int, choices=range(1, 10), default=3)
    parser.add_argument("--operation-timeout", type=float, default=180)
    parser.add_argument("--upstream", default="http://127.0.0.1:11434")
    args = parser.parse_args()
    if args.operation_timeout <= 0:
        parser.error("operation timeout must be positive")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
