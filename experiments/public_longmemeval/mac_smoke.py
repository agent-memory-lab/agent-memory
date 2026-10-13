"""One bounded, local-Ollama agent-memory plumbing smoke; no model downloads."""
from __future__ import annotations

if __package__ in (None, ""):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


import argparse
import asyncio
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sqlite3
from contextlib import closing
import sys
import time
import urllib.parse
import urllib.request

from experiments.public_longmemeval.public_retrieval import REPO_REVISION, sha


HTTP_WORKER = r'''
import json, sys, urllib.request
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError("Redirect refused: keep public corpus on local Ollama")
item = json.load(sys.stdin)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
request = urllib.request.Request(item["url"], data=None if item["data"] is None else json.dumps(item["data"]).encode(),
                                 headers={"Content-Type": "application/json"})
with opener.open(request, timeout=item["timeout"]) as response:
    sys.stdout.write(response.read().decode())
'''


class LocalOllama:
    def __init__(self, host, model, embedding_model, max_calls, max_seconds):
        parsed = urllib.parse.urlparse(host)
        if (parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.path not in {"", "/"}):
            raise ValueError("This smoke only contacts Ollama on this computer's loopback address")
        local_host = "[::1]" if parsed.hostname == "::1" else "127.0.0.1"
        self.host = f"http://{local_host}:{parsed.port or 80}"
        self.model, self.embedding_model = model, embedding_model
        self.max_calls, self.deadline = max_calls, time.monotonic() + max_seconds
        self.receipts = []

    def request(self, route, data=None, timeout=10):
        # A killable helper gives a real request deadline, unlike urllib's
        # inactivity timeout. It disables proxies and rejects every redirect.
        completed = subprocess.run([sys.executable, "-c", HTTP_WORKER],
            input=json.dumps({"url": self.host + route, "data": data, "timeout": timeout}),
            text=True, capture_output=True, timeout=timeout, check=True)
        return json.loads(completed.stdout)

    async def generate(self, route, data, phase):
        remaining = self.deadline - time.monotonic()
        if len(self.receipts) >= self.max_calls or remaining <= 0:
            raise RuntimeError("Bounded smoke budget exhausted; no more model requests started")
        receipt = {"phase": phase, "route": route, "status": "started", "seconds": None}
        self.receipts.append(receipt)
        started = time.monotonic()
        def call():
            return self.request(route, data, min(120, remaining))
        try:
            result = await asyncio.to_thread(call)
            receipt.update({"status": "completed", "seconds": time.monotonic() - started,
                            "prompt_tokens": result.get("prompt_eval_count"),
                            "output_tokens": result.get("eval_count"),
                            "done_reason": result.get("done_reason"),
                            "provider_total_duration_ns": result.get("total_duration")})
            if result.get("done_reason") == "length":
                raise RuntimeError("Model output reached its generation cap; do not score truncated output")
            return result
        except BaseException as error:
            receipt.update({"status": "failed", "seconds": time.monotonic() - started,
                            "error_type": type(error).__name__})
            raise

    async def chat(self, prompt, phase, *, format=None, limit=512):
        data = {"model": self.model, "messages": [{"role": "user", "content": prompt}],
                "stream": False, "think": False,
                "options": {"temperature": 0, "num_ctx": 32768, "num_predict": limit}}
        if format is not None:
            data["format"] = format
        return (await self.generate("/api/chat", data, phase))["message"]["content"]


class Embeddings:
    dimensions = 768
    def __init__(self, client):
        self.client = client
    async def embed(self, texts):
        # Ollama defaults to truncating overlong embedding input. Explicitly
        # disable that so a failure is visible rather than silently changing data.
        result = await self.client.generate("/api/embed", {
            "model": self.client.embedding_model, "input": list(texts), "truncate": False}, "embedding")
        values = result["embeddings"]
        if any(len(value) != self.dimensions for value in values):
            raise ValueError("Expected 768-dimensional nomic-embed-text vectors")
        return values


from experiments.public_longmemeval.extraction import ClaimGenerator, chunks
from experiments.public_longmemeval.core import core_identity, input_identity
from experiments.public_longmemeval.run_experiment import pack
from experiments.public_longmemeval.transport import atomic_write_json


def preflight(client, embedding_mode="auto"):
    models = client.request("/api/tags").get("models", [])
    def find(name, required=True):
        names = {name, name + ":latest"}
        matches = [row for row in models if row.get("name") in names or row.get("model") in names]
        if not matches:
            if not required:
                return None
            raise RuntimeError(f"Required installed model missing: {name}; nothing downloaded")
        if (name.endswith(":cloud") or any(matches[0].get(key) for key in ("remote_host", "remote_model"))
                or not isinstance(matches[0].get("size"), (int, float)) or matches[0]["size"] <= 0):
            raise RuntimeError("Remote/cloud model metadata refused; choose an already installed local model")
        return {key: matches[0].get(key) for key in ("name", "digest", "size", "details")}
    return {"host": client.host, "ollama_version": client.request("/api/version").get("version"),
            "generation": find(client.model),
            "embedding": None if embedding_mode == "none" else find(client.embedding_model, embedding_mode == "required"),
            "num_ctx": 32768, "num_predict_extraction": 1536, "no_models_downloaded": True}


async def run(args, client, preflight_result):
    if sys.version_info < (3, 13):
        raise RuntimeError("agent-memory requires Python 3.13 or newer")
    identity = core_identity()
    inputs = input_identity(args.smoke)
    from agent_memory import MemoryScope
    from agent_memory.capture.policy import CaptureSanitizer
    from agent_memory.composition import build_local_kernel
    from agent_memory.evaluation.comparison import AgentMemoryComparisonAdapter, RawInteraction
    from agent_memory.providers import GeneratedTrajectoryClaimExtractor, EmbeddingReranker
    from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory

    manifest = json.loads((args.smoke / "manifest.json").read_text())
    for name, digest in manifest["artifacts"].items():
        if sha(args.smoke / name) != digest:
            raise ValueError("Smoke artifact changed: " + name)
    # No answer, evidence label, question type or native dataset ID reaches indexing.
    record = json.loads((args.smoke / "corpus.json").read_text())
    query = json.loads((args.smoke / "query.json").read_text())
    from experiments.public_longmemeval.public_retrieval import blind_sessions
    blind_sessions(record)
    if set(query) != {"case_id", "question", "question_date"} or query["case_id"] != record["case_id"]:
        raise ValueError("Unexpected query metadata")
    args.output.mkdir(parents=True, exist_ok=False)
    database = args.output / "agent-memory.sqlite3"
    generator = ClaimGenerator(client)
    memory = UnifiedMemory(build_local_kernel(database,
        extractor=GeneratedTrajectoryClaimExtractor(generator, provider="ollama",
            model=client.model, event_types=("user.message", "agent.model.completed", "agent.tool.completed"),
            fail_open=False),
        reranker=EmbeddingReranker(Embeddings(client), fail_open=False) if preflight_result["embedding"] else None),
        MemoryScope("public-longmemeval-smoke", user_id=record["case_id"], session_id="one-question"),
        journal=SQLiteDeletionJournal(str(database) + ".deletions.db"),
        sanitizer=CaptureSanitizer(redact_builtin=False))
    result = {"status": "running", "evidence_class": "public_benchmark", "official_score_equivalent": False,
              "purpose": "One-question path, latency and resource smoke; not comparative accuracy",
              "core_identity": identity, "input_artifacts": inputs, "models": preflight_result, "case_id": record["case_id"],
              "runner_sha256": sha(Path(__file__)), "source_manifest_sha256": sha(args.smoke / "manifest.json"),
              "max_model_calls": args.max_calls, "max_seconds": args.max_seconds,
              "embedding_truncation": False, "agent_memory_recall_token_budget": 2048,
              "agent_memory_mode": "embedding-reranked" if preflight_result["embedding"] else "default-no-embedding",
              "shared_reader_context_utf8_byte_cap": 6000, "context_budget_is_model_tokens": False,
              "events_completed": 0, "receipts": client.receipts, "arms": {}}
    started = time.monotonic()
    initialized = False
    async def answer_arm(name, memories):
        context, delivered = pack(memories)
        started_answer = time.monotonic()
        result["arms"][name] = {"retrieved_memories": memories, "delivered_memories": delivered, "context_utf8_bytes": len(context.encode()),
                                "status": "answering"}
        hypothesis = await client.chat(
            "Answer the question using only the retrieved memory. Do not use outside knowledge. "
            "If the memory does not provide enough information, say you do not know. Answer concisely.\n"
            f"Question date: {query['question_date']}\nQuestion: {query['question']}\nRetrieved memory:\n{context}",
            "answer:" + name, limit=384)
        result["arms"][name].update({"status": "completed", "hypothesis": hypothesis,
                                      "answer_seconds": time.monotonic() - started_answer})
    try:
        # Same reader and byte cap, cheap baselines first. These are one-case
        # plumbing comparisons, not official benchmark QA estimates.
        from experiments.public_longmemeval.public_retrieval import BlindSession, bm25
        raw_chunks = [BlindSession(session["source_id"] + f"-part-{index:03d}", session["occurred_at"],
                                   "[" + session["occurred_at"] + "] " + content)
                      for session in record["sessions"]
                      for index, content in enumerate(chunks("\n".join(turn["role"] + ": " + turn["content"]
                                                                         for turn in session["turns"]))) ]
        by_id = {chunk.source_id: chunk for chunk in raw_chunks}
        await answer_arm("no-memory", [])
        ranked = bm25(query["question"], raw_chunks, 8)
        await answer_arm("bm25-raw-chunk", [{"id": key, "text": by_id[key].text} for key in ranked])
        recent = sorted(raw_chunks, key=lambda value: (value.occurred_at, value.source_id), reverse=True)[:8]
        await answer_arm("recent-raw-chunk", [{"id": value.source_id, "text": value.text} for value in recent])
        await memory.initialize()
        initialized = True
        adapter = AgentMemoryComparisonAdapter(memory)
        for session in record["sessions"]:
            body = "\n".join(turn["role"] + ": " + turn["content"] for turn in session["turns"])
            for index, content in enumerate(chunks(body)):
                await adapter.add(RawInteraction(session["source_id"] + f"-part-{index:03d}", "user",
                                                  content, datetime.fromisoformat(session["occurred_at"])))
                result["events_completed"] += 1
                print(json.dumps({"event_completed": result["events_completed"], "model_calls": len(client.receipts)}), flush=True)
        memories = await adapter.search(query["question"], limit=8)
        result["extraction_claims_proposed"] = generator.proposed
        result["extraction_claims_meeting_confidence_threshold"] = generator.meeting_confidence_threshold
        if generator.meeting_confidence_threshold == 0:
            raise RuntimeError("No generated claim meets the core confidence threshold; extraction path is not validated")
        with closing(sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True)) as connection:
            result["persisted_claims"] = connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        if result["persisted_claims"] == 0:
            raise RuntimeError("No generated claims persisted in the fresh database; extraction is not validated")
        result["embedding_calls_completed"] = sum(row["phase"] == "embedding" and row["status"] == "completed"
                                                   for row in client.receipts)
        if preflight_result["embedding"] and not result["embedding_calls_completed"]:
            raise RuntimeError("Embedding was selected but no embedding request completed; path is not validated")
        await answer_arm("agent-memory", memories)
        # Deliberately stop before judging. The parent can review gold after the
        # blind answer is saved; an independent calibrated judge is still needed.
        result["status"] = "completed"
        result["answer_accuracy"] = None
    except BaseException as error:
        result["status"] = "blocked_or_failed"
        result["error_type"] = type(error).__name__

        raise
    finally:
        try:
            if initialized:
                await memory.close()
        except Exception as cleanup_error:
            result["status"] = "blocked_or_failed"
            result["cleanup_error_type"] = type(cleanup_error).__name__
            raise
        finally:
            result["seconds"] = time.monotonic() - started
            result["model_calls"] = len(client.receipts)
            result["provider_cost"] = None
            atomic_write_json(args.output / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "run"))
    parser.add_argument("--host", default="http://127.0.0.1:11434")
    parser.add_argument("--model", default="qwen3.5:9b")
    parser.add_argument("--embedding-model", default="nomic-embed-text")
    parser.add_argument("--embedding-mode", choices=("auto", "required", "none"), default="auto")
    parser.add_argument("--smoke", type=Path, default=Path(__file__).parent / "smoke")
    parser.add_argument("--output", type=Path, default=Path("agent-memory-one-question-run"))
    parser.add_argument("--max-calls", type=int, default=160)
    parser.add_argument("--max-seconds", type=int, default=1800)
    args = parser.parse_args()
    if not 1 <= args.max_calls <= 500 or not 1 <= args.max_seconds <= 7200:
        parser.error("Bounded limits required: calls 1..500, seconds 1..7200")
    client = LocalOllama(args.host, args.model, args.embedding_model, args.max_calls, args.max_seconds)
    snapshot = preflight(client, args.embedding_mode)
    print(json.dumps(snapshot, indent=2))
    if args.command == "run":
        result = asyncio.run(run(args, client, snapshot))
        print(json.dumps({key: result[key] for key in ("status", "seconds", "model_calls", "answer_accuracy")}))


if __name__ == "__main__":
    main()
