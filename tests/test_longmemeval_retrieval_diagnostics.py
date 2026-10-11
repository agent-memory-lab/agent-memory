"""Real SQLite snapshot replay and governed diagnostic export, without model SDKs."""

import ast
import asyncio
import importlib.util
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

from agent_memory import MemoryScope
from agent_memory.domain import MemoryItem, MemoryKind
from agent_memory.evaluation.comparison import AgentMemoryComparisonAdapter
from agent_memory.unified_memory import PendingMemoryDeletion, UnifiedMemory

EXAMPLES = Path(__file__).parents[1] / "examples"
SPEC = importlib.util.spec_from_file_location(
    "longmemeval_retrieval_diagnostics", EXAMPLES / "longmemeval_retrieval_diagnostics.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _item():
    return {
        "question_id": "offline-case",
        "haystack_sessions": [[{"role": "user", "content": "I take pottery at Elm Studio."}]],
        "haystack_dates": ["2024/01/01 (Mon) 12:00"],
        "question": "Where do I take pottery classes?",
        "answer": "DO_NOT_EXPORT_GOLD",
        "answer_session_ids": ["DO_NOT_EXPORT_LABELS"],
    }


def test_export_keeps_scores_and_exact_span_identity_but_excludes_provider_bodies():
    item = MemoryItem(
        "event-1",
        MemoryKind.EVENT,
        "permitted passage",
        0.6,
        datetime.now(UTC),
        {
            "candidate_identity": "span-one",
            "source_event_ids": ("event-1",),
            "source_span": {"start": 0, "end": 17, "unit": "characters", "body": "SECRET"},
            "raw_payload": {"body": "SECRET"},
            "source_revision": "a" * 64,
            "semantic_score": float("nan"),
            "semantic_reranker": "fixture",
            "query_anchor_policy": "content-anchor/1",
            "query_anchor_mode": "required",
            "query_anchor_count": 2,
            "matched_source_span": {"start": 1, "end": 12, "unit": "characters"},
        },
    )
    bundle = SimpleNamespace(
        retrieval_metadata={
            "candidate_count": 20,
            "selected_count": 1,
            "packing_infeasible": 4,
            "packing_unrecognized_body": "SECRET",
            "internal_source_text": "SECRET",
            "source_context_expansion": {
                "policy": "reranked-adjacent-parent/1", "status": "applied",
                "anchor_count": 2, "candidate_count": 3, "semantic_scoring": False,
                "source_text": "SECRET",
            },
            "citation_spans": [
                {
                    "memory_id": "event-1",
                    "candidate_id": "span-one",
                    "source_event_ids": ["event-1"],
                    "source_span": {"start": 0, "end": 17, "unit": "characters"},
                },
                {"memory_id": "event-1", "candidate_id": "other-span", "text": "SECRET"},
                {"memory_id": "unreturned", "candidate_id": "hidden", "text": "SECRET"},
            ],
            "retrieval_stages": {
                "candidates": {
                    "count": 20,
                    "items": [
                        {"id": "candidate-1", "kind": "claim", "score": 0.5, "text": "SECRET"}
                    ],
                },
                "SECRET": {"text": "SECRET"},
            },
        }
    )
    rows, diagnostic = MODULE.retrieval_export(bundle, [item])
    assert rows[0]["score"] == 0.6
    assert rows[0]["lineage"]["semantic_score"] is None
    assert rows[0]["lineage"]["source_event_ids"] == ["event-1"]
    assert rows[0]["lineage"]["query_anchor_mode"] == "required"
    assert rows[0]["lineage"]["query_anchor_count"] == 2
    assert rows[0]["lineage"]["matched_source_span"]["start"] == 1
    assert [proof["candidate_id"] for proof in rows[0]["citation_spans"]] == ["span-one"]
    assert diagnostic["candidate_count"] == 20 and diagnostic["packing_infeasible"] == 4
    assert diagnostic["returned_kinds"] == {"event": 1}
    assert diagnostic["source_context_expansion"]["candidate_count"] == 3
    assert diagnostic["source_context_expansion"]["semantic_scoring"] is False
    assert diagnostic["retrieval_stages"]["candidates"]["items"] == [
        {"id": "candidate-1", "kind": "claim", "score": 0.5}
    ]
    assert "SECRET" not in json.dumps([rows, diagnostic], allow_nan=False)


def test_common_adapter_uses_governed_bundle_and_preserves_ranking_without_optional_sdks():
    source = ast.parse((EXAMPLES / "unified_memory_benchmark.py").read_text())
    definition = next(
        node
        for node in source.body
        if isinstance(node, ast.ClassDef) and node.name == "CommonAgentAdapter"
    )
    namespace = {
        "AgentMemoryComparisonAdapter": AgentMemoryComparisonAdapter,
        "retrieval_export": MODULE.retrieval_export,
    }
    exec(
        compile(ast.Module(body=[definition], type_ignores=[]), "common-adapter", "exec"), namespace
    )
    observed = []

    async def recall(query, **kwargs):
        observed.append((query, kwargs))
        return SimpleNamespace(
            relevant_memories=tuple(
                MemoryItem(
                    str(index), MemoryKind.CLAIM, f"governed-{index}", score, datetime.now(UTC)
                )
                for index, score in ((1, 0.2), (2, 0.8), (3, 0.5))
            ),
            episodes=(),
            procedures=(),
            retrieval_metadata={
                "candidate_count": 99,
                "citation_spans": [
                    {"candidate_id": str(index)} for index in (3, 1, 2)
                ],
            },
        )

    adapter = namespace["CommonAgentAdapter"](SimpleNamespace(recall=recall))
    rows = asyncio.run(adapter.search("query", limit=2))
    assert [row["id"] for row in rows] == ["3", "1"]
    assert observed == [
        ("query", {"limit": 2, "token_budget": 2046, "include_current_state": False})
    ]
    assert adapter.last_retrieval_diagnostics["candidate_count"] == 99
    assert adapter.last_retrieval_diagnostics["returned_count"] == 2


async def _fixture(path, *, pending=False):
    scope = MemoryScope("longmemeval-full", user_id="user-one", session_id="session-one")
    memory = UnifiedMemory.local(path, scope)
    await memory.initialize()
    captured = await memory.capture(
        event_id="source-one",
        role="user",
        content="I take pottery at Elm Studio.",
        run_id="comparison",
        occurred_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    if pending:
        await memory.journal.begin(
            scope,
            {
                "ids": [captured.provider_event_id],
                "all_in_scope": False,
                "mode": "erase",
                "targets": [],
            },
        )
    return memory


def _retain(target, path, memory):
    return MODULE.retain_snapshot(
        target,
        path,
        memory.scope,
        _item(),
        dataset_sha256="frozen-fixture-digest",
        provenance={"core_source_sha256": "source-version", "runner_sha256": {"runner.py": "abc"}},
        ingestion={"ingested_events": 1, "sessions_completed": 1},
    )


def test_snapshot_replay_survives_original_cleanup_and_keeps_scope_and_deletion_gates(tmp_path):
    async def run():
        database = tmp_path / "original.sqlite3"
        memory = await _fixture(database)
        snapshot = tmp_path / "snapshot"
        try:
            retained = _retain(snapshot, database, memory)
            assert retained["inventory"]["events"] == 1
            assert "DO_NOT_EXPORT" not in (snapshot / "snapshot.json").read_text()
            await memory.forget_sources(all_in_scope=True)
            assert not (await memory.recall("pottery")).relevant_memories
        finally:
            await memory.close()

        before = {
            name: sha256(path.read_bytes()).hexdigest()
            for name, path in ((name, snapshot / name) for name in retained["files"])
        }
        restored_path = tmp_path / "restored.sqlite3"
        restored = MODULE.restore_snapshot(
            snapshot, restored_path, _item(), dataset_sha256="frozen-fixture-digest"
        )
        replay = UnifiedMemory.local(restored_path, MemoryScope(**restored["scope"]))
        wrong_scope = UnifiedMemory.local(
            restored_path,
            MemoryScope("longmemeval-full", user_id="other-user", session_id="other-session"),
        )
        await replay.initialize()
        await wrong_scope.initialize()
        try:
            result = (await replay.recall("pottery")).relevant_memories[0]
            assert "Elm Studio" in result.text
            assert not (await wrong_scope.recall("pottery")).relevant_memories
            await replay.forget_sources([result.id])
            assert not (await replay.recall("pottery")).relevant_memories
        finally:
            await replay.close()
            await wrong_scope.close()
        assert before == {
            name: sha256((snapshot / name).read_bytes()).hexdigest() for name in before
        }

    asyncio.run(run())


def test_pending_deletion_journal_is_preserved_and_cannot_be_bypassed_on_replay(tmp_path):
    async def run():
        database = tmp_path / "source.sqlite3"
        memory = await _fixture(database, pending=True)
        try:
            _retain(tmp_path / "snapshot", database, memory)
        finally:
            await memory.close()
        restored = MODULE.restore_snapshot(
            tmp_path / "snapshot",
            tmp_path / "copy.sqlite3",
            _item(),
            dataset_sha256="frozen-fixture-digest",
        )
        replay = UnifiedMemory.local(tmp_path / "copy.sqlite3", MemoryScope(**restored["scope"]))
        await replay.initialize()
        try:
            with pytest.raises(PendingMemoryDeletion):
                await replay.recall("pottery")
            await replay.resume_deletion()
            assert not (await replay.recall("pottery")).relevant_memories
        finally:
            await replay.close()

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["input", "dataset", "database", "journal", "manifest"])
def test_replay_rejects_mixed_inputs_or_tampered_artifacts_before_restore(tmp_path, problem):
    async def run():
        database = tmp_path / "source.sqlite3"
        memory = await _fixture(database)
        try:
            _retain(tmp_path / "snapshot", database, memory)
        finally:
            await memory.close()

    asyncio.run(run())
    snapshot = tmp_path / "snapshot"
    manifest_sha = sha256((snapshot / "snapshot.json").read_bytes()).hexdigest()
    item, digest = _item(), "frozen-fixture-digest"
    if problem == "input":
        item["haystack_sessions"][0][0]["content"] = "different raw input"
    elif problem == "dataset":
        digest = "another-dataset"
    elif problem == "manifest":
        (snapshot / "snapshot.json").write_text((snapshot / "snapshot.json").read_text() + " ")
    else:
        name = "agent.sqlite3" if problem == "database" else "agent.sqlite3.deletions.db"
        with (snapshot / name).open("ab") as stream:
            stream.write(b"tampered")
    with pytest.raises(ValueError, match="snapshot"):
        MODULE.restore_snapshot(
            snapshot,
            tmp_path / "copy.sqlite3",
            item,
            dataset_sha256=digest,
            expected_manifest_sha256=manifest_sha,
        )
    assert not (tmp_path / "copy.sqlite3").exists()


def test_retention_refuses_production_scope_and_overwriting_existing_artifacts(tmp_path):
    async def run():
        database = tmp_path / "source.sqlite3"
        memory = await _fixture(database)
        try:
            _retain(tmp_path / "snapshot", database, memory)
            with pytest.raises(FileExistsError, match="immutable"):
                _retain(tmp_path / "snapshot", database, memory)
            with pytest.raises(ValueError, match="LongMemEval"):
                MODULE.retain_snapshot(
                    tmp_path / "production",
                    database,
                    MemoryScope("production"),
                    _item(),
                    dataset_sha256="x",
                    provenance={},
                    ingestion={},
                )
        finally:
            await memory.close()

    asyncio.run(run())


def test_answer_or_query_changes_do_not_contaminate_ingestion_binding():
    item = _item()
    digest = MODULE.ingestion_digest(item)
    item.update(
        answer="different gold", question="different diagnostic query", answer_session_ids=[]
    )
    assert MODULE.ingestion_digest(item) == digest


@pytest.mark.parametrize("text,budget", [("a" * 20, 10), ("汉" * 20, 10), ("fixture-body", 100)])
def test_shared_evidence_byte_accounting_detects_downstream_truncation(text, budget):
    spec = importlib.util.spec_from_file_location(
        "benchmark_protocol", EXAMPLES / "memory_benchmark_protocol.py"
    )
    protocol = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(protocol)
    items = [
        {"id": "one", "kind": "event", "text": text},
        {"id": "two", "kind": "claim", "text": "later evidence"},
    ]
    evidence = protocol.bounded_evidence(items, byte_budget=budget)
    result = MODULE.evidence_diagnostic(items, evidence, byte_budget=budget)
    assert result["emitted_utf8_bytes"] == len(evidence.encode()) <= budget
    assert result["truncated"] == (budget == 10)
    assert result["omitted_items"] == int(budget == 10)
    assert text not in json.dumps(result)


@pytest.mark.parametrize("mode", ["replay", "retention_failure"])
def test_worker_replay_skips_extraction_and_snapshot_failure_still_cleans_up(tmp_path, mode):
    source = ast.parse((EXAMPLES / "longmemeval_full_comparison.py").read_text())
    function = next(
        node
        for node in source.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "execute_task"
    )
    operations = []

    class Proxy:
        url, pending_requests = "http://offline.invalid", 0

        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def drain(self, timeout):
            return True

    class Client:
        def __init__(self, **kwargs):
            self._client = SimpleNamespace(aclose=self.close)

        async def close(self):
            operations.append("client-close")

        async def chat(self, **kwargs):
            operations.append("model-call")
            return SimpleNamespace(message=SimpleNamespace(content="yes"))

    class Memory:
        def __init__(self, kernel, scope, **kwargs):
            self.scope = scope

        async def initialize(self):
            operations.append("initialize")

    class Adapter:
        def __init__(self, memory):
            self.last_retrieval_diagnostics = {"candidate_count": 0}

        async def add(self, event):
            operations.append("extract")
            return SimpleNamespace()

        async def search(self, query, **kwargs):
            operations.append("search")
            return []

        async def clear(self):
            operations.append("clear")

    async def make_clients(*args, **kwargs):
        assert kwargs["selected_arm"] == "agent-memory"
        return None, None, None

    async def close_resources(*args):
        operations.append("resource-close")
        return []

    async def checked(ledger, row, name, call, timeout):
        return await call

    def retain(*args, **kwargs):
        operations.append("retain")
        raise RuntimeError("PRIVATE source failure details")

    snapshot = tmp_path / "source-snapshot"
    snapshot.mkdir()
    (snapshot / "snapshot.json").write_text("{}")
    digest = sha256((snapshot / "snapshot.json").read_bytes()).hexdigest()

    def restore(*args, **kwargs):
        operations.append("restore")
        assert kwargs["expected_manifest_sha256"] == digest
        return {
            "scope": {"tenant_id": "longmemeval-full", "user_id": "u", "session_id": "s"},
            "inventory": {"events": 1},
            "ingestion": {"ingested_events": 1},
        }

    namespace = {
        "asyncio": asyncio,
        "datetime": datetime,
        "UTC": UTC,
        "Path": Path,
        "sha256": sha256,
        "uuid4": lambda: SimpleNamespace(hex="unique"),
        "perf_counter": lambda: 0,
        "tempfile": SimpleNamespace(TemporaryDirectory=TemporaryDirectory),
        "ModelCallLedger": lambda path: SimpleNamespace(
            start_phase=lambda *args: None, summary=lambda: {"calls": 0}
        ),
        "OllamaAccountingProxy": Proxy,
        "ControlledClient": Client,
        "ARMS": ("agent-memory",),
        "native": SimpleNamespace(
            _make_arm_clients=make_clients, MODEL="offline", OllamaEmbeddings=lambda client: None
        ),
        "UnifiedMemory": Memory,
        "CommonAgentAdapter": Adapter,
        "MemoryScope": MemoryScope,
        "build_local_kernel": lambda *args, **kwargs: None,
        "GeneratedTrajectoryClaimExtractor": lambda *args, **kwargs: None,
        "BenchmarkClaimGenerator": lambda *args, **kwargs: None,
        "SQLiteDeletionJournal": lambda path: None,
        "CaptureSanitizer": lambda **kwargs: None,
        "restore_snapshot": restore,
        "retain_snapshot": retain,
        "DATA_SHA256": "frozen",
        "snapshot_key": MODULE.snapshot_key,
        "checked_call": checked,
        "close_trial_resources": close_resources,
        "record_extraction": lambda *args: None,
        "write_json": lambda *args: None,
        "bounded_evidence": lambda rows: "",
        "evidence_diagnostic": MODULE.evidence_diagnostic,
        "ANSWER_PROMPT": "unchanged",
        "parse_judge": lambda text: True,
        "lme": SimpleNamespace(
            _session_event=lambda *args: [SimpleNamespace(event_id="event")],
            _judge_prompt=lambda *args: "judge only",
        ),
    }
    answer_function = next(
        node for node in source.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "generate_answer"
    )
    exec(compile(ast.Module(body=[answer_function, function], type_ignores=[]), "worker", "exec"),
         namespace)
    args = SimpleNamespace(
        upstream="offline",
        timeout=1,
        request_budget=10,
        snapshot_sources={"offline-case": snapshot} if mode == "replay" else {},
        snapshot_inputs={"offline-case": {"manifest_sha256": digest}},
        retain_agent_snapshots=mode == "retention_failure",
        snapshot_provenance={},
    )
    item = {**_item(), "question_type": "single-session-user", "question_date": "2024/01/02"}
    row = asyncio.run(
        namespace["execute_task"](item, {"arm": "agent-memory", "attempt": 1}, 1, tmp_path, args)
    )
    assert operations[-3:] == ["clear", "resource-close", "client-close"]
    if mode == "replay":
        assert row["status"] == "completed" and row["ingested_events"] == 0
        assert "restore" in operations and "extract" not in operations
        assert operations.count("model-call") == 2  # Shared answer and judge only.
    else:
        assert row["status"] == "failed" and row["error_type"] == "RuntimeError"
        assert "retain" in operations and "extract" in operations
        assert "model-call" not in operations
        assert "PRIVATE" not in json.dumps(row)
