"""Offline legacy benchmark contracts; scripted output is not model-quality evidence."""
import asyncio
from datetime import UTC, datetime
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_memory import MemoryScope
from agent_memory.capture.policy import CaptureSanitizer
from agent_memory.composition import build_local_kernel
from agent_memory.evaluation.comparison import AgentMemoryComparisonAdapter, RawInteraction
from agent_memory.providers import GeneratedTrajectoryClaimExtractor
from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory


PATH = Path(__file__).parents[1] / "examples" / "longmemeval_ollama_comparison.py"
spec = importlib.util.spec_from_file_location("longmemeval_contract", PATH)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class ScriptedClient:
    def __init__(self, claims):
        self.claims = claims
        self.requests = []

    async def chat(self, **request):
        self.requests.append(request)
        return SimpleNamespace(message=SimpleNamespace(content=json.dumps({"claims": self.claims})),
                               prompt_eval_count=123, eval_count=41)


def fact(**overrides):
    return {"key": "drink.preference", "value": "tea", "text": "User prefers tea.",
            "confidence": 0.95, **overrides}


def test_schema_output_survives_real_core_ingestion_and_recall(tmp_path):
    async def scenario():
        client = ScriptedClient([fact()])
        calls = {"agent-memory": 0}
        generator = benchmark.BenchmarkClaimGenerator(client, calls)
        path = tmp_path / "memory.db"
        memory = UnifiedMemory(
            build_local_kernel(path, extractor=benchmark.BenchmarkClaimExtractor(
                generator, fail_open=False, minimum_confidence=benchmark.MINIMUM_CONFIDENCE)),
            MemoryScope("fixture", user_id="alice", session_id="one"),
            journal=SQLiteDeletionJournal(str(path) + ".deletions.db"),
            sanitizer=CaptureSanitizer(redact_builtin=False),
        )
        await memory.initialize()
        try:
            adapter = AgentMemoryComparisonAdapter(memory)
            receipt = await adapter.add(RawInteraction("fact-1", "user", "I prefer tea.",
                                                      datetime(2026, 10, 1, tzinfo=UTC)))
            assert receipt.status == "done"
            state = await memory.provider.get_state(memory.scope)
            assert len(state) == 1  # Required positive fact, not merely no exception.
            assert state[0].value == "tea"
            assert state[0].provenance.source_event_ids == (receipt.provider_event_id,)
            results = await adapter.search("tea")
            assert any(row["text"] == "User prefers tea." for row in results)
            assert generator.generated_claims == calls["agent-memory"] == 1
            assert generator.receipts == [{"phase": "extraction", "prompt_eval_count": 123,
                                            "eval_count": 41, "total_duration": None}]
            schema = client.requests[0]["format"]["properties"]["claims"]["items"]
            assert "confidence" in schema["required"]
            assert schema["properties"]["confidence"] == {"type": "number", "minimum": 0, "maximum": 1}
            assert "confidence" in client.requests[0]["messages"][0]["content"]
        finally:
            await memory.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("score", [None, True, "0.95", -0.1, 1.1, float("nan"), float("inf")])
def test_invalid_scores_fail_visibly_without_synthetic_confidence(score):
    client = ScriptedClient([fact(confidence=score)])
    generator = benchmark.BenchmarkClaimGenerator(client, {"agent-memory": 0})
    with pytest.raises(ValueError, match="scored-claim schema"):
        asyncio.run(generator.generate_claims(SimpleNamespace(content="I prefer tea")))
    assert generator.generated_claims == 0


def test_old_unscored_schema_is_rejected_and_empty_is_valid():
    claim = fact()
    del claim["confidence"]
    generator = benchmark.BenchmarkClaimGenerator(ScriptedClient([claim]), {"agent-memory": 0})
    with pytest.raises(ValueError, match="scored-claim schema"):
        asyncio.run(generator.generate_claims(SimpleNamespace(content="I prefer tea")))
    empty = benchmark.BenchmarkClaimGenerator(ScriptedClient([]), {"agent-memory": 0})
    assert asyncio.run(empty.generate_claims(SimpleNamespace(content="Hello"))) == []
    assert empty.generated_claims == 0


def test_low_score_is_preserved_but_rejected_by_real_extractor():
    from agent_memory.domain import MemoryEvent
    event = MemoryEvent(scope=MemoryScope("fixture", session_id="one"),
                        event_type="user.message", actor="user", content="I prefer tea")
    generator = benchmark.BenchmarkClaimGenerator(ScriptedClient([fact(confidence=0.2)]),
                                                  {"agent-memory": 0})
    extractor = GeneratedTrajectoryClaimExtractor(generator, fail_open=False)
    assert asyncio.run(extractor.extract(event)) == ()
    assert generator.generated_claims == 1


def test_frozen_run_rejects_stale_outputs_and_changed_inputs(tmp_path):
    data = tmp_path / "data.json"
    selected = [{"question_id": "q1"}]
    data.write_text(json.dumps(selected))
    output = tmp_path / "out"
    output.mkdir()
    digest = benchmark._freeze_run(data, selected, output, 0, 1)
    assert benchmark._freeze_run(data, selected, output, 0, 1) == digest
    with pytest.raises(ValueError, match="configuration changed"):
        benchmark._freeze_run(data, selected, output, 0, None)
    data.write_text(json.dumps(selected) + "\n")
    with pytest.raises(ValueError, match="configuration changed"):
        benchmark._freeze_run(data, selected, output, 0, 1)
    (output / "run-manifest.json").unlink()
    (output / "agent-memory.jsonl").write_text('{"question_id":"q1"}\n')
    with pytest.raises(ValueError, match="unversioned"):
        benchmark._freeze_run(data, selected, output, 0, 1)


def test_session_chunk_controls_exclude_gold_and_preserve_utf8():
    body = "茶" * 5000
    events = benchmark._session_event("q", 0, [{"role": "user", "content": body,
                                               "has_answer": True}], "2026-10-01")
    assert len(events) == 2
    assert "".join(event.content for event in events) == "user: " + body
    assert all(len(event.content.encode()) <= 9000 for event in events)
    assert all("has_answer" not in event.body() for event in events)


@pytest.mark.parametrize("offset,limit", [(-1, None), (0, 0), (0, -1), (True, 1)])
def test_invalid_selection_fails_before_loading_live_clients(tmp_path, offset, limit):
    with pytest.raises(ValueError, match="offset"):
        asyncio.run(benchmark.run(tmp_path / "missing", tmp_path, tmp_path, offset, limit))


@pytest.mark.parametrize("row_digest", [None, "other-run"])
def test_matching_manifest_does_not_authorize_stale_result_rows(tmp_path, row_digest):
    data = tmp_path / "data.json"
    selected = [{"question_id": "q1"}]
    data.write_text(json.dumps(selected))
    output = tmp_path / "out"
    output.mkdir()
    benchmark._freeze_run(data, selected, output, 0, 1)
    (output / "agent-memory.jsonl").write_text(json.dumps({
        "question_id": "q1", "run_digest": row_digest,
    }) + "\n")
    # This must fail before importing optional clients or making any model call.
    with pytest.raises(ValueError, match="result row belongs"):
        asyncio.run(benchmark.run(data, output, tmp_path / "work", 0, 1))


def test_changed_core_identity_invalidates_resume(tmp_path, monkeypatch):
    data = tmp_path / "data.json"
    selected = [{"question_id": "q1"}]
    data.write_text(json.dumps(selected))
    output = tmp_path / "out"
    output.mkdir()
    benchmark._freeze_run(data, selected, output, 0, 1)
    monkeypatch.setattr(benchmark, "_implementation_identity", lambda: {
        "core_source_sha256": "changed", "installed_versions": {},
    })
    with pytest.raises(ValueError, match="configuration changed"):
        benchmark._freeze_run(data, selected, output, 0, 1)


def test_offline_runner_persists_diagnostics_and_same_input_for_each_arm(tmp_path, monkeypatch):
    """Actual core + contract doubles for third-party arms, not comparative quality."""
    import sys

    class Client(ScriptedClient):
        def __init__(self, **kwargs):
            super().__init__([fact()])
            self._client = self

        async def aclose(self):
            pass

        async def chat(self, **request):
            if "format" in request:
                return await super().chat(**request)
            return SimpleNamespace(message=SimpleNamespace(content="yes"),
                                   prompt_eval_count=10, eval_count=1)

    class External:
        def __init__(self, *args, **kwargs):
            self.rows = []

        async def add(self, event):
            self.rows.append({"text": event.content})

        async def search(self, query, limit):
            return self.rows[:limit]

        async def clear(self):
            self.rows.clear()

    async def clients(*args, **kwargs):
        return None, None, None

    monkeypatch.setitem(sys.modules, "ollama", SimpleNamespace(AsyncClient=Client))
    monkeypatch.setitem(sys.modules, "ollama_memory_comparison", SimpleNamespace(
        OllamaEmbeddings=lambda client: None, _make_arm_clients=clients,
        _stop_falkordblite=None,
    ))
    monkeypatch.setattr(benchmark, "Mem0ComparisonAdapter", External)
    monkeypatch.setattr(benchmark, "GraphitiComparisonAdapter", External)
    data = tmp_path / "data.json"
    data.write_text(json.dumps([{
        "question_id": "q1", "question_type": "single-session-user",
        "question": "What drink?", "answer": "tea", "question_date": "2026-10-02",
        "haystack_sessions": [[{"role": "user", "content": "I prefer tea."}]],
        "haystack_dates": ["2026-10-01"],
    }]))
    output = tmp_path / "out"
    summary = asyncio.run(benchmark.run(data, output, tmp_path / "work"))
    rows = [json.loads((output / f"{arm}.jsonl").read_text())
            for arm in ("agent-memory", "mem0-oss", "graphiti")]
    assert len({row["input_digest"] for row in rows}) == 1
    assert all(row["run_digest"] == summary["run_digest"] for row in rows)
    diagnostic = rows[0]["extraction_diagnostics"]
    assert diagnostic["generated_claims"] == diagnostic["validated_claims"] == 1
    assert diagnostic["stored_current_claims"] == 1
    assert diagnostic["zero_current_claims"] is False
    assert rows[0]["retrieved_count"] > 0
    assert len(rows[0]["model_usage_receipts"]) == 3
    assert rows[1]["extraction_diagnostics"] is None
    assert summary["official_score_equivalent"] is False
    # Idempotent resume is permitted only for this frozen implementation/input.
    again = asyncio.run(benchmark.run(data, output, tmp_path / "work"))
    assert again["metrics"] == summary["metrics"]
