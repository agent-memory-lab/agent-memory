"""Exercise the benchmark generator through real local capture without model SDKs."""

import asyncio
import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_memory import MemoryScope
from agent_memory.capture.sink import CaptureError
from agent_memory.evaluation.comparison import AgentMemoryComparisonAdapter, RawInteraction
from agent_memory.unified_memory import UnifiedMemory

SOURCE = Path(__file__).resolve().parents[1] / "examples" / "longmemeval_claim_extraction.py"
SPEC = importlib.util.spec_from_file_location("longmemeval_claim_extraction", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RecordedClient:
    def __init__(self, claims):
        self.claims = claims
        self.requests = []

    async def chat(self, **request):
        self.requests.append(request)
        return SimpleNamespace(message=SimpleNamespace(content=json.dumps({"claims": self.claims})))


@pytest.mark.parametrize("output", ["accepted", "low_confidence", "missing_confidence", "empty"])
def test_real_generator_capture_contract_and_body_free_accounting(tmp_path, output):
    async def run():
        claim = dict(
            key="transport", value="tram", text="The user travels by tram.", confidence=0.95
        )
        if output == "missing_confidence":
            claim.pop("confidence")
        elif output == "low_confidence":
            claim["confidence"] = 0.1
        client = RecordedClient([] if output == "empty" else [claim])
        calls = {"agent-memory": 0}
        generator = MODULE.BenchmarkClaimGenerator(client, calls, model="offline-fixture")
        scope = MemoryScope("contract-test", user_id="user", session_id="session")
        memory = UnifiedMemory.local(tmp_path / "memory.db", scope, generator=generator)
        await memory.initialize()
        event = RawInteraction(
            "transport-source", "user", "I travel by tram.", datetime(2023, 5, 1, tzinfo=UTC)
        )
        summary = {}
        try:
            adapter = AgentMemoryComparisonAdapter(memory)
            if output == "missing_confidence":
                with pytest.raises(CaptureError) as error:
                    await adapter.add(event)
                assert error.value.code == "capture_extraction_failed"
                MODULE.record_extraction(summary, error.value)
                assert summary["status_counts"] == {"error": 1}
                assert summary["rejection_counts"] == {"missing_confidence": 1}
                assert await memory.provider.get_state(scope) == ()
            else:
                submission = await adapter.add(event)
                MODULE.record_extraction(summary, submission)
                states = await memory.provider.get_state(scope)
                expected = {
                    "accepted": "completed",
                    "low_confidence": "rejected",
                    "empty": "empty",
                }[output]
                assert summary["status_counts"] == {expected: 1}
                assert summary["accepted_count"] == len(states) == int(output == "accepted")
                assert summary["candidate_count"] == int(output != "empty")
                if states:
                    assert states[0].value == "tram"
            assert calls == {"agent-memory": 1}
            request = client.requests[0]
            assert request["model"] == "offline-fixture"
            item_schema = request["format"]["properties"]["claims"]["items"]
            assert "confidence" in item_schema["required"]
            assert item_schema["properties"]["confidence"] == {
                "type": "number",
                "minimum": 0,
                "maximum": 1,
            }
            assert event.content in request["messages"][0]["content"]
            assert (
                not {"question", "gold_answer", "answer_session_ids"}
                & item_schema["properties"].keys()
            )
            # Counters must never duplicate source bodies or candidate values.
            assert "tram" not in json.dumps(summary)
        finally:
            await memory.close()

    asyncio.run(run())


def test_missing_diagnostic_stays_unknown_instead_of_becoming_zero_candidates():
    summary = {}
    MODULE.record_extraction(summary, SimpleNamespace())
    assert summary == {
        "events": 1,
        "status_counts": {"unreported": 1},
        "unknown_candidate_count_events": 1,
    }


@pytest.mark.parametrize("malformed,reason", [
    ({"key": "k", "value": "v", "text": "t"}, "missing_confidence"),
    ({"key": "k", "value": "v", "text": "t", "confidence": True}, "invalid_confidence"),
    ({"key": "k", "value": "v", "text": "t", "confidence": 0.9, "extra": "private"},
     "unsupported_fields"),
    ({"key": "k", "value": " ", "text": "t", "confidence": 0.9}, "invalid_candidate"),
    ("private candidate body", "invalid_candidate"),
])
def test_shared_generator_strict_batch_failure_has_no_synthetic_acceptances(malformed, reason):
    from dataclasses import asdict

    valid = {"key": "k", "value": "v", "text": "t", "confidence": 0.9}
    generator = MODULE.BenchmarkClaimGenerator(
        RecordedClient([valid, malformed]), {"agent-memory": 0},
    )
    with pytest.raises(MODULE.BenchmarkClaimFormatError, match="scored-claim schema") as error:
        asyncio.run(generator.generate_claims(SimpleNamespace(content="private source")))
    diagnostic = error.value.diagnostic
    assert diagnostic.status == "error"
    assert diagnostic.candidate_count == 2 and diagnostic.accepted_count == 0
    assert diagnostic.rejections[0].reason == reason
    assert diagnostic.rejections[1].reason == "batch_aborted"
    assert diagnostic.rejected_count == 2
    assert generator.generated_claims == 0 and len(generator.receipts) == 1
    assert "private" not in json.dumps(asdict(diagnostic))
    assert "private" not in str(error.value)


def test_shared_generator_enforces_root_schema_and_bounded_candidate_limit():
    valid = {"key": "k", "value": "v", "text": "t", "confidence": 0.9}
    for payload in ([], {}, {"claims": [], "extra": "private"}, {"claims": None}):
        with pytest.raises(MODULE.BenchmarkClaimFormatError) as error:
            MODULE._validate_claims(payload)
        assert error.value.diagnostic.candidate_count is None
        assert error.value.diagnostic.rejections[0].reason == "invalid_output"
    with pytest.raises(MODULE.BenchmarkClaimFormatError, match="claim limit") as error:
        MODULE._validate_claims({"claims": [valid] * 9})
    assert error.value.diagnostic.candidate_count == 9
    assert error.value.diagnostic.accepted_count == 0
    assert error.value.diagnostic.rejected_count == 9
