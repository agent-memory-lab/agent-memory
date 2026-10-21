"""Native SQLite integration with offline generated and checked answers."""

import argparse
import asyncio
import json

import pytest

from experiments.public_longmemeval import mac_smoke, run_experiment
from experiments.public_longmemeval.reader_context import ReaderContextError
from experiments.public_longmemeval.tests.fixtures import write_smoke
from experiments.public_longmemeval.tests.test_reader_v3_runner import MockClient


class CheckedClient(MockClient):
    async def chat(self, prompt, phase, **kwargs):
        if phase == "extraction":
            return await super().chat(prompt, phase, **kwargs)
        self.receipts.append({"phase": phase, "mock": True})
        if phase.endswith(":verify"):
            return json.dumps({"decision": "supported", "reason": "Explicit source support."})
        blocks = json.loads(prompt.split("EVIDENCE_JSON=", 1)[1])
        value = {
            "schema_version": "evidence-reader/4",
            "mode": "fact" if blocks else "abstain",
            "answer": "A blue cube." if blocks else "",
            "supporting_quotes": [{"span_id": blocks[0]["spans"][0]["span_id"]}] if blocks else [],
            "missing_information": [] if blocks else ["The requested fact."],
            "computation": {"kind": "none", "operands": [], "unit": ""},
        }
        if self.invalid and phase == "answer:agent-memory:draft":
            value["supporting_quotes"][0] = {"span_id": "E999:S1"}
        return json.dumps(value)


@pytest.mark.parametrize("entry", ["public", "local"])
@pytest.mark.parametrize("invalid", [False, True])
@pytest.mark.parametrize("packing_version", ["v2", "v5"])
def test_opt_in_v4_records_both_calls_and_preserves_native_delivery(
    tmp_path, monkeypatch, entry, invalid, packing_version
):
    smoke = write_smoke(tmp_path)
    client = CheckedClient(invalid)
    if entry == "public":
        monkeypatch.setattr(run_experiment, "ROOT", tmp_path)

        def call():
            return run_experiment.experiment(
                client, tmp_path / "run", reader_version="v4", packing_version=packing_version
            )
    else:
        args = argparse.Namespace(
            smoke=smoke,
            output=tmp_path / "run",
            max_calls=160,
            max_seconds=1800,
            reader_version="v4",
            packing_version=packing_version,
        )

        def call():
            return mac_smoke.run(args, client, {"embedding": None})

    if invalid:
        with pytest.raises(ReaderContextError, match="invalid_v4_reader_response"):
            asyncio.run(call())
    else:
        asyncio.run(call())
    saved = json.loads((tmp_path / "run/result.json").read_text())
    native = saved["arms"]["agent-memory"]
    assert native["answer_reader"]["schema_version"] == "evidence-reader/4"
    assert native["answer_reader"]["status"] == ("invalid" if invalid else "supported")
    assert "hypothesis" in native if not invalid else "hypothesis" not in native
    assert len(client.receipts) == (8 if invalid else 9)
    assert native["reader_delivery"]["evidence_text_not_repacked"] is True
    if entry == "public":
        assert saved["protocol"]["answer_calls"] == 8
        assert saved["protocol"]["max_calls_needed"] == saved["protocol"]["extraction_calls"] + 8
