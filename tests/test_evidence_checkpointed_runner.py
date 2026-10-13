"""Offline process-interruption replay; no provider calls or credentials."""

import asyncio
import json

import pytest
from test_evidence_obligation_runner import Scripted

from experiments.evidence_obligations import runner as r
from experiments.evidence_obligations.checkpoints import DurableCallCache


@pytest.mark.parametrize("completed_phase", [1, 2, 3])
def test_interrupt_after_extract_gap_answer_replays_without_redispatch(
    tmp_path, monkeypatch, completed_phase
):
    client = Scripted()
    original = DurableCallCache.complete
    count = 0

    def crash(self, *args, **kwargs):
        nonlocal count
        result = original(self, *args, **kwargs)
        count += 1
        if count == completed_phase:
            raise KeyboardInterrupt("offline simulated termination after durable response")
        return result

    monkeypatch.setattr(DurableCallCache, "complete", crash)
    args = (tmp_path / "results", tmp_path / "work", client)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(r.run(*args, arms=("per_query_gap",), execution="scripted-test"))
    known = list(client.requests)
    monkeypatch.setattr(DurableCallCache, "complete", original)
    report = asyncio.run(r.run(*args, arms=("per_query_gap",), execution="scripted-test"))
    clean = Scripted()
    clean_report = asyncio.run(
        r.run(
            tmp_path / "clean",
            tmp_path / "cleanwork",
            clean,
            arms=("per_query_gap",),
            execution="scripted-test",
        )
    )
    assert client.requests[:completed_phase] == known
    assert len(client.requests) == len(clean.requests) == 32
    assert report["model_execution"] == clean_report["model_execution"] == "scripted-test"
    result_path = next((tmp_path / "results").glob("episode-*--per_query_gap.json"))
    receipts = json.loads(result_path.read_text())["receipts"]
    assert all(receipt["checkpoint_reused"] for receipt in receipts[:completed_phase])
    checkpoints = list((tmp_path / "results" / "checkpoints").rglob("query-*.json"))
    assert len(checkpoints) == 14


def test_interrupt_after_query_checkpoint_never_loses_answer(tmp_path, monkeypatch):
    client = Scripted()
    original = r.atomic_json
    fired = False

    def crash(path, value):
        nonlocal fired
        original(path, value)
        if path.name.startswith("query-") and not fired:
            fired = True
            raise KeyboardInterrupt("offline simulated termination after query persistence")

    monkeypatch.setattr(r, "atomic_json", crash)
    args = (tmp_path / "results", tmp_path / "work", client)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(r.run(*args, arms=("per_query_gap",), execution="scripted-test"))
    assert len(list((tmp_path / "results" / "checkpoints").rglob("query-*.json"))) == 1
    monkeypatch.setattr(r, "atomic_json", original)
    asyncio.run(r.run(*args, arms=("per_query_gap",), execution="scripted-test"))
    assert len(client.requests) == 32
