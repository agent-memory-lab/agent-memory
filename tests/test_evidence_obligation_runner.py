"""Scripted wiring tests, never model quality or provider usage evidence."""

import asyncio
import json

import pytest

from experiments.evidence_obligations import runner as r
from experiments.evidence_obligations.dataset import build_dataset


class Scripted:
    def __init__(self):
        self.requests = []

    def complete(self, messages, *, max_output_tokens, phase):
        payload = json.loads(messages[-1]["content"])
        self.requests.append((phase, payload))
        if phase == "extract":
            output = {
                "quotes": [
                    {"source_id": s["source_id"], "quote": s["text"].split(". ")[0]}
                    for s in payload["documents"]
                ][:8]
            }
        elif phase == "gap":
            output = {
                "missing": ["Conditions or temporal support"],
                "search_terms": payload["question"],
            }
        else:
            evidence = payload["evidence"]
            output = {
                "answer": evidence[0]["text"] if evidence else "",
                "abstain": not evidence,
                "quotes": [{"source_id": evidence[0]["source_id"], "quote": evidence[0]["text"]}]
                if evidence
                else [],
            }
        return dict(
            content=json.dumps(output),
            input_tokens=100,
            output_tokens=30,
            latency=0.001,
            receipt_id="scripted-" + str(len(self.requests)),
        )


def test_explicit_lifecycle_and_revocation_safety():
    state = r.ObligationState("persistent_obligations")
    state.frontier = "v1"
    gap = {"missing": ["deadline qualifier"], "search_terms": "deadline"}
    state.save("deadline for project", gap)
    assert state.records[0]["state"] == "unresolved"
    state.resolve("deadline for project", {"abstain": False}, True)
    assert state.get("project deadline")["missing"] == []
    state.source_changed("v2")
    assert state.records[0]["state"] == "reopened" and state.get("project deadline") is None
    state.source_changed("v3", purge=True)
    assert state.records == []
    ablation = r.ObligationState("no_source_invalidation")
    ablation.save("deadline for project", gap)
    ablation.source_changed("v2")
    assert ablation.get("deadline for project") == gap
    ablation.source_changed("v3", purge=True)
    assert not ablation.records


def test_shared_extraction_immutable_and_actual_calls_once(tmp_path):
    client = Scripted()
    cache = r.SharedExtractionClient(client, tmp_path)
    messages = [
        {
            "role": "user",
            "content": json.dumps({"documents": [{"source_id": "s", "text": "hello source"}]}),
        }
    ]
    a = cache.complete(messages, max_output_tokens=100, phase="extract")
    b = cache.complete(messages, max_output_tokens=100, phase="extract")
    assert len(client.requests) == 1 and a["receipt_id"] == b["receipt_id"]
    assert b["shared_extraction_reused"]
    path = next((tmp_path / "shared-extraction").glob("*.json"))
    data = json.loads(path.read_text())
    data["receipt"]["content"] = "changed"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="corrupt"):
        cache.complete(messages, max_output_tokens=100, phase="extract")


def test_all_arms_real_core_no_gold_and_stale_delivery(tmp_path):
    async def scenario():
        episode = build_dataset().dev[0]
        client = Scripted()
        for arm in r.ARMS:
            work = tmp_path / arm
            work.mkdir()
            result = await r.run_episode(episode, arm, client, work)
            assert result["status"] == "complete"
            assert result["counts"]["initial_quotes"] > 0
            assert len(result["rows"]) == len(result["expected_query_ids"])
            assert any(row["retrieved_atoms"] for row in result["rows"])
            for row in result["rows"]:
                live = {s["source_id"]: s for s in row["live_sources"]}
                for citation in row["citations"]:
                    assert citation["source_id"] in live
                    assert citation["revision"] == live[citation["source_id"]]["revision"]
            if arm == "no_cross_query_sharing":
                assert result["counts"]["gap_reuses"] == 0
        serialized = json.dumps(client.requests)
        assert "required_values" not in serialized and "required_qualifiers" not in serialized
        assert "supporting_source_ids" not in serialized

    asyncio.run(scenario())


def test_freeze_detects_configuration_changes(tmp_path, monkeypatch):
    data = build_dataset()
    r.freeze(tmp_path, data, "dev", 1, r.ARMS, execution="scripted-test")
    monkeypatch.setitem(r.LIMITS, "sequence_tokens", 39999)
    with pytest.raises(ValueError, match="manifest mismatch"):
        r.freeze(tmp_path, data, "dev", 1, r.ARMS, execution="scripted-test")


def test_dispatch_cap_survives_resume(tmp_path):
    client = Scripted()
    payload = [{"role": "user", "content": json.dumps({"documents": []})}]
    r.RunClient(client, 1, tmp_path).complete(payload, max_output_tokens=10, phase="extract")
    with pytest.raises(RuntimeError, match="run_call_limit"):
        r.RunClient(client, 1, tmp_path).complete(payload, max_output_tokens=10, phase="extract")
    assert len(client.requests) == 1


def test_full_frozen_offline_run_and_no_spend_resume(tmp_path):
    client = Scripted()
    first = asyncio.run(
        r.run(tmp_path / "output", tmp_path / "work", client, execution="scripted-test")
    )
    before = len(client.requests)
    second = asyncio.run(
        r.run(tmp_path / "output", tmp_path / "work", client, execution="scripted-test")
    )
    assert len(client.requests) == before
    assert first["arms"] == second["arms"]
    assert first["model_execution"] == "scripted-test"
    assert len(first["arms"]) == 8
