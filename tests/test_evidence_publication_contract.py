"""Publication contract regressions; scripted only, with no provider calls."""

import asyncio
import json

import pytest

from experiments.evidence_obligations import runner as r
from experiments.evidence_obligations.dataset import (
    Episode, Query, SourceRecord, build_dataset,
)
from test_evidence_obligation_runner import Scripted


def test_custom_dispatch_cap_is_frozen_and_cannot_change(tmp_path):
    dataset = build_dataset()
    _, envelope = r.freeze(
        tmp_path, dataset, "dev", 1, ("baseline",),
        execution="scripted-test", max_calls=37,
    )
    assert envelope["manifest"]["run_call_cap"] == 37
    with pytest.raises(ValueError, match="manifest mismatch"):
        r.freeze(
            tmp_path, dataset, "dev", 1, ("baseline",),
            execution="scripted-test", max_calls=38,
        )


def test_runner_passes_custom_dispatch_cap_to_manifest(tmp_path):
    asyncio.run(r.run(
        tmp_path / "output", tmp_path / "work", Scripted(),
        arms=("baseline",), max_calls=37, execution="scripted-test",
    ))
    manifest = json.loads((tmp_path / "output" / "manifest.json").read_text())
    assert manifest["manifest"]["run_call_cap"] == 37


class ShortQuoteClient:
    def complete(self, messages, *, max_output_tokens, phase):
        if phase == "extract":
            output = {"quotes": [{"source_id": "source", "quote": "Project limit is 42."}]}
        elif phase == "gap":
            output = {"missing": [], "search_terms": ""}
        else:
            output = {
                "answer": "Project limit is 42.", "abstain": False,
                "quotes": [{"source_id": "source", "quote": "Project limit is 42."}],
            }
        return dict(content=json.dumps(output), input_tokens=100, output_tokens=30,
                    latency=0.001, receipt_id="scripted-" + phase)


def test_cached_answer_does_not_append_over_budget_source(tmp_path, monkeypatch):
    monkeypatch.setitem(r.LIMITS, "context_chars", 200)
    source = SourceRecord("source", "group", 1, "Project limit is 42. " + "padding " * 500)
    episode = Episode("episode", "dev", "group", (source,), (
        Query("first", "What is the project limit?"),
        Query("repeat", "What is the project limit?"),
    ))
    result = asyncio.run(r.run_episode(episode, "ordinary_cache", ShortQuoteClient(), tmp_path))
    assert result["rows"][1]["answer_cache_reused"]
    assert all(not row["delivery_blocked"] for row in result["rows"])
    assert all(sum(len(r.canonical(item)) for item in row["packed_evidence"]) <= 200
               for row in result["rows"])


def test_cache_falls_back_when_dependency_cannot_fit(tmp_path, monkeypatch):
    monkeypatch.setitem(r.LIMITS, "context_chars", 200)
    original_search = r.QuoteStore.search
    calls = 0

    async def search_once(self, query, limit=4):
        nonlocal calls
        calls += 1
        return await original_search(self, query, limit=limit) if calls == 1 else []

    monkeypatch.setattr(r.QuoteStore, "search", search_once)
    source = SourceRecord("source", "group", 1, "Project limit is 42. " + "padding " * 500)
    episode = Episode("episode", "dev", "group", (source,), (
        Query("first", "What is the project limit?"),
        Query("repeat", "What is the project limit?"),
    ))
    result = asyncio.run(r.run_episode(episode, "ordinary_cache", ShortQuoteClient(), tmp_path))
    repeated = result["rows"][1]
    assert not repeated["answer_cache_reused"]
    assert repeated["model_calls"] == 1  # Fresh answer; cached gap state is still reused.
    assert repeated["packed_evidence"] == []
    assert repeated["delivery_blocked"]  # Scripted answer lacks delivered support.


def test_key_prompt_rejects_echo_fallback(monkeypatch):
    import warnings

    def echoed_fallback(prompt):
        warnings.warn("Cannot control echo on the terminal.", r.getpass.GetPassWarning)
        raise AssertionError("The fallback must stop before reading echoed input")

    monkeypatch.setattr(r.getpass, "getpass", echoed_fallback)
    with pytest.raises(RuntimeError, match="non-echoing terminal"):
        r._prompt_api_key()


def test_key_prompt_accepts_non_echoing_input(monkeypatch):
    monkeypatch.setattr(r.getpass, "getpass", lambda prompt: "offline-test-value")
    assert r._prompt_api_key() == "offline-test-value"
