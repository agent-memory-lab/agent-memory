"""Rubric and shared evidence limits must not silently inflate diagnostics."""

import asyncio
import importlib.util
from pathlib import Path
from time import sleep
from types import SimpleNamespace

import pytest

_PATH = Path(__file__).parents[1] / "examples" / "memory_benchmark_protocol.py"
_SPEC = importlib.util.spec_from_file_location("memory_benchmark_protocol_test", _PATH)
protocol = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(protocol)


@pytest.mark.parametrize("budget", [1, 2, 3, 4, 7, 10, 16])
def test_multibyte_evidence_never_exceeds_shared_budget(budget):
    evidence = protocol.bounded_evidence(
        [{"text": "中文城市成都"}, {"text": "extra"}], byte_budget=budget
    )
    assert len(evidence.encode()) <= budget
    assert "\ufffd" not in evidence


def test_evidence_keeps_order_and_top_k_instead_of_gold_selection():
    assert protocol.bounded_evidence([{"text": "first"}, {"text": "second"}], limit=1) == "- first"
    assert protocol.bounded_evidence([]) == ""


def test_current_fact_containing_both_obsolete_and_new_value_is_rejected():
    spec = {"required_aliases": [["成都", "Chengdu"]], "forbidden_aliases": ["南京"]}
    assert not protocol.grade_answer(spec, {"status": "answered", "answer": "南京或成都"})["passed"]
    assert protocol.grade_answer(spec, {"status": "answered", "answer": "成都"})["passed"]


@pytest.mark.parametrize(
    "response",
    [
        {"status": "answered", "answer": "未知"},
        {"status": "unknown", "answer": "护照号码为1234，其他未知"},
        {"status": "other", "answer": "未知"},
        {},
        "未知",
    ],
)
def test_abstention_cannot_hide_unsupported_answers(response):
    assert not protocol.grade_answer({"unanswerable": True}, response)["passed"]


def test_short_explicit_abstention_passes():
    assert protocol.grade_answer({"unanswerable": True}, {"status": "unknown", "answer": "未知。"})[
        "passed"
    ]


def test_failed_or_missing_cases_remain_in_planned_denominator():
    summary = protocol.summarize_cases([{"grade": {"passed": True}}], planned=4)
    assert summary == {
        "planned": 4,
        "completed": 1,
        "correct": 1,
        "failed_or_missing": 3,
        "correct_rate_all_planned": 0.25,
    }
    with pytest.raises(ValueError):
        protocol.summarize_cases([{}], planned=0)


def test_canonical_digest_ignores_mapping_order_and_tracks_content():
    assert protocol.canonical_digest({"a": 1, "b": 2}) == protocol.canonical_digest(
        {"b": 2, "a": 1}
    )
    assert protocol.canonical_digest({"a": 1}) != protocol.canonical_digest({"a": 2})


def test_timeout_waits_for_synchronous_write_before_returning():
    effects = []
    phases = []
    ledger = SimpleNamespace(start_phase=lambda *value: phases.append(value))
    row = {"trial": 1, "name": "sync-sdk", "operations": []}

    def write():
        sleep(0.03)
        effects.append("committed")

    async def run():
        with pytest.raises(protocol.BenchmarkTimeout):
            await protocol.checked_call(ledger, row, "add", asyncio.to_thread(write), 0.005)
        assert effects == ["committed"]

    asyncio.run(run())
    assert phases == [(1, "sync-sdk", "add")]
    assert row["operations"][0]["status"] == "failed"


def test_cancellation_preserves_attribution_until_background_write_settles():
    from threading import Event

    started, release = Event(), Event()
    effects = []
    phases = []
    ledger = SimpleNamespace(start_phase=lambda *value: phases.append(value))
    row = {"trial": 1, "name": "sync-sdk", "operations": []}

    def write():
        started.set()
        assert release.wait(1)
        effects.append("committed")

    async def run():
        task = asyncio.create_task(
            protocol.checked_call(
                ledger,
                row,
                "add",
                asyncio.to_thread(write),
                1,
            )
        )
        assert await asyncio.to_thread(started.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert effects == ["committed"]

    asyncio.run(run())
    assert phases == [(1, "sync-sdk", "add")]
    assert row["operations"][0]["error_type"] == "CancelledError"


def test_incomplete_model_accounting_aborts_before_another_arm():
    proxy = SimpleNamespace(drain=lambda timeout: False, pending_requests=1)
    state = {"status": "completed"}

    async def run():
        with pytest.raises(protocol.BenchmarkTimeout, match="unfinished_model_accounting"):
            await protocol.require_model_accounting(proxy, state, 0.01)

    asyncio.run(run())
    assert state == {
        "status": "aborted",
        "accounting_complete": False,
        "pending_model_requests": 1,
        "error_type": "IncompleteModelAccounting",
    }


def test_complete_model_accounting_preserves_success_state():
    proxy = SimpleNamespace(drain=lambda timeout: True, pending_requests=0)
    state = {"status": "completed"}
    assert asyncio.run(protocol.require_model_accounting(proxy, state, 0.01))
    assert state["status"] == "completed" and state["accounting_complete"] is True
