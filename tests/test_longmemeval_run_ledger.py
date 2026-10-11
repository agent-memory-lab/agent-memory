"""Small durable experiment ledgers; no dataset, model, or framework calls."""

import importlib.util
import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "longmemeval_run_ledger", Path(__file__).parents[1] / "examples" / "longmemeval_run_ledger.py"
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
RunLedger = _MODULE.RunLedger

MANIFEST = {
    "source": {"commit": "fixture-source", "script_sha256": "fixture-script"},
    "data": {"sha256": "fixture-data"},
    "model": {"digest": "fixture-model"},
    "dependencies": {"framework": "1.0"},
    "settings": {"seed": 7, "max_requests": 1000},
}
QUESTIONS = ("q1", "q2", "q3")
ARMS = ("agent-memory", "mem0-oss", "graphiti")


def new_ledger(path, **kwargs):
    return RunLedger(
        path,
        kwargs.pop("manifest", MANIFEST),
        kwargs.pop("task_ids", QUESTIONS),
        kwargs.pop("arms", ARMS),
        **kwargs,
    )


def complete_task(ledger, label=True):
    task = ledger.next_task()
    ledger.complete(
        task["question_id"], task["arm"], {"judge_label": label, "hypothesis": "answer"}
    )
    return task


def test_task_order_explicit_planned_denominator_and_unknown_judgments(tmp_path):
    with new_ledger(tmp_path / "run.sqlite3") as ledger:
        assert ledger.summary()["planned"] == 9
        first = complete_task(ledger, True)
        second = complete_task(ledger, None)
        third = complete_task(ledger, False)
        assert [task["arm"] for task in (first, second, third)] == list(ARMS)
        assert all(
            task["question_id"] == "q1" and task["attempt"] == 1 for task in (first, second, third)
        )
        summary = ledger.summary()
        assert summary["completed"] == 3 and summary["scored"] == 2 and summary["correct"] == 1
        assert summary["pending"] == 6 and summary["accuracy_all_planned"] == 1 / 9
        assert summary["started"] == 3 and summary["attempts"] == 3
        mem0 = summary["by_arm"][1]
        assert mem0["planned"] == 3 and mem0["unscored_completed"] == 1
        assert mem0["scored"] == 0 and mem0["accuracy_all_planned"] == 0


def test_retry_retains_failed_attempt_and_terminal_failure_keeps_denominator(tmp_path):
    with new_ledger(tmp_path / "run.sqlite3", task_ids=("q1",), arms=("agent-memory",)) as ledger:
        task = ledger.next_task()
        first_payload = {
            "calls": 5,
            "cost": None,
            "answer_reference": "first.json",
            "judge_label": None,
        }
        ledger.fail(task["question_id"], task["arm"], "ProviderError", first_payload)
        assert ledger.summary()["pending"] == 1 and ledger.summary()["failed"] == 0
        retry = ledger.next_task()
        assert retry == {"question_id": "q1", "arm": "agent-memory", "attempt": 2}
        ledger.fail(
            "q1", "agent-memory", "RequestBudgetExceeded", {"calls": 8, "judge_label": None}
        )
        assert ledger.next_task() is None
        summary = ledger.summary()
        assert summary["planned"] == summary["failed"] == 1
        assert summary["completed"] == summary["scored"] == summary["correct"] == 0
        assert summary["accuracy_all_planned"] == 0 and summary["attempts"] == 2
        history = ledger.attempt_history("q1", "agent-memory")
        assert [item["status"] for item in history] == ["failed", "failed"]
        assert history[0]["payload"] == first_payload
        results = list(ledger.results("agent-memory"))
        assert len(results) == 1 and results[0]["status"] == "failed"
        assert results[0]["payload"]["calls"] == 8


def test_durable_resume_preserves_results_and_recovers_running_attempt(tmp_path):
    path = tmp_path / "run.sqlite3"
    with new_ledger(path, arms=("agent-memory",)) as ledger:
        complete_task(ledger)
        assert ledger.next_task()["question_id"] == "q2"
        protocol_hash = ledger.protocol_hash
    with new_ledger(path, arms=("agent-memory",)) as ledger:
        assert ledger.protocol_hash == protocol_hash
        assert ledger.summary()["completed"] == ledger.summary()["running"] == 1
        assert ledger.recover() == 1 and ledger.recover() == 0
        retry = ledger.next_task()
        assert retry["question_id"] == "q2" and retry["attempt"] == 2
        history = ledger.attempt_history("q2", "agent-memory")
        assert [item["status"] for item in history] == ["abandoned", "running"]
        ledger.complete("q2", "agent-memory", {"judge_label": None, "calls": 4})
        assert ledger.next_task()["question_id"] == "q3"
        results = list(ledger.results("agent-memory"))
        assert len(results) == 2 and results[0]["payload"]["hypothesis"] == "answer"


def test_abandoned_attempts_exhaust_finite_budget_without_silent_success(tmp_path):
    with new_ledger(tmp_path / "run.sqlite3", task_ids=("q1",), arms=("graphiti",)) as ledger:
        assert ledger.next_task()["attempt"] == 1
        ledger.recover()
        assert ledger.next_task()["attempt"] == 2
        ledger.recover()
        assert ledger.summary()["pending"] == 1
        assert ledger.next_task() is None
        assert ledger.summary()["failed"] == 1 and ledger.summary()["correct"] == 0
        assert [row["status"] for row in ledger.attempt_history("q1", "graphiti")] == [
            "abandoned",
            "abandoned",
        ]
        assert list(ledger.results("graphiti"))[0]["error_type"] == "AttemptBudgetExhausted"


@pytest.mark.parametrize("section", ["source", "data", "model", "dependencies", "settings"])
def test_changed_protocol_sections_cannot_mix_resumed_results(tmp_path, section):
    path = tmp_path / "run.sqlite3"
    with new_ledger(path) as ledger:
        complete_task(ledger)
    changed = deepcopy(MANIFEST)
    changed[section]["changed"] = "new revision"
    with pytest.raises(ValueError, match="protocol"):
        new_ledger(path, manifest=changed)
    with new_ledger(path) as ledger:
        assert ledger.summary()["completed"] == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"task_ids": ("q1", "q2")},
        {"task_ids": ("q2", "q1", "q3")},
        {"arms": ("graphiti", "agent-memory", "mem0-oss")},
        {"max_attempts": 1},
    ],
)
def test_changed_task_set_order_or_attempt_budget_cannot_resume(tmp_path, kwargs):
    path = tmp_path / "run.sqlite3"
    with new_ledger(path):
        pass
    with pytest.raises(ValueError, match="protocol"):
        new_ledger(path, **kwargs)


def test_canonical_manifest_is_order_independent_and_detached(tmp_path):
    path = tmp_path / "run.sqlite3"
    source = deepcopy(MANIFEST)
    with new_ledger(path, manifest=source) as ledger:
        original_hash = ledger.protocol_hash
        source["model"]["digest"] = "changed"
        detached = ledger.manifest
        detached["settings"]["seed"] = 100
        assert ledger.manifest["settings"]["seed"] == 7
    reordered = dict(reversed(list(MANIFEST.items())))
    with new_ledger(path, manifest=reordered) as ledger:
        assert ledger.protocol_hash == original_hash


def test_atomic_claims_across_two_connections_do_not_duplicate_work(tmp_path):
    path = tmp_path / "run.sqlite3"
    with new_ledger(path) as first, new_ledger(path) as second:
        with ThreadPoolExecutor(max_workers=2) as pool:
            claimed = list(pool.map(lambda ledger: ledger.next_task(), (first, second)))
        assert len({(task["question_id"], task["arm"]) for task in claimed}) == 2
        assert first.summary()["running"] == 2


def test_completed_and_terminal_failed_states_are_not_implicitly_reclaimed(tmp_path):
    with new_ledger(
        tmp_path / "run.sqlite3",
        task_ids=("q1",),
        arms=("agent-memory", "graphiti"),
        max_attempts=1,
    ) as ledger:
        complete_task(ledger, None)
        ledger.next_task()
        ledger.fail("q1", "graphiti", "ProviderError")
        assert ledger.recover() == 0 and ledger.next_task() is None
        summary = ledger.summary()
        assert summary["planned"] == 2 and summary["correct"] == summary["scored"] == 0
        assert summary["completed"] == summary["failed"] == 1
        with pytest.raises(ValueError, match="running"):
            ledger.complete("q1", "graphiti", {"judge_label": True})


def test_invalid_payload_is_rejected_transactionally_and_history_preserves_json(tmp_path):
    with new_ledger(tmp_path / "run.sqlite3", task_ids=("q1",), arms=("agent-memory",)) as ledger:
        ledger.next_task()
        with pytest.raises(ValueError, match="judge_label"):
            ledger.complete("q1", "agent-memory", {"judge_label": "yes"})
        with pytest.raises(ValueError, match="JSON"):
            ledger.complete("q1", "agent-memory", {"calls": float("nan")})
        assert ledger.summary()["running"] == 1
        payload = {"judge_label": True, "hypothesis": "原答案", "cost": None, "calls": {"chat": 2}}
        ledger.complete("q1", "agent-memory", payload)
        payload["calls"]["chat"] = 99
        assert list(ledger.results("agent-memory"))[0]["payload"]["calls"]["chat"] == 2
        assert (
            json.dumps(ledger.attempt_history("q1", "agent-memory"), ensure_ascii=False).find(
                "原答案"
            )
            >= 0
        )


def test_corrupt_protocol_hash_or_missing_planned_task_rejects_resume(tmp_path):
    path = tmp_path / "run.sqlite3"
    with new_ledger(path):
        pass
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("UPDATE run_metadata SET value='tampered' WHERE key='protocol_hash'")
    with pytest.raises(ValueError, match="hash"):
        new_ledger(path)


@pytest.mark.parametrize(
    "mutation",
    [
        "DELETE FROM tasks WHERE question_id='q3' AND arm='graphiti'",
        "UPDATE tasks SET question_id='unplanned' WHERE question_id='q3' AND arm='graphiti'",
    ],
)
def test_task_table_mutations_cannot_silently_change_the_planned_run(tmp_path, mutation):
    path = tmp_path / "run.sqlite3"
    with new_ledger(path):
        pass
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(mutation)
    with pytest.raises(ValueError, match="planned task"):
        new_ledger(path)


def test_committed_attempts_survive_process_exit_without_ledger_close(tmp_path):
    path = tmp_path / "run.sqlite3"
    script = """
import importlib.util, json, os, sys
spec = importlib.util.spec_from_file_location('run_ledger', sys.argv[2])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
ledger = module.RunLedger(sys.argv[1], json.loads(sys.argv[3]), ('q1','q2','q3'), ('agent-memory',))
ledger.next_task()
ledger.complete('q1','agent-memory', {'judge_label': True, 'hypothesis': 'durable answer'})
ledger.next_task()
os._exit(0)
"""
    subprocess.run(
        [sys.executable, "-c", script, str(path), str(_SPEC.origin), json.dumps(MANIFEST)],
        check=True,
        timeout=10,
    )
    with new_ledger(path, arms=("agent-memory",)) as ledger:
        assert ledger.summary()["completed"] == ledger.summary()["running"] == 1
        assert ledger.summary()["correct"] == 1
        assert ledger.recover() == 1
        assert ledger.next_task()["attempt"] == 2
        assert list(ledger.results("agent-memory"))[0]["payload"]["hypothesis"] == "durable answer"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"manifest": {}},
        {"task_ids": ("q1", "q1")},
        {"arms": ()},
        {"max_attempts": True},
    ],
)
def test_invalid_plan_is_rejected_before_work(tmp_path, kwargs):
    with pytest.raises(ValueError):
        new_ledger(tmp_path / "run.sqlite3", **kwargs)
