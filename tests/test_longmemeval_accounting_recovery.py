"""Crash-safe accounting publication using SQLite and local files, no models."""

import ast
import importlib.util
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).parents[1] / "examples"


def load_module(name):
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ledger_module = load_module("longmemeval_run_ledger")
reporting = load_module("longmemeval_full_reporting")
MANIFEST = {"arms": ["agent-memory"], "question_count": 1, "planned_tasks": 1}


def open_ledger(path):
    return ledger_module.RunLedger(path, MANIFEST, ["q1"], ["agent-memory"], max_attempts=2)


def publish(output, ledger, status="interrupted"):
    # Load the production publication function without importing optional model
    # SDKs; its real RunLedger and filesystem dependencies are used unmodified.
    source = ast.parse((EXAMPLES / "longmemeval_full_comparison.py").read_text())
    functions = [
        node for node in source.body
        if isinstance(node, ast.FunctionDef) and node.name in ("publish_progress", "all_records")
    ]
    namespace = {
        "build_report": reporting.build_report,
        "render_markdown": reporting.render_markdown,
        "write_json": lambda path, value: path.write_text(json.dumps(value)),
        "datetime": datetime,
        "UTC": UTC,
        "json": json,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), "publication", "exec"), namespace)
    namespace["publish_progress"](output, MANIFEST, ledger, status, datetime.now(UTC))
    return json.loads((output / "progress.json").read_text())


def test_two_crashes_after_committed_recovery_never_lose_abandoned_count(tmp_path):
    path = tmp_path / "run.sqlite3"
    with open_ledger(path) as ledger:
        ledger.next_task()
    # Simulate the historical JSON counter being unchanged by both crashes.
    original_execution = json.dumps({"abandoned_attempts": 0})
    (tmp_path / "execution.json").write_text(original_execution)
    script = """
import importlib.util, json, os, sys
spec = importlib.util.spec_from_file_location('ledger', sys.argv[2])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
ledger = module.RunLedger(sys.argv[1], json.loads(sys.argv[3]), ['q1'], ['agent-memory'])
recovered = ledger.recover()
assert recovered == int(sys.argv[4])
os._exit(0)  # SQLite committed; execution.json/progress.json have not been updated.
"""
    for expected in (1, 0):
        subprocess.run(
            [sys.executable, "-c", script, str(path), str(EXAMPLES / "longmemeval_run_ledger.py"),
             json.dumps(MANIFEST), str(expected)],
            check=True, timeout=10,
        )
    with open_ledger(path) as ledger:
        assert ledger.recover() == 0
        assert ledger.summary()["abandoned_attempts"] == 1
        report = publish(tmp_path, ledger)
        assert report["accounting_integrity"]["abandoned_attempts"] == 1
        assert report["accounting_integrity"]["interrupted_usage_may_be_unknown"]
        assert report["task_summary"]["completed"] == 0
        assert report["task_summary"]["planned"] == 1
        assert not report["is_complete"]
        # A later successful retry must not erase the earlier unknown cost.
        ledger.next_task()
        ledger.complete("q1", "agent-memory", {"judge_label": True})
        completed = publish(tmp_path, ledger, "completed")
        assert completed["is_complete"]
        assert completed["accounting_integrity"]["interrupted_usage_may_be_unknown"]
    assert (tmp_path / "execution.json").read_text() == original_execution


@pytest.mark.parametrize("kind", ["running", "incomplete", "marker"])
def test_publication_keeps_unknown_usage_even_without_abandoned_attempts(tmp_path, kind):
    with open_ledger(tmp_path / "run.sqlite3") as ledger:
        if kind in ("running", "incomplete"):
            ledger.next_task()
        if kind == "incomplete":
            ledger.fail("q1", "agent-memory", "IncompleteModelAccounting", {"calls": 0})
        if kind == "marker":
            directory = tmp_path / "calls"
            directory.mkdir()
            # Even an interrupted marker write means cost must remain unknown.
            (directory / "attempt1.jsonl.accounting-error.json").write_text("{")
        report = publish(tmp_path, ledger)
        integrity = report["accounting_integrity"]
        assert integrity["abandoned_attempts"] == 0
        assert integrity["interrupted_usage_may_be_unknown"]
        assert integrity["running_attempts"] == (1 if kind == "running" else 0)
        assert integrity["incomplete_accounting_attempts"] == (1 if kind == "incomplete" else 0)
        assert bool(integrity["persistence_error_logs"]) == (kind == "marker")
        assert report["task_summary"]["completed"] == 0
        assert any("unrecorded model usage" in item for item in report["limitations"])


def test_stale_execution_counter_cannot_override_authoritative_attempt_history(tmp_path):
    (tmp_path / "execution.json").write_text(json.dumps({"abandoned_attempts": 999}))
    with open_ledger(tmp_path / "run.sqlite3") as ledger:
        ledger.next_task()
        ledger.complete("q1", "agent-memory", {"judge_label": True})
        integrity = publish(tmp_path, ledger, "completed")["accounting_integrity"]
        assert integrity["abandoned_attempts"] == 0
        assert not integrity["interrupted_usage_may_be_unknown"]
