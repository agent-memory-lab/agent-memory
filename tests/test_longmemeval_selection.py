"""Offline selection, fixed denominator and selected-arm publication contracts."""

import argparse
import ast
import asyncio
import importlib.util
import json
import sys
from contextlib import nullcontext
from datetime import UTC, datetime
from hashlib import sha256
from math import isfinite
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).parents[1] / "examples"
ARMS = ("agent-memory", "mem0-oss", "graphiti")


def module(name):
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


support = module("longmemeval_worker_support")
ledger_module = module("longmemeval_run_ledger")
reporting = module("longmemeval_full_reporting")
reader = module("memory_evidence_reader")


@pytest.fixture
def frozen_source(tmp_path):
    path = tmp_path / "source.json"
    path.write_text(json.dumps([{"question_id": f"q{index:03d}"} for index in range(500)]))
    return path, sha256(path.read_bytes()).hexdigest()


def select(source, **kwargs):
    return support.load_evaluation_selection(*source, ARMS, **kwargs)


def manifest(selection):
    return {key: value for key, value in selection.items() if key != "items"}


def test_default_remains_full_500_by_three_without_filtering(frozen_source):
    selection = select(frozen_source)
    assert selection["question_count"] == selection["source_question_count"] == 500
    assert selection["planned_tasks"] == 1500
    assert selection["arms"] == list(ARMS)
    assert selection["evaluation_scope"] == "full"
    assert selection["selected_question_ids"] == [f"q{index:03d}" for index in range(500)]


def test_explicit_regression_keeps_source_order_and_actual_plan(frozen_source):
    selection = select(frozen_source, arms=["agent-memory"], question_ids=["q017", "q004", "q011"])
    assert selection["selected_question_ids"] == ["q004", "q011", "q017"]
    assert selection["items"] == [
        {"question_id": key} for key in selection["selected_question_ids"]
    ]
    assert selection["source_question_count"] == 500
    assert selection["question_count"] == selection["planned_tasks"] == 3
    assert selection["evaluation_scope"] == "selected_regression"
    assert selection["arms"] == ["agent-memory"]


def test_single_arm_with_all_questions_is_disclosed_as_selected_scope(frozen_source):
    selection = select(frozen_source, arms=["agent-memory"])
    assert selection["question_count"] == 500 and selection["planned_tasks"] == 500
    assert selection["evaluation_scope"] == "selected_regression"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"arms": []},
        {"arms": ["agent-memory", "agent-memory"]},
        {"arms": ["imaginary-framework"]},
        {"arms": "agent-memory"},
        {"question_ids": []},
        {"question_ids": ["q001", "q001"]},
        {"question_ids": ["absent"]},
        {"question_ids": ["q001", ""]},
        {"question_ids": "q001"},
    ],
)
def test_invalid_selections_never_silently_expand_or_drop_tasks(frozen_source, kwargs):
    with pytest.raises(ValueError):
        select(frozen_source, **kwargs)


def test_entire_source_hash_is_checked_even_for_an_unselected_question(frozen_source):
    path, digest = frozen_source
    values = json.loads(path.read_text())
    values[-1]["question"] = "mutated unselected content"
    path.write_text(json.dumps(values))
    with pytest.raises(ValueError, match="frozen"):
        select((path, digest), arms=["agent-memory"], question_ids=["q001"])


@pytest.mark.parametrize("problem", ["499_questions", "duplicate", "invalid_id"])
def test_selection_requires_complete_valid_500_question_source(tmp_path, problem):
    values = [{"question_id": f"q{index:03d}"} for index in range(500)]
    if problem == "499_questions":
        values.pop()
    elif problem == "duplicate":
        values[-1] = values[0]
    else:
        values[-1]["question_id"] = None
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(values))
    digest = sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="source"):
        select((path, digest), arms=["agent-memory"], question_ids=["q001"])


def test_selected_ledger_denominator_and_resume_cannot_mix_question_or_arm_sets(
    tmp_path, frozen_source
):
    selection = select(frozen_source, arms=["agent-memory"], question_ids=["q001", "q002", "q003"])
    path = tmp_path / "run.sqlite3"
    with ledger_module.RunLedger(
        path,
        manifest(selection),
        selection["selected_question_ids"],
        selection["arms"],
        max_attempts=1,
    ) as ledger:
        first = ledger.next_task()
        assert first == {"question_id": "q001", "arm": "agent-memory", "attempt": 1}
        ledger.complete(first["question_id"], first["arm"], {"judge_label": True})
        second = ledger.next_task()
        ledger.complete(second["question_id"], second["arm"], {"judge_label": None})
        third = ledger.next_task()
        ledger.fail(third["question_id"], third["arm"], "ProviderError")
        assert ledger.next_task() is None
        summary = ledger.summary()
        assert summary["planned"] == 3 and summary["accuracy_all_planned"] == 1 / 3
        assert summary["scored"] == 1 and summary["failed"] == 1
    for changed in (
        select(frozen_source, arms=["graphiti"], question_ids=["q001", "q002", "q003"]),
        select(frozen_source, arms=["agent-memory"], question_ids=["q001", "q002", "q004"]),
    ):
        with pytest.raises(ValueError, match="planned task set changed"):
            ledger_module.RunLedger(
                path,
                manifest(changed),
                changed["selected_question_ids"],
                changed["arms"],
                max_attempts=1,
            )


@pytest.mark.parametrize("selected", [False, True])
def test_real_cli_forwards_explicit_selection_and_defaults_remain_full(
    tmp_path, frozen_source, monkeypatch, selected
):
    observed = []

    async def offline_run(args):
        observed.append(select(frozen_source, arms=args.arms, question_ids=args.question_ids))

    source = ast.parse((EXAMPLES / "longmemeval_full_comparison.py").read_text())
    functions = [
        node for node in source.body if isinstance(node, ast.FunctionDef)
        and node.name in {"main", "answer_reader_manifest"}
    ]
    namespace = {
        "argparse": argparse,
        "Path": Path,
        "ARMS": ARMS,
        "isfinite": isfinite,
        "worker_lock": lambda output: nullcontext(),
        "recover_call_log_tails": lambda output: [],
        "asyncio": asyncio,
        "run": offline_run,
        "evidence_reader": reader,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), "worker-cli", "exec"), namespace)
    arguments = ["worker", "--data", str(frozen_source[0]), "--output-dir", str(tmp_path)]
    if selected:
        arguments.extend(["--arms", "agent-memory", "--question-ids", "q003", "q001", "q002"])
    monkeypatch.setattr("sys.argv", arguments)
    namespace["main"]()
    assert len(observed) == 1
    assert observed[0]["planned_tasks"] == (3 if selected else 1500)
    assert observed[0]["arms"] == (["agent-memory"] if selected else list(ARMS))


def test_real_publish_function_exports_only_manifest_arms_without_sdk_imports(
    tmp_path, frozen_source
):
    # Execute the actual publication function with only its pure dependencies;
    # importing the full worker would initialize optional model/graph SDKs.
    source = ast.parse((EXAMPLES / "longmemeval_full_comparison.py").read_text())
    function = next(
        node
        for node in source.body
        if isinstance(node, ast.FunctionDef) and node.name == "publish_progress"
    )
    namespace = {
        "build_report": reporting.build_report,
        "render_markdown": reporting.render_markdown,
        "all_records": lambda output: iter(()),
        "write_json": lambda path, value: path.write_text(json.dumps(value)),
        "datetime": datetime,
        "UTC": UTC,
        "json": json,
    }
    exec(
        compile(ast.Module(body=[function], type_ignores=[]), "worker-publication", "exec"),
        namespace,
    )
    (tmp_path / "execution.json").write_text("{}")
    selection = select(frozen_source, arms=["agent-memory"], question_ids=["q001", "q002", "q003"])
    protocol = manifest(selection)
    with ledger_module.RunLedger(
        tmp_path / "run.sqlite3",
        protocol,
        selection["selected_question_ids"],
        selection["arms"],
    ) as ledger:
        namespace["publish_progress"](tmp_path, protocol, ledger, "running", datetime.now(UTC))
    assert (tmp_path / "agent-memory.jsonl").exists()
    assert not (tmp_path / "mem0-oss.jsonl").exists()
    assert not (tmp_path / "graphiti.jsonl").exists()
    report = json.loads((tmp_path / "progress.json").read_text())
    assert report["task_summary"]["planned"] == 3
    assert [row["name"] for row in report["task_summary"]["by_arm"]] == ["agent-memory"]
    assert report["manifest"]["source_question_count"] == 500
