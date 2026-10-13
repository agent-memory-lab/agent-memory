"""Reporting contracts preserve unfinished tasks, failed attempts and unknown usage."""

import importlib.util
import json
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "longmemeval_full_reporting_test",
    Path(__file__).parents[1] / "examples" / "longmemeval_full_reporting.py",
)
reporting = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(reporting)


def arm(
    name,
    *,
    planned=500,
    completed=0,
    scored=0,
    correct=0,
    failed=0,
    running=0,
    started=None,
    attempts=None,
):
    started = completed + failed + running if started is None else started
    return {
        "name": name,
        "planned": planned,
        "started": started,
        "completed": completed,
        "scored": scored,
        "correct": correct,
        "failed": failed,
        "pending": planned - completed - failed - running,
        "running": running,
        "attempts": started if attempts is None else attempts,
        # Deliberately incorrect copied ratios must not override fixed counts.
        "accuracy_all_planned": 1.0,
    }


def summary(*arms):
    return {
        **{key: sum(row[key] for row in arms) for key in reporting._COUNTS},
        "by_arm": list(arms),
        "protocol_hash": "frozen-protocol",
        "max_attempts": 2,
    }


def record(
    arm_name,
    operation,
    *,
    kind="chat",
    success=True,
    input_tokens=10,
    output_tokens=2,
    request_id=1,
    settings=None,
):
    return {
        "arm": arm_name,
        "operation": operation,
        "kind": kind,
        "success": success,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "elapsed_seconds": 0.25,
        "request_id": request_id,
        "model": "qwen-local" if kind == "chat" else "local-embedding",
        "generation_settings": settings or {},
    }


def build(task_summary, records=(), status="running", manifest=None):
    return reporting.build_report(manifest or {}, task_summary, records, 12.5, status)


def test_formal_plan_keeps_failed_unknown_judge_and_pending_in_1500_denominator():
    tasks = summary(
        arm("agent-memory", completed=2, scored=1, correct=1, failed=1, running=1),
        arm("mem0-oss"),
        arm("graphiti"),
    )
    report = build(tasks)
    overall = report["task_summary"]
    assert overall["planned"] == 1500
    assert overall["started"] == 4
    assert overall["completed"] == 2 and overall["scored"] == 1
    assert overall["unscored_completed"] == 1
    assert overall["pending"] == 1496 and overall["running"] == 1
    assert overall["accuracy_all_planned"] == 1 / 1500
    assert overall["accuracy_scored_only"] == 1
    assert not report["is_complete"]


def test_single_arm_recovery_uses_its_actual_plan_instead_of_a_constant():
    report = build(
        summary(arm("agent-memory", completed=500, scored=499, correct=400)), status="completed"
    )
    assert report["is_complete"]
    assert report["task_summary"]["planned"] == 500
    assert report["task_summary"]["accuracy_all_planned"] == 0.8
    assert report["task_summary"]["unscored_completed"] == 1


@pytest.mark.parametrize("status", ["paused", "running", "stillrunning", "aborted", "failed"])
def test_stopped_or_active_status_never_claims_completion(status):
    report = build(
        summary(arm("agent-memory", completed=500, scored=500, correct=500)), status=status
    )
    assert not report["is_complete"]
    assert "进度快照（尚未完成）" in reporting.render_markdown(report)


def test_completed_label_cannot_hide_unfinished_tasks():
    report = build(
        summary(arm("agent-memory", completed=1, scored=1, correct=1)), status="completed"
    )
    assert report["status"] == "incomplete"
    assert report["requested_status"] == "completed"
    assert not report["is_complete"]


def test_terminal_failures_are_explicit_and_remain_in_the_accuracy_denominator():
    report = build(
        summary(arm("agent-memory", completed=499, scored=499, correct=499, failed=1)),
        status="completed",
    )
    assert report["is_complete"]
    assert report["status"] == "completed_with_failures"
    assert report["task_summary"]["accuracy_all_planned"] == 499 / 500


def test_started_includes_a_pending_task_that_already_consumed_an_attempt():
    report = build(summary(arm("agent-memory", started=1, attempts=2)))
    counts = report["task_summary"]
    assert counts["started"] == 1 and counts["pending"] == 500
    assert counts["attempts"] == 2


def test_all_retries_count_and_metadata_does_not_count_as_a_model_call():
    records = [
        record("agent-memory", "add.source", success=False, input_tokens=None, output_tokens=None),
        record("agent-memory", "add.source", input_tokens=15, output_tokens=4),
        record("agent-memory", "answer.question", input_tokens=6, output_tokens=2),
        record("agent-memory", "judge.question", input_tokens=5, output_tokens=1),
        record(
            "agent-memory",
            "search.question",
            kind="embedding",
            input_tokens=None,
            output_tokens=None,
        ),
        record("shared", "manifest", kind="other", input_tokens=None, output_tokens=None),
    ]
    usage = build(summary(arm("agent-memory")), records)["model_usage"]
    assert usage["total"]["calls"] == 5
    assert usage["total"]["failed_calls"] == 1
    assert usage["metadata_requests_excluded_from_model_calls"] == 1
    stages = usage["by_arm"][0]["by_stage"]
    assert {name: value["calls"] for name, value in stages.items()} == {
        "memory": 2,
        "answer": 1,
        "judge": 1,
        "embedding": 1,
    }
    assert stages["memory"]["input_tokens_measured"] == 15
    assert stages["memory"]["missing_input_usage_calls"] == 1
    assert stages["embedding"]["input_tokens_measured"] is None
    assert stages["embedding"]["output_tokens_measured"] is None
    assert usage["cost_status"] == "not_priced"


def test_zero_usage_is_measured_but_absent_or_invalid_usage_stays_unknown():
    usage = build(
        summary(arm("agent-memory")),
        [
            record("agent-memory", "answer", input_tokens=0, output_tokens=0),
            record("agent-memory", "judge", input_tokens=True, output_tokens=-1),
        ],
    )["model_usage"]["by_arm"][0]["by_stage"]
    assert usage["answer"]["input_tokens_measured"] == 0
    assert usage["answer"]["missing_input_usage_calls"] == 0
    assert usage["judge"]["input_tokens_measured"] is None
    assert usage["judge"]["missing_output_usage_calls"] == 1


def test_record_generator_is_consumed_once_and_per_file_request_ids_are_not_deduplicated():
    class Once:
        def __init__(self):
            self.iterations = 0

        def __iter__(self):
            self.iterations += 1
            assert self.iterations == 1
            for _ in range(2):
                yield record("agent-memory", "add.source", request_id=1)

    values = Once()
    report = build(summary(arm("agent-memory")), values)
    assert values.iterations == 1
    assert report["model_usage"]["total"]["calls"] == 2


def test_actual_model_configurations_include_graphiti_edge_limit_and_shared_overhead():
    settings = {"num_ctx": 32768, "num_predict": 16384, "seed": 42, "temperature": 0}
    records = [
        record("graphiti", "add.source", settings=settings),
        record("shared", "warmup", input_tokens=None, output_tokens=None),
    ]
    report = build(summary(arm("graphiti")), records)
    usage = report["model_usage"]
    assert usage["shared_or_unassigned"][0]["name"] == "shared"
    assert usage["total"]["calls"] == 2
    assert usage["actual_model_configurations"][0]["generation_settings"]["num_predict"] == 16384
    assert "16384" in reporting.render_markdown(report)


def test_report_is_json_serializable_and_does_not_mutate_the_manifest():
    manifest = {"dataset_sha256": "abc", "dependency_versions": {"mem0ai": "2.2.1"}}
    report = build(summary(arm("agent-memory")), manifest=manifest)
    report["manifest"]["dependency_versions"]["mem0ai"] = "changed"
    assert manifest["dependency_versions"]["mem0ai"] == "2.2.1"
    json.dumps(report, ensure_ascii=False, allow_nan=False)


def test_markdown_discloses_judge_protocol_unknown_cost_and_live_denominator():
    manifest = {
        "dataset_sha256": "dataset-hash",
        "model": "qwen3.5:9b",
        "dependency_versions": {"mem0ai": "2.2.1"},
    }
    report = build(
        summary(arm("agent-memory", completed=2, scored=1, correct=1)),
        [record("agent-memory", "answer", input_tokens=None, output_tokens=None)],
        status="paused",
        manifest=manifest,
    )
    text = reporting.render_markdown(report)
    assert "1/500" in text and "未评分 1 项" in text
    assert "dataset-hash" in text and "2.2.1" in text and "qwen3.5:9b" in text
    assert "未知" in text and "未测量美元成本" in text
    assert "not an official LongMemEval judge" in text
    assert "assistant semantic review are separate" in text


def test_inconsistent_aggregates_are_rejected_instead_of_silently_changing_denominators():
    task_summary = summary(arm("agent-memory"))
    task_summary["planned"] = 1500
    with pytest.raises(ValueError):
        build(task_summary)


def test_actual_run_ledger_summary_integrates_without_loading_result_payloads(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "longmemeval_run_ledger_reporting_test",
        Path(__file__).parents[1] / "examples" / "longmemeval_run_ledger.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names = ("agent-memory", "mem0-oss", "graphiti")
    manifest = {"dataset_sha256": "fixture-dataset"}
    with module.RunLedger(
        tmp_path / "run.sqlite3", manifest, ("q1", "q2"), names, max_attempts=1
    ) as ledger:
        for label in (True, None):
            task = ledger.next_task()
            ledger.complete(task["question_id"], task["arm"], {"judge_label": label})
        task = ledger.next_task()
        ledger.fail(task["question_id"], task["arm"], "ProviderError")
        ledger.next_task()
        report = reporting.build_report(manifest, ledger.summary(), iter(()), 1, "running")
    counts = report["task_summary"]
    assert counts["planned"] == 6
    assert counts["completed"] == 2 and counts["scored"] == counts["correct"] == 1
    assert counts["failed"] == counts["running"] == counts["unscored_completed"] == 1
    assert counts["pending"] == 2
    assert counts["accuracy_all_planned"] == 1 / 6
    assert counts["by_arm"][1]["unscored_completed"] == 1


@pytest.mark.parametrize("elapsed", [-1, float("nan"), float("inf"), True])
def test_invalid_elapsed_measurements_are_rejected(elapsed):
    with pytest.raises(ValueError):
        reporting.build_report({}, summary(arm("agent-memory")), (), elapsed, "running")


def regression_manifest():
    return {
        "arms": ["agent-memory"],
        "selected_question_ids": ["q1", "q2", "q3"],
        "evaluation_scope": "selected_regression",
        "source_question_count": 500,
        "question_count": 3,
        "planned_tasks": 3,
    }


def test_selected_regression_discloses_scope_source_and_actual_fixed_denominator():
    manifest = regression_manifest()
    report = build(
        summary(arm("agent-memory", planned=3, completed=2, scored=2, correct=2, failed=1)),
        status="completed",
        manifest=manifest,
    )
    assert report["is_complete"]
    assert report["evaluation_scope"] == "selected_regression"
    assert report["source_question_count"] == 500
    assert report["question_count"] == 3
    assert report["planned_tasks"] == 3
    assert report["arms"] == ["agent-memory"]
    assert report["selected_question_ids"] == ["q1", "q2", "q3"]
    assert report["task_summary"]["accuracy_all_planned"] == 2 / 3
    assert report["task_summary"]["accuracy_scored_only"] == 1
    assert [row["name"] for row in report["model_usage"]["by_arm"]] == ["agent-memory"]
    text = reporting.render_markdown(report)
    assert "回归子集" in text and "不是完整 500 道题的准确率" in text
    assert "3 道题 × 1 个框架 = 3 项" in text
    assert "2/3" in text
    assert "1500" not in text and "| mem0-oss |" not in text and "| graphiti |" not in text
    report["selected_question_ids"].append("other")
    assert manifest["selected_question_ids"] == ["q1", "q2", "q3"]


@pytest.mark.parametrize(
    "declared_arms",
    [
        [],
        "agent-memory",
        ["mem0-oss"],
        ["agent-memory", "graphiti"],
        ["agent-memory", "agent-memory"],
        [1],
    ],
)
def test_manifest_arms_must_exactly_match_the_task_summary(declared_arms):
    with pytest.raises(ValueError, match="arms"):
        build(summary(arm("agent-memory", planned=3)), manifest={"arms": declared_arms})


def test_manifest_arm_order_must_match_the_ledger_and_selected_order_is_preserved():
    tasks = summary(arm("graphiti", planned=3), arm("agent-memory", planned=3))
    with pytest.raises(ValueError, match="arms"):
        build(tasks, manifest={"arms": ["agent-memory", "graphiti"]})
    manifest = {**regression_manifest(), "arms": ["graphiti", "agent-memory"], "planned_tasks": 6}
    report = build(tasks, manifest=manifest)
    assert report["arms"] == ["graphiti", "agent-memory"]
    assert [row["name"] for row in report["task_summary"]["by_arm"]] == report["arms"]
    assert "3 道题 × 2 个框架 = 6 项" in reporting.render_markdown(report)


@pytest.mark.parametrize(
    "changes",
    [
        {"question_count": 4},
        {"question_count": True},
        {"planned_tasks": 1500},
        {"source_question_count": 2},
        {"selected_question_ids": ["q1", "q2"]},
        {"selected_question_ids": ["q1", "q1", "q3"]},
        {"selected_question_ids": "q1"},
        {"evaluation_scope": "subset"},
        {"evaluation_scope": "full"},
    ],
)
def test_selection_metadata_cannot_change_denominators_or_claim_full_scope(changes):
    with pytest.raises(ValueError):
        build(
            summary(arm("agent-memory", planned=3)),
            manifest={**regression_manifest(), **changes},
        )


def test_legacy_full_manifest_derives_actual_arms_without_selection_metadata():
    report = build(summary(arm("agent-memory"), arm("mem0-oss"), arm("graphiti")))
    assert report["evaluation_scope"] == "full"
    assert report["source_question_count"] == report["question_count"] == 500
    assert report["planned_tasks"] == 1500
    assert report["arms"] == ["agent-memory", "mem0-oss", "graphiti"]
    assert report["selected_question_ids"] is None
    text = reporting.render_markdown(report)
    assert "500 道题 × 3 个框架 = 1500 项" in text
    assert "回归子集" not in text


def test_three_question_selected_report_integrates_with_the_actual_ledger(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "longmemeval_selected_ledger_reporting_test",
        Path(__file__).parents[1] / "examples" / "longmemeval_run_ledger.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = regression_manifest()
    with module.RunLedger(
        tmp_path / "selected.sqlite3",
        manifest,
        manifest["selected_question_ids"],
        manifest["arms"],
        max_attempts=1,
    ) as ledger:
        task = ledger.next_task()
        ledger.complete(task["question_id"], task["arm"], {"judge_label": True})
        report = reporting.build_report(manifest, ledger.summary(), (), 1, "running")
    assert report["planned_tasks"] == 3
    assert report["task_summary"]["accuracy_all_planned"] == 1 / 3
    assert report["task_summary"]["pending"] == 2
    assert report["evaluation_scope"] == "selected_regression"
