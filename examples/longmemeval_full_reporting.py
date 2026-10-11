"""Standard-library progress reports for the resumable LongMemEval comparison.

Task counts come from RunLedger.summary(), never from a partial result file.
Model records are consumed once, including failed and retried request attempts.
This module does not call a model, load the dataset, or infer missing usage.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from copy import deepcopy
from math import isfinite

_COUNTS = (
    "planned",
    "started",
    "completed",
    "scored",
    "correct",
    "failed",
    "pending",
    "running",
    "attempts",
)
_STAGES = ("memory", "answer", "judge", "embedding")
_CHAT_KINDS = {"chat"}
_COMPLETE_STATUSES = {"completed", "completed_with_failures"}


def _nonnegative_int(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _task_counts(summary):
    counts = {key: _nonnegative_int(summary[key], key) for key in _COUNTS}
    if (
        counts["completed"] + counts["failed"] + counts["pending"] + counts["running"]
        != (counts["planned"])
    ):
        raise ValueError("task statuses must cover the fixed planned denominator")
    if not counts["correct"] <= counts["scored"] <= counts["completed"] <= counts["started"]:
        raise ValueError("correct/scored/completed/started counts are inconsistent")
    if counts["started"] > counts["planned"] or counts["attempts"] < counts["started"]:
        raise ValueError("started task and request attempt counts are inconsistent")
    counts["unscored_completed"] = counts["completed"] - counts["scored"]
    counts["accuracy_all_planned"] = (
        counts["correct"] / counts["planned"] if counts["planned"] else None
    )
    counts["accuracy_scored_only"] = (
        counts["correct"] / counts["scored"] if counts["scored"] else None
    )
    counts["completion_fraction"] = (
        counts["completed"] / counts["planned"] if counts["planned"] else None
    )
    return counts


def _usage_bucket():
    return {
        "calls": 0,
        "successful_calls": 0,
        "failed_calls": 0,
        "unknown_status_calls": 0,
        "input_tokens_measured": None,
        "output_tokens_measured": None,
        "missing_input_usage_calls": 0,
        "missing_output_usage_calls": 0,
        "elapsed_seconds": 0.0,
    }


def _add_usage(bucket, record):
    bucket["calls"] += 1
    success = record.get("success")
    key = (
        "successful_calls"
        if success is True
        else "failed_calls"
        if success is False
        else "unknown_status_calls"
    )
    bucket[key] += 1
    for direction in ("input", "output"):
        value = record.get(f"{direction}_tokens")
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            measured = f"{direction}_tokens_measured"
            bucket[measured] = value + (bucket[measured] or 0)
        else:
            bucket[f"missing_{direction}_usage_calls"] += 1
    seconds = record.get("elapsed_seconds")
    if isinstance(seconds, (int, float)) and not isinstance(seconds, bool):
        if isfinite(seconds) and seconds >= 0:
            bucket["elapsed_seconds"] += seconds


def _stage(record):
    if record.get("kind") == "embedding":
        return "embedding"
    operation = record.get("operation", "")
    if not isinstance(operation, str):
        return "memory"
    for label in ("answer", "judge"):
        if operation == label or operation.startswith(label + "."):
            return label
    return "memory"


def _model_usage(records: Iterable[Mapping], arm_names):
    total = _usage_bucket()
    by_arm = {
        name: {
            "name": name,
            "total": _usage_bucket(),
            "by_stage": {stage: _usage_bucket() for stage in _STAGES},
        }
        for name in arm_names
    }
    other_arms = {}
    configurations = {}
    metadata_requests = 0
    for record in records:
        if record.get("kind") not in (_CHAT_KINDS | {"embedding"}):
            metadata_requests += 1
            continue
        _add_usage(total, record)
        name = record.get("arm")
        if not isinstance(name, str) or not name:
            name = "unassigned"
        target = by_arm if name in by_arm else other_arms
        if name not in target:
            target[name] = {
                "name": name,
                "total": _usage_bucket(),
                "by_stage": {stage: _usage_bucket() for stage in _STAGES},
            }
        group = target[name]
        stage = _stage(record)
        _add_usage(group["total"], record)
        _add_usage(group["by_stage"][stage], record)
        settings = record.get("generation_settings") or {}
        if not isinstance(settings, Mapping):
            settings = {}
        key = json.dumps(
            [name, stage, record.get("kind"), record.get("model"), settings],
            sort_keys=True,
            ensure_ascii=False,
        )
        if key not in configurations:
            configurations[key] = {
                "arm": name,
                "stage": stage,
                "kind": record.get("kind"),
                "model": record.get("model"),
                "generation_settings": dict(settings),
                "calls": 0,
            }
        configurations[key]["calls"] += 1
    return {
        "total": total,
        "by_arm": list(by_arm.values()),
        "shared_or_unassigned": list(other_arms.values()),
        "metadata_requests_excluded_from_model_calls": metadata_requests,
        "actual_model_configurations": list(configurations.values()),
        "includes_failed_requests_and_retries": True,
        "cost_status": "not_priced",
    }


def _selection_metadata(manifest, counts, arms):
    names = [row["name"] for row in arms]
    if "arms" in manifest:
        declared = manifest["arms"]
        if not isinstance(declared, list) or declared != names or not declared:
            raise ValueError("manifest arms must exactly match the ordered task summary arms")
    planned = counts["planned"]
    if "planned_tasks" in manifest:
        if _nonnegative_int(manifest["planned_tasks"], "planned_tasks") != planned:
            raise ValueError("manifest planned_tasks must equal the fixed planned denominator")
    source_count = _nonnegative_int(
        manifest.get("source_question_count", 500), "source_question_count"
    )
    plans = {row["planned"] for row in arms}
    question_count = next(iter(plans)) if len(plans) == 1 else None
    if "question_count" in manifest:
        question_count = _nonnegative_int(manifest["question_count"], "question_count")
        if any(row["planned"] != question_count for row in arms):
            raise ValueError("manifest question_count must equal each arm's planned count")
        if question_count * len(names) != planned:
            raise ValueError("question_count times arms must equal the planned denominator")
    if question_count is not None and question_count > source_count:
        raise ValueError("question_count must not exceed source_question_count")
    selected_ids = manifest.get("selected_question_ids")
    if "selected_question_ids" in manifest:
        if (
            not isinstance(selected_ids, list)
            or any(not isinstance(value, str) or not value for value in selected_ids)
            or len(set(selected_ids)) != len(selected_ids)
        ):
            raise ValueError("selected_question_ids must be a list of distinct nonempty labels")
        if question_count is None or len(selected_ids) != question_count:
            raise ValueError("selected_question_ids must match question_count")
    scope = manifest.get("evaluation_scope", "full")
    if scope not in ("full", "selected_regression"):
        raise ValueError("evaluation_scope must be full or selected_regression")
    if "evaluation_scope" in manifest and scope == "full" and question_count != source_count:
        raise ValueError("full evaluation_scope requires all source questions")
    return {
        "evaluation_scope": scope,
        "source_question_count": source_count,
        "question_count": question_count,
        "selected_question_ids": deepcopy(selected_ids),
        "arms": names,
        "planned_tasks": planned,
    }


def build_report(manifest, task_summary, records, elapsed_seconds, status):
    """Build JSON-ready progress from an aggregate task snapshot and a record iterator.

    ``task_summary`` is exactly RunLedger.summary(): the count fields in _COUNTS,
    and ``by_arm`` with the same fields and ``name``. A record iterator may span
    every per-question model ledger; request IDs need not be globally unique.
    """
    if not isinstance(manifest, Mapping) or not isinstance(task_summary, Mapping):
        raise ValueError("manifest and task_summary must be mappings")
    if not isinstance(status, str) or not status:
        raise ValueError("status must be a nonempty label")
    if (
        not isinstance(elapsed_seconds, (int, float))
        or isinstance(elapsed_seconds, bool)
        or not isfinite(elapsed_seconds)
        or elapsed_seconds < 0
    ):
        raise ValueError("elapsed_seconds must be finite and nonnegative")
    counts = _task_counts(task_summary)
    arms = []
    for value in task_summary["by_arm"]:
        name = value["name"]
        if not isinstance(name, str) or not name or any(row["name"] == name for row in arms):
            raise ValueError("arm names must be distinct nonempty labels")
        arms.append({"name": name, **_task_counts(value)})
    if any(sum(row[key] for row in arms) != counts[key] for key in _COUNTS):
        raise ValueError("per-arm counts must equal the task summary")
    selection = _selection_metadata(manifest, counts, arms)
    is_complete = status in _COMPLETE_STATUSES and not counts["pending"] and not counts["running"]
    effective_status = status
    if status in _COMPLETE_STATUSES and not is_complete:
        effective_status = "incomplete"
    elif is_complete and counts["failed"]:
        effective_status = "completed_with_failures"
    return {
        "schema_version": 1,
        "run_kind": "longmemeval-s-local-model-comparison",
        "manifest": deepcopy(dict(manifest)),
        **selection,
        "status": effective_status,
        "requested_status": status,
        "is_complete": bool(is_complete),
        "elapsed_seconds": float(elapsed_seconds),
        "task_summary": {
            **counts,
            "by_arm": arms,
            "protocol_hash": task_summary.get("protocol_hash"),
            "max_attempts": task_summary.get("max_attempts"),
        },
        "model_usage": _model_usage(records, [row["name"] for row in arms]),
        "limitations": (
            [
                f"This selected regression covers {selection['question_count']} questions from "
                f"a {selection['source_question_count']}-question source "
                "and only its selected arms; "
                "its accuracy is not a full-dataset or all-framework benchmark score."
            ]
            if selection["evaluation_scope"] == "selected_regression"
            else []
        )
        + [
            "Correct / planned is a progress coverage metric until the run finishes; "
            "pending, failed, and completed-but-unscored tasks remain in its denominator.",
            "Scored-only accuracy has a different denominator and does not replace "
            "planned accuracy.",
            "The local Qwen judge is not an official LongMemEval judge, independent "
            "human review, or an independent paid judge; its labels can be wrong.",
            "Native memory prompts, retained representations, and generation limits differ; "
            "the shared answering protocol and actual configurations must be disclosed.",
            "All recorded model attempts, including retries and failures, count toward usage. "
            "Missing token usage remains unknown; measured subtotals are not complete totals.",
            "No monetary cost or energy cost has been measured.",
            "The earlier small synthetic pilot and assistant semantic review are separate "
            "experiments and are not included in these benchmark scores.",
        ],
    }


def _cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def _percentage(value):
    return f"{value:.2%}" if value is not None else "未知"


def _usage_value(bucket, direction):
    value = bucket[f"{direction}_tokens_measured"]
    missing = bucket[f"missing_{direction}_usage_calls"]
    if value is None:
        return "未知"
    return f"{value}（另有 {missing} 次未知）" if missing else str(value)


def render_markdown(report):
    """Render a progress or final report without making completion or cost claims."""
    counts = report["task_summary"]
    usage = report["model_usage"]
    title = "最终结果" if report["is_complete"] else "进度快照（尚未完成）"
    source_count = report.get("source_question_count", 500)
    question_count = report.get("question_count")
    names = report.get("arms", [row["name"] for row in counts["by_arm"]])
    scope = report.get("evaluation_scope", "full")
    selection_lines = [
        f"来源数据共 {source_count} 道题；本次计划 "
        f"{question_count if question_count is not None else '未知'} 道题 × "
        f"{len(names)} 个框架 = {counts['planned']} 项，固定分母以任务账本的计划数为准。",
        "所选框架：" + "、".join(f"`{_cell(name)}`" for name in names) + "。",
    ]
    if scope == "selected_regression":
        selection_lines.insert(
            0,
            f"评价范围：回归子集（`selected_regression`）。本次结果不是完整 "
            f"{source_count} 道题的准确率，也不代表所有框架的完整对比。",
        )
    lines = [
        f"# LongMemEval-S 本地统一对比：{title}",
        "",
        f"状态：`{_cell(report['status'])}`。计划 {counts['planned']} 个框架×问题任务；"
        f"已启动 {counts['started']}，完成 {counts['completed']}，已评分 {counts['scored']}，"
        f"正确 {counts['correct']}，失败 {counts['failed']}，待运行 {counts['pending']}，"
        f"运行中 {counts['running']}。累计尝试 {counts['attempts']} 次。",
        f"运行耗时：{report['elapsed_seconds']:.1f} 秒。",
        "",
        *selection_lines,
        "",
        "| 框架 | 计划 | 已启动 | 完成 | 已评分 | 正确 | 失败 | 待运行 | 运行中 "
        "| 正确/计划 | 正确/已评分 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm in counts["by_arm"]:
        values = [
            arm["name"],
            *(arm[key] for key in _COUNTS[:8]),
            _percentage(arm["accuracy_all_planned"]),
            _percentage(arm["accuracy_scored_only"]),
        ]
        lines.append("| " + " | ".join(_cell(value) for value in values) + " |")
    lines.extend(
        [
            "",
            f"总体正确/计划：{counts['correct']}/{counts['planned']} "
            f"（{_percentage(counts['accuracy_all_planned'])}）。完成但未评分 "
            f"{counts['unscored_completed']} 项仍保留在计划分母。",
            "进行中的正确/计划是进度覆盖指标，不能当作最终成绩；正确/已评分使用较小分母。",
            "",
            "| 框架 | 阶段 | 模型调用 | 失败调用 | 已测输入 token | 已测输出 token |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for arm in (*usage["by_arm"], *usage["shared_or_unassigned"]):
        for stage, bucket in arm["by_stage"].items():
            lines.append(
                "| "
                + " | ".join(
                    _cell(value)
                    for value in (
                        arm["name"],
                        stage,
                        bucket["calls"],
                        bucket["failed_calls"],
                        _usage_value(bucket, "input"),
                        _usage_value(bucket, "output"),
                    )
                )
                + " |"
            )
    lines.extend(
        [
            "",
            f"模型调用合计 {usage['total']['calls']} 次，包含失败和重试；"
            f"另有 {usage['metadata_requests_excluded_from_model_calls']} 次服务元数据请求，"
            "未计为模型调用。未测量美元成本或能耗；未知 token 没有补零。",
            "",
            "协议与环境清单（含数据 SHA、SDK 版本、模型与答题配置，以实际清单为准）：",
            "",
            "```json",
            json.dumps(report["manifest"], ensure_ascii=False, indent=2),
            "```",
            "",
            "实际模型配置及对应调用次数：",
            "",
            "```json",
            json.dumps(usage["actual_model_configurations"], ensure_ascii=False, indent=2),
            "```",
            "",
        ]
    )
    lines.extend("- " + item for item in report["limitations"])
    return "\n".join(lines) + "\n"
