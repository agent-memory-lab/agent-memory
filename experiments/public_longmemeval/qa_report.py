"""Read-only acceptance projection for a closed frozen-evidence QA replay."""

import json
import math
import re
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path

from agent_memory.evaluation.public_qa_acceptance import summarize_public_qa
from agent_memory.evaluation.question_cost import CostEntry, CostPhase, CostSnapshot, TokenBasis
from experiments.public_longmemeval.replay_qa import read, sha, verify
from experiments.public_longmemeval.transport import atomic_write_json


def nanos_to_microunits(value):
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError("invalid_nano_cny")
    return (value + 999) // 1000


def report(output, *, destination=None):
    output = Path(output).resolve()
    destination = Path(destination).resolve() if destination is not None else output
    plan = read(output / "manifest.json")
    verify(output, plan, check_working_runtime=False)
    completion = read(output / "completion.json")
    freeze = read(output / "answers-frozen.json")
    for name, expected in freeze["files"].items():
        if sha(output / "answers" / name) != expected:
            raise ValueError("frozen_answer_changed")
    for suffix in ("-wal", "-journal"):
        path = Path(str(output / "api-ledger.sqlite3") + suffix)
        if path.exists() and path.stat().st_size:
            raise ValueError("ledger_not_closed")
    with closing(
        sqlite3.connect((output / "api-ledger.sqlite3").as_uri() + "?mode=ro", uri=True)
    ) as connection:
        connection.row_factory = sqlite3.Row
        calls = [dict(r) for r in connection.execute("SELECT * FROM calls")]
    if any(row["status"] not in ("completed", "failed", "unknown") for row in calls):
        raise ValueError("unresolved_transport_dispatch")
    entries = []
    for row in calls:
        usage = (row["prompt_tokens"], row["completion_tokens"])
        if (usage[0] is None) != (usage[1] is None) or any(
            v is not None and (type(v) is not int or v < 0) for v in usage
        ):
            raise ValueError("invalid_ledger_token_usage")
        known = row["prompt_tokens"] is not None and row["completion_tokens"] is not None
        failed = row["status"] != "completed"
        entries.append(
            CostEntry(
                row["id"],
                CostPhase.FAILURE if failed else CostPhase.FOREGROUND,
                request_ids=(row["case_id"],),
                model_call=True,
                provider="dashscope-beijing",
                provider_request_id=row["id"],
                estimated_microunits=nanos_to_microunits(row["charged_nano_cny"])
                if known
                else None,
                reservation_microunits=nanos_to_microunits(row["reserved_nano_cny"]),
                tokens=row["prompt_tokens"] + row["completion_tokens"] if known else None,
                token_basis=TokenBasis.MEASURED if known else TokenBasis.UNKNOWN,
            )
        )
    source = Path(plan["source_summary"]).parent
    all_ids = read(source / "prepared/blind-manifest.json")["case_ids"]
    if sha(plan["gold"]) != plan["gold_sha256"]:
        raise ValueError("gold_changed")
    gold = {
        r["case_id"]: r
        for r in (json.loads(line) for line in Path(plan["gold"]).read_text(encoding="utf-8").splitlines())
    }
    outcomes, paired, new_only = [], [], []
    stages, latencies = Counter(), {}
    for cid in plan["case_ids"]:
        answer_path = output / "answers" / (cid + ".json")
        if not answer_path.exists():
            continue
        answer = read(answer_path)
        judgment_path = output / "judgments" / (cid + ".json")
        judgment = read(judgment_path) if judgment_path.exists() else {}
        correct = judgment.get("correct") if judgment.get("status") == "completed" else None
        if judgment.get("status") == "completed":
            raw = judgment.get("raw")
            match = re.fullmatch(r"\s*(yes|no)[.!]?\s*", raw, re.I) if type(raw) is str else None
            if (
                type(correct) is not bool
                or match is None
                or correct != (match.group(1).lower() == "yes")
            ):
                raise ValueError("judge_verdict_mismatch")
            if answer["status"] not in ("supported", "unsupported"):
                raise ValueError("invalid_reader_result_cannot_be_scored")
        status = (
            "completed"
            if correct is not None
            else "invalid"
            if answer["status"] == "invalid" or judgment.get("status") == "invalid"
            else ("budget_stopped" if answer.get("error_type") == "BudgetExceeded" else "invalid")
            if answer["status"] == "transport_or_budget_stopped"
            else "unjudged"
        )
        outcomes.append(
            {
                "case_id": cid,
                "status": status,
                "correct": correct,
                "answered": answer["status"] == "supported",
                "answerable": not gold[cid]["abstention"],
                "exposure": "development",
            }
        )
        old = read(plan["source_bindings"][cid]["source_result"])
        if correct is not None:
            if type(old.get("judge_correct")) is bool:
                paired.append(
                    {
                        "case_id": cid,
                        "before": old["judge_correct"],
                        "after": correct,
                        "before_status": old["status"],
                        "after_status": answer["status"],
                    }
                )
            else:
                new_only.append({"case_id": cid, "correct": correct, "old_status": old["status"]})
        for path in (output / "calls" / cid).glob("*.json"):
            row = read(path)
            if row["status"] == "completed":
                stages[row["purpose"]] += 1
                latencies.setdefault(row["purpose"], []).append(row["seconds"])
    snapshot = CostSnapshot("CNY", tuple(entries), (), (CostPhase.FOREGROUND, CostPhase.FAILURE))
    acceptance = summarize_public_qa(all_ids, outcomes, snapshot, local_resources_measured=False)
    if (
        any(type(completion.get(k)) is not int for k in ("answers", "judged", "correct"))
        or completion["answers"] != len(outcomes)
        or completion["judged"] != acceptance["scored"]
        or completion["correct"] != acceptance["correct"]
    ):
        raise ValueError("completion_summary_mismatch")
    performance = {}
    for name, values in latencies.items():
        values.sort()
        performance[name] = {
            "n": len(values),
            "p50_seconds": values[(len(values) - 1) // 2],
            "p95_seconds": values[math.ceil(len(values) * 0.95) - 1],
        }
    before, after = sum(r["before"] for r in paired), sum(r["after"] for r in paired)
    document = {
        "schema": "qa-reliability-replay-report/2",
        "acceptance": acceptance,
        "selected_regression_cases": len(plan["case_ids"]),
        "newly_judged": completion["judged"],
        "correct": completion["correct"],
        "paired": {
            "n": len(paired),
            "before_correct": before,
            "after_correct": after,
            "improved": sum(not r["before"] and r["after"] for r in paired),
            "regressed": sum(r["before"] and not r["after"] for r in paired),
            "cases": paired,
        },
        "newly_validated_previous_technical_failures": new_only,
        "budget": completion["budget"],
        "call_purposes": dict(stages),
        "performance": performance,
        "full_dataset_accuracy": None,
        "holdout_accuracy": None,
        "limits": [
            "Frozen delivered evidence only; retrieval recall and extraction unchanged.",
            "Development regression; no independent holdout, no significance claim.",
            "New spend covers reader, support audit and independent judging; "
            "inherited build costs are not zero.",
            "Model audit does not prove entailment; "
            "source authority and real business acceptance unchanged.",
            "Local CPU/I/O/storage tariffs and background costs are not fully measured.",
        ],
        "outcomes": outcomes,
    }
    atomic_write_json(destination / "REPORT_V3.json", document)
    budget = completion["budget"]
    scored = completion["judged"]
    text = f"""# 通用问答可靠性：冻结证据回归

新版已评分 **{completion["correct"]}/{scored}**；同题可比子集 **{before}/{len(paired)} → {after}/{len(paired)}**。
改善{document["paired"]["improved"]}题，退化{document["paired"]["regressed"]}题。属于开发回归，不能作为独立泛化成绩。
同时要求回答状态与gold可回答性一致时，有效正确 **{acceptance["effective_correct"]}/{scored}**。
评分器可能给包含正确信息的弃答说明打yes；不能据此把错误弃答计入可靠回答。

候选清单{len(plan["case_ids"])}题；新启动{len(outcomes)}题；完整{len(all_ids)}题准确率仍为未取得。
旧成绩、原数据库、原账本和协议均保留。生成未读取gold，所有回答冻结后才进行独立Qwen评分。

本轮新增费用与保留{budget["segment_cost_cny"]}元；累计{budget["combined_total_cny"]}元；余额{budget["remaining_cny"]}元。
费用为冻结费率估算和未知用量保留，并非服务商账单。
复用了原抽取和排序产物；这不代表原建库费用为零，也不能把本轮增量费用当作端到端费用。

## 验收结论

通用工程验收：**{"通过" if acceptance["accepted"] else "未通过完整验收"}**。
缺口：{", ".join(acceptance["acceptance_gaps"])}。
新版协议显式选择，默认仍保留原版。模型核验、有限证据覆盖与真实业务权威核验分别统计。
详细逐题比较、调用和性能数据见REPORT_V3.json。
"""  # noqa: E501
    (destination / "REPORT_V3.md").write_text(text, encoding="utf-8")
    return document


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--destination", type=Path)
    arguments = parser.parse_args()
    result = report(arguments.output, destination=arguments.destination)
    print(
        json.dumps(
            {
                "scored": result["newly_judged"],
                "correct": result["correct"],
                "paired_n": result["paired"]["n"],
                "before": result["paired"]["before_correct"],
                "after": result["paired"]["after_correct"],
                "accepted": result["acceptance"]["accepted"],
            },
            ensure_ascii=False,
        )
    )
