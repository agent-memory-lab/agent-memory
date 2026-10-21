"""Frozen-evidence QA regression; prepare by default, paid calls only with --execute.

Never re-extracts or re-ranks. Generation reads only copied blind queries and
delivered evidence. Gold is opened only after every generated answer is frozen.
An external, hash-bound durable transport owns cumulative budget enforcement;
this runner cannot clear, increase, or reset its cap. No automatic retry/resume.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import re
import shutil
import time
from pathlib import Path

from examples.memory_evidence_reader_v4 import manifest as reader_manifest
from examples.memory_evidence_reader_v4 import read_answer, validate_candidate
from experiments.public_longmemeval.transport import atomic_write_json


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _contained(root, name):
    path = (root / name).resolve(strict=True)
    if not path.is_relative_to(root.resolve()):
        raise ValueError("input_path_outside_source_root")
    return path


def prepare(
    source,
    output,
    *,
    transport_path,
    prior_ledger,
    rubric_path,
    selected_case_ids=None,
    judge_headroom_cny=0.20,
):
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists():
        raise ValueError("new_output_directory_required")
    summary_path = source / "ALL_RESULTS.json"
    summary = read(summary_path)
    order = read(source / "prepared/blind-manifest.json")["case_ids"]
    if selected_case_ids is not None:
        selected_case_ids = tuple(selected_case_ids)
        if len(set(selected_case_ids)) != len(selected_case_ids) or not set(
            selected_case_ids
        ) <= set(order):
            raise ValueError("invalid_selected_cases")
        order = [cid for cid in order if cid in selected_case_ids]
    if type(judge_headroom_cny) not in (int, float) or not 0.05 <= judge_headroom_cny <= 1:
        raise ValueError("invalid_judge_headroom")
    requests, bindings = [], {}
    for cid in order:
        if cid not in summary["row_origins"]:
            continue
        result_path = _contained(source, summary["row_origins"][cid])
        saved = read(result_path)
        if not saved.get("delivered") or not saved.get("reader_raw"):
            continue  # Provider-rejected extraction is not retried or rewritten.
        query_path = _contained(source, "prepared/blind/" + cid + "/query.json")
        query = read(query_path)
        if set(query) != {"case_id", "question", "question_date"} or query["case_id"] != cid:
            raise ValueError("blind_query_contract_changed")
        requests.append({"query": query, "records": saved["delivered"]})
        bindings[cid] = {
            "source_result": str(result_path),
            "source_result_sha256": sha(result_path),
            "query": str(query_path),
            "query_sha256": sha(query_path),
        }
    if not requests:
        raise ValueError("no_replayable_delivered_evidence")
    output.mkdir(parents=True)
    (output / "requests").mkdir()
    (output / "runtime").mkdir()
    for request in requests:
        atomic_write_json(output / "requests" / (request["query"]["case_id"] + ".json"), request)
    runtime_files = [
        Path(__file__),
        Path(__file__).parents[2] / "examples/memory_evidence_reader_v4.py",
        Path(__file__).parents[2] / "examples/memory_evidence_reader.py",
        Path(__file__).parents[2] / "src/agent_memory/retrieval/answer_computation.py",
        Path(__file__).parents[2] / "src/agent_memory/retrieval/temporal_precision.py",
        Path(__file__).parents[2] / "src/agent_memory/evaluation/public_qa_acceptance.py",
        Path(__file__).with_name("provider_contract.py"),
        Path(transport_path),
        Path(rubric_path),
    ]
    runtime = {}
    for path in runtime_files:
        target = output / "runtime" / path.name
        if target.exists():
            raise ValueError("runtime_snapshot_name_collision")
        shutil.copyfile(path, target)
        runtime[str(path.resolve())] = sha(path)
    gold = source / "prepared/evaluator-only/gold.jsonl"
    plan = {
        "schema": "frozen-evidence-qa-replay/1",
        "reader": reader_manifest(),
        "source_summary": str(summary_path),
        "source_summary_sha256": sha(summary_path),
        "source_bindings": bindings,
        "runtime": runtime,
        "requests": {p.name: sha(p) for p in sorted((output / "requests").glob("*.json"))},
        "case_ids": [r["query"]["case_id"] for r in requests],
        "gold": str(gold),
        "gold_sha256": sha(gold),
        "gold_access": "after_answers_frozen_only",
        "transport_path": str(Path(transport_path).resolve()),
        "transport_sha256": sha(transport_path),
        "prior_ledger": str(Path(prior_ledger).resolve()),
        "prior_ledger_sha256": sha(prior_ledger),
        "rubric_path": str(Path(rubric_path).resolve()),
        "rubric_sha256": sha(rubric_path),
        "selection": (
            "explicit development diagnostic subset in original fixed order"
            if selected_case_ids is not None
            else "all completed or reader-invalid cases with delivered evidence "
            "in original fixed order"
        ),
        "judge_headroom_cny": judge_headroom_cny,
        "exposure": "development_regression_not_holdout",
        "cost_scope": "reader_and_support_check_and_independent_judge_only",
        "extraction_and_ranking": "reused_frozen_not_reexecuted_not_free_end_to_end",
        "automatic_retries": 0,
        "full_dataset_accuracy": None,
    }
    atomic_write_json(output / "manifest.json", plan)
    return plan


def verify(output, plan, *, check_working_runtime=True):
    for path, expected in plan["runtime"].items():
        if sha(output / "runtime" / Path(path).name) != expected:
            raise ValueError("runtime_snapshot_changed")
        if check_working_runtime and sha(path) != expected:
            raise ValueError("runtime_changed_since_freeze")
    for name, expected in plan["requests"].items():
        if sha(output / "requests" / name) != expected:
            raise ValueError("blind_request_changed")
    if sha(plan["prior_ledger"]) != plan["prior_ledger_sha256"]:
        raise ValueError("prior_ledger_changed")
    if sha(plan["source_summary"]) != plan["source_summary_sha256"]:
        raise ValueError("source_summary_changed")
    for item in plan["source_bindings"].values():
        if (
            sha(item["source_result"]) != item["source_result_sha256"]
            or sha(item["query"]) != item["query_sha256"]
        ):
            raise ValueError("source_evidence_changed")


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def replay_candidates(source, output):
    """Offline contract replay, with no generation, gold read, or new accuracy."""
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists() or output.is_relative_to(source):
        raise ValueError("new_separate_output_required")
    plan = read(source / "manifest.json")
    verify(source, plan, check_working_runtime=False)
    freeze = read(source / "answers-frozen.json")
    rows = []
    for name, expected in freeze["files"].items():
        path = _contained(source, "answers/" + name)
        if sha(path) != expected:
            raise ValueError("frozen_answer_changed")
        saved = read(path)
        old = saved.get("result", {}).get("candidate")
        if type(old) is not dict or type(old.get("raw_response")) is not str:
            continue
        if saved["case_id"] not in plan["case_ids"] or name != saved["case_id"] + ".json":
            raise ValueError("frozen_case_identity_mismatch")
        request = read(source / "requests" / name)
        candidate = validate_candidate(old["raw_response"], request["records"])
        rows.append(
            {
                "case_id": saved["case_id"],
                "raw_response_sha256": hashlib.sha256(old["raw_response"].encode()).hexdigest(),
                "before_candidate_status": old["status"],
                "after_candidate_status": candidate["status"],
                "before_candidate_answer": old.get("answer"),
                "after_candidate_answer": candidate["answer"],
                "candidate_answer_changed": old.get("answer") != candidate["answer"],
                "errors": candidate["errors"],
                "temporal_interpretation": candidate.get("temporal_interpretation"),
            }
        )
    result = {
        "schema": "offline-qa-contract-replay/1",
        "source_manifest_sha256": sha(source / "manifest.json"),
        "source_answer_freeze_sha256": sha(source / "answers-frozen.json"),
        "policy": reader_manifest(),
        "cases": rows,
        "paid_calls": 0,
        "gold_opened": False,
        "new_accuracy": None,
        "limit": "Candidate contract only; "
        "the old model audit and independent judgment were not rerun.",
    }
    output.mkdir(parents=True)
    atomic_write_json(output / "candidate-replay.json", result)
    return result


class RecordedClient:
    def __init__(self, client, output, case_id):
        self.client, self.output, self.case_id = client, output, case_id

    async def chat(self, prompt, phase, *, format, limit):
        purpose = phase.rsplit(":", 1)[-1]
        path = self.output / "calls" / self.case_id / (purpose + ".json")
        if path.exists():
            raise ValueError("attempt_already_exists_no_automatic_retry")
        row = {
            "purpose": purpose,
            "prompt": prompt,
            "schema": format,
            "limit": limit,
            "status": "started",
        }
        atomic_write_json(path, row)
        started = time.monotonic()
        try:
            raw, receipt = await self.client.chat(
                [{"role": "user", "content": prompt}],
                phase="answer",
                case_id=self.case_id,
                max_tokens=limit,
                schema=format,
            )
        except Exception as error:
            row.update(
                status="failed",
                error_type=type(error).__name__,
                receipt=getattr(error, "metadata", None),
            )
            atomic_write_json(path, row)
            raise
        row.update(
            status="completed",
            raw_response=raw,
            receipt=receipt,
            seconds=time.monotonic() - started,
        )
        atomic_write_json(path, row)
        return raw


async def execute(output, *, credential_pointer):
    output = Path(output).resolve()
    plan = read(output / "manifest.json")
    verify(output, plan)
    if (output / "api-ledger.sqlite3").exists():
        raise ValueError("existing_ledger_requires_explicit_continuation_review")
    helper = load_module(plan["transport_path"], "_qa_replay_transport")
    pointer = Path(credential_pointer)
    credential_path = Path(pointer.read_text(encoding="utf-8").strip())
    if credential_path.stat().st_mode & 0o077:
        raise ValueError("credential_permissions_not_private")
    credential = credential_path.read_text(encoding="utf-8").strip()
    client = helper.BudgetedClient(
        key=credential,
        ledger_path=output / "api-ledger.sqlite3",
        prior_ledger_path=plan["prior_ledger"],
        max_concurrency=1,
        provider_contract_path=output / "runtime/provider_contract.py",
    )
    credential = None
    rows, stop_reason = [], None
    try:
        for cid in plan["case_ids"]:
            verify(output, plan)
            # Keep conservative headroom for independent judging. The durable
            # client still admits each real payload against the exact cap.
            if float(client.summary()["remaining_cny"]) < plan.get("judge_headroom_cny", 0.20):
                stop_reason = "judge_headroom"
                break
            request = read(output / "requests" / (cid + ".json"))
            query = request["query"]
            try:
                result = await read_answer(
                    RecordedClient(client, output, cid),
                    question=query["question"],
                    question_date=query["question_date"],
                    records=request["records"],
                    phase="answer:" + cid,
                )
                row = {"case_id": cid, "status": result["status"], "result": result}
            except Exception as error:
                stop_reason = type(error).__name__
                row = {
                    "case_id": cid,
                    "status": "transport_or_budget_stopped",
                    "error_type": stop_reason,
                }
            atomic_write_json(output / "answers" / (cid + ".json"), row)
            rows.append(row)
            if stop_reason:
                break
        answer_files = {p.name: sha(p) for p in sorted((output / "answers").glob("*.json"))}
        atomic_write_json(
            output / "answers-frozen.json", {"files": answer_files, "generation_stop": stop_reason}
        )
        # Release no gold before the entire bounded generation stage is closed.
        judgments = []
        if sha(plan["gold"]) != plan["gold_sha256"]:
            raise ValueError("gold_changed")
        gold = {
            r["case_id"]: r
            for r in (json.loads(line) for line in Path(plan["gold"]).read_text(encoding="utf-8").splitlines())
        }
        import ast

        tree = ast.parse(Path(plan["rubric_path"]).read_text(encoding="utf-8"))
        function = next(
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "get_anscheck_prompt"
        )
        namespace = {}
        exec(
            compile(ast.Module(body=[function], type_ignores=[]), "pinned-official-rubric", "exec"),
            namespace,
        )
        for row in rows:
            cid = row["case_id"]
            if row["status"] not in ("supported", "unsupported"):
                continue
            if sha(output / "answers" / (cid + ".json")) != answer_files[cid + ".json"]:
                raise ValueError("answer_changed_after_freeze")
            item = gold[cid]
            query = read(output / "requests" / (cid + ".json"))["query"]
            prompt = namespace["get_anscheck_prompt"](
                item["question_type"],
                query["question"],
                item["answer"],
                row["result"]["answer"],
                False,
            )
            attempt = {"case_id": cid, "prompt": prompt, "status": "started"}
            path = output / "judgments" / (cid + ".json")
            atomic_write_json(path, attempt)
            try:
                raw, receipt = await client.chat(
                    [{"role": "user", "content": prompt}], phase="judge", case_id=cid, max_tokens=16
                )
                match = re.fullmatch(r"\s*(yes|no)[.!]?\s*", raw, re.I)
                attempt.update(
                    raw=raw,
                    receipt=receipt,
                    status="completed" if match else "invalid",
                    correct=(match.group(1).lower() == "yes") if match else None,
                )
            except Exception as error:
                attempt.update(
                    status="failed",
                    error_type=type(error).__name__,
                    receipt=getattr(error, "metadata", None),
                )
                stop_reason = type(error).__name__
            atomic_write_json(path, attempt)
            judgments.append(attempt)
            if attempt["status"] == "failed":
                break
        atomic_write_json(
            output / "completion.json",
            {
                "answers": len(rows),
                "judged": sum(j["status"] == "completed" for j in judgments),
                "correct": sum(j.get("correct") is True for j in judgments),
                "stop_reason": stop_reason,
                "budget": client.summary(),
                "full_dataset_completed": False,
                "gold_after_answer_freeze": True,
            },
        )
    finally:
        try:
            atomic_write_json(output / "budget-final.json", client.summary())
        finally:
            await client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--transport", type=Path)
    parser.add_argument("--prior-ledger", type=Path)
    parser.add_argument("--rubric", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Replay frozen candidates without any model or gold access",
    )
    parser.add_argument(
        "--cases",
        nargs="+",
        help="Explicit development diagnostic subset; never an accuracy holdout",
    )
    parser.add_argument("--judge-headroom-cny", type=float, default=0.20)
    parser.add_argument("--credential-pointer", type=Path)
    args = parser.parse_args()
    if args.execute and args.offline:
        parser.error("--execute and --offline are mutually exclusive")
    if args.offline:
        if args.source is None:
            parser.error("--offline requires --source")
        result = replay_candidates(args.source, args.output)
        print(json.dumps({"cases": len(result["cases"]), "paid_calls": 0, "new_accuracy": None}))
    elif args.execute:
        if args.credential_pointer is None:
            parser.error("--credential-pointer is required for an explicitly requested execution")
        asyncio.run(execute(args.output, credential_pointer=args.credential_pointer))
    else:
        if any(
            value is None for value in (args.source, args.transport, args.prior_ledger, args.rubric)
        ):
            parser.error("prepare requires --source --transport --prior-ledger --rubric")
        plan = prepare(
            args.source,
            args.output,
            transport_path=args.transport,
            prior_ledger=args.prior_ledger,
            rubric_path=args.rubric,
            selected_case_ids=args.cases,
            judge_headroom_cny=args.judge_headroom_cny,
        )
        print(
            json.dumps(
                {"cases": len(plan["case_ids"]), "paid_calls": 0, "output": str(args.output)},
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
