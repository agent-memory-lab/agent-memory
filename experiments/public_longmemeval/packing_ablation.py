"""Offline 2x2 packing/reader delivery audit; no model accuracy is measured.

Inputs contain blind queries and fixed, already ranked candidates. Requirements
are a separate evaluator file opened only after all four arms have been frozen.
This isolates packing and framing; it does not evaluate recall or reranking.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from time import perf_counter

from agent_memory.domain import (
    MemoryBundle, MemoryCapabilities, MemoryItem, MemoryKind, MemoryQuery, MemoryScope,
)
from agent_memory.retrieval.packing import pack_native_evidence
from agent_memory.retrieval.packing_policy import PACKING_VERSIONS, packing_contract
from .core import core_identity
from .reader_context import core_reader_context, reader_contract


def _digest(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def _read_rows(path):
    with Path(path).open(encoding="utf-8") as stream:
        rows = [json.loads(line) for line in stream if line.strip()]
    ids = [row["case_id"] for row in rows]
    if (not ids or any(not isinstance(value, str) or not value.strip() for value in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("case IDs must be unique and nonempty")
    return rows


def _write(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


def freeze(inputs: Path, output: Path, *, packing_versions=("v2", "v3")):
    packing_versions = tuple(packing_versions)
    if ("v2" not in packing_versions or len(packing_versions) < 2
            or len(set(packing_versions)) != len(packing_versions)
            or any(version not in PACKING_VERSIONS for version in packing_versions)):
        raise ValueError("distinct packing versions including baseline v2 are required")
    rows = _read_rows(inputs)
    allowed = {"case_id", "question", "candidates", "token_budget", "item_limit", "source_metadata"}
    if any(set(row) - allowed for row in rows):
        raise ValueError("unexpected blind input fields")
    input_digest = _digest(inputs)
    identity = core_identity()
    output.mkdir(parents=True, exist_ok=False)
    results = []
    for row in rows:
        query = MemoryQuery(
            MemoryScope("packing-ablation", session_id=row["case_id"]), row["question"],
            token_budget=row["token_budget"], limit=row["item_limit"], include_current_state=False,
        )
        candidates = tuple(MemoryItem(
            value["id"], MemoryKind(value["kind"]), value["text"], value["score"],
            datetime.fromisoformat(value["occurred_at"]), value["metadata"],
        ) for value in row["candidates"])
        source_map = {source: source for item in candidates
                      for source in item.metadata.get("source_event_ids", ())}
        arms = {}
        for packing_version in packing_versions:
            started = perf_counter()
            packed = pack_native_evidence(query, (), candidates, version=packing_version)
            seconds = perf_counter() - started
            bundle = MemoryBundle(
                (), tuple(v for v in packed.items if v.kind not in (MemoryKind.EPISODE, MemoryKind.PROCEDURE)),
                tuple(v for v in packed.items if v.kind == MemoryKind.EPISODE),
                tuple(v for v in packed.items if v.kind == MemoryKind.PROCEDURE),
                packed.citations, packed.tokens, packed.metadata, MemoryCapabilities(),
            )
            for reader_version in ("v2", "v3"):
                context = core_reader_context(
                    bundle, source_map, token_budget=query.token_budget, item_limit=query.limit,
                    version=reader_version, source_metadata=row.get("source_metadata"),
                )
                arms[f"packing-{packing_version}/reader-{reader_version}"] = {
                    "selected": list(context.delivered), "context": context.context,
                    "delivery": context.diagnostics, "packing_metadata": packed.metadata,
                    "packing_seconds": seconds,
                }
        results.append({"case_id": row["case_id"], "arms": arms})
    if _digest(inputs) != input_digest or core_identity() != identity:
        raise ValueError("inputs or runtime changed during freezing")
    _write(output / "arms.json", results)
    _write(output / "manifest.json", {
        "schema": "offline-packing-reader-ablation/1", "inputs_sha256": input_digest,
        "arms_sha256": _digest(output / "arms.json"), "core_identity": identity,
        "packing_contracts": [packing_contract(v) for v in packing_versions],
        "reader_contracts": [reader_contract(v) for v in ("v2", "v3")],
        "evaluator_requirements_opened": False, "model_calls": 0,
        "scope": "Fixed ranked candidates: packing and reader framing only; not answer accuracy",
    })


def evaluate(output: Path, requirements: Path, *, candidate_version="v3"):
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    if _digest(output / "arms.json") != manifest["arms_sha256"]:
        raise ValueError("frozen arms changed")
    versions = tuple(policy["version"] for policy in manifest["packing_contracts"])
    if candidate_version == "v2" or candidate_version not in versions or "v2" not in versions:
        raise ValueError("candidate and baseline must exist in the frozen manifest")
    # Deliberately load labels only after the frozen outputs have been verified.
    labels = {row["case_id"]: row for row in _read_rows(requirements)}
    results = json.loads((output / "arms.json").read_text(encoding="utf-8"))
    if {row["case_id"] for row in results} != set(labels):
        raise ValueError("requirements must cover every frozen case exactly")
    summary, cases, regressions, improvements = {}, [], [], []
    expected_arms = {f"packing-{p}/reader-{r}" for p in versions for r in ("v2", "v3")}
    for row in results:
        if set(row["arms"]) != expected_arms:
            raise ValueError("all manifest-declared frozen arms are mandatory")
        label = labels[row["case_id"]]
        facts = label["required_evidence"]
        if not facts or any(not fact["text"] or not fact["memory_id"] for fact in facts):
            raise ValueError("nonempty source-bound requirements are mandatory")
        case = {"case_id": row["case_id"], "category": label["category"], "arms": {}}
        for name, arm in row["arms"].items():
            found = [any(value["id"] == fact["memory_id"] and fact["text"] in value["text"]
                         for value in arm["selected"]) for fact in facts]
            case["arms"][name] = {"all_required_evidence_delivered": all(found),
                                   "missing": [fact for fact, yes in zip(facts, found) if not yes]}
            stats = summary.setdefault(name, {"cases": 0, "complete_cases": 0,
                "required_facts": 0, "delivered_facts": 0, "context_estimated_tokens": 0,
                "categories": {}})
            stats["cases"] += 1
            stats["complete_cases"] += int(all(found))
            stats["required_facts"] += len(facts)
            stats["delivered_facts"] += sum(found)
            stats["context_estimated_tokens"] += arm["delivery"]["serialized_context_estimated_tokens"]
            group = stats["categories"].setdefault(label["category"], {"cases": 0, "complete_cases": 0})
            group["cases"] += 1
            group["complete_cases"] += int(all(found))
        old_missing = case["arms"]["packing-v2/reader-v2"]["missing"]
        new_missing = case["arms"][f"packing-{candidate_version}/reader-v2"]["missing"]
        lost = [fact for fact in new_missing if fact not in old_missing]
        gained = [fact for fact in old_missing if fact not in new_missing]
        if lost:
            regressions.append({"case_id": row["case_id"], "category": label["category"], "newly_missing": lost})
        if gained:
            improvements.append({"case_id": row["case_id"], "category": label["category"], "newly_delivered": gained})
        cases.append(case)
    report = {"schema": "offline-packing-reader-delivery-evaluation/1", "summary": summary,
        "cases": cases, "requirements_sha256": _digest(requirements),
        "manifest_sha256": _digest(output / "manifest.json"), "answer_accuracy": None,
        "model_calls": 0, "comparison": {"candidate_version": candidate_version,
            "regressions": regressions, "improvements": improvements,
            "no_new_evidence_loss_gate": "rejected" if regressions else "passed",
            "production_readiness_established": False},
        "limit": "Exact required-text delivery on supplied candidates; not recall, entailment, or benchmark accuracy"}
    _write(output / "evaluation.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("freeze")
    run.add_argument("--inputs", required=True, type=Path)
    run.add_argument("--output", required=True, type=Path)
    run.add_argument("--packing-versions", nargs="+", choices=PACKING_VERSIONS, default=("v2", "v3"))
    score = commands.add_parser("evaluate")
    score.add_argument("--requirements", required=True, type=Path)
    score.add_argument("--output", required=True, type=Path)
    score.add_argument("--candidate-version", choices=PACKING_VERSIONS, default="v3")
    score.add_argument("--require-no-regression", action="store_true",
                       help="Preserve the report and exit 2 if experimental packing loses required evidence")
    args = parser.parse_args()
    if args.command == "freeze":
        freeze(args.inputs, args.output, packing_versions=args.packing_versions)
    else:
        report = evaluate(args.output, args.requirements, candidate_version=args.candidate_version)
        print(json.dumps({"summary": report["summary"], "comparison": report["comparison"]}, ensure_ascii=False, indent=2))
        if args.require_no_regression and report["comparison"]["regressions"]:
            raise SystemExit(2)


if __name__ == "__main__":
    main()
