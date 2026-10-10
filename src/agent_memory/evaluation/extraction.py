"""Replay authored extraction cases; report useful recall alongside false accepts.

Use a disposable initialized repository. Each case gets an isolated namespace so
state from one case cannot turn another case into a conflict. Candidate overrides
are explicit adversarial fixtures, not output from the measured generator.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from time import perf_counter
from typing import Any
from uuid import uuid4

from ..consolidation.admission import AdmissionPolicy
from ..consolidation.atom_extraction import AtomExtractionPipeline
from ..domain import MemoryEvent, MemoryScope, SourceAuthority, canonical_json
from ..ports import AdmissionRepository


class _FixtureGenerator:
    version = "adversarial-fixture-v1"

    def __init__(self, proposals):
        self.proposals = proposals

    async def generate_atoms(self, event):
        return self.proposals


def _identity(value):
    return canonical_json({name: value[name] for name in ("subject_id", "predicate", "value")})


async def evaluate_extraction(
    repository: AdmissionRepository,
    cases: Sequence[Mapping[str, Any]],
    *,
    pipeline: AtomExtractionPipeline,
    scope: MemoryScope,
    authority: SourceAuthority,
    policy: AdmissionPolicy,
) -> dict[str, Any]:
    """Evaluate per-case admitted semantic triples, including known rule gaps.

    Expected atoms are authored labels, not reviewer output. Empty predictions
    yield undefined precision and zero recall when useful expected atoms exist.
    """
    counts = dict(
        true_positives=0,
        false_positives=0,
        false_negatives=0,
        negative_cases=0,
        false_accept_cases=0,
        adversarial_cases=0,
        adversarial_rejected=0,
        failed_cases=0,
        generation_calls=0,
        review_calls=0,
        source_audit_calls=0,
        source_audit_missing_candidate_findings=0,
        source_audit_unresolved_scopes=0,
    )
    results, latencies = [], []
    for case in cases:
        expected = {_identity(item) for item in case["expected"]}
        selected = pipeline
        if "proposals" in case:
            selected = AtomExtractionPipeline(
                _FixtureGenerator(case["proposals"]),
                pipeline.reviewer,
                scope_level=pipeline.scope_level,
                max_candidates=pipeline.max_candidates,
                timeout_seconds=pipeline.timeout_seconds,
                source_audit=pipeline.source_audit,
            )
        event = MemoryEvent(
            replace(scope, namespace=f"extraction-eval-{uuid4().hex}"),
            "evaluation",
            case["content"],
        )
        started = perf_counter()
        receipt = await selected.process(repository, event, authority=authority, policy=policy)
        latencies.append((perf_counter() - started) * 1000)
        actual = set()
        for decision in receipt.admission.decisions:
            if decision.action == "ACCEPT":
                row = await repository.admission_record(event.scope, decision.candidate_id)
                if row is None:
                    raise RuntimeError("evaluation candidate disappeared")
                actual.add(_identity(row["payload"]["draft"]))
        counts["true_positives"] += len(expected & actual)
        counts["false_positives"] += len(actual - expected)
        counts["false_negatives"] += len(expected - actual)
        if not expected:
            counts["negative_cases"] += 1
            counts["false_accept_cases"] += bool(actual)
        if case.get("must_reject", False):
            counts["adversarial_cases"] += 1
            counts["adversarial_rejected"] += bool(receipt.decisions) and all(
                item.action == "REJECT" for item in receipt.decisions
            )
        counts["failed_cases"] += receipt.processing_state == "failed"
        counts["generation_calls"] += receipt.generation_calls
        counts["review_calls"] += receipt.review_calls
        coverage = receipt.source_audit or {}
        counts["source_audit_calls"] += coverage.get("calls", 0)
        counts["source_audit_missing_candidate_findings"] += sum(
            observation["finding_status"] == "missing_candidate"
            for observation in coverage.get("observations", ())
        )
        counts["source_audit_unresolved_scopes"] += sum(
            observation["status"] == "unresolved"
            for observation in coverage.get("observations", ())
        )
        results.append(
            {
                "id": case["id"],
                "expected": len(expected),
                "accepted": len(actual),
                "missed": len(expected - actual),
                "false_accepts": len(actual - expected),
                "actions": [item.action for item in receipt.decisions],
            }
        )

    def ratio(numerator, denominator):
        return numerator / denominator if denominator else None

    tp, fp, fn = (counts[key] for key in ("true_positives", "false_positives", "false_negatives"))
    return {
        "case_count": len(cases),
        **counts,
        "precision": ratio(tp, tp + fp),
        "recall": ratio(tp, tp + fn),
        "negative_case_false_accept_rate": ratio(
            counts["false_accept_cases"], counts["negative_cases"]
        ),
        "adversarial_rejection_rate": ratio(
            counts["adversarial_rejected"], counts["adversarial_cases"]
        ),
        "mean_latency_ms": sum(latencies) / len(latencies) if latencies else None,
        "max_latency_ms": max(latencies) if latencies else None,
        "token_usage": None,
        "monetary_cost": None,
        "cases": results,
    }
