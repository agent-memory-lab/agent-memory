"""Precommitted paired cluster bootstrap for A9, without third-party dependencies.

Projects/sessions, never individual requests or repeated model draws, are the
resampling unit. Ratios use pooled numerators/denominators after resampling whole
groups. Shared setup/drain costs use the existing fixed allocation rule. Undefined
replicates are retained as a blocker, not discarded to manufacture an interval.
"""

import math
import random
from dataclasses import dataclass
from hashlib import sha256

from .evidence import _digest
from .question_cost import PairedComparisonEvidence, request_allocations, summarize_costs


@dataclass(frozen=True, slots=True)
class BootstrapProtocol:
    seed: int
    resamples: int
    confidence_level: float
    minimum_groups: int
    minimum_requests: int
    version: str = "paired-cluster-percentile/1"
    allocation_rule: str = "equal_triggering_requests_else_all_requests/1"
    failure_rule: str = "retain-all-requests-block-undefined-replicates/1"

    def __post_init__(self):
        for name in ("seed", "resamples", "minimum_groups", "minimum_requests"):
            value = getattr(self, name)
            if type(value) is not int or value < (0 if name == "seed" else 1):
                raise ValueError(f"invalid {name}")
        if self.minimum_groups > self.minimum_requests or self.resamples > 1_000_000:
            raise ValueError("invalid sample bounds")
        if type(self.confidence_level) not in (float, int) or not 0 < self.confidence_level < 1:
            raise ValueError("invalid confidence level")
        if (self.version, self.allocation_rule, self.failure_rule) != (
            "paired-cluster-percentile/1",
            "equal_triggering_requests_else_all_requests/1",
            "retain-all-requests-block-undefined-replicates/1",
        ):
            raise ValueError("unsupported statistics protocol")

    @property
    def fingerprint(self):
        return _digest(self)


def _interval(values, confidence):
    if not values or any(value is None for value in values):
        return None
    ordered = sorted(values)

    def quantile(p):
        position = (len(ordered) - 1) * p
        lo, hi = math.floor(position), math.ceil(position)
        return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)

    alpha = (1 - confidence) / 2
    return quantile(alpha), quantile(1 - alpha)


def paired_bootstrap(candidate, baseline, protocol, *, expected_protocol_sha256):
    """Return inspectable intervals and, only when defined, B0 gate evidence.

    The expected fingerprint must have been recorded before execution. This checks
    binding, not who approved the protocol, dataset, prices, or annotations.
    """
    if protocol.fingerprint != expected_protocol_sha256:
        raise ValueError("statistics protocol changed after freeze")
    if (
        candidate.workload != baseline.workload
        or candidate.costs.currency != baseline.costs.currency
    ):
        raise ValueError("incomparable workload or currency")
    if any(
        getattr(candidate, name) != getattr(baseline, name)
        for name in (
            "dataset_sha256",
            "dataset_kind",
            "dataset_license_reference",
            "judge_sha256",
            "semantic_model",
            "generation_model",
        )
    ):
        raise ValueError("incomparable dataset, judge or model configuration")

    def manifest(run):
        return tuple(
            (r.request_id, r.group_id, r.answerable, r.diagnostic_only) for r in run.requests
        )

    if manifest(candidate) != manifest(baseline):
        raise ValueError("incomparable judging manifest")
    groups = sorted({r.group_id for r in candidate.requests if not r.diagnostic_only})
    if {r.group_id for r in candidate.requests} != set(groups):
        raise ValueError("diagnostic-only groups require a frozen semantic group binding")
    reasons = []
    if len(groups) < protocol.minimum_groups:
        reasons.append("insufficient_groups")
    if len(candidate.requests) < protocol.minimum_requests:
        reasons.append("insufficient_requests")
    summaries = [summarize_costs(run) for run in (candidate, baseline)]
    known_cost = all(s.total_actual_microunits is not None for s in summaries)
    if not known_cost:
        reasons.append("unknown_or_pending_total_cost")

    def grouped(run):
        allocations = request_allocations(run)
        rows = {key: [0, 0, 0, 0.0] for key in groups}
        for r in run.requests:
            row = rows[r.group_id]
            row[0] += 1
            row[1] += not r.diagnostic_only and r.answerable
            row[2] += r.effective_answer
            row[3] += allocations[r.request_id]["known_microunits"]
        return rows

    paired = [grouped(run) for run in (candidate, baseline)]

    def difference(sample):
        values = []
        for rows in paired:
            requests, answerable, effective, cost = (
                sum(rows[key][i] for key in sample) for i in range(4)
            )
            values.append(
                (
                    effective / answerable if answerable else None,
                    cost / requests if known_cost and requests else None,
                    cost / effective if known_cost and effective else None,
                )
            )
        return tuple(
            left - right if left is not None and right is not None else None
            for left, right in zip(*values, strict=True)
        )

    rng = random.Random(protocol.seed)
    draws, draw_hash = [], sha256()
    for _ in range(protocol.resamples):
        indices = [rng.randrange(len(groups)) for _ in groups]
        draw_hash.update((_digest(indices) + "\n").encode())
        draws.append(difference([groups[index] for index in indices]))
    names = (
        "effective_rate",
        "cost_per_request_microunits",
        "cost_per_effective_answer_microunits",
    )
    metrics = {}
    point = difference(groups)
    for index, name in enumerate(names):
        values = [draw[index] for draw in draws]
        missing = sum(value is None for value in values)
        metrics[name] = dict(
            difference=point[index],
            interval=_interval(values, protocol.confidence_level),
            undefined_resamples=missing,
        )
        if missing:
            reasons.append(f"undefined_{name}_resamples")
    artifact = dict(
        schema="question-paired-bootstrap/1",
        protocol_sha256=protocol.fingerprint,
        candidate_report_sha256=candidate.fingerprint,
        baseline_report_sha256=baseline.fingerprint,
        group_count=len(groups),
        request_count=len(candidate.requests),
        resamples=protocol.resamples,
        confidence_level=protocol.confidence_level,
        metrics=metrics,
        reasons=reasons,
        # Bind the actual draw sequence without exposing private group identities.
        draws_sha256=draw_hash.hexdigest(),
    )
    evidence = None
    if not reasons:
        quality = metrics["effective_rate"]["interval"]
        costs = metrics["cost_per_effective_answer_microunits"]["interval"]
        evidence = PairedComparisonEvidence(
            candidate.fingerprint,
            baseline.fingerprint,
            protocol.fingerprint,
            "sha256:" + _digest(artifact),
            len(groups),
            protocol.confidence_level,
            *quality,
            *costs,
        )
    return artifact, evidence
