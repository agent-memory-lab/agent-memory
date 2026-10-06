"""AM61: sufficient support, temporal eligibility and frozen quality gates."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from agent_memory.domain import MemoryScope
from agent_memory.evaluation.acceptance import (
    AcceptanceProfile,
    QualityThresholds,
    evaluate_quality_gate,
    evidence_metrics,
)
from agent_memory.evaluation.evidence import (
    EvidenceDataset,
    EvidenceEvalCase,
    EvidenceObservation,
    EvidenceSpan,
    SupportAlternative,
    evaluate_evidence,
    evidence_dataset_from_dict,
    score_evidence_case,
)
from agent_memory.serialization import to_jsonable

WHEN = datetime(2026, 10, 6, tzinfo=UTC)
SCOPE = MemoryScope("test", user_id="alice")
CONFIG = "a" * 64


def span(name, **changes):
    value = EvidenceSpan(
        name,
        f"source:{name}",
        f"family:{name}",
        SCOPE,
        0,
        len(name),
        sha256(name.encode()).hexdigest(),
        ("value",),
        WHEN - timedelta(days=1),
        WHEN - timedelta(days=1),
    )
    return replace(value, **changes)


def case(**changes):
    value = EvidenceEvalCase(
        "case-1",
        SCOPE,
        "What is the value?",
        WHEN,
        WHEN,
        True,
        (span("A"), span("B"), span("C"), span("unrelated")),
        (SupportAlternative(("A", "B"), 2), SupportAlternative(("C",))),
        ("value",),
    )
    return replace(value, **changes)


@pytest.mark.parametrize(
    "ids,complete,recall",
    [
        (("C",), True, 1),
        (("A", "B"), True, 1),
        (("A",), False, 0.5),
        (("B",), False, 0.5),
        (("unrelated",), False, 0),
        ((), False, 0),
    ],
)
def test_and_or_support_is_not_flattened(ids, complete, recall):
    result = score_evidence_case(case(), EvidenceObservation(ids, "answered", True))
    assert result.support_complete is complete
    assert result.evidence_recall == recall
    assert (result.outcome == "correct_answer") is complete


def test_copied_sources_cannot_satisfy_independence():
    value = case(evidence=(span("A"), span("B", source_family="family:A"), span("C")))
    result = score_evidence_case(value, EvidenceObservation(("A", "B"), "answered", True))
    assert not result.support_complete
    assert score_evidence_case(
        value, EvidenceObservation(("C",), "answered", True)
    ).support_complete


def test_expired_or_branch_does_not_block_survivor_or_fill_time_gap():
    before = WHEN - timedelta(days=2)
    gap_start = WHEN - timedelta(days=1)
    value = case(evidence=(span("A", valid_from=before, valid_to=gap_start), span("B"), span("C")))
    assert score_evidence_case(
        value, EvidenceObservation(("C",), "answered", True)
    ).support_complete
    # No valid AND overlap and no valid OR branch means unknown, never a hull.
    value = replace(
        value,
        evidence=tuple(
            replace(e, valid_from=WHEN + timedelta(days=1)) if e.evidence_id == "C" else e
            for e in value.evidence
        ),
        answerable=False,
    )
    result = score_evidence_case(value, EvidenceObservation(("A", "B", "C"), "answered", True))
    assert result.evidence_recall is None
    assert result.outcome == "incorrect_answer"


@pytest.mark.parametrize(
    "changes",
    [
        {"readable": False},
        {"valid_to": WHEN},
        {"known_from": WHEN + timedelta(seconds=1)},
        {"scope": MemoryScope("another-tenant")},
        {"scope": MemoryScope("test", user_id="bob")},
    ],
)
def test_temporal_and_current_safety_exclusions_are_not_missing_gold(changes):
    value = case(evidence=(span("A", **changes), span("B"), span("C")))
    result = score_evidence_case(value, EvidenceObservation(("A", "C"), "answered", True))
    assert result.support_complete
    assert result.evidence_recall == 1
    assert result.forbidden_hits == ("A",)


def test_current_erasure_applies_to_historical_query():
    value = case(
        evidence=tuple(span(x, readable=False) for x in ("A", "B", "C")),
        answerable=False,
        known_at=WHEN - timedelta(hours=1),
    )
    result = score_evidence_case(value, EvidenceObservation(("C",), "answered", True))
    assert result.forbidden_hits == ("C",)


def test_missing_required_fields_and_vacuous_support_are_rejected():
    with pytest.raises(ValueError, match="necessary fields"):
        case(required_fields=("value", "currency"))
    with pytest.raises(ValueError, match="empty AND"):
        SupportAlternative(())
    with pytest.raises(ValueError, match="answerability"):
        case(answerable=False)


def dataset():
    return EvidenceDataset("test", "1", (case(),), "manual-test-1")


def report(ids=("C",), status="answered", judgment=True):
    return evaluate_evidence(
        dataset(),
        {"case-1": EvidenceObservation(ids, status, judgment)},
        run_configuration_sha256=CONFIG,
    )


def profile(baseline):
    return AcceptanceProfile(
        "contract-only",
        "1",
        ("evidence-evaluation",),
        dataset().fingerprint,
        CONFIG,
        baseline.fingerprint,
        1,
        1,
        QualityThresholds(1, 1, 1, 1, 0, 0, 0, 100, 1, 100),
        "synthetic scorer regression, not a production calibration",
    )


def test_complete_evidence_does_not_judge_the_answer_for_us():
    baseline = report()
    config = profile(baseline)
    for judgment, reason in (
        (None, "unjudged_answers"),
        (False, "correct_answer_rate_below_floor"),
    ):
        result = evaluate_quality_gate(
            config, report(judgment=judgment), baseline, expected_profile_sha256=config.fingerprint
        )
        assert not result.ready and reason in result.reasons


def test_all_abstention_cannot_pass_quality_gate():
    baseline = report()
    config = profile(baseline)
    candidate = report((), "abstained", None)
    result = evaluate_quality_gate(
        config, candidate, baseline, expected_profile_sha256=config.fingerprint
    )
    assert not result.ready
    assert "answer_coverage_below_floor" in result.reasons
    assert "correct_answer_rate_regressed" in result.reasons


def test_zero_floors_cannot_enable_an_all_abstaining_candidate():
    baseline = report((), "abstained", None)
    config = replace(
        profile(baseline),
        thresholds=QualityThresholds(
            0,
            0,
            0,
            0,
            1,
            1,
            1,
            100,
            1,
            100,
        ),
    )
    result = evaluate_quality_gate(
        config, baseline, baseline, expected_profile_sha256=config.fingerprint
    )
    assert result.reasons == ("no_answer_coverage",)


def test_absolute_floor_cannot_hide_relative_regression():
    baseline = report()
    config = profile(baseline)
    config = replace(
        config,
        thresholds=replace(
            config.thresholds,
            minimum_evidence_recall=0.5,
            minimum_support_rate=0,
            minimum_correct_answer_rate=0,
        ),
    )
    result = evaluate_quality_gate(
        config, report(("A",)), baseline, expected_profile_sha256=config.fingerprint
    )
    assert "evidence_recall_regressed" in result.reasons


def test_configuration_and_profile_mutation_block_release():
    baseline = report()
    config = profile(baseline)
    frozen = config.fingerprint
    changed = replace(config, version="2")
    result = evaluate_quality_gate(changed, baseline, baseline, expected_profile_sha256=frozen)
    assert result.reasons == ("profile_changed_after_freeze",)
    altered = replace(baseline, run_configuration_sha256="b" * 64)
    assert (
        "candidate_configuration_mismatch"
        in evaluate_quality_gate(
            config,
            altered,
            baseline,
            expected_profile_sha256=frozen,
        ).reasons
    )
    changed = replace(config, thresholds=None, calibration_reference=None)
    assert (
        "profile_uncalibrated"
        in evaluate_quality_gate(
            changed,
            baseline,
            baseline,
            expected_profile_sha256=changed.fingerprint,
        ).reasons
    )


def test_unknown_and_forbidden_evidence_cannot_hide_behind_good_answers():
    baseline = report()
    config = profile(baseline)
    assert (
        "unknown_evidence_hits"
        in evaluate_quality_gate(
            config,
            report(("C", "made-up")),
            baseline,
            expected_profile_sha256=config.fingerprint,
        ).reasons
    )
    assert (
        evaluate_quality_gate(
            config, baseline, baseline, expected_profile_sha256=config.fingerprint
        )
        .release_outcome()
        .status
        == "pass"
    )


def test_missing_cases_stay_in_denominator_and_diagnostics_do_not_inflate_accuracy():
    diagnostic = replace(case(), case_id="diagnostic", diagnostic_only=True)
    frozen = replace(dataset(), cases=(case(), diagnostic))
    result = evaluate_evidence(
        frozen,
        {"diagnostic": EvidenceObservation(("C",), "answered", True, cost=5)},
        run_configuration_sha256=CONFIG,
    )
    metrics = evidence_metrics(result)
    assert metrics["case_count"] == 1
    assert metrics["failure_rate"] == 1
    assert metrics["total_cost"] == 5
    assert metrics["correct_answer_rate"] == 0


def test_dataset_roundtrip_strict_schema_and_immutable_fingerprint():
    original = dataset()
    payload = to_jsonable(original)
    assert evidence_dataset_from_dict(payload) == original
    assert evidence_dataset_from_dict(payload).fingerprint == original.fingerprint
    payload["cases"][0]["surprise"] = True
    with pytest.raises(ValueError, match="unknown or missing"):
        evidence_dataset_from_dict(payload)
    assert replace(original, annotation_version="changed").fingerprint != original.fingerprint


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, True])
def test_nonfinite_costs_and_metrics_cannot_bypass_thresholds(value):
    with pytest.raises(ValueError):
        EvidenceObservation((), "error", cost=value)
    with pytest.raises(ValueError):
        replace(report().results[0], evidence_recall=value)
