"""Malformed model output must not masquerade as a successful empty extraction."""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import FrozenInstanceError, asdict
from datetime import UTC, datetime

import pytest

from agent_memory import MemoryEvent, MemoryScope
from agent_memory.capture.sink import CaptureError
from agent_memory.composition import build_local_kernel
from agent_memory.extraction_result import (
    ExtractionDiagnostic,
    ExtractionFormatError,
    ExtractionProviderError,
)
from agent_memory.providers import (
    GeneratedTrajectoryClaimExtractor,
    MetadataClaimExtractor,
    build_trajectory_extractor,
)
from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory

SCOPE = MemoryScope("extraction-review", session_id="session")


class Generator:
    def __init__(self, output):
        self.output = output

    async def generate_claims(self, event):
        if isinstance(self.output, Exception):
            raise self.output
        return self.output


def claim(**changes):
    return {
        "key": "drink",
        "value": "tea",
        "text": "The user prefers tea.",
        "confidence": 0.95,
        **changes,
    }


def reported(output, **config):
    return asyncio.run(
        GeneratedTrajectoryClaimExtractor(Generator(output), **config).extract_with_report(
            MemoryEvent(SCOPE, "user.message", "I prefer tea.")
        )
    )


def test_legal_empty_and_quality_rejection_are_successful_and_distinct_in_strict_mode():
    empty = reported([], fail_open=False)
    rejected = reported([claim(confidence=0.3)], fail_open=False)
    assert empty.diagnostic.status == "empty" and empty.diagnostic.candidate_count == 0
    assert rejected.diagnostic.status == "rejected" and rejected.diagnostic.candidate_count == 1
    assert rejected.diagnostic.rejections[0].reason == "low_confidence"
    assert empty.diagnostic.successful and rejected.diagnostic.successful
    assert empty.drafts == rejected.drafts == ()


@pytest.mark.parametrize(
    "output,reason",
    [
        (None, "invalid_output"),
        ({"claims": []}, "invalid_output"),
        (["not a candidate"], "invalid_candidate"),
        ([{"key": "drink", "value": "tea", "text": "The user prefers tea."}], "missing_confidence"),
        ([claim(confidence=True)], "invalid_confidence"),
        ([claim(confidence=float("nan"))], "invalid_confidence"),
        ([claim(text=3)], "invalid_candidate"),
    ],
)
def test_malformed_output_raises_strict_error_or_explicit_fail_open_diagnostic(output, reason):
    with pytest.raises(ExtractionFormatError) as error:
        reported(output, fail_open=False)
    assert error.value.diagnostic.status == "error"
    assert error.value.diagnostic.rejections[0].reason == reason
    open_report = reported(output, fail_open=True)
    assert open_report.diagnostic.status == "error" and not open_report.diagnostic.successful
    assert open_report.drafts == () and open_report.diagnostic.rejections[0].reason == reason


@pytest.mark.parametrize(
    "field", ["conditions", "source_quote", "negated", "exceptions", "modality"]
)
def test_unsupported_qualifiers_never_turn_into_an_unconditional_fact(field):
    conditional = claim(**{field: "only while approved"})
    with pytest.raises(ExtractionFormatError):
        reported([conditional], fail_open=False)
    report = reported([conditional], fail_open=True)
    assert not report.drafts and report.diagnostic.rejections[0].reason == "unsupported_fields"
    event = MemoryEvent(
        SCOPE, "user.message", "Conditional source.", metadata={"claims": [conditional]}
    )
    trusted = asyncio.run(MetadataClaimExtractor().extract_with_report(event))
    assert not trusted.drafts and trusted.diagnostic.status == "error"


def test_partial_fail_open_retains_only_safe_candidates_and_reports_degradation():
    report = reported([claim(), claim(key="conditional", conditions=["approval"])])
    assert len(report.drafts) == 1 and report.drafts[0].key == "drink"
    assert report.diagnostic.status == "degraded" and not report.diagnostic.successful
    assert report.diagnostic.accepted_count == 1 and report.diagnostic.candidate_count == 2
    with pytest.raises(ExtractionFormatError) as error:
        reported([claim(), claim(key="conditional", conditions=["approval"])], fail_open=False)
    assert error.value.diagnostic.accepted_count == 0
    assert error.value.diagnostic.rejected_count == 2
    assert error.value.diagnostic.status == "error"


def test_unavailable_scope_is_quality_rejection_and_explicit_empty_event_types_skip():
    rejected = reported([claim(scope="user")], fail_open=False)
    assert rejected.diagnostic.status == "rejected"
    assert rejected.diagnostic.rejections[0].reason == "scope_unavailable"
    skipped = reported(RuntimeError("must never run"), event_types=[])
    assert skipped.diagnostic.status == "skipped" and not skipped.diagnostic.successful


def test_public_builder_propagates_strict_provider_failure_and_hides_private_message():
    event = MemoryEvent(SCOPE, "user.message", "I prefer tea.")
    extractor = build_trajectory_extractor(
        Generator(RuntimeError("private provider detail")), fail_open=False
    )
    with pytest.raises(ExtractionProviderError) as error:
        asyncio.run(extractor.extract(event))
    assert error.value.diagnostic.error_type == "RuntimeError"
    assert "private" not in str(error.value)
    opened = build_trajectory_extractor(Generator(RuntimeError("private provider detail")))
    report = asyncio.run(opened.extract_with_report(event))
    assert report.diagnostic.status == "error" and not report.diagnostic.successful


def test_trusted_sdk_default_score_remains_and_partial_provider_failure_is_visible():
    event = MemoryEvent(
        SCOPE,
        "user.message",
        "SDK source.",
        metadata={
            "claims": [{"key": "sdk.fact", "value": "tea", "text": "Explicit SDK statement."}]
        },
    )
    extractor = build_trajectory_extractor(Generator(None))
    report = asyncio.run(extractor.extract_with_report(event))
    assert report.drafts[0].confidence == 1.0 and report.drafts[0].key == "sdk.fact"
    assert report.diagnostic.status == "degraded"
    assert len(report.diagnostic.components) == 2


def test_diagnostics_are_immutable_and_survive_receipt_json_shape():
    report = reported([claim(confidence=0.2)])
    with pytest.raises(FrozenInstanceError):
        report.diagnostic.status = "completed"
    restored = ExtractionDiagnostic.from_dict(asdict(report.diagnostic))
    assert restored == report.diagnostic


def test_scope_alias_is_supported_but_conflicting_scope_is_not_silently_ignored():
    report = reported([claim(scope_level="session")], fail_open=False)
    assert report.diagnostic.accepted_count == 1
    with pytest.raises(ExtractionFormatError) as error:
        reported([claim(scope="session", scope_level="tenant")], fail_open=False)
    assert error.value.diagnostic.rejections[0].reason == "conflicting_scope"


def test_candidate_limit_has_bounded_diagnostics_with_exact_rejected_count():
    report = reported([claim(key=f"claim-{index}") for index in range(1000)], max_claims=2)
    assert report.diagnostic.candidate_count == 1000 and report.diagnostic.accepted_count == 2
    assert len(report.diagnostic.rejections) == 1 and report.diagnostic.rejected_count == 998
    assert report.diagnostic.rejections[0].reason == "candidate_limit"


def test_strict_composite_failure_does_not_count_prior_valid_sdk_drafts_as_accepted():
    event = MemoryEvent(
        SCOPE,
        "user.message",
        "SDK source.",
        metadata={
            "claims": [{"key": "sdk.fact", "value": "tea", "text": "Explicit SDK statement."}]
        },
    )
    with pytest.raises(ExtractionProviderError) as error:
        asyncio.run(
            build_trajectory_extractor(
                Generator(RuntimeError("provider failed")), fail_open=False
            ).extract_with_report(event)
        )
    assert error.value.diagnostic.accepted_count == 0
    assert error.value.diagnostic.candidate_count is None
    assert error.value.diagnostic.rejections[0].reason == "batch_aborted"


def test_successful_receipt_diagnostic_survives_restart_and_duplicate_is_not_reinferred(tmp_path):
    async def run():
        path, recovery = tmp_path / "core.db", tmp_path / "recovery.db"
        event = dict(
            event_id="empty",
            role="user",
            content="Hello.",
            run_id="run",
            occurred_at=datetime(2026, 10, 11, tzinfo=UTC),
        )
        memory = UnifiedMemory.local(path, SCOPE, generator=Generator([]), recovery_path=recovery)
        await memory.initialize()
        try:
            original = await memory.capture(**event)
            assert original.extraction.status == "empty"
        finally:
            await memory.close()
        restarted = UnifiedMemory.local(
            path,
            SCOPE,
            generator=Generator(RuntimeError("must not reextract")),
            recovery_path=recovery,
        )
        await restarted.initialize()
        try:
            duplicate = await restarted.capture(**event)
            assert duplicate.duplicate and duplicate.extraction is None
            receipt = await restarted.capture_receipt("empty")
            assert receipt.extraction.status == "empty" and receipt.extracted
        finally:
            await restarted.close()

    asyncio.run(run())


def test_strict_capture_failure_is_recorded_without_raw_commit_then_can_retry(tmp_path):
    async def run():
        generator = Generator(None)
        memory = UnifiedMemory.local(
            tmp_path / "core.db", SCOPE, generator=generator, recovery_path=tmp_path / "recovery.db"
        )
        await memory.initialize()
        event = dict(
            event_id="source",
            role="user",
            content="I prefer tea.",
            run_id="run",
            occurred_at=datetime(2026, 10, 11, tzinfo=UTC),
        )
        try:
            with pytest.raises(CaptureError) as error:
                await memory.capture(**event)
            assert error.value.code == "capture_extraction_failed"
            receipt = await memory.capture_receipt("source")
            assert not receipt.persisted and not receipt.extracted
            assert receipt.extraction.status == "error" and receipt.stage == "received"
            assert await memory.provider.get_state(SCOPE) == ()
            generator.output = [claim()]
            result = await memory.capture(**event)
            assert not result.duplicate
            receipt = await memory.capture_receipt("source")
            assert receipt.persisted and receipt.extracted and receipt.error_code is None
            assert receipt.extraction.status == "completed"
        finally:
            await memory.close()

    asyncio.run(run())


def test_failing_legacy_extractor_capture_is_safe_extraction_error_without_source_commit(tmp_path):
    class LegacyExtractor:
        async def extract(self, event):
            raise RuntimeError("private provider message must not appear in diagnostics")

    async def run():
        path = tmp_path / "core.db"
        kernel = build_local_kernel(path, extractor=LegacyExtractor())
        memory = UnifiedMemory(
            kernel, SCOPE, journal=SQLiteDeletionJournal(tmp_path / "journal.db")
        )
        await memory.initialize()
        try:
            with pytest.raises(CaptureError) as error:
                await memory.capture(
                    event_id="failed", role="user", content="I prefer tea.", run_id="run"
                )
            assert error.value.code == "capture_extraction_failed"
            assert error.value.extraction.error_type == "RuntimeError"
            assert error.value.extraction.candidate_count is None
            assert "private" not in str(error.value) + str(asdict(error.value.extraction))
            with closing(sqlite3.connect(path)) as connection:
                assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
            assert await kernel.get_state(SCOPE) == ()
        finally:
            await memory.close()

    asyncio.run(run())


def test_fail_open_public_capture_discloses_error_instead_of_claiming_empty_extraction(tmp_path):
    async def run():
        path = tmp_path / "core.db"
        extractor = GeneratedTrajectoryClaimExtractor(Generator(None), fail_open=True)
        kernel = build_local_kernel(path, extractor=extractor)
        memory = UnifiedMemory(
            kernel, SCOPE, journal=SQLiteDeletionJournal(tmp_path / "journal.db")
        )
        await memory.initialize()
        try:
            result = await memory.capture(
                event_id="raw", role="user", content="I prefer tea.", run_id="run"
            )
            assert result.status == "done" and result.extraction.status == "error"
            assert not result.extraction.successful and result.extraction.accepted_count == 0
            with closing(sqlite3.connect(path)) as connection:
                assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        finally:
            await memory.close()

    asyncio.run(run())


@pytest.mark.parametrize("fail_open", [True, False])
def test_typed_generator_validation_errors_preserve_body_free_counts(fail_open):
    from agent_memory.extraction_result import ExtractionRejection

    diagnostic = ExtractionDiagnostic(
        "error", candidate_count=2, accepted_count=0,
        rejections=(
            ExtractionRejection(0, "missing_confidence"),
            ExtractionRejection(1, "batch_aborted"),
        ),
        error_type="ExtractionFormatError",
    )
    failure = ExtractionFormatError(diagnostic)
    if fail_open:
        report = reported(failure, fail_open=True)
        assert report.drafts == ()
        result = report.diagnostic
    else:
        with pytest.raises(ExtractionFormatError) as error:
            reported(failure, fail_open=False)
        assert error.value is failure
        result = error.value.diagnostic
    assert result.status == "error"
    assert result.candidate_count == 2 and result.accepted_count == 0
    assert result.rejections == diagnostic.rejections
    assert result.error_type == "ExtractionFormatError"


@pytest.mark.parametrize("fail_open", [True, False])
def test_untyped_generator_failures_keep_unknown_counts_and_no_private_body(fail_open):
    import json

    failure = RuntimeError("private source body and provider token")
    if fail_open:
        result = reported(failure, fail_open=True).diagnostic
    else:
        with pytest.raises(ExtractionProviderError) as error:
            reported(failure, fail_open=False)
        assert "private" not in str(error.value)
        result = error.value.diagnostic
    assert result.status == "error" and result.error_type == "RuntimeError"
    assert result.candidate_count is None and result.accepted_count == 0
    assert not result.rejections
    assert "private" not in json.dumps(asdict(result))
