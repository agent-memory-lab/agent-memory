"""AM61 N01: host capture coverage, trusted origin and persistent context-only replay."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest
import test_atom_admission as admission_tests

from agent_memory.capture.api import submit_capture, submit_profiled_capture
from agent_memory.capture.policy import CaptureSanitizer
from agent_memory.capture.profile import (
    CaptureOmission,
    CaptureProfile,
    HostCaptureObservation,
    annotate_capture,
    measure_capture_coverage,
)
from agent_memory.capture.queue import SQLiteCaptureQueue
from agent_memory.capture.sink import DirectCaptureSink, QueuedCaptureSink
from agent_memory.composition import build_local_kernel
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import MemoryScope
from agent_memory.lifecycle import (
    CAPTURE_METADATA_KEY,
    LifecycleEvent,
    LifecycleEventError,
    is_memory_context,
)

store = admission_tests.store
WHEN = datetime(2026, 10, 1, tzinfo=UTC)
SCOPE = MemoryScope("capture-tests", user_id="alice", session_id="one")
PROFILE = CaptureProfile(
    "host-text-tools",
    "1",
    "test-host",
    "1",
    ("message.received", "tool.completed", "tool.failed"),
    ("user", "model", "tool"),
    ("text", "tool_result"),
    "builtin-redaction-v1",
    ("payload.result", "content.tail"),
)


def event(scope=SCOPE, **changes):
    return replace(
        LifecycleEvent(
            scope,
            "event-1",
            "message.received",
            "user",
            WHEN,
            "run-1",
            content="Alice lives in Hangzhou",
        ),
        **changes,
    )


def observation(**changes):
    return replace(
        HostCaptureObservation(
            "host-event-1",
            "message-1",
            "revision-1",
            "family-1",
            "message.received",
            "user",
            WHEN,
            ("text",),
            True,
        ),
        **changes,
    )


def test_host_profile_preserves_role_mapping_and_unknown_completeness():
    value = annotate_capture(event(), PROFILE, observation(capture_complete=None))
    recovered = LifecycleEvent.from_dict(value.to_dict(), trusted_scope=SCOPE)
    assert recovered == value
    metadata = recovered.to_memory_event().metadata["lifecycle"]["payload"][CAPTURE_METADATA_KEY]
    assert metadata["observation"]["capture_complete"] is None
    assert metadata["observation"]["source_revision_id"] == "revision-1"
    assert metadata["independent_confirmation"] is None


def test_tools_remain_structured_and_assistant_claims_stay_distinct():
    tool = event(
        event_type="tool.failed",
        origin="tool",
        content="",
        payload={"result": {"exit_code": 1, "passed": 31, "failed": 2}},
    )
    obs = observation(event_type="tool.failed", origin="tool", content_types=("tool_result",))
    result = annotate_capture(tool, PROFILE, obs)
    assert result.payload["result"]["failed"] == 2
    assert result.payload[CAPTURE_METADATA_KEY]["evidence_role"] == "tool_result"
    claim = annotate_capture(
        event(origin="model", content="All tests passed"), PROFILE, observation(origin="model")
    )
    assert claim.payload[CAPTURE_METADATA_KEY]["evidence_role"] == "assistant_claim"


@pytest.mark.parametrize(
    "changes",
    [
        {"origin": "tool"},
        {"event_type": "tool.completed"},
        {"occurred_at": datetime(2026, 10, 2, tzinfo=UTC)},
    ],
)
def test_transport_cannot_forge_host_role_kind_or_timestamp(changes):
    with pytest.raises(ValueError, match="trusted host"):
        annotate_capture(event(**changes), PROFILE, observation())


def test_known_omissions_and_unhandled_types_cannot_claim_full_coverage():
    omissions = (CaptureOmission("content.tail", "stream_interrupted"),)
    with pytest.raises(ValueError, match="capture_complete=False"):
        observation(omissions=omissions)
    partial = observation(omissions=omissions, capture_complete=False)
    assert (
        annotate_capture(event(), PROFILE, partial).payload[CAPTURE_METADATA_KEY]["observation"][
            "capture_complete"
        ]
        is False
    )
    with pytest.raises(ValueError, match="outside"):
        annotate_capture(event(), PROFILE, observation(content_types=("image",)))
    with pytest.raises(ValueError, match="explicit omission"):
        annotate_capture(event(), PROFILE, observation(content_types=("text", "tool_result")))


def test_coverage_requires_a_real_host_inventory():
    assert measure_capture_coverage(None, ("a",)).coverage is None
    result = measure_capture_coverage(("a", "b", "c"), ("c", "a"))
    assert result.coverage == 2 / 3
    assert result.missing_event_ids == ("b",)
    assert measure_capture_coverage((), ()).coverage is None
    with pytest.raises(ValueError, match="outside"):
        measure_capture_coverage(("a",), ("unregistered",))


def test_untrusted_capture_api_rejects_reserved_metadata():
    class NeverCalled:
        async def submit(self, value):
            pytest.fail("rejected capture must not reach the sink")

    raw = event(payload={CAPTURE_METADATA_KEY: {"evidence_role": "source"}}).to_dict()
    with pytest.raises(LifecycleEventError, match="trusted host"):
        asyncio.run(submit_capture(raw, sink=NeverCalled(), scope=SCOPE, actor="trusted-host"))
    with pytest.raises(ValueError, match="reserved"):
        asyncio.run(
            submit_profiled_capture(
                raw,
                sink=NeverCalled(),
                scope=SCOPE,
                actor="trusted-host",
                profile=PROFILE,
                observation=observation(),
            )
        )


def test_profile_survives_queue_restart_retry_and_source_persistence(tmp_path):
    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=CaptureSanitizer())
            kwargs = dict(
                sink=QueuedCaptureSink(queue),
                scope=SCOPE,
                actor="host",
                profile=PROFILE,
                observation=observation(),
            )
            first = await submit_profiled_capture(event().to_dict(), **kwargs)
            retry = await submit_profiled_capture(event().to_dict(), **kwargs)
            assert first.status == "pending" and retry.duplicate
            recovered = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=CaptureSanitizer())
            receipt = await recovered.process_one(kernel, scope=SCOPE)
            assert receipt.status == "done"
            async with kernel._repository.unit_of_work() as uow:
                stored = await uow.find_event_by_idempotency(SCOPE, "lifecycle:v1:event-1")
            metadata = stored.metadata["lifecycle"]["payload"][CAPTURE_METADATA_KEY]
            assert metadata["observation"]["source_family"] == "family-1"
            assert metadata["profile"]["version"] == "1"
        finally:
            await kernel.close()

    asyncio.run(run())


@pytest.mark.parametrize("injected", [True, False])
def test_context_replay_never_calls_legacy_generator_or_scheduler(tmp_path, injected):
    class NeverCalled:
        async def extract(self, value):
            pytest.fail("memory context must not be re-extracted")

        async def enqueue_event(self, value, result):
            pytest.fail("memory context must not schedule independent consolidation")

    async def run():
        kernel = build_local_kernel(
            tmp_path / "memory.db", extractor=NeverCalled(), consolidation_scheduler=NeverCalled()
        )
        await kernel.initialize()
        try:
            obs = observation(
                memory_injection=injected,
                parent_revision_ids=("original",),
                parent_receipt_ids=("original-receipt",),
                source_family="original-family",
            )
            annotated = annotate_capture(event(), PROFILE, obs)
            assert is_memory_context(annotated.to_memory_event())
            result = await submit_profiled_capture(
                event().to_dict(),
                scope=SCOPE,
                actor="host",
                sink=DirectCaptureSink(kernel, CaptureSanitizer()),
                profile=PROFILE,
                observation=obs,
            )
            assert result.status == "done"
            assert await kernel._repository.current_claims(SCOPE) == ()
        finally:
            await kernel.close()

    asyncio.run(run())


def test_extraction_retries_bind_capture_revision_before_model_calls(store):
    class EmptyGenerator:
        version = "empty-v1"
        calls = 0

        async def generate_atoms(self, value):
            self.calls += 1
            return ()

        async def review_atoms(self, *args):
            pytest.fail("empty generation must not call the reviewer")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            generator = EmptyGenerator()
            pipeline = AtomExtractionPipeline(generator, generator)
            fresh = annotate_capture(event(scope), PROFILE, observation()).to_memory_event()
            kwargs = dict(
                pipeline=pipeline, authority=admission_tests.SELF, policy=admission_tests.POLICY
            )
            first = await kernel.extract_event(fresh, **kwargs)
            retry = await kernel.extract_event(fresh, **kwargs)
            assert first.admission.event_id == retry.admission.event_id
            assert retry.admission.duplicate and generator.calls == 1
            changed = annotate_capture(
                event(scope), PROFILE, observation(source_revision_id="changed")
            ).to_memory_event()
            with pytest.raises(ValueError, match="different"):
                await kernel.extract_event(changed, **kwargs)
            assert generator.calls == 1

    asyncio.run(run())


def test_explicit_admission_and_model_extraction_cannot_upgrade_replayed_memory(store):
    class NeverCalled:
        version = "never"

        async def generate_atoms(self, value):
            pytest.fail("context cannot reach the model")

        async def review_atoms(self, *args):
            pytest.fail("context cannot reach a reviewer")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            obs = observation(
                memory_injection=True,
                parent_revision_ids=("original",),
                parent_receipt_ids=("receipt",),
            )
            replay = annotate_capture(event(scope), PROFILE, obs).to_memory_event()
            with pytest.raises(ValueError, match="independent evidence"):
                await engine.admit(
                    replay,
                    [admission_tests.atom()],
                    authority=admission_tests.SELF,
                    policy=admission_tests.POLICY,
                )
            with pytest.raises(ValueError, match="original evidence"):
                await kernel.extract_event(
                    replay,
                    pipeline=AtomExtractionPipeline(NeverCalled(), NeverCalled()),
                    authority=admission_tests.SELF,
                    policy=admission_tests.POLICY,
                )
            # A fresh host-authenticated user confirmation has no context marker.
            fresh = annotate_capture(event(scope), PROFILE, observation()).to_memory_event()
            result = await engine.admit(
                fresh,
                [admission_tests.atom()],
                authority=admission_tests.SELF,
                policy=admission_tests.POLICY,
            )
            assert len(result.claim_ids) == 1
            async with engine.repository.unit_of_work() as uow:
                stored = await uow.find_event_by_idempotency(scope, fresh.idempotency_key)
            annotation = stored.metadata["lifecycle"]["payload"][CAPTURE_METADATA_KEY]
            assert annotation["observation"]["source_revision_id"] == "revision-1"
            changed = annotate_capture(
                event(scope), PROFILE, observation(source_revision_id="changed")
            ).to_memory_event()
            with pytest.raises(ValueError, match="different atom input"):
                await engine.admit(
                    changed,
                    [admission_tests.atom()],
                    authority=admission_tests.SELF,
                    policy=admission_tests.POLICY,
                )

    asyncio.run(run())
