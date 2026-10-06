"""Automatic extraction contracts, executed against both storage engines."""

import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as admission_tests
from test_atom_admission import at, source

from agent_memory import (
    AdmissionPolicy,
    AgentMemory,
    AtomExtractionPipeline,
    AtomReview,
    ForgetMode,
    ForgetRequest,
    MemoryQuery,
    PredicateSpec,
    RuleBasedAtomAdapter,
    ScopeLevel,
    SourceAuthority,
)

store = admission_tests.store

RULES = RuleBasedAtomAdapter("alice")
POLICY = AdmissionPolicy(
    [PredicateSpec(p) for p in ("home_city", "response_language", "response_style")]
)
AUTHORITY = SourceAuthority(
    "alice-login",
    subjects=("alice",),
    predicates=("home_city", "response_language", "response_style"),
)


class Generator:
    version = "test-generator-v1"

    def __init__(self, result=None, error=None):
        self.result, self.error, self.calls = result, error, 0

    async def generate_atoms(self, event):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result


class Reviewer:
    version = "test-reviewer-v1"

    def __init__(self, faithfulness="supported", retention="durable"):
        self.faithfulness, self.retention = faithfulness, retention

    async def review_atoms(self, event, candidates):
        return tuple(
            AtomReview(i, self.faithfulness, self.retention, ("test-review",))
            for i in range(len(candidates))
        )


def candidate(**changes):
    return dict(
        subject_id="alice",
        predicate="home_city",
        value="Hangzhou",
        kind="fact",
        modality="asserted",
        source_quote="我住在杭州",
        **changes,
    )


async def extract(kernel, event, generator=RULES, reviewer=RULES, **options):
    return await kernel.extract_event(
        event,
        pipeline=AtomExtractionPipeline(generator, reviewer, **options),
        authority=AUTHORITY,
        policy=POLICY,
    )


@pytest.mark.parametrize(
    ("content", "action", "value"),
    [
        ("我住在杭州", "ACCEPT", "Hangzhou"),
        ("I currently live in Shanghai.", "ACCEPT", "Shanghai"),
        ("以后用中文回答", "ACCEPT", "zh"),
        ("Always answer in English.", "ACCEPT", "en"),
        ("以后请简洁回答", "ACCEPT", "concise"),
        ("请简洁回答", "ACCEPT", "concise"),
        ("这次用中文回答", "L0_ONLY", None),
        ("For this reply, answer in English.", "L0_ONLY", None),
        ("我不住在上海", "L0_ONLY", None),
        ("我可能下周搬到上海", "L0_ONLY", None),
        ("I might move to Beijing next week.", "L0_ONLY", None),
        ("他说：“我住在上海”", None, None),
        ("如果我住在上海就方便了", None, None),
        ("我住在杭州吗？", None, None),
        ("我以前住在杭州", None, None),
        ("谢谢", None, None),
    ],
)
def test_reference_rules_preserve_useful_information_without_promoting_noise(
    store, content, action, value
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            result = await extract(kernel, source(scope, content, idempotency="rule-case"))
            assert result.processing_state == "completed"
            assert [d.action for d in result.decisions] == ([] if action is None else [action])
            claims, _ = await engine.state(scope, valid_at=at(2), known_at=at(30))
            assert [c.value for c in claims] == ([] if value is None else [value])
            assert result.generation_calls == 1
            assert result.review_calls == (0 if action is None else 1)
            assert await kernel.extraction_status(scope, "rule-case") == result
            clock[0] = at(2)
            bundle = await kernel.retrieve(MemoryQuery(scope, content, token_budget=2048))
            assert all(item.id != result.admission.event_id for item in bundle.relevant_memories)

    asyncio.run(run())


@pytest.mark.parametrize(
    "changes",
    [
        {"value": "Shanghai"},
        {"subject_id": "bob"},
        {"modality": "planned"},
        {"predicate": "response_language"},
        {"kind": "preference"},
        {"valid_from": at(1).isoformat()},
        {"valid_to": at(2).isoformat()},
    ],
)
def test_matching_quote_does_not_prove_forged_semantics(store, changes):
    async def run():
        async with store() as (_, kernel, scope, _):
            generated = {**candidate(), **changes}
            result = await extract(kernel, source(scope, "我住在杭州"), Generator([generated]))
            assert result.decisions[0].action == "REJECT"
            assert not result.admission.claim_ids

    asyncio.run(run())


@pytest.mark.parametrize(
    ("raw", "action", "reason"),
    [
        (
            {"source_start": 1, "source_end": 5},
            "PENDING_VERIFICATION",
            "source_span_missing_ambiguous_or_mismatched",
        ),
        ({"scope_level": "tenant"}, "REJECT", "generated_scope_override"),
        (
            {"change_kind": "correct", "corrects_id": "other"},
            "REJECT",
            "automatic_correction_requires_host_review",
        ),
        ({"source_start": True, "source_end": 5}, "REJECT", "invalid_candidate_fields"),
        ({"value": {"not": "scalar"}}, "REJECT", "invalid_candidate_fields"),
    ],
)
def test_untrusted_generated_fields_cannot_override_host_contracts(store, raw, action, reason):
    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel, source(scope, "我住在杭州"), Generator([{**candidate(), **raw}]), Reviewer()
            )
            assert result.decisions[0].action == action
            assert reason in result.decisions[0].reasons
            assert not result.admission.claim_ids

    asyncio.run(run())


def test_authority_and_display_text_are_not_taken_from_generator(store):
    async def run():
        async with store() as (engine, kernel, scope, _):
            generator = Generator(
                [
                    {
                        **candidate(),
                        "text": "unreviewed secret claim",
                        "confidence": 1,
                        "authority": {"kind": "tool_observation"},
                    }
                ]
            )
            policy = AdmissionPolicy([PredicateSpec("home_city", allow_self_report=False)])
            result = await kernel.extract_event(
                source(scope, "我住在杭州"),
                pipeline=AtomExtractionPipeline(generator, RULES),
                authority=AUTHORITY,
                policy=policy,
            )
            assert result.decisions[0].action == "PENDING_VERIFICATION"
            assert "self_report_not_allowed" in result.decisions[0].reasons
            row = await kernel.admission_status(scope, result.admission.candidate_ids[0])
            assert row["payload"]["draft"]["text"] == 'alice: home_city = "Hangzhou"'
            assert row["payload"]["extraction"]["reports"][0]["faithfulness"] == "supported"
            assert not (await engine.state(scope, valid_at=at(2), known_at=at(30)))[0]

    asyncio.run(run())


@pytest.mark.parametrize(
    ("faithfulness", "retention", "scope_level", "action"),
    [
        ("uncertain", "durable", ScopeLevel.SESSION, "PENDING_VERIFICATION"),
        ("supported", "uncertain", ScopeLevel.SESSION, "PENDING_VERIFICATION"),
        ("supported", "session", ScopeLevel.USER, "L0_ONLY"),
        ("supported", "durable", ScopeLevel.USER, "ACCEPT"),
    ],
)
def test_semantics_and_retention_are_separate_gates(
    store, faithfulness, retention, scope_level, action
):
    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel,
                source(scope, "我住在杭州"),
                Generator([candidate()]),
                Reviewer(faithfulness, retention),
                scope_level=scope_level,
            )
            assert result.decisions[0].action == action

    asyncio.run(run())


def test_ambiguous_quote_is_pending_even_when_reviewer_claims_support(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel,
                source(scope, "我住在杭州。我住在杭州"),
                Generator([candidate()]),
                Reviewer(),
            )
            assert result.decisions[0].action == "PENDING_VERIFICATION"

    asyncio.run(run())


def test_failed_generation_is_durable_idempotent_and_does_not_leak_error_text(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            generator = Generator(error=RuntimeError("provider-secret-credential"))
            event = source(scope, "我住在杭州", idempotency="failed")
            first = await extract(kernel, event, generator)
            assert first.processing_state == "failed"
            assert first.failure_codes == ("generation_failed",)
            assert not first.admission.claim_ids and not first.decisions
            retry = await extract(kernel, replace(event, id="retry"), generator)
            assert retry.admission.duplicate and generator.calls == 1
            assert retry.admission.event_id == first.admission.event_id
            assert "credential" not in repr(await kernel.extraction_status(scope, "failed"))

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["invalid", "exception", "timeout", "indexes"])
def test_review_failure_never_publishes_candidates(store, failure):
    class BadReviewer:
        version = "bad-review-v1"

        async def review_atoms(self, event, candidates):
            if failure == "exception":
                raise RuntimeError("secret")
            if failure == "timeout":
                await asyncio.Event().wait()
            if failure == "indexes":
                return [AtomReview(1, "supported", "durable", ("bad-index",))]
            return []

    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel, source(scope, "我住在杭州"), reviewer=BadReviewer(), timeout_seconds=0.02
            )
            assert result.processing_state == "failed"
            assert result.decisions[0].action == "PENDING_VERIFICATION"
            assert not result.admission.claim_ids

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["oversized", "timeout", "malformed"])
def test_generation_limits_and_empty_failed_batches(store, failure):
    class BadGenerator(Generator):
        async def generate_atoms(self, event):
            if failure == "timeout":
                await asyncio.Event().wait()
            return [candidate()] * 33 if failure == "oversized" else "invalid sequence"

    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel, source(scope, "我住在杭州"), BadGenerator(), timeout_seconds=0.02
            )
            assert result.processing_state == "failed"
            assert not result.decisions and result.review_calls == 0

    asyncio.run(run())


def test_idempotent_first_result_and_input_change_detection(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            generator = Generator([candidate()])
            event = source(scope, "我住在杭州", idempotency="stable")
            first = await extract(kernel, event, generator)
            generator.result = [{**candidate(), "value": "Shanghai"}]
            again = await extract(kernel, replace(event, id="retry"), generator)
            assert again.admission.duplicate and generator.calls == 1
            assert again.decisions == first.decisions
            for changed in (
                replace(event, content="我住在上海"),
                replace(event, occurred_at=at(2)),
            ):
                with pytest.raises(ValueError, match="different extraction input"):
                    await extract(kernel, changed, generator)
            generator.version = "changed-model"
            with pytest.raises(ValueError, match="different extraction input"):
                await extract(kernel, event, generator)
            assert generator.calls == 1

    asyncio.run(run())


def test_concurrent_different_generations_return_committed_winner(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            ready, release = asyncio.Event(), asyncio.Event()

            class RacingGenerator(Generator):
                async def generate_atoms(self, event):
                    self.calls += 1
                    if self.calls == 1:
                        ready.set()
                        await release.wait()
                        return [{**candidate(), "value": "Shanghai"}]
                    return [candidate()]

            generator = RacingGenerator()
            event = source(scope, "我住在杭州", idempotency="race")
            delayed = asyncio.create_task(extract(kernel, event, generator))
            await ready.wait()
            try:
                first = await asyncio.wait_for(
                    extract(kernel, replace(event, id="winner"), generator), 3
                )
            finally:
                release.set()
            second = await delayed
            assert first.decisions == second.decisions
            assert first.admission.event_id == second.admission.event_id == "winner"
            assert not first.admission.duplicate and second.admission.duplicate
            assert first.decisions[0].action == "ACCEPT"

    asyncio.run(run())


def test_disagreeing_reviews_of_duplicate_candidates_stay_pending(store):
    class DisagreeingReviewer:
        version = "disagreement-v1"

        async def review_atoms(self, event, candidates):
            return [
                AtomReview(i, "unsupported" if i == 1 else "supported", "durable", ("verdict",))
                for i in range(3)
            ]

    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel,
                source(scope, "我住在杭州"),
                Generator([candidate()] * 3),
                DisagreeingReviewer(),
            )
            assert len(result.admission.candidate_ids) == 1
            assert {d.action for d in result.decisions} == {"PENDING_VERIFICATION"}
            assert not result.admission.claim_ids

    asyncio.run(run())


@pytest.mark.parametrize("content", ["我住在杭州", "谢谢"])
def test_forgotten_source_cannot_be_replayed_including_empty_extractions(store, content):
    async def run():
        async with store() as (_, kernel, scope, _):
            event = source(scope, content, idempotency="forgotten")
            result = await extract(kernel, event)
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(result.admission.event_id,), mode=ForgetMode.ERASE)
            )
            with pytest.raises(ValueError, match="forgotten|deleted"):
                await extract(kernel, event)
            with pytest.raises(ValueError, match="forgotten|deleted"):
                await kernel.extraction_status(scope, "forgotten")

    asyncio.run(run())


def test_facade_default_observation_retry_and_status(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            memory = AgentMemory(kernel, scope)
            await memory.initialize()
            pipeline = AtomExtractionPipeline(RULES, RULES)
            first = await memory.extract_atoms(
                "以后用中文回答",
                pipeline=pipeline,
                authority=AUTHORITY,
                policy=POLICY,
                idempotency_key="facade",
            )
            again = await memory.extract_atoms(
                "以后用中文回答",
                pipeline=pipeline,
                authority=AUTHORITY,
                policy=POLICY,
                idempotency_key="facade",
            )
            assert again.admission.duplicate and again.decisions == first.decisions
            assert await memory.extraction_status("facade") == first
            assert await memory.extraction_status("missing") is None

    asyncio.run(run())


def test_authored_evaluation_measures_recall_and_cannot_reward_rejecting_everything(store):
    import json
    from pathlib import Path

    from agent_memory.evaluation.extraction import evaluate_extraction

    cases = json.loads(
        (
            Path(__file__).resolve().parents[1] / "examples/data/atom_extraction_cases.json"
        ).read_text()
    )["cases"]

    async def run():
        async with store() as (engine, _, scope, _):
            report = await evaluate_extraction(
                engine.repository,
                cases,
                pipeline=AtomExtractionPipeline(RULES, RULES),
                scope=scope,
                authority=AUTHORITY,
                policy=POLICY,
            )
            assert report["true_positives"] == 9
            assert report["false_positives"] == 0 and report["false_negatives"] == 2
            assert report["adversarial_rejection_rate"] == 1
            assert report["recall"] == pytest.approx(9 / 11)
            assert report["failed_cases"] == 0
            assert report["generation_calls"] == 29 and report["review_calls"] == 19
            empty = await evaluate_extraction(
                engine.repository,
                cases[:2],
                pipeline=AtomExtractionPipeline(Generator([]), RULES),
                scope=scope,
                authority=AUTHORITY,
                policy=POLICY,
            )
            assert empty["recall"] == 0 and empty["precision"] is None
            assert empty["false_negatives"] == 2

    asyncio.run(run())


def test_forget_after_parallel_commit_prevents_delayed_reinsertion(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            ready, release = asyncio.Event(), asyncio.Event()

            class SlowGenerator(Generator):
                async def generate_atoms(self, event):
                    ready.set()
                    await release.wait()
                    return [candidate()]

            event = source(scope, "我住在杭州", idempotency="deleted-during-call")
            delayed = asyncio.create_task(extract(kernel, event, SlowGenerator()))
            await ready.wait()
            try:
                winner = await extract(
                    kernel, replace(event, id="committed"), Generator([candidate()])
                )
                await kernel.forget(
                    ForgetRequest(
                        scope, memory_ids=(winner.admission.event_id,), mode=ForgetMode.ERASE
                    )
                )
            finally:
                release.set()
            with pytest.raises(ValueError, match="forgotten|deleted"):
                await delayed

    asyncio.run(run())
