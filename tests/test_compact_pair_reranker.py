"""Contract-only tests. No model inference or efficacy evidence is produced."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from test_governed_recall_integration import loaded_retriever

from agent_memory.domain import MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.retrieval.bundle import BundleBudget, pack_memory_bundle
from agent_memory.retrieval.feature_gate import (
    RetrievalFeatureApproval,
    validate_feature_enablement,
)
from agent_memory.retrieval.fusion import FusedCandidate
from agent_memory.retrieval.governed import GovernedRecallPipeline
from agent_memory.retrieval.guard import GovernedCandidate
from agent_memory.retrieval.pair_reranker import (
    CompactPairReranker,
    FinalTokenLogits,
    FinalTokenPairScorer,
    PairAuthorizationError,
    PairCandidate,
    PairModelSpec,
    PairScore,
)
from agent_memory.retrieval.scoped_lexical import ScopeIsolationError

SCOPE = MemoryScope("tenant-a", session_id="session-a")
NOW = datetime(2026, 1, 1, tzinfo=UTC)
SPEC = PairModelSpec("qwen3-reranker-0.6b", *(["a" * 64] * 5), 10, 11)


def candidates():
    return tuple(
        FusedCandidate(
            MemoryItem(key, MemoryKind.EVENT, f"original {key}", 1, NOW, {"untouched": [key]}),
            score,
            (f"source-{key}",),
            (("lexical", index + 1),),
            ("lexical",),
        )
        for index, (key, score) in enumerate((("a", 0.3), ("b", 0.2), ("c", 0.1)))
    )


async def authorized(request):
    assert request.scope_key == SCOPE.partition_key()
    assert len(request.payload_sha256) == 64
    return True


class Scorer:
    model_spec = SPEC

    def __init__(self, response=None, fail=None):
        self.response, self.fail, self.calls = response, fail, 0

    async def score(self, query, values):
        self.calls += 1
        assert type(values) is tuple
        assert all(type(value) is PairCandidate for value in values)
        if self.fail:
            raise self.fail
        return (
            self.response
            if self.response is not None
            else tuple(
                PairScore(value.memory_id, float(index)) for index, value in enumerate(values)
            )
        )


def reranker(scorer=None, **kwargs):
    return CompactPairReranker(
        scorer or Scorer(), authorize=authorized, enabled=True, contract_test_only=True, **kwargs
    )


def test_default_off_and_host_verified_exact_production_approval():
    scorer = Scorer()
    ranker = CompactPairReranker(scorer, authorize=authorized)
    result = asyncio.run(ranker.rank(SCOPE, "query", candidates()))
    assert result.candidates == candidates()
    assert result.trace.activation_mode == "disabled" and scorer.calls == 0
    with pytest.raises(ValueError, match="qualified"):
        CompactPairReranker(scorer, authorize=authorized, enabled=True)
    approval = RetrievalFeatureApproval(
        "pair-reranker", ranker.configuration_sha256, "b" * 64, "c" * 64, "disable-rank-v1"
    )
    for answer in (False, 1, "yes", None):
        with pytest.raises(PermissionError):
            CompactPairReranker(
                scorer,
                authorize=authorized,
                enabled=True,
                approval=approval,
                verify_approval=lambda _, answer=answer: answer,
            )
    enabled = CompactPairReranker(
        scorer,
        authorize=authorized,
        enabled=True,
        approval=approval,
        verify_approval=lambda value: value == approval,
    )
    assert enabled.activation_mode == "controlled-real"
    with pytest.raises(ValueError, match="configuration"):
        CompactPairReranker(
            scorer,
            authorize=authorized,
            enabled=True,
            max_candidates=2,
            approval=approval,
            verify_approval=lambda _: True,
        )
    with pytest.raises(ValueError, match="contract-only"):
        validate_feature_enablement(
            feature="pair-reranker",
            configuration_sha256="a" * 64,
            enabled=True,
            approval=approval,
            contract_test_only=True,
        )


def test_rank_selection_survives_pack_sort_preserves_original_scores_and_text():
    original = candidates()
    result = asyncio.run(reranker(max_candidates=2).rank(SCOPE, "query", original))
    assert [value.item.id for value in result.candidates] == ["b", "a", "c"]
    assert result.trace.scored_count == 2
    bundle = pack_memory_bundle(SCOPE, result.candidates, budget=BundleBudget(max_items=1)).bundle
    item = bundle.relevant_memories[0]
    assert item.id == "b" and item.text == "original b"
    assert item.metadata["fusion_score"] == 0.2
    assert item.metadata["pair_score"] == 1
    assert item.metadata["pair_rank"] == 1
    assert item.metadata["selection_score_kind"] == "pair-rank-order/1"
    assert bundle.citations[0].source_event_ids == ("source-b",)
    assert original == candidates()


def test_real_logit_adapter_retains_saturated_margin_order_and_validates_runtime_identity():
    async def inference(spec, query, pairs):
        assert spec == SPEC and query == "query"
        return (
            FinalTokenLogits("a", 1000, 0),
            FinalTokenLogits("b", 1001, 0),
            FinalTokenLogits("c", -1000, 0),
        )

    scorer = FinalTokenPairScorer(SPEC, inference)
    result = asyncio.run(reranker(scorer).rank(SCOPE, "query", candidates()))
    assert [value.item.id for value in result.candidates] == ["b", "a", "c"]
    assert result.candidates[0].pair_probability == result.candidates[1].pair_probability == 1
    assert result.candidates[0].pair_score > result.candidates[1].pair_score
    assert result.candidates[-1].pair_probability == 0


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), -float("inf"), "1"])
def test_scores_and_logits_reject_nonfinite_or_boolean_data(value):
    with pytest.raises(ValueError):
        PairScore("a", value)
    with pytest.raises(ValueError):
        FinalTokenLogits("a", value, 0)


@pytest.mark.parametrize(
    "response",
    [
        (),
        (PairScore("a", 1),),
        (PairScore("a", 1), PairScore("a", 1), PairScore("c", 1)),
        (PairScore("a", 1), PairScore("b", 1), PairScore("extra", 1)),
        [PairScore("a", 1), PairScore("b", 1), PairScore("c", 1)],
        (object(), object(), object()),
    ],
)
def test_malformed_scorer_output_is_rejected_with_explicit_degraded_fallback(response):
    original = candidates()
    result = asyncio.run(reranker(Scorer(response)).rank(SCOPE, "query", original))
    assert result.candidates == original
    assert result.trace.status == "degraded"


def test_host_can_neither_smuggle_invalid_frozen_scores_nor_rewrite_pair_payload():
    invalid = PairScore("a", 1)
    object.__setattr__(invalid, "score", True)
    result = asyncio.run(
        reranker(Scorer((invalid, PairScore("b", 2), PairScore("c", 3)))).rank(
            SCOPE, "query", candidates()
        )
    )
    assert result.trace.status == "degraded"

    class Rewriter(Scorer):
        async def score(self, query, values):
            object.__setattr__(values[0], "text", "rewritten")
            return await super().score(query, values)

    with pytest.raises(PairAuthorizationError, match="altered"):
        asyncio.run(reranker(Rewriter()).rank(SCOPE, "query", candidates()))


def test_no_authorization_or_revocation_is_fail_closed_even_after_scoring_error():
    for allowed_values in ((False,), (True, False)):
        calls = []

        async def authorize(request, allowed_values=allowed_values, calls=calls):
            value = allowed_values[min(len(calls), len(allowed_values) - 1)]
            calls.append(value)
            return value

        scorer = Scorer(fail=RuntimeError("do not echo private body"))
        ranker = CompactPairReranker(
            scorer, authorize=authorize, enabled=True, contract_test_only=True
        )
        with pytest.raises(PairAuthorizationError):
            asyncio.run(ranker.rank(SCOPE, "query", candidates()))
        assert scorer.calls == (0 if not allowed_values[0] else 1)


@pytest.mark.parametrize(
    "change", ["deleted", "archived", "untrusted", "missing", "text", "provenance", "scope"]
)
def test_governance_reresolves_after_scoring_and_fails_closed_on_stale_or_changed_sources(change):
    class Resolver:
        calls = 0

        async def resolve(self, scope, values):
            self.calls += 1
            if self.calls == 1:
                return tuple(GovernedCandidate(scope, value) for value in values)
            if change == "missing":
                return ()
            value = values[0]
            if change == "text":
                value = replace(value, item=replace(value.item, text="new version"))
            if change == "provenance":
                value = replace(value, source_event_ids=("new-source",))
            return (
                GovernedCandidate(
                    MemoryScope("other") if change == "scope" else scope,
                    value,
                    trusted=change != "untrusted",
                    deleted=change == "deleted",
                    archived=change == "archived",
                ),
            )

    pipeline = GovernedRecallPipeline(
        (loaded_retriever(SCOPE),), Resolver(), pair_reranker=reranker()
    )
    with pytest.raises(ScopeIsolationError):
        asyncio.run(pipeline.retrieve(MemoryQuery(SCOPE, "query"), ()))


def test_ordinary_scoring_error_still_gets_fresh_governance_and_degraded_trace():
    class Resolver:
        calls = 0

        async def resolve(self, scope, values):
            self.calls += 1
            return tuple(GovernedCandidate(scope, value) for value in values)

    resolver = Resolver()
    pipeline = GovernedRecallPipeline(
        (loaded_retriever(SCOPE),),
        resolver,
        pair_reranker=reranker(Scorer(fail=RuntimeError("private"))),
    )
    bundle = asyncio.run(pipeline.retrieve(MemoryQuery(SCOPE, "query"), ()))
    assert resolver.calls == 2 and len(bundle.episodes) == 1
    assert bundle.retrieval_metadata["degraded"] is True
    assert bundle.retrieval_metadata["pair_rerank"]["reason"] == "RuntimeError"
    assert "private" not in str(bundle.retrieval_metadata)


def test_mutated_model_spec_cannot_reuse_old_approval_digest():
    spec = replace(SPEC)

    class Mutating(Scorer):
        model_spec = spec

        async def score(self, query, pairs):
            object.__setattr__(spec, "model_sha256", "b" * 64)
            return await super().score(query, pairs)

    with pytest.raises(PairAuthorizationError, match="configuration"):
        asyncio.run(reranker(Mutating()).rank(SCOPE, "query", candidates()))


def test_final_authorizer_cannot_mutate_retained_scores_or_configuration():
    scores = (PairScore("a", 1), PairScore("b", 2), PairScore("c", 3))
    calls = []

    async def authorize(request):
        calls.append(request)
        if len(calls) == 2:
            object.__setattr__(scores[0], "score", float("nan"))
        return True

    ranker = CompactPairReranker(
        Scorer(scores), authorize=authorize, enabled=True, contract_test_only=True
    )
    result = asyncio.run(ranker.rank(SCOPE, "query", candidates()))
    assert [value.pair_score for value in result.candidates] == [3, 2, 1]
    assert all(value.pair_score == value.pair_score for value in result.candidates)


def test_authorizer_cannot_rewrite_scope_or_source_processing_request():
    async def authorize(request):
        object.__setattr__(request, "scope_key", "forged")
        return True

    ranker = CompactPairReranker(
        Scorer(), authorize=authorize, enabled=True, contract_test_only=True
    )
    with pytest.raises(PairAuthorizationError, match="request changed"):
        asyncio.run(ranker.rank(SCOPE, "query", candidates()))


def test_direct_rank_api_detects_mutation_of_candidate_metadata_during_await():
    values = candidates()

    class Mutating(Scorer):
        async def score(self, query, pairs):
            values[0].item.metadata["untouched"].append("mutated")
            return await super().score(query, pairs)

    with pytest.raises(PairAuthorizationError):
        asyncio.run(reranker(Mutating()).rank(SCOPE, "query", values))


def test_final_governance_await_cannot_mutate_retained_rank_result_metadata():
    class Resolver:
        saved = None

        async def resolve(self, scope, values):
            if self.saved is not None:
                self.saved[0].item.metadata["rewritten"] = "must not ship"
            self.saved = values
            return tuple(GovernedCandidate(scope, value) for value in values)

    pipeline = GovernedRecallPipeline(
        (loaded_retriever(SCOPE),), Resolver(), pair_reranker=reranker()
    )
    with pytest.raises(ScopeIsolationError, match="stale"):
        asyncio.run(pipeline.retrieve(MemoryQuery(SCOPE, "query"), ()))


@pytest.mark.parametrize("bound", ["provenance", "query", "text"])
def test_valid_fused_input_exceeding_scorer_bounds_degrades_without_truncation(bound):
    scorer = Scorer()
    values, query = candidates(), "query"
    if bound == "provenance":
        values = (replace(values[0], source_event_ids=tuple(f"source-{i}" for i in range(33))),)
    elif bound == "query":
        query = "x" * 8193
    else:
        values = (replace(values[0], item=replace(values[0].item, text="x" * 1_000_001)),)
    result = asyncio.run(reranker(scorer).rank(SCOPE, query, values))
    assert result.candidates == values and scorer.calls == 0
    assert result.trace.status == "degraded" and result.trace.reason == "input_bound"


def approved_ranker(scorer, verify, *, authorize=authorized):
    # Fabricated approval boundary fixture only, never controlled efficacy evidence.
    probe = CompactPairReranker(scorer, authorize=authorize)
    approval = RetrievalFeatureApproval(
        "pair-reranker", probe.configuration_sha256, "b" * 64, "c" * 64, "disable-rank"
    )
    return CompactPairReranker(
        scorer, authorize=authorize, enabled=True, approval=approval, verify_approval=verify
    )


def test_revoked_promotion_blocks_next_rank_without_inference():
    state, scorer = {"allowed": True}, Scorer()
    ranker = approved_ranker(scorer, lambda _: state["allowed"])
    state["allowed"] = False
    with pytest.raises(PairAuthorizationError, match="revoked"):
        asyncio.run(ranker.rank(SCOPE, "query", candidates()))
    assert scorer.calls == 0


@pytest.mark.parametrize(
    "stage", ["first-authorization", "score", "failed-score", "last-authorization"]
)
def test_revoked_promotion_after_any_rank_await_fails_closed(stage):
    state, calls = {"allowed": True}, []

    async def authorize(request):
        calls.append(request)
        if (
            stage == "first-authorization"
            and len(calls) == 1
            or stage == "last-authorization"
            and len(calls) == 2
        ):
            state["allowed"] = False
        return True

    class RevokingScorer(Scorer):
        async def score(self, query, pairs):
            if stage in ("score", "failed-score"):
                state["allowed"] = False
            if stage == "failed-score":
                raise RuntimeError("ordinary scoring error must not hide revocation")
            return await super().score(query, pairs)

    scorer = RevokingScorer()
    ranker = approved_ranker(scorer, lambda _: state["allowed"], authorize=authorize)
    with pytest.raises(PairAuthorizationError, match="revoked"):
        asyncio.run(ranker.rank(SCOPE, "query", candidates()))
    if stage == "first-authorization":
        assert scorer.calls == 0


@pytest.mark.parametrize("change", ["bound", "digest", "scorer", "approval", "verifier", "mode"])
def test_live_promotion_rejects_configuration_or_verification_rebinding(change):
    ranker = approved_ranker(Scorer(), lambda _: True)
    if change == "bound":
        ranker._max_candidates = 1
    elif change == "digest":
        ranker.configuration_sha256 = "d" * 64
    elif change == "scorer":
        ranker._scorer = Scorer()
    elif change == "approval":
        ranker._approval = replace(ranker._approval, rollback_id="replacement")
    elif change == "verifier":
        ranker._verify_approval = lambda _: True
    else:
        ranker.activation_mode = "contract-test-only"
    with pytest.raises(PairAuthorizationError, match="changed"):
        asyncio.run(ranker.rank(SCOPE, "query", candidates()))


def test_final_pipeline_governance_await_cannot_outlive_promotion_revocation():
    state = {"allowed": True}

    class Resolver:
        calls = 0

        async def resolve(self, scope, values):
            self.calls += 1
            if self.calls == 2:
                state["allowed"] = False
            return tuple(GovernedCandidate(scope, value) for value in values)

    pipeline = GovernedRecallPipeline(
        (loaded_retriever(SCOPE),),
        Resolver(),
        pair_reranker=approved_ranker(Scorer(), lambda _: state["allowed"]),
    )
    with pytest.raises(PairAuthorizationError, match="revoked"):
        asyncio.run(pipeline.retrieve(MemoryQuery(SCOPE, "query"), ()))


def test_final_synchronous_promotion_callback_precedes_expiry_guard():
    from datetime import timedelta

    class Clock:
        value = NOW

        def now(self):
            return self.value

    clock, state = Clock(), {"final": False}

    class Resolver:
        calls = 0

        async def resolve(self, scope, values):
            self.calls += 1
            state["final"] = self.calls == 2
            return tuple(
                GovernedCandidate(scope, value, valid_to=NOW + timedelta(seconds=1))
                for value in values
            )

    def verify(approval):
        if state["final"]:
            clock.value = NOW + timedelta(seconds=2)
        return True

    plugin = loaded_retriever(SCOPE)
    plugin = replace(plugin, context=replace(plugin.context, clock=clock))
    pipeline = GovernedRecallPipeline(
        (plugin,), Resolver(), pair_reranker=approved_ranker(Scorer(), verify)
    )
    with pytest.raises(ScopeIsolationError, match="stale"):
        asyncio.run(pipeline.retrieve(MemoryQuery(SCOPE, "query"), ()))


@pytest.mark.parametrize("target", ["approval", "model"])
def test_synchronous_verifier_cannot_mutate_shared_frozen_approval_or_spec(target):
    spec = replace(SPEC)
    scorer = Scorer()
    scorer.model_spec = spec
    calls = []

    def verify(approval):
        calls.append(approval)
        if len(calls) > 1:
            if target == "approval":
                object.__setattr__(approval, "evidence_sha256", "f" * 64)
            else:
                object.__setattr__(spec, "runtime_sha256", "f" * 64)
        return True

    ranker = approved_ranker(scorer, verify)
    with pytest.raises(PairAuthorizationError, match="changed"):
        asyncio.run(ranker.rank(SCOPE, "query", candidates()))
    assert scorer.calls == 0
