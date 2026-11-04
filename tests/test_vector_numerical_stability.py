"""Extreme finite embeddings retain direction without overflow or underflow."""

import asyncio
from datetime import UTC, datetime
from hashlib import sha256
from math import hypot, isfinite, sqrt
from sys import float_info

import pytest

from agent_memory import (
    AgentMemory,
    EmbeddingReranker,
    MemoryItem,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
)
from agent_memory.extensions.registry import PluginError
from agent_memory.retrieval.evidence_research_adapters import (
    FrozenQueryVectors,
    SQLiteVectorResearchRoute,
    VectorSource,
    _vector,
)
from agent_memory.retrieval.semantic import (
    SemanticVectorSpec,
    VectorNormalization,
    _validated_vector,
)

SCOPE = MemoryScope("stable-vectors", session_id="one")
SPEC = SemanticVectorSpec("fixture", "vectors", 2, VectorNormalization.L2, "v1")
SCALES = (float_info.max, 1e200, 1.0, 1e-200, float_info.min, 5e-324)


@pytest.mark.parametrize("scale", SCALES)
@pytest.mark.parametrize(
    ("direction", "expected"),
    (((1, 1), 1.0), ((-1, -1), -1.0), ((1, -1), 0.0), ((1, 0), 1 / sqrt(2))),
)
def test_cosine_preserves_direction_across_finite_float_range(scale, direction, expected):
    actual = EmbeddingReranker._cosine_similarity(
        (scale, scale), tuple(scale * value for value in direction),
    )
    assert isfinite(actual)
    assert -1.0 <= actual <= 1.0
    assert actual == pytest.approx(expected, abs=1e-15)


@pytest.mark.parametrize("left_scale", (1e300, 1.0, 1e-300))
@pytest.mark.parametrize("right_scale", (1e300, 1.0, 1e-300))
def test_cosine_is_invariant_to_independent_vector_scales(left_scale, right_scale):
    assert EmbeddingReranker._cosine_similarity(
        (3 * left_scale, 4 * left_scale), (4 * right_scale, 3 * right_scale),
    ) == pytest.approx(0.96, abs=1e-15)


@pytest.mark.parametrize(
    "normalize", [lambda v: _validated_vector(v, SPEC), lambda v: _vector(v, 2)],
)
@pytest.mark.parametrize("scale", SCALES)
def test_normalization_preserves_unit_length_for_extreme_finite_vectors(normalize, scale):
    actual = normalize((scale, -scale))
    assert all(isfinite(value) for value in actual)
    assert hypot(*actual) == pytest.approx(1.0, abs=1e-15)
    assert actual == pytest.approx((1 / sqrt(2), -1 / sqrt(2)), abs=1e-15)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), 10**1000])
def test_invalid_components_remain_rejected(bad):
    with pytest.raises((ValueError, OverflowError)):
        EmbeddingReranker._cosine_similarity((bad, 1.0), (1.0, 0.0))
    with pytest.raises(PluginError, match="non-finite"):
        _validated_vector((bad, 1.0), SPEC)
    with pytest.raises(ValueError, match="invalid_semantic_vector"):
        _vector((bad, 1.0), 2)


def test_zero_vector_policies_and_unnormalized_vectors_are_unchanged():
    with pytest.raises(ValueError, match="zero vectors"):
        EmbeddingReranker._cosine_similarity((0.0, 0.0), (1.0, 0.0))
    with pytest.raises(PluginError, match="zero vector"):
        _validated_vector((0.0, 0.0), SPEC)
    assert _vector((0.0, 0.0), 2) == (0.0, 0.0)
    raw_spec = SemanticVectorSpec("fixture", "vectors", 2, VectorNormalization.NONE, "v1")
    assert _validated_vector((float_info.max, -float_info.max), raw_spec) == (
        float_info.max, -float_info.max,
    )


@pytest.mark.parametrize("weight_name", ("lexical_weight", "semantic_weight"))
@pytest.mark.parametrize("bad", (float("nan"), float("inf"), -float("inf"), -1.0,
                                 True, "1.0", 10**1000))
def test_reranker_rejects_invalid_weights_at_construction(weight_name, bad):
    with pytest.raises(ValueError, match="finite non-negative"):
        EmbeddingReranker(object(), **{weight_name: bad})


def test_reranker_rejects_all_zero_weights():
    with pytest.raises(ValueError, match="must be positive"):
        EmbeddingReranker(object(), lexical_weight=0.0, semantic_weight=0.0)


@pytest.mark.parametrize("bad", (True, 1.0, 48.5, float("nan"), float("inf"), 0, 501))
def test_reranker_rejects_invalid_candidate_budget_at_construction(bad):
    with pytest.raises(ValueError, match="max_candidates"):
        EmbeddingReranker(object(), max_candidates=bad)


@pytest.mark.parametrize("weight", (float_info.max, 1e200, 1e-200, 5e-324))
def test_reranker_weights_preserve_scale_invariant_positive_fusion_scores(weight):
    class Embeddings:
        async def embed(self, texts):
            return ((1.0, 0.0), (1.0, 0.0), (0.0, 1.0))

    async def run():
        now = datetime.now(UTC)
        candidates = (
            MemoryItem("first", MemoryKind.EVENT, "first", 2.0, now),
            MemoryItem("second", MemoryKind.EVENT, "second", 1.0, now),
        )
        query = MemoryQuery(SCOPE, "direction")
        baseline = await EmbeddingReranker(
            Embeddings(), lexical_weight=1.0, semantic_weight=1.0, fail_open=False,
        ).rerank(query, candidates)
        ranked = await EmbeddingReranker(
            Embeddings(), lexical_weight=weight, semantic_weight=weight, fail_open=False,
        ).rerank(query, candidates)
        assert [item.id for item in ranked] == [item.id for item in baseline]
        assert [item.score for item in ranked] == pytest.approx([item.score for item in baseline])
        assert all(isfinite(item.score) and item.score > 0.0 for item in ranked)

    asyncio.run(run())


@pytest.mark.parametrize("scale", (float_info.max, 5e-324))
def test_reranker_distinguishes_antiparallel_and_orthogonal_extreme_vectors(scale):
    class Embeddings:
        async def embed(self, texts):
            return (
                (scale, scale),
                (-scale, -scale),
                (scale, -scale),
                (scale, scale),
            )

    async def run():
        now = datetime.now(UTC)
        candidates = tuple(
            MemoryItem(name, MemoryKind.EVENT, name, score, now)
            for name, score in (("opposite", 3.0), ("orthogonal", 2.0), ("same", 1.0))
        )
        ranked = await EmbeddingReranker(
            Embeddings(), lexical_weight=0.0, semantic_weight=1.0, fail_open=False,
        ).rerank(MemoryQuery(SCOPE, "direction"), candidates)
        assert [item.id for item in ranked] == ["same", "orthogonal", "opposite"]
        assert [item.metadata["semantic_score"] for item in ranked] == pytest.approx(
            [1.0, 0.5, 0.0], abs=1e-15,
        )
        assert all(isfinite(item.score) for item in ranked)

    asyncio.run(run())


@pytest.mark.parametrize("scale", (float_info.max, 5e-324))
def test_explicit_vector_route_recalls_zero_lexical_overlap_with_extreme_vectors(tmp_path, scale):
    async def run():
        text = "Mira purchased a scarlet automobile."
        async with AgentMemory.local(tmp_path / "memory.db", scope=SCOPE) as memory:
            stored = await memory.remember(text)
            query = MemoryQuery(SCOPE, "red car owner")
            repository = memory.provider._repository
            assert await repository.search(query, 8) == ()
            route = SQLiteVectorResearchRoute(
                repository,
                FrozenQueryVectors(SPEC, {query.text: (scale, scale)}),
                (VectorSource(
                    stored.event_id, sha256(text.encode()).hexdigest(), (scale, scale),
                ),),
            )
            bundle = await route.retrieve(query, None)
            assert [item.id for item in bundle.relevant_memories] == [stored.event_id]
            assert bundle.relevant_memories[0].score == pytest.approx(1.0, abs=1e-15)
            assert bundle.retrieval_metadata["lexical_gate"] is False
            assert bundle.retrieval_metadata["delivery_guard"]["status"] == "validated"

    asyncio.run(run())
