"""Frozen native packing policies, independent of reader and recall settings."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class _PackingPolicy:
    version: str
    strategy: str
    experimental: bool = False
    content_anchors: bool = False
    discount_narrowing: bool = False
    discount_context: bool = False
    topic_novelty_weight: float = 0.0
    topic_repetition_weight: float = 0.0
    cost_weight: float = 0.0
    relevance_frontier: float | None = None
    expand_selected_context: bool = False
    dependency_units: bool = False
    coverage_reservations: bool = False


_V2 = _PackingPolicy("v2", "relevance_bounded_diversity_v2")
_V3 = _PackingPolicy(
    "v3", "support_cost_bounded_v3_r3", experimental=True,
    content_anchors=True, discount_narrowing=True, discount_context=True,
    topic_novelty_weight=0.08, topic_repetition_weight=0.08, cost_weight=0.10,
    relevance_frontier=0.12, expand_selected_context=True,
)
_V4 = _PackingPolicy("v4", "source_context_units_v4", experimental=True, dependency_units=True)
_V5 = _PackingPolicy("v5", "coverage_passage_units_v5", experimental=True,
                     dependency_units=True, coverage_reservations=True)
PACKING_VERSIONS = (_V2.version, _V3.version, _V4.version, _V5.version)


def _packing_policy(version: str) -> _PackingPolicy:
    if version == "v2":
        return _V2
    if version == "v3":
        return _V3
    if version == "v4":
        return _V4
    if version == "v5":
        return _V5
    raise ValueError("unsupported native packing version; expected v2, v3, v4 or v5")


def packing_contract(version: str = "v2") -> dict:
    """Return an independent copy of the exact selected policy parameters."""
    return {"schema": "native-evidence-packing/1", **asdict(_packing_policy(version))}
