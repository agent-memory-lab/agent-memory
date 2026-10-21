"""One opt-in bounded reranker call over exact, feasible evidence text."""
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from json import dumps
from math import isfinite

from .analyzer import query_content_anchors
from .candidate_identity import candidate_identity
from .evidence_units import evidence_proof

MAX_PASSAGES = 48


class PassageRerankingError(ValueError):
    pass


class GeneratedPassageReranker:
    """Validate an externally generated permutation; never generate evidence.

    The generator implements ``rank_passages(query, records)``. API credentials,
    model selection, accounting and request transport remain with the host.
    """
    def __init__(self, generator, *, allow_unranked_tail: bool = False):
        if type(allow_unranked_tail) is not bool:
            raise ValueError("allow_unranked_tail must be a boolean")
        self._generator = generator
        self._allow_unranked_tail = allow_unranked_tail

    async def rerank(self, query, candidates):
        if not candidates:
            return ()
        records = tuple({"id": f"P{index}", "kind": value.kind.value, "text": value.text}
                        for index, value in enumerate(candidates))
        result = await self._generator.rank_passages(query, records)
        order = result.get("ranked_ids") if isinstance(result, Mapping) else None
        expected = {record["id"] for record in records}
        if (not isinstance(order, (tuple, list)) or not order
                or any(not isinstance(identity, str) for identity in order)
                or len(set(order)) != len(order) or not set(order) <= expected
                or not self._allow_unranked_tail and set(order) != expected):
            requirement = "a nonempty unique subset of supplied IDs" if self._allow_unranked_tail else "every supplied ID exactly once"
            raise PassageRerankingError(f"generated ranking must contain {requirement}")
        by_id = {record["id"]: value for record, value in zip(records, candidates, strict=True)}
        missing = [record["id"] for record in records if record["id"] not in order]
        ranked = set(order)
        return tuple(replace(by_id[identity], score=1/(rank+1), metadata={
            **by_id[identity].metadata, "passage_rank_status": "ranked" if identity in ranked else "unranked_tail",
            "passage_rank_protocol": "generated-partial-order/2" if self._allow_unranked_tail else "generated-permutation/1",
        }) for rank, identity in enumerate((*order, *missing)))


def passage_key(value):
    return "passage:" + sha256(dumps(evidence_proof(value), sort_keys=True).encode()).hexdigest()


def query_key(query):
    return sha256(dumps(asdict(query), sort_keys=True, default=str).encode()).hexdigest()


def _snapshot(value):
    if isinstance(value, Mapping):
        return {key: _snapshot(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return tuple(_snapshot(item) for item in value)
    return value


def _binding(value):
    protected = ("event_type", "key", "version", "channel", "lexical_chunk_id",
                 "retrieved_lexical_chunk_id", "retrieved_source_span", "retrieved_chunk_text_sha256")
    return dumps([evidence_proof(value), value.kind, str(value.occurred_at),
                  {key: value.metadata.get(key) for key in protected}], sort_keys=True, default=str)


@dataclass(frozen=True, slots=True)
class PassageRanking:
    query_binding: str
    items: Mapping
    relevance: Mapping
    diagnostics: Mapping
    ranked_ids: frozenset = frozenset()


def feasible_passages(query, candidates):
    from .packing import _indexed_event_variants, fit_event, token_estimate
    from ..domain import MemoryKind

    selected, seen = {}, set()
    share = query.token_budget / max(1, query.limit)
    text = " ".join(query_content_anchors(query.text)) or query.text
    for value in candidates:
        key = candidate_identity(value)
        if key in seen or value.metadata.get("context_only") is True:
            continue
        seen.add(key)
        if value.kind == MemoryKind.EVENT and any(k in value.metadata for k in ("source_span", "lexical_chunk_id")):
            variants = _indexed_event_variants(value, text)
        else:
            fitted = fit_event(value, text, query.token_budget)
            variants = () if fitted is None else (fitted,)
        fitting = [v for v in variants if token_estimate(v.text) <= query.token_budget]
        preferred = [v for v in fitting if token_estimate(v.text) <= share]
        if fitting:
            selected[key] = max(preferred, key=lambda v: token_estimate(v.text)) if preferred else min(
                fitting, key=lambda v: token_estimate(v.text))
        if len(selected) == MAX_PASSAGES:
            break
    return selected


async def rerank_passages(query, candidates, provider):
    binding = query_key(query)
    selected = feasible_passages(query, candidates)
    if not selected:
        return PassageRanking(binding, {}, {}, {"status": "empty", "reranker_calls": 0, "scored_count": 0})
    # Scoring IDs are ephemeral. No source locator, text or authority returned by
    # the provider is accepted into the bundle; only checked ordering is used.
    requests = {passage_key(value): replace(value, id=passage_key(value), metadata=_snapshot(value.metadata))
                for value in selected.values()}
    expected = {key: _binding(value) for key, value in requests.items()}
    try:
        response = tuple(await provider.rerank(query, tuple(requests.values())))
        if len(response) != len(requests) or {v.id for v in response} != set(requests):
            raise PassageRerankingError("passage reranker omitted or duplicated evidence")
        for value in response:
            if not isfinite(value.score) or _binding(value) != expected[value.id]:
                raise PassageRerankingError("passage reranker changed bound evidence")
        if query_key(query) != binding:
            raise PassageRerankingError("passage reranker changed the bound query")
        response = sorted(response, key=lambda value: -value.score)
    except PassageRerankingError:
        raise
    except Exception as error:
        raise PassageRerankingError("passage reranker failed; no automatic retry") from error
    parent_for = {passage_key(value): parent for parent, value in selected.items()}
    ordered = [parent_for[value.id] for value in response]
    ranked_ids = frozenset(parent_for[value.id] for value in response
                           if value.metadata.get("passage_rank_status") != "unranked_tail")
    # One request-local ordinal scale for scored representations; the unscored
    # tail stays below it. Provider scores never mix with parent score scales.
    tail = list(dict.fromkeys(candidate_identity(value) for value in candidates
                             if candidate_identity(value) not in selected
                             and value.metadata.get("context_only") is not True))
    relevance = {key: (len(ordered)+len(tail)-rank)/(len(ordered)+len(tail))
                 for rank, key in enumerate((*ordered, *tail))}
    return PassageRanking(binding, selected, relevance, {"status": "applied", "reranker_calls": 1,
        "scored_count": len(ranked_ids), "submitted_count": len(selected), "candidate_limit": MAX_PASSAGES,
        "input_characters": sum(len(v.text) for v in selected.values()),
        "unscored_tail_count": len(tail)+len(selected)-len(ranked_ids), "provider": type(provider).__name__,
        "provider_unranked_count": len(selected)-len(ranked_ids),
        "score_policy": "request_local_passage_ordinal/1", "entailment_verified": False}, ranked_ids)
