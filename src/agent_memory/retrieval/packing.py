"""Deterministic, non-generative packing for the native kernel retrieval path."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from hashlib import sha256
from json import dumps

from ..domain import Citation, Claim, MemoryItem, MemoryKind, MemoryQuery
from .analyzer import lexical_terms


def token_estimate(text: str) -> int:
    ascii_chars = sum(ord(char) < 128 for char in text)
    return max(1, (ascii_chars + 3) // 4 + len(text) - ascii_chars)


def source_ids(value: Claim | MemoryItem) -> tuple[str, ...]:
    sources = (
        value.provenance.source_event_ids
        if isinstance(value, Claim)
        else value.metadata.get("source_event_ids", ())
    )
    if not isinstance(sources, (list, tuple)):
        return ()
    return tuple(dict.fromkeys(source for source in sources if isinstance(source, str) and source))


def contiguous_excerpt(text: str, query: str, max_characters: int) -> tuple[str, int, int]:
    """Return one exact substring; offsets always refer to the supplied text."""
    size = max(0, min(len(text), max_characters))
    if size == 0:
        return "", 0, 0
    if size == len(text):
        return text, 0, len(text)
    terms = set(lexical_terms(query))
    starts = {0}
    # Bounded candidate windows. Regex offsets are in the original string, even
    # when Unicode casefold changes length (e.g. sharp s).
    if terms:
        pattern = "|".join(re.escape(term) for term in sorted(terms, key=lambda x: (-len(x), x)))
        for index, match in enumerate(re.finditer(pattern, text, re.IGNORECASE)):
            if index >= 32:
                break
            starts.add(min(max(0, match.start() - size // 3), len(text) - size))
    start = max(
        starts,
        key=lambda offset: (
            len(terms.intersection(lexical_terms(text[offset : offset + size]))),
            -offset,
        ),
    )
    return text[start : start + size], start, start + size


def _indexed_event_span(item: MemoryItem) -> dict | None:
    """Validate the B2 EVENT locator format, not source authenticity.

    The repository remains the authority for the full-source hash. Only when
    the full source is present can its hash be independently checked here.
    """
    span = item.metadata.get("source_span")
    source_chars = item.metadata.get("source_chars")
    revision = item.metadata.get("source_revision")
    chunk_id = item.metadata.get("lexical_chunk_id")
    if not isinstance(span, Mapping) or span.get("unit") != "characters":
        return None
    start, end = span.get("start"), span.get("end")
    if (
        type(start) is not int
        or type(end) is not int
        or type(source_chars) is not int
        or not 0 <= start < end <= source_chars
        or end - start != len(item.text)
        or len(item.text) > 1024
    ):
        return None
    if (
        not isinstance(revision, str)
        or not re.fullmatch(r"[0-9a-f]{64}", revision)
        or not isinstance(chunk_id, str)
    ):
        return None
    expected = sha256(dumps(["events", item.id, revision, start, end]).encode("utf-8")).hexdigest()
    if chunk_id != expected:
        return None
    if (
        start == 0
        and end == source_chars
        and sha256(item.text.encode("utf-8")).hexdigest() != revision
    ):
        return None
    return {"start": start, "end": end, "unit": "characters"}


def _indexed_event_variants(item: MemoryItem, query: str) -> tuple[MemoryItem, ...]:
    """At most eleven exact contiguous variants of one <=1024-character chunk."""
    original_span = _indexed_event_span(item)
    if original_span is None:
        return ()
    result = [item]
    chunk_text_hash = sha256(item.text.encode("utf-8")).hexdigest()
    width = len(item.text) // 2
    while width:
        text, start, end = contiguous_excerpt(item.text, query, width)
        metadata = dict(item.metadata)
        chunk_id = metadata.pop("lexical_chunk_id")
        metadata.update(
            {
                "excerpt": True,
                "retrieved_lexical_chunk_id": chunk_id,
                "retrieved_source_span": original_span,
                "retrieved_chunk_text_sha256": chunk_text_hash,
                "source_span": {
                    "start": original_span["start"] + start,
                    "end": original_span["start"] + end,
                    "unit": "characters",
                },
            }
        )
        result.append(replace(item, text=text, metadata=metadata))
        width //= 2
    return tuple(result)


def fit_event(item: MemoryItem, query: str, available: int) -> MemoryItem | None:
    """Validate located events; only directly excerpt complete legacy events."""
    cost = token_estimate(item.text)
    # Indexed narrowing is handled separately through verified variants.
    located = any(key in item.metadata for key in ("source_span", "lexical_chunk_id"))
    if item.kind == MemoryKind.EVENT and located and _indexed_event_span(item) is None:
        return None
    if item.kind != MemoryKind.EVENT or located or item.metadata.get("excerpt"):
        return item if cost <= available else None
    if len(item.text) <= 1200 and cost <= available:
        return item
    if available < 64:
        return None
    excerpt, start, end = contiguous_excerpt(item.text, query, min(1200, available))
    if not excerpt or token_estimate(excerpt) > available:
        return None
    return replace(
        item,
        text=excerpt,
        metadata={
            **item.metadata,
            "excerpt": True,
            "source_revision": sha256(item.text.encode("utf-8")).hexdigest(),
            "source_chars": len(item.text),
            "source_span": {"start": start, "end": end, "unit": "characters"},
        },
    )


@dataclass(frozen=True, slots=True)
class NativePackingResult:
    state: tuple[Claim, ...]
    items: tuple[MemoryItem, ...]
    citations: tuple[Citation, ...]
    tokens: int
    metadata: dict


@dataclass(frozen=True, slots=True)
class _PreparedEvidence:
    value: Claim | MemoryItem
    cost: int
    terms: frozenset[str]
    sources: tuple[str, ...]
    event_signature: tuple | None


def pack_native_evidence(
    query: MemoryQuery,
    current: Sequence[Claim],
    ranked: Sequence[MemoryItem],
) -> NativePackingResult:
    """Greedy feasible term/source coverage, with one-time text analysis.

    Sources are not truth units: different claims from the same source are never
    collapsed. The full input pool is analyzed once without pre-truncating
    conflicts; selection rescans cached sets for at most 64 + query.limit rounds.
    Coverage is only a heuristic over retrieved evidence, not a corpus proof.
    """
    terms = set(lexical_terms(query.text))

    def prepare(value: Claim | MemoryItem) -> _PreparedEvidence:
        sources = source_ids(value)
        signature = None
        if isinstance(value, MemoryItem) and value.kind == MemoryKind.EVENT and sources:
            signature = (
                sources,
                repr(value.metadata.get("source_revision")),
                repr(value.metadata.get("source_span")),
                value.text,
            )
        return _PreparedEvidence(
            value,
            token_estimate(value.text),
            frozenset(terms.intersection(lexical_terms(value.text))),
            sources,
            signature,
        )

    prepared_state = [prepare(claim) for claim in current] if query.include_current_state else []
    ordered_state = sorted(
        prepared_state,
        key=lambda entry: (
            -len(entry.terms),
            -entry.value.importance,
            -entry.value.confidence,
            entry.value.id,
        ),
    )
    ordered_items = sorted(ranked, key=lambda item: (-item.score, item.id))
    prepared_items = []
    item_variants: dict[str, tuple[_PreparedEvidence, ...]] = {}
    infeasible = 0

    def feasible_variant(identity: str, available: int) -> _PreparedEvidence | None:
        variants = item_variants.get(identity, ())
        # Preserve full context whenever it fits. A slice can accidentally turn
        # a word prefix into a query token; that is never a reason to discard
        # an otherwise feasible original chunk.
        if variants and variants[0].cost <= available:
            return variants[0]
        fitting = (entry for entry in variants[1:] if entry.cost <= available)
        return max(fitting, key=lambda entry: (len(entry.terms), entry.cost), default=None)

    for item in ordered_items:
        if item.id in item_variants:
            continue
        located = any(key in item.metadata for key in ("source_span", "lexical_chunk_id"))
        if item.kind == MemoryKind.EVENT and located:
            variants = _indexed_event_variants(item, query.text)
        else:
            # One initial-budget excerpt of each legacy complete source. Only
            # bounded indexed chunks get multiple precomputed variants.
            fitted = fit_event(item, query.text, query.token_budget)
            variants = () if fitted is None else (fitted,)
        item_variants[item.id] = tuple(prepare(variant) for variant in variants)
        primary = feasible_variant(item.id, query.token_budget)
        if primary is None:
            infeasible += 1
        else:
            prepared_items.append(primary)
    state_by_id = {entry.value.id: entry for entry in ordered_state}
    entries: list[tuple[_PreparedEvidence, int]] = []
    seen_ids: set[str] = set()
    for rank, entry in enumerate([*prepared_items, *ordered_state], 1):
        if entry.value.id in seen_ids:
            continue
        seen_ids.add(entry.value.id)
        entries.append((state_by_id.get(entry.value.id, entry), rank))
    covered_terms: set[str] = set()
    covered_sources: set[str] = set()
    claim_sources: set[str] = set()
    event_signatures: set[tuple] = set()
    state: list[Claim] = []
    selected: list[MemoryItem] = []
    citations: list[Citation] = []
    used_tokens = duplicate_sources = 0
    while entries and used_tokens < query.token_budget:
        feasible = []
        remaining = []
        for original, rank in entries:
            entry = original
            is_state = isinstance(entry.value, Claim)
            available = query.token_budget - used_tokens
            if is_state and (len(state) >= 64 or entry.cost > available):
                # Preserve a feasible, already indexed representation of a
                # full state claim under its same original memory ID.
                entry = feasible_variant(entry.value.id, available)
                is_state = False
            elif not is_state and entry.cost > available:
                entry = feasible_variant(entry.value.id, available)
            if entry is None:
                infeasible += 1
                continue
            if not is_state and len(selected) >= query.limit:
                continue
            if entry.cost > available:
                infeasible += 1
                continue
            if entry.event_signature is not None and entry.event_signature in event_signatures:
                duplicate_sources += 1
                continue
            score = (
                len(entry.terms - covered_terms) / max(1, len(terms))
                + len(entry.terms) / max(1, len(terms))
                + (
                    0.15
                    if entry.event_signature is not None
                    and claim_sources.intersection(entry.sources)
                    else 0
                )
                + (0.1 if set(entry.sources) - covered_sources else 0)
                + 1 / (60 + rank)
            )
            feasible.append((score, -rank, entry))
            remaining.append((original, rank))
        if not feasible:
            break
        _, _, entry = max(feasible, key=lambda row: (row[0], row[1]))
        value = entry.value
        if isinstance(value, Claim):
            state.append(value)
            claim_sources.update(entry.sources)
        else:
            selected.append(value)
            if value.kind == MemoryKind.CLAIM:
                claim_sources.update(entry.sources)
        if entry.event_signature is not None:
            event_signatures.add(entry.event_signature)
        citations.append(Citation(value.id, entry.sources))
        covered_terms.update(entry.terms)
        covered_sources.update(entry.sources)
        used_tokens += entry.cost
        entries = [
            (candidate, rank) for candidate, rank in remaining if candidate.value.id != value.id
        ]
    return NativePackingResult(
        tuple(state),
        tuple(selected),
        tuple(citations),
        used_tokens,
        {
            "strategy": "feasible_evidence_coverage_v1",
            "coverage": "partial",
            "world_negative": False,
            "packing_query_terms_covered": len(covered_terms),
            "packing_query_terms_total": len(terms),
            "packing_source_count": len(covered_sources),
            "packing_duplicate_sources": duplicate_sources,
            "packing_infeasible": infeasible,
            "packing_analyzed_state": len(prepared_state),
            "packing_analyzed_items": len(prepared_items),
        },
    )
