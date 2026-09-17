"""Bounded lexical candidate ranking over host-authorized memory items.

The caller is responsible for Trusted Scope visibility and evidence lookup.
This module neither keeps an index nor changes the default memory provider.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from math import log
import re

from .domain import MemoryChannel, MemoryItem
from .plugin_protocol import RetrievalCandidate


_WORDS = re.compile(r"[a-z0-9]+|[\u3400-\u9fff]+")


def _terms(text: str) -> tuple[str, ...]:
    tokens: list[str] = []
    for word in _WORDS.findall(text.casefold()):
        if "\u3400" <= word[0] <= "\u9fff":
            tokens.extend(word)
            tokens.extend(word[index : index + 2] for index in range(len(word) - 1))
        else:
            tokens.append(word)
    return tuple(tokens)


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    item: MemoryItem
    channel: MemoryChannel
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LexicalSearchTrace:
    input_count: int
    indexed_count: int
    rejected_count: int
    conflict_count: int
    matched_count: int
    returned_count: int


@dataclass(frozen=True, slots=True)
class LexicalSearchResult:
    candidates: tuple[RetrievalCandidate, ...]
    trace: LexicalSearchTrace


def lexical_candidates(
    query_text: str,
    evidence_items: Sequence[EvidenceItem],
    *,
    limit: int = 8,
    max_items: int = 128,
    max_item_chars: int = 2_048,
) -> LexicalSearchResult:
    """Rank a preauthorized, finite candidate pool with deterministic BM25.

    Han text uses unigrams and bigrams; ASCII words use casefolded tokens.
    No-hit items never become evidence just to fill the requested limit.
    """

    if not isinstance(query_text, str) or len(query_text) > 512:
        raise ValueError("query_text must be a string of at most 512 characters")
    if isinstance(evidence_items, (str, bytes)) or not isinstance(evidence_items, Sequence):
        raise ValueError("evidence_items must be a finite sequence")
    if type(limit) is not int or not 1 <= limit <= 128:
        raise ValueError("limit must be between 1 and 128")
    if type(max_items) is not int or not 1 <= max_items <= 512:
        raise ValueError("max_items must be between 1 and 512")
    if type(max_item_chars) is not int or not 1 <= max_item_chars <= 8_192:
        raise ValueError("max_item_chars must be between 1 and 8192")
    if len(evidence_items) > max_items:
        raise ValueError("authorized candidate pool exceeds max_items")

    indexed: dict[str, tuple[EvidenceItem, Counter[str]]] = {}
    sources: dict[str, set[str]] = {}
    conflicting: set[str] = set()
    rejected = 0
    for entry in evidence_items:
        if (
            not isinstance(entry, EvidenceItem)
            or not isinstance(entry.item, MemoryItem)
            or not isinstance(entry.channel, MemoryChannel)
            or not entry.item.id
            or not isinstance(entry.item.text, str)
            or len(entry.item.text) > max_item_chars
            or not entry.source_event_ids
            or len(entry.source_event_ids) > 32
            or any(not isinstance(event_id, str) or not event_id.strip() for event_id in entry.source_event_ids)
        ):
            rejected += 1
            continue
        previous = indexed.get(entry.item.id)
        if previous is not None:
            if (
                previous[0].item.kind != entry.item.kind
                or previous[0].item.text != entry.item.text
                or previous[0].channel != entry.channel
            ):
                conflicting.add(entry.item.id)
                continue
            sources[entry.item.id].update(entry.source_event_ids)
            continue
        indexed[entry.item.id] = (entry, Counter(_terms(entry.item.text)))
        sources[entry.item.id] = set(entry.source_event_ids)

    for item_id in conflicting:
        indexed.pop(item_id, None)
        sources.pop(item_id, None)

    query_terms = set(_terms(query_text))
    document_count = len(indexed)
    average_length = (
        sum(sum(frequencies.values()) for _, frequencies in indexed.values()) / document_count
        if document_count else 0.0
    )
    document_frequency: Counter[str] = Counter()
    for _, frequencies in indexed.values():
        document_frequency.update(query_terms.intersection(frequencies))

    ranked: list[tuple[float, EvidenceItem, tuple[str, ...]]] = []
    for item_id, (entry, frequencies) in indexed.items():
        length = sum(frequencies.values())
        if not length or not average_length:
            continue
        score = 0.0
        for term in query_terms.intersection(frequencies):
            tf = frequencies[term]
            idf = log(1 + (document_count - document_frequency[term] + 0.5) / (document_frequency[term] + 0.5))
            score += idf * (tf * 2.2) / (tf + 1.2 * (0.25 + 0.75 * length / average_length))
        if score > 0:
            ranked.append((score, entry, tuple(sorted(sources[item_id]))))

    ranked.sort(key=lambda row: (-row[0], row[1].item.id))
    selected = tuple(
        RetrievalCandidate(
            item=entry.item,
            channel=entry.channel,
            rank=rank,
            source_event_ids=event_ids,
            retriever="builtin.lexical.bm25",
            retrieval_method="lexical",
            metadata={"lexical_score": score},
        )
        for rank, (score, entry, event_ids) in enumerate(ranked[:limit], start=1)
    )
    return LexicalSearchResult(
        candidates=selected,
        trace=LexicalSearchTrace(
            input_count=len(evidence_items),
            indexed_count=document_count,
            rejected_count=rejected,
            conflict_count=len(conflicting),
            matched_count=len(ranked),
            returned_count=len(selected),
        ),
    )
