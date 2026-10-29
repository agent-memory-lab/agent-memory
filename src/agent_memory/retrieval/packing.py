"""Deterministic, non-generative packing for the native kernel retrieval path."""

from __future__ import annotations

import re
from bisect import bisect_right
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from hashlib import sha256
from json import dumps
from math import isfinite, log1p

from ..domain import Citation, Claim, MemoryItem, MemoryKind, MemoryQuery
from .analyzer import lexical_terms, query_content_anchors
from .candidate_identity import MAX_SOURCE_WINDOW_CHARS, candidate_identity
from .coverage_plan import coverage_plan
from .evidence_units import context_relations, dependency_group
from .packing_policy import _packing_policy, packing_contract
from .passage_reranking import passage_key, query_key


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


def ordered_bundle_evidence(bundle) -> tuple[MemoryItem, ...]:
    """Restore the packer's cross-kind order without re-ranking its selections.

    Bundle fields group evidence by kind; citation spans retain the selection
    order and disambiguate multiple windows of one public source ID. Providers
    without this native contract keep the legacy score ordering. References
    can order returned evidence only, never add evidence to the bundle.
    """
    items = (*bundle.relevant_memories, *bundle.episodes, *bundle.procedures)
    proofs = bundle.retrieval_metadata.get("citation_spans", ())
    if not isinstance(proofs, (list, tuple)) or not proofs:
        return tuple(sorted(items, key=lambda item: (-item.score, item.id)))
    positions = {}
    for proof in proofs:
        if isinstance(proof, Mapping):
            key = proof.get("candidate_id")
            if isinstance(key, str) and key not in positions:
                positions[key] = len(positions)
    # Unknown/unreturned references confer no inclusion or permissions.
    return tuple(sorted(items, key=lambda item: positions.get(
        candidate_identity(item), len(positions)
    )))


def contiguous_excerpt(text: str, query: str, max_characters: int) -> tuple[str, int, int]:
    """Return one exact substring; offsets always refer to the supplied text."""
    size = max(0, min(len(text), max_characters))
    if size == 0:
        return "", 0, 0
    if size == len(text):
        return text, 0, len(text)
    terms = set(lexical_terms(query))
    starts = {0}
    # Give each query term its own bounded match budget. A shared first-32-match
    # budget lets frequent words exhaust the search before a rare late hit.
    # Regex offsets refer to original text even if casefold changes its length.
    for term in sorted(terms, key=lambda value: (-len(value), value))[:64]:
        pattern = re.escape(term)
        if term.isascii():
            pattern = rf"(?<![a-z0-9]){pattern}(?![a-z0-9])"
        for index, match in enumerate(re.finditer(pattern, text, re.IGNORECASE)):
            if index >= 8:
                break
            starts.add(min(max(0, match.start() - size // 3), len(text) - size))
    window_terms = {
        offset: terms.intersection(lexical_terms(text[offset : offset + size]))
        for offset in starts
    }
    frequencies = Counter(term for found in window_terms.values() for term in found)
    weights = {
        term: log1p((len(starts) + 0.5) / (frequency + 0.5))
        for term, frequency in frequencies.items()
    }
    start = max(
        starts,
        key=lambda offset: (
            sum(weights[term] for term in sorted(window_terms[offset])),
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
        or len(item.text) > MAX_SOURCE_WINDOW_CHARS
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


_MIN_UNBROKEN_PASSAGE_CHARS = 64
_TURN_HEADER = re.compile(
    r"^[ \t]*(?:\[[^\]\n]{1,64}\][ \t]+)?(?:user|assistant|tool):[ \t]*",
    re.MULTILINE | re.IGNORECASE,
)
_SENTENCE_END = re.compile(r"[。！？!?]+[\"'”’）)\]]*|\.[\"'”’）)\]]*(?=\s|$)")
_ABBREVIATIONS = frozenset({"mr", "mrs", "ms", "dr", "prof", "sr", "jr", "st", "vs", "etc"})


def _passage_boundaries(
    text: str, *, source_start: bool, source_end: bool,
) -> tuple[list[int], list[int], bool]:
    """Conservative sentence/line boundaries, not a linguistic sentence parser.

    A located parent's cut edges are not complete sentence boundaries. Decimals,
    initials and common abbreviations do not create artificial sentence ends.
    Newlines also retain list items and speaker-labelled conversation turns.
    """
    ends = {match.start() for match in re.finditer(r"\n+", text)}
    for match in _SENTENCE_END.finditer(text):
        if text[match.start()] == ".":
            line_start = text.rfind("\n", 0, match.start()) + 1
            if re.fullmatch(r"[ \t]*[0-9]+", text[line_start:match.start()]):
                # A line-leading enumerator belongs to the following list item.
                continue
            word = re.search(r"([\w.]+)$", text[:match.start()])
            prefix = word.group(1).casefold() if word else ""
            if (
                prefix in _ABBREVIATIONS
                or (len(prefix) == 1 and prefix.isalpha())
                or re.fullmatch(r"(?:[a-z]\.)+[a-z]", prefix)
            ):
                continue
        ends.add(match.end())
    structural = bool(ends)
    starts = {0} if source_start else set()
    for end in ends:
        start = end
        while start < len(text) and text[start].isspace():
            start += 1
        if start < len(text):
            starts.add(start)
    if source_end:
        ends.add(len(text))
    trimmed_ends = set()
    for end in ends:
        while end and text[end - 1].isspace():
            end -= 1
        if end:
            trimmed_ends.add(end)
    return sorted(starts), sorted(trimmed_ends), structural


def _passage_excerpt(
    text: str, query: str, width: int | None, starts: list[int], ends: list[int],
) -> tuple[str, int, int]:
    """Select a query-centred, exact span between known passage boundaries."""
    if not starts or not ends:
        return "", 0, 0
    terms = set(lexical_terms(query))
    proposed = {starts[0]}
    for term in sorted(terms, key=lambda value: (-len(value), value))[:64]:
        pattern = re.escape(term)
        if term.isascii():
            pattern = rf"(?<![a-z0-9]){pattern}(?![a-z0-9])"
        for index, match in enumerate(re.finditer(pattern, text, re.IGNORECASE)):
            if index >= 8:
                break
            for anchor in (match.start() - (width or 0) // 3, match.start()):
                position = bisect_right(starts, max(0, anchor)) - 1
                if position >= 0:
                    proposed.add(starts[position])
    spans = {}
    for start in proposed:
        position = (
            bisect_right(ends, start)
            if width is None else bisect_right(ends, start + width) - 1
        )
        if 0 <= position < len(ends) and ends[position] > start:
            end = ends[position]
            spans[(start, end)] = terms.intersection(lexical_terms(text[start:end]))
    frequencies = Counter(term for found in spans.values() for term in found)
    weights = {
        term: log1p((len(spans) + 0.5) / (frequency + 0.5))
        for term, frequency in frequencies.items()
    }
    if not spans:
        return "", 0, 0
    start, end = max(spans, key=lambda span: (
        sum(weights[term] for term in sorted(spans[span])), span[1] - span[0], -span[0],
    ))
    return text[start:end], start, end


def _unbroken_excerpt(text: str, query: str, width: int) -> tuple[str, int, int]:
    """Explicit fallback for unpunctuated text; never emit small budget scraps.

    Latin words/numeric literals stay intact. Han writing has no word separator,
    so a >=64-character exact window is retained without claiming sentence
    completeness. An overlong unbroken Latin identifier cannot be split.
    """
    if width < _MIN_UNBROKEN_PASSAGE_CHARS:
        return "", 0, 0
    _, start, end = contiguous_excerpt(text, query, width)

    def word_character(char: str) -> bool:
        return char.isascii() and (char.isalnum() or char in "_.$%+-")

    # Expand to whole lexical tokens instead of dropping the matched suffix or
    # trimming below the minimum. The packer rechecks the resulting token cost;
    # an overlong identifier stays infeasible rather than becoming a fragment.
    while start and word_character(text[start - 1]) and word_character(text[start]):
        start -= 1
    while end < len(text) and word_character(text[end - 1]) and word_character(text[end]):
        end += 1
    return text[start:end], start, end


def _indexed_event_variants(item: MemoryItem, query: str) -> tuple[MemoryItem, ...]:
    """At most thirteen exact, boundary-aware variants of a <=4096-char window.

    Tiny fragments do not become valid evidence just because they fit remaining
    tokens. Whole short sources remain valid; only narrowed unpunctuated windows
    require the explicit minimum-length fallback.
    """
    original_span = _indexed_event_span(item)
    if original_span is None:
        return ()
    result = [item]
    seen = {(0, len(item.text))}
    starts, ends, structural = _passage_boundaries(
        item.text, source_start=original_span["start"] == 0,
        source_end=original_span["end"] == item.metadata["source_chars"],
    )
    # Keep literal speaker headers and all preceding qualifications inside a
    # visible turn. This preserves source context; it does not authenticate the
    # speaker asserted by text. A parent already cut mid-turn remains a known
    # limitation when no subsequent explicit turn header is visible.
    turn_starts = [
        match.start() for match in _TURN_HEADER.finditer(item.text)
        if match.start() > 0 or original_span["start"] == 0
    ]
    if turn_starts:
        starts, structural = turn_starts, True
    chunk_text_hash = sha256(item.text.encode("utf-8")).hexdigest()
    widths = []
    width = len(item.text) // 2
    while width:
        widths.append(width)
        width //= 2
    if not structural:
        widths = sorted({
            width for width in (*widths, _MIN_UNBROKEN_PASSAGE_CHARS)
            if _MIN_UNBROKEN_PASSAGE_CHARS <= width < len(item.text)
        }, reverse=True)
    # Reserve one slot for the smallest complete query-bearing unit. Geometric
    # widths otherwise can jump past a sentence that fits the remaining budget.
    variant_widths = (*widths[:11], None) if structural else widths[:12]
    for width in variant_widths:
        if structural:
            text, start, end = _passage_excerpt(item.text, query, width, starts, ends)
        else:
            text, start, end = _unbroken_excerpt(item.text, query, width)
        if (
            not text or (start, end) in seen
            or re.fullmatch(r"\s*[0-9]+[.)、]\s*", text)
            or (turn_starts and _TURN_HEADER.fullmatch(text))
        ):
            # Only narrowed variants reject bare list markers. A complete short
            # source remains valid as the original representation above.
            continue
        seen.add((start, end))
        metadata = dict(item.metadata)
        chunk_id = metadata.pop("lexical_chunk_id")
        metadata.update(
            {
                "excerpt": True,
                "passage_boundary": (
                    "dialogue_turn_prefix" if turn_starts else
                    "sentence_or_line" if structural else "unbroken_min64"
                ),
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
    turn_starts = [match.start() for match in _TURN_HEADER.finditer(item.text)]
    boundary_metadata = {}
    if turn_starts:
        _, ends, _ = _passage_boundaries(item.text, source_start=True, source_end=True)
        excerpt, start, end = _passage_excerpt(
            item.text, query, min(1200, available), turn_starts, ends,
        )
        boundary_metadata["passage_boundary"] = "dialogue_turn_prefix"
    else:
        excerpt, start, end = contiguous_excerpt(item.text, query, min(1200, available))
    if (
        not excerpt or token_estimate(excerpt) > available
        or (turn_starts and _TURN_HEADER.fullmatch(excerpt))
    ):
        return None
    return replace(
        item,
        text=excerpt,
        metadata={
            **item.metadata,
            **boundary_metadata,
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
    relevance: float
    support: frozenset[str]
    parent_relevance: float
    support_retention: float
    context_score_factor: float


def _event_interval(entry: _PreparedEvidence) -> tuple[int, int] | None:
    """Validate local interval geometry; this is never source authorization."""
    if (
        entry.event_signature is None or not isinstance(entry.value, MemoryItem)
        or entry.value.kind != MemoryKind.EVENT
    ):
        return None
    metadata = entry.value.metadata
    span = metadata.get("source_span")
    total = metadata.get("source_chars")
    revision = metadata.get("source_revision")
    if not isinstance(span, Mapping) or span.get("unit") != "characters":
        return None
    start, end = span.get("start"), span.get("end")
    if (
        type(start) is not int or type(end) is not int or type(total) is not int
        or not 0 <= start < end <= total
        or end - start != len(entry.value.text)
        or end - start > MAX_SOURCE_WINDOW_CHARS
        or not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{64}", revision)
    ):
        return None
    return start, end


def _event_redundancy(entry: _PreparedEvidence, selected: _PreparedEvidence) -> float:
    """Softly discount repeated EVENT coverage; claims are never truth-deduped.

    Adjacent, non-overlapping passages from one source have zero redundancy.
    Shared provenance alone does not imply that evidence repeats the same fact.
    """
    if entry.event_signature is None or selected.event_signature is None:
        return 0.0
    if entry.value.text == selected.value.text:
        return 1.0
    if entry.sources != selected.sources:
        return 0.0
    left, right = entry.value.metadata, selected.value.metadata
    if left.get("source_revision") != right.get("source_revision"):
        return 0.0
    a, b = _event_interval(entry), _event_interval(selected)
    if a is None or b is None:
        return 0.0
    overlap = max(0, min(a[1], b[1]) - max(a[0], b[0]))
    return overlap / min(a[1] - a[0], b[1] - b[0])


def _event_fully_covered(entry: _PreparedEvidence, selected: Sequence[_PreparedEvidence]) -> bool:
    """Only exact same-source/revision interval coverage can prove zero new text.

    Pairwise overlap penalties miss a window covered by several selected windows.
    Claims and different source revisions never share this deduplication rule.
    """
    if entry.event_signature is None:
        return False
    metadata = entry.value.metadata
    target = _event_interval(entry)
    revision = metadata.get("source_revision")
    if target is None:
        return False
    intervals = []
    for previous in selected:
        if (
            previous.event_signature is None or previous.sources != entry.sources
            or previous.value.id != entry.value.id
            or previous.value.metadata.get("source_revision") != revision
        ):
            continue
        span = _event_interval(previous)
        if span is not None:
            intervals.append(span)
    cursor = target[0]
    for start, end in sorted(intervals):
        if start > cursor:
            break
        cursor = max(cursor, end)
        if cursor >= target[1]:
            return True
    return False


def pack_native_evidence(
    query: MemoryQuery,
    current: Sequence[Claim],
    ranked: Sequence[MemoryItem],
    *,
    version: str = "v2",
    passages=None,
) -> NativePackingResult:
    """Select native evidence using a frozen, explicitly selected policy.

    Stable v2 retains the historical relevance-led choices. Experimental v3
    uses the heuristics below. V4 uses identity/coverage deduplication
    and source-bound context units, with parent scores explicitly unverified for
    narrowed passages. Selecting a reader never selects a packing policy.
    All policies share source, span and budget checks.

    Sources are not truth units: different claims from the same source are never
    collapsed. The full input pool is analyzed once without pre-truncating
    conflicts; selection rescans cached sets for at most 64 + query.limit rounds.
    Reranker score gaps are preserved on a request-local [0, 1] scale. Only the
    best effective-relevance frontier (within 0.12) competes on coverage, cost
    and provenance. Query-topic repetition costs at most 0.08 (soft MMR, never
    claim removal). New query topics contribute at most 0.08, other coverage
    at most 0.02, provenance at most 0.005, overlap costs at most 0.03 and token
    cost at most 0.10. Narrowed passages must retain their parent's query support
    to retain its score. After selection, remaining tokens grow exact selected
    source windows fairly without consuming more slots. These are lexical/budget
    heuristics, not answer-completeness or truth judgments.
    """
    policy = _packing_policy(version)
    if passages is not None and (version != "v5" or passages.query_binding != query_key(query)):
        raise ValueError("passage ranking must bind the same v5 query")
    plan = coverage_plan(query.text, ranked) if policy.coverage_reservations else None
    covered_groups: set[str] = set()
    relations = context_relations(ranked) if policy.dependency_units else None
    terms = set(lexical_terms(query.text))
    support_terms = (set(query_content_anchors(query.text)) or terms) if policy.content_anchors else terms
    variant_query = (
        (" ".join(query_content_anchors(query.text)) or query.text)
        if policy.dependency_units else query.text
    )
    finite_scores = [item.score for item in ranked if isfinite(item.score)
                     and (not policy.dependency_units or item.metadata.get("context_only") is not True)]
    floor = min(0.0, min(finite_scores, default=0.0))
    ceiling = max(finite_scores, default=0.0)
    span = ceiling - floor
    relevance_by_id = {
        candidate_identity(item): (
            (item.score - floor) / span if span and isfinite(item.score) else 0.0
        )
        for item in ranked
    }
    ranked_by_id = {candidate_identity(item): item for item in ranked}
    if passages is not None:
        relevance_by_id.update(passages.relevance)
    if relations is not None:
        # Context carries its anchor's allocation bound, never an independently
        # measured passage score. Child-provided scores cannot inflate the scale.
        for child, parent in relations.parents.items():
            relevance_by_id[child] = relevance_by_id.get(parent, 0.0)
    match_cache: dict[str, frozenset[str]] = {}

    def matches_for(text):
        if text not in match_cache:
            match_cache[text] = frozenset(terms.intersection(lexical_terms(text)))
        return match_cache[text]

    context_factors: dict[str, float] = {}
    for key, value in ranked_by_id.items():
        if not (
            policy.discount_context
            and value.kind == MemoryKind.EVENT and value.metadata.get("context_only") is True
            and value.metadata.get("relevance_origin") == "inherited_context_anchor"
        ):
            continue
        anchor = ranked_by_id.get(value.metadata.get("context_anchor_candidate_id"))
        anchor_support = (
            matches_for(anchor.text).intersection(support_terms)
            if anchor is not None and anchor.kind == MemoryKind.EVENT
            and anchor.id == value.id
            and anchor.metadata.get("source_revision") == value.metadata.get("source_revision")
            else frozenset()
        )
        retention = (
            len(matches_for(value.text).intersection(anchor_support)) / len(anchor_support)
            if anchor_support else 1.0
        )
        # Adjacency has not been reranked. It cannot inherit the anchor's full
        # score, but a pronoun or answer-only neighbour can have no lexical
        # overlap and still be useful: keep this discount bounded, never zero.
        context_factors[key] = 0.8 + 0.1 * retention

    def identity(value):
        return candidate_identity(value) if isinstance(value, MemoryItem) else value.id

    def prepare(
        value: Claim | MemoryItem, parent: _PreparedEvidence | None = None,
    ) -> _PreparedEvidence:
        sources = source_ids(value)
        matches = matches_for(value.text)
        support = matches.intersection(support_terms)
        parent_relevance = (
            relevance_by_id.get(identity(value), 0.0)
            if isinstance(value, MemoryItem)
            else len(matches) / max(1, len(terms))
        )
        context_score_factor = context_factors.get(identity(value), 1.0)
        parent_relevance *= context_score_factor
        retention = 1.0
        if policy.discount_narrowing and parent is not None and value.text != parent.value.text:
            # A locator keeps its source identity after narrowing, not the
            # relevance of words that no longer reach the reader. If the parent
            # was a semantic-only hit, no lexical check can validate its new
            # passage: conservatively discount by retained text instead.
            retention = (
                len(support.intersection(parent.support)) / len(parent.support)
                if parent.support else len(value.text) / max(1, len(parent.value.text))
            )
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
            matches,
            sources,
            signature,
            parent_relevance * retention,
            support,
            parent_relevance,
            retention,
            context_score_factor,
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
    # Provider ordering is the deterministic tie-breaker, including fallback
    # tails. Do not run another lexical sort after semantic reranking.
    ordered_items = ranked
    prepared_items = []
    item_variants: dict[str, tuple[_PreparedEvidence, ...]] = {}
    infeasible = 0

    def feasible_variant(
        identity: str, available: int, *, exclude_covered: bool = False,
        score_variants: bool = False,
    ) -> _PreparedEvidence | None:
        variants = tuple(
            entry for entry in item_variants.get(identity, ())
            if entry.support_retention > 0
            and (not exclude_covered or not _event_fully_covered(entry, selected_entries))
        )
        if score_variants:
            return max(
                (entry for entry in variants if entry.cost <= available),
                key=lambda entry: (selection_metrics(entry)[0], entry.cost),
                default=None,
            )
        # Without budget competition preserve full context whenever it fits.
        # A slice can accidentally turn
        # a word prefix into a query token; that is never a reason to discard
        # an otherwise feasible original chunk.
        if variants and variants[0].cost <= available:
            return variants[0]
        fitting = (entry for entry in variants[1:] if entry.cost <= available)
        return max(
            fitting,
            key=lambda entry: (
                (entry.relevance, len(entry.support), len(entry.terms), entry.cost)
                if policy.discount_narrowing else (len(entry.terms), entry.cost)
            ),
            default=None,
        )

    for item in ordered_items:
        key = identity(item)
        if relations is not None and key in relations.context_ids and key not in relations.parents:
            continue
        if key in item_variants:
            continue
        located = any(key in item.metadata for key in ("source_span", "lexical_chunk_id"))
        if item.kind == MemoryKind.EVENT and located:
            variants = _indexed_event_variants(item, variant_query)
        else:
            # One initial-budget excerpt of each legacy complete source. Only
            # bounded indexed chunks get multiple precomputed variants.
            fitted = fit_event(item, variant_query, query.token_budget)
            variants = () if fitted is None else (fitted,)
        if passages is not None and key in passages.items:
            expected = passage_key(passages.items[key])
            variants = tuple(value for value in variants if passage_key(value) == expected)
            if not variants:
                raise ValueError("scored passage no longer matches a source variant")
        if variants:
            parent = prepare(item)
            item_variants[key] = tuple(
                parent if variant is item else prepare(variant, parent)
                for variant in variants
            )
        else:
            item_variants[key] = ()
        primary = feasible_variant(key, query.token_budget)
        if primary is None:
            infeasible += 1
        else:
            prepared_items.append(primary)
    state_by_id = {entry.value.id: entry for entry in ordered_state}
    entries: list[tuple[_PreparedEvidence, int]] = []
    seen_ids: set[str] = set()
    for rank, entry in enumerate([*prepared_items, *ordered_state], 1):
        key = identity(entry.value)
        if key in seen_ids:
            continue
        seen_ids.add(key)
        state_entry = state_by_id.get(entry.value.id)
        entries.append((
            replace(state_entry, relevance=entry.relevance)
            if state_entry is not None and isinstance(entry.value, MemoryItem)
            else entry,
            rank,
        ))
    covered_terms: set[str] = set()
    covered_sources: set[str] = set()
    event_signatures: set[tuple] = set()
    state: list[Claim] = []
    selected: list[MemoryItem] = []
    citations: list[Citation] = []
    citation_spans: list[dict] = []
    selected_entries: list[_PreparedEvidence] = []
    selection_trace: list[dict] = []
    dependency_groups: list[dict] = []
    used_tokens = duplicate_sources = 0
    # The item cap also bounds the initial cost frontier: discarded tail
    # candidates must not manufacture pressure when the leading evidence fits.
    # Current state retains its existing precedence; cost optimization applies
    # to retrieved representations, not to replacing authoritative state.
    state_costs = [entry.cost for entry, _ in entries if isinstance(entry.value, Claim)]
    item_costs = [entry.cost for entry, _ in entries if not isinstance(entry.value, Claim)
                  and (relations is None or identity(entry.value) not in relations.context_ids)]
    budget_pressure = (bool(policy.cost_weight) or policy.reserve_evidence_budget) and (
        sum(state_costs[:64]) + sum(item_costs[:query.limit]) > query.token_budget
    )
    initial_cost_share = query.token_budget / max(1, min(query.limit, len(entries)))

    def selection_metrics(entry):
        coverage = len(entry.support) / max(1, len(support_terms))
        novelty = len(entry.support - covered_terms) / max(1, len(support_terms))
        # Reward coverage of the still missing topics, not repeated mentions of
        # already covered topics. Dividing by this evidence's complete support
        # would dilute the final missing topic whenever it shares common nouns
        # with previous evidence. This marginal reward always stays in [0, 1].
        topic_novelty = len(entry.support - covered_terms) / max(1, len(support_terms - covered_terms))
        topic_repetition = max(
            (len(entry.support.intersection(previous.support)) / max(1, len(entry.support))
             for previous in selected_entries),
            default=0.0,
        )
        redundancy = max(
            (_event_redundancy(entry, previous) for previous in selected_entries),
            default=0.0,
        )
        cost_share = (query.token_budget - used_tokens) / max(1, query.limit - len(selected))
        cost_penalty = (
            policy.cost_weight * min(1.0, entry.cost / cost_share)
            if budget_pressure and not isinstance(entry.value, Claim) else 0.0
        )
        score = (
            entry.relevance
            + 0.01 * coverage
            + 0.01 * novelty
            + policy.topic_novelty_weight * topic_novelty
            - policy.topic_repetition_weight * topic_repetition
            + (0.005 if set(entry.sources) - covered_sources else 0.0)
            - 0.03 * redundancy
            - cost_penalty
        )
        return score, coverage, novelty, redundancy, cost_penalty, topic_novelty, cost_share, topic_repetition

    def uncovered_unit(rows):
        # Variants with different parent identities can narrow to the same
        # source interval. Check within the unit before costing it or binding
        # its dependency proof, not only against earlier selected units.
        members = []
        covered = list(selected_entries)
        for row in rows:
            if not _event_fully_covered(row[2], covered):
                members.append(row)
                covered.append(row[2])
        return members

    # Reservations use complete variants retaining every lexical support term.
    # With no lexical support, keep the semantic hit intact: its shorter text
    # has not been independently ranked. These are capacity bounds, not scores.
    minimum_variants = {
        key: min((entry for entry in variants
                  if entry.support >= variants[0].support
                  and (variants[0].support or entry is variants[0])),
                 key=lambda entry: entry.cost)
        for key, variants in item_variants.items() if variants and policy.reserve_evidence_budget
    }

    def evidence_reservations():
        frontier = []
        covered = list(selected_entries)
        for entry, _ in entries:
            key = identity(entry.value)
            if (isinstance(entry.value, Claim)
                or (relations is not None and key in relations.context_ids)):
                continue
            minimum = minimum_variants[key]
            if _event_fully_covered(minimum, covered):
                continue
            frontier.append((key, minimum))
            covered.append(minimum)
            if len(frontier) >= query.limit - len(selected):
                break
        reservations = {key: entry.cost for key, entry in frontier}
        slots = query.limit - len(selected) - len(frontier)
        if relations is not None and slots > 0:
            for child, parent in relations.parents.items():
                minimum = minimum_variants.get(child)
                if (parent in reservations and minimum is not None
                    and not _event_fully_covered(minimum, covered)):
                    reservations[parent] += minimum.cost
                    covered.append(minimum)
                    slots -= 1
                    if slots == 0:
                        break
        return reservations

    def unit_allowance(key, reservations):
        reserved = sum(cost for other, cost in reservations.items() if other != key)
        return max(0, query.token_budget - used_tokens - reserved)

    while entries and used_tokens < query.token_budget:
        pending = (tuple(group for group in plan.groups if group.id not in covered_groups)
                   if plan is not None and passages is None else ())
        feasible = []
        remaining = []
        reservations = (evidence_reservations()
                        if policy.reserve_evidence_budget and budget_pressure else {})

        for original, rank in entries:
            entry = original
            is_state = isinstance(entry.value, Claim)
            available = query.token_budget - used_tokens
            if is_state and (len(state) >= 64 or entry.cost > available):
                # Preserve a feasible, already indexed representation of a
                # full state claim under its same original memory ID.
                entry = feasible_variant(identity(entry.value), available)
                is_state = False
            elif not is_state and policy.reserve_evidence_budget and budget_pressure:
                # Earlier evidence can keep long context after reserving only
                # the minimum complete representations of the leading roots.
                key = identity(entry.value)
                parent = relations.parents.get(key, key) if relations is not None else key
                allowance = min(available, max(unit_allowance(parent, reservations),
                                               minimum_variants[key].cost))
                entry = (feasible_variant(key, allowance)
                         or feasible_variant(key, available))
            elif not is_state and pending:
                allowance = int(available / len(pending))
                entry = (feasible_variant(identity(entry.value), allowance)
                         or feasible_variant(identity(entry.value), available))
            elif not is_state and (budget_pressure or entry.cost > available):
                entry = feasible_variant(
                    identity(entry.value), available, score_variants=budget_pressure,
                )
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
            if _event_fully_covered(entry, selected_entries):
                entry = feasible_variant(
                    identity(entry.value), available, exclude_covered=True,
                    score_variants=budget_pressure,
                )
                if entry is None:
                    duplicate_sources += 1
                    continue
            score, coverage, novelty, redundancy, cost_penalty, topic_novelty, cost_share, topic_repetition = selection_metrics(entry)
            feasible.append((score, -rank, entry, coverage, novelty, redundancy,
                             cost_penalty, topic_novelty, cost_share, topic_repetition))
            remaining.append((original, rank))
        if not feasible:
            break
        if relations is not None:
            by_id = {identity(row[2].value): row for row in feasible}
            units = []
            for root in feasible:
                root_id = identity(root[2].value)
                if root_id in relations.context_ids:
                    continue
                children = [child for child, parent in relations.parents.items() if parent == root_id]
                available_children = [by_id[child] for child in children if child in by_id]
                options = [uncovered_unit(option) for option in (
                    [root, *available_children],
                    *([root, child] for child in available_children), [root],
                )]
                if policy.reserve_evidence_budget and budget_pressure:
                    fitted_options = []
                    allowance = max(unit_allowance(root_id, reservations),
                                    minimum_variants.get(root_id, root[2]).cost)
                    for option in options:
                        # Fit parent and neighbours together. Independently
                        # giving each the entire unit allowance loses children
                        # even when complete compact forms fit as a group.
                        available = min(allowance, query.token_budget - used_tokens)
                        if sum(minimum_variants.get(identity(row[2].value), row[2]).cost
                               for row in option) > available:
                            continue
                        fitted = []
                        for position, row in enumerate(option):
                            reserve = sum(
                                minimum_variants.get(identity(later[2].value), later[2]).cost
                                for later in option[position + 1:]
                            )
                            entry = feasible_variant(
                                identity(row[2].value), available - reserve, exclude_covered=True,
                            )
                            if entry is None:
                                break
                            metrics = selection_metrics(entry)
                            fitted.append((metrics[0], row[1], entry, *metrics[1:]))
                            available -= entry.cost
                        if len(fitted) == len(option):
                            fitted_options.append(uncovered_unit(fitted))
                    options = fitted_options or [[root]]
                # Adjacency is an optional association, not proof every neighbour
                # is needed. Prefer the largest feasible unit, then provider
                # order; an oversized neighbour must not block a fitting pair.
                members = next((option for option in options if option and (
                    sum(row[2].cost for row in option) <= query.token_budget-used_tokens
                    and len(selected)+sum(not isinstance(row[2].value, Claim) for row in option) <= query.limit
                )), None)
                if members:
                    units.append(members)
            if not units:
                break
            if pending:
                # Reserve a representation for the next explicit topic before
                # filling spare capacity with repeated coverage. Match the actual
                # feasible text, not query words that were removed by narrowing.
                for group in pending:
                    matching = [unit for unit in units if any(
                        group.anchors.intersection(row[2].terms) for row in unit)]
                    if matching:
                        units = matching
                        break
                reserve = max(0, len(pending)-1)
                bounded = [unit for unit in units if (
                    len(selected)+sum(not isinstance(row[2].value, Claim) for row in unit)
                    <= query.limit-reserve
                    and sum(row[2].cost for row in unit) <= (query.token_budget-used_tokens)/len(pending)
                )]
                if bounded:
                    units = bounded
                else:
                    units = [[unit[0]] for unit in units]
            chosen = max(units, key=lambda rows: (rows[0][0], rows[0][1]))
        else:
            best_relevance = max(row[2].relevance for row in feasible)
            chosen = [max(
                (row for row in feasible if policy.relevance_frontier is None
                 or row[2].relevance >= best_relevance - policy.relevance_frontier),
                key=lambda row: (row[0], row[1]),
            )]
        group = dependency_group([row[2].value for row in chosen]) if len(chosen) > 1 else None
        if group is not None:
            dependency_groups.append(group)
        chosen_ids = set()
        for row in chosen:
            score, negative_rank, entry, coverage, novelty, redundancy, cost_penalty, topic_novelty, cost_share, topic_repetition = row
            value = entry.value
            chosen_ids.add(identity(value))
            if isinstance(value, Claim):
                state.append(value)
            else:
                selected.append(value)
            selected_entries.append(entry)
            selection_trace.append({
                "memory_id": value.id,
                "candidate_id": identity(value),
                "kind": value.kind.value if isinstance(value, MemoryItem) else MemoryKind.CLAIM.value,
                "input_rank": -negative_rank,
                "relevance": entry.relevance,
                "parent_relevance": entry.parent_relevance,
                "support_retention": entry.support_retention,
                "context_score_factor": entry.context_score_factor,
                "selection_score": score,
                "query_coverage": coverage,
                "new_query_coverage": novelty,
                "event_redundancy": redundancy,
                "cost_penalty": cost_penalty,
                "cost_share_tokens": cost_share,
                "new_topic_fraction": topic_novelty,
                "query_topic_repetition": topic_repetition,
                "tokens": entry.cost,
                "initial_tokens": entry.cost,
                "initial_relevance": entry.relevance,
                "initial_support_retention": entry.support_retention,
                "context_expansion_tokens": 0,
            })
            if relations is not None:
                selection_trace[-1]["relevance_basis"] = (
                    "reranked_passage" if passages is not None and isinstance(value, MemoryItem)
                    and identity(value) in passages.ranked_ids
                    and passage_key(value) == passage_key(passages.items[identity(value)]) else
                    "unranked_passage" if passages is not None and isinstance(value, MemoryItem)
                    and identity(value) in passages.items
                    and passage_key(value) == passage_key(passages.items[identity(value)]) else
                    "current_state_lexical" if isinstance(value, Claim) else
                    "inherited_anchor_bound" if identity(value) in relations.parents else
                    "unverified_parent_bound" if isinstance(value, MemoryItem)
                    and value.text != ranked_by_id[identity(value)].text else "reranked_candidate"
                )
                if group is not None:
                    selection_trace[-1]["dependency_group_id"] = group["group_id"]
            if isinstance(value, MemoryItem) and "source_span" in value.metadata:
                selection_trace[-1]["initial_source_span"] = value.metadata["source_span"]
            if entry.event_signature is not None:
                event_signatures.add(entry.event_signature)
            covered_terms.update(entry.support)
            covered_sources.update(entry.sources)
            if plan is not None:
                covered_groups.update(plan.matches(entry.terms))
            used_tokens += entry.cost
        if relations is not None:
            chosen_ids.update(child for child, parent in relations.parents.items() if parent in chosen_ids)
        entries = [
            (candidate, rank) for candidate, rank in remaining
            if identity(candidate.value) not in chosen_ids
        ]

    # Growing already selected source windows cannot evict independent claims.
    # Only precomputed variants of the same locator are eligible, and they must
    # contain every character already selected. In each round distinct source
    # events share the available budget; multiple windows of one source do not
    # get extra shares. A smallest-increment fallback borrows unused shares only
    # when no group could grow. Strict cost increases bound this loop by the
    # finite variant count (at most 13 per selected indexed window).
    def expansion_options():
        groups: dict[str, list[tuple[int, _PreparedEvidence, int]]] = {}
        available = query.token_budget - used_tokens
        for position, previous in enumerate(selected_entries):
            previous_span = _event_interval(previous)
            if previous_span is None:
                continue
            for grown in item_variants.get(identity(previous.value), ()):
                grown_span = _event_interval(grown)
                delta = grown.cost - previous.cost
                if (
                    grown_span is not None and 0 < delta <= available
                    and grown_span[0] <= previous_span[0] < previous_span[1] <= grown_span[1]
                    and not _event_fully_covered(grown, selected_entries)
                    # Growth must not make another selected group member
                    # redundant after the group's selection/proof was formed.
                    and not any(
                        other is not previous and _event_fully_covered(other, (grown,))
                        and not _event_fully_covered(other, (previous,))
                        for other in selected_entries
                    )
                ):
                    groups.setdefault(previous.value.id, []).append((position, grown, delta))
        return groups

    while policy.expand_selected_context and used_tokens < query.token_budget:
        groups = expansion_options()
        if not groups:
            break
        progressed = False
        for group_index, group in enumerate(groups):
            # Re-evaluate against earlier groups' expansions in this round.
            options = expansion_options().get(group, ())
            allowance = (query.token_budget - used_tokens) / (len(groups) - group_index)
            choice = max(
                (option for option in options if option[2] <= allowance),
                key=lambda option: (option[2], -option[0]), default=None,
            )
            if choice is None:
                continue
            position, grown, delta = choice
            selected_entries[position] = grown
            used_tokens += delta
            progressed = True
        if not progressed:
            position, grown, delta = min(
                (option for options in groups.values() for option in options),
                key=lambda option: (option[2], option[0]),
            )
            selected_entries[position] = grown
            used_tokens += delta

    # Context growth changes span proofs, but not group membership. Bind every
    # dependency and trace to the grown text before the delivery guard sees it.
    selected_by_id = {identity(entry.value): entry.value for entry in selected_entries}
    refreshed_groups = {}
    for group in dependency_groups:
        refreshed_groups[group["group_id"]] = dependency_group([
            selected_by_id[proof["candidate_id"]] for proof in group["members"]
        ])
    dependency_groups = list(refreshed_groups.values())
    for trace in selection_trace:
        if "dependency_group_id" in trace:
            trace["dependency_group_id"] = refreshed_groups[
                trace["dependency_group_id"]
            ]["group_id"]

    # Bind every final proof and cost to exactly what the reader receives.
    # Selection scores intentionally describe the compact selection stage;
    # final relevance and spans describe the subsequently expanded evidence.
    state = []
    selected = []
    covered_terms = set()
    for entry, trace in zip(selected_entries, selection_trace, strict=True):
        value = entry.value
        (state if isinstance(value, Claim) else selected).append(value)
        covered_terms.update(entry.support)
        trace.update({
            "tokens": entry.cost,
            "relevance": entry.relevance,
            "support_retention": entry.support_retention,
            "context_expansion_tokens": entry.cost - trace["initial_tokens"],
        })
        citations.append(Citation(value.id, entry.sources))
        proof = {"memory_id": value.id, "candidate_id": identity(value),
                 "source_event_ids": entry.sources}
        if isinstance(value, MemoryItem):
            proof.update({key: value.metadata[key] for key in (
                "source_span", "source_revision", "source_chars",
            ) if key in value.metadata})
            if "source_span" in value.metadata:
                trace["source_span"] = value.metadata["source_span"]
        citation_spans.append(proof)
    used_tokens = sum(entry.cost for entry in selected_entries)
    return NativePackingResult(
        tuple(state),
        tuple(selected),
        tuple(citations),
        used_tokens,
        {
            "strategy": policy.strategy,
            "packing_contract": packing_contract(version),
            "coverage": "partial",
            "world_negative": False,
            "packing_query_terms_covered": len(covered_terms),
            "packing_query_terms_total": len(support_terms),
            "packing_support_policy": (
                "content_anchors_or_full_query/1" if policy.content_anchors else "full_query/1"
            ),
            "packing_budget_pressure": budget_pressure,
            "packing_initial_cost_share_tokens": initial_cost_share,
            "packing_relevance_frontier": policy.relevance_frontier,
            "packing_topic_novelty_weight": policy.topic_novelty_weight,
            "packing_topic_repetition_weight": policy.topic_repetition_weight,
            "packing_context_relevance_policy": (
                "inherited_anchor_bounded_0.8_0.9/1" if policy.discount_context else "unchanged/1"
            ),
            "packing_trace_score_stage": (
                "compact_selection_before_context_growth" if policy.expand_selected_context
                else "selection_without_context_growth"
            ),
            "packing_context_expansion_tokens": sum(
                row["context_expansion_tokens"] for row in selection_trace
            ),
            "packing_zero_support_variants": sum(
                entry.support_retention == 0
                for variants in item_variants.values() for entry in variants
            ),
            "packing_source_count": len(covered_sources),
            "packing_duplicate_sources": duplicate_sources,
            "packing_infeasible": infeasible,
            "packing_analyzed_state": len(prepared_state),
            "packing_analyzed_items": len(prepared_items),
            "citation_spans": tuple(citation_spans),
            "packing_selection_trace": tuple(selection_trace),
            **({"packing_coverage_plan": plan.metadata((*state, *selected)),
                "packing_selection_driver": "actual_passage_ranking" if passages is not None else "lexical_coverage_reservations",
                "packing_passage_reranking": dict(passages.diagnostics) if passages is not None else
                    {"status": "not_configured", "reranker_calls": 0, "scored_count": 0}}
               if plan is not None else {}),
            **({
                "packing_dependency_groups": tuple(dependency_groups),
                "packing_dependency_rejected": relations.rejected,
                "packing_dependency_deferred": sum(
                    key not in {identity(entry.value) for entry in selected_entries}
                    for key in relations.parents
                ),
                "packing_fact_deduplication": "exact_identity_or_verified_event_coverage/1",
                "packing_passage_relevance_verified": False,
                "packing_relevance_policy": "parent_bounds_without_length_discount/1",
            } if relations is not None else {}),
        },
    )
