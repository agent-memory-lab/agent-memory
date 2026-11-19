"""Opt-in atomic packing. No model, annotation, source fetch or text rewriting.

The application supplies a deterministic counter for the *complete* reader
payload (including dates/provenance/prompt overhead). Missing counters fail
closed. Inputs must already be authorized and carry their final ranking.
"""
from dataclasses import dataclass, replace
from typing import Callable
from .candidate_identity import candidate_identity
from .packing import NativePackingResult, source_ids, _indexed_event_span, pack_native_evidence
from ..domain import Citation, Claim, MemoryItem, MemoryKind


@dataclass(frozen=True)
class LosslessBudget:
    count: Callable[[tuple[Claim, ...], tuple[MemoryItem, ...]], int]
    tokenizer: str

    def __post_init__(self):
        if not callable(self.count) or not isinstance(self.tokenizer, str) or not self.tokenizer:
            raise ValueError('lossless packing requires an explicit tokenizer and counter')

    def measure(self, state, items):
        value = self.count(tuple(state), tuple(items))
        if type(value) is not int or value < 0:
            raise ValueError('token counter must return a nonnegative integer')
        return value


def _preserves_event(old, selected):
    """Require verbatim containment in the same source, not a derived claim.

    This is a conservative text/provenance guard, not a semantic equivalence
    test. It never reads relevance labels, answers or question identifiers.
    """
    for new in selected:
        if new.kind != MemoryKind.EVENT or new.id != old.id or old.text not in new.text:
            continue
        if old.metadata.get('source_revision') and new.metadata.get('source_revision') != old.metadata['source_revision']:
            continue
        if 'source_span' in old.metadata:
            # Repeated wording elsewhere in the same revision is not the same
            # source occurrence. Native excerpts use retrieved_* locators, so
            # validate with candidate_identity rather than the original-chunk
            # validator, then require aligned interval containment.
            if candidate_identity(old) == old.id or candidate_identity(new) == new.id:
                continue
            lo, hi = old.metadata['source_span'], new.metadata['source_span']
            if not hi['start'] <= lo['start'] < lo['end'] <= hi['end']:
                continue
            if new.text[lo['start']-hi['start']:lo['end']-hi['start']] != old.text:
                continue
        return True
    return False


def pack_preserving_evidence(query, current, ranked, *, budget):
    """Try atomic packing; retain the v2 bundle if any old event text is lost.

    Fallback is explicit and can retain v2 excerpts; it does not promise all
    source turns fit. Even the fallback must pass the supplied payload counter.
    """
    proposed = pack_lossless_evidence(query, current, ranked, budget=budget)
    baseline = pack_native_evidence(query, current, ranked, version='v2')
    missing = tuple(i for i, old in enumerate(baseline.items)
                    if old.kind == MemoryKind.EVENT
                    and not _preserves_event(old, proposed.items))
    chosen = baseline if missing else proposed
    tokens = budget.measure(chosen.state, chosen.items)
    if tokens > query.token_budget:
        raise ValueError('baseline-preserving payload exceeds token budget')
    return replace(chosen, tokens=tokens, metadata={
        **chosen.metadata,
        'packing_preservation_guard': 'same-source-verbatim/1',
        'packing_fallback': 'baseline_v2' if missing else None,
        'packing_fallback_reason': 'event_text_not_preserved' if missing else None,
        'packing_missing_baseline_positions': missing,
        'packing_omitted': chosen.metadata.get('packing_omitted', ()),
        'packing_payload_tokenizer': budget.tokenizer,
    })


def pack_lossless_evidence(query, current, ranked, *, budget):
    if not isinstance(budget, LosslessBudget):
        raise ValueError('lossless packing requires LosslessBudget')
    if budget.measure((), ()) > query.token_budget:
        raise ValueError('reader overhead exceeds token budget')
    state, items, omitted, proofs, seen = [], [], [], [], set()
    state_proofs = []
    def whole_sources(values):
        result = set()
        for item in values:
            if item.kind != MemoryKind.EVENT:
                continue
            span = _indexed_event_span(item)
            if span is not None and span['start'] == 0 and span['end'] == item.metadata['source_chars']:
                result.add(item.id)
            elif not any(k in item.metadata for k in ('source_span', 'lexical_chunk_id', 'excerpt')):
                result.add(item.id)
        return result

    def covered_claim(item, sources):
        ids = source_ids(item)
        return item.kind == MemoryKind.CLAIM and bool(ids) and set(ids) <= sources

    # Preserve upstream rank among retained records. A full original source
    # supersedes derived renderings of that same source, never distinct sources.
    # Current-state claims are not coalesced: validity/status have separate meaning.
    input_positions = {}
    for input_index, value in enumerate((*current, *ranked)):
        is_state = isinstance(value, Claim)
        key = ('state:' + value.id) if is_state else candidate_identity(value)
        input_positions.setdefault(key, input_index)
        reason = None
        sources = whole_sources(items)
        replacement_items = items
        if not is_state:
            incoming_sources = whole_sources((value,))
            if incoming_sources:
                replacement_items = [v for v in items if not covered_claim(v, sources | incoming_sources)]
        if not is_state and covered_claim(value, sources):
            reason = 'covered_by_complete_source'
        elif key in seen:
            reason = 'duplicate_identity'
        elif not is_state and value.kind == MemoryKind.EVENT and any(
            k in value.metadata for k in ('source_span', 'lexical_chunk_id')
        ) and _indexed_event_span(value) is None:
            reason = 'invalid_source_span'
        elif (len(state) >= 64 if is_state else len(replacement_items) >= query.limit):
            reason = 'item_limit'
        else:
            next_state = (*state, value) if is_state else tuple(state)
            next_items = tuple(items) if is_state else (*replacement_items, value)
            if budget.measure(next_state, next_items) > query.token_budget:
                reason = 'token_budget'
        if reason:
            omitted.append({'input_index': input_index, 'reason': reason})
            continue
        seen.add(key)
        if not is_state and len(replacement_items) != len(items):
            removed = {candidate_identity(v) for v in items if v not in replacement_items}
            omitted.extend({'input_index': input_positions[k], 'reason': 'covered_by_complete_source'} for k in sorted(removed))
            proofs = [p for p in proofs if p['candidate_id'] not in removed
                      or any(p is state_proof for state_proof in state_proofs)]
            items = replacement_items
        (state if is_state else items).append(value)
        # Internal state deduplication is namespaced; public proof identity must
        # match the native delivery guard's Claim identity contract.
        proof = {'memory_id': value.id, 'candidate_id': value.id if is_state else key,
                 'source_event_ids': source_ids(value)}
        if not is_state:
            proof.update({k: value.metadata[k] for k in
                          ('source_span', 'source_revision', 'source_chars') if k in value.metadata})
        proofs.append(proof)
        if is_state:
            state_proofs.append(proof)
    tokens = budget.measure(state, items)
    if tokens > query.token_budget:
        raise ValueError('non-deterministic counter or changed reader payload')
    return NativePackingResult(tuple(state), tuple(items), tuple(
        Citation(v.id, source_ids(v)) for v in (*state, *items)), tokens, {
        'strategy': 'lossless-ranked-units/1', 'coverage': 'partial', 'world_negative': False,
        'packing_contract': {'schema': 'lossless-ranked-units/1', 'experimental': True,
                             'tokenizer': budget.tokenizer, 'budget_unit': 'complete_reader_payload'},
        'citation_spans': tuple(proofs), 'packing_omitted': tuple(omitted),
        'packing_text_rewrites': 0, 'packing_input_count': len(current) + len(ranked),
        'packing_selected_count': len(state) + len(items),
    })
