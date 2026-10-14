"""Versioned evidence-to-reader contracts for public memory experiments.

Native retrieval owns the evidence-content budget. Its already selected texts
are serialized verbatim; framing is separately measured and bounded, never paid
for by silently clipping evidence. Raw baselines retain the explicitly named
legacy byte-prefix contract so their different budgets remain visible.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

CORE_READER_MODE = 'core-evidence-verbatim/v2'
LEGACY_READER_MODE = 'legacy-utf8-prefix/v1'
CORE_TOKEN_BUDGET = 2048
CORE_ITEM_LIMIT = 8
LEGACY_CONTEXT_BYTES = 6000
EMPTY_CONTEXT = '(no memory retrieved)'


class ReaderContextError(ValueError):
    """An explicit contract violation; callers must not send a clipped fallback."""


@dataclass(frozen=True, slots=True)
class ReaderContext:
    context: str
    candidates: tuple[dict[str, Any], ...]
    delivered: tuple[dict[str, Any], ...]
    diagnostics: dict[str, Any]


def reader_contract() -> dict[str, Any]:
    """Serializable protocol declaration; no optional core imports or I/O."""
    return {
        'version': 'public-longmemeval-reader/2',
        'agent-memory': {
            'mode': CORE_READER_MODE, 'budget_owner': 'native_core_packer',
            'evidence_estimated_token_budget': CORE_TOKEN_BUDGET,
            'item_limit': CORE_ITEM_LIMIT,
            'serialization_utf8_guard': 4*CORE_TOKEN_BUDGET + 3*CORE_ITEM_LIMIT,
            'guard_policy': 'fail_without_truncation',
            'framing': 'two-character bullet prefix and one newline per item',
            'framing_in_core_budget': False,
        },
        'raw-baselines': {
            'mode': LEGACY_READER_MODE, 'budget_owner': 'reader_byte_prefix_packer',
            'context_utf8_byte_cap': LEGACY_CONTEXT_BYTES,
            'partial_final_item_allowed': True,
        },
        'cross_arm_budget_equivalence': False,
        'comparison_note': (
            'Native evidence has one 2048-estimated-token selection budget; raw baselines '
            'retain their historical 6000-byte prefix budget. Report delivered size and '
            'partial/omitted evidence per arm; this is not an equal-context-budget comparison.'
        ),
    }


def _render(items: Sequence[Mapping[str, Any]]) -> str:
    return ''.join('- '+item['text']+'\n' for item in items) or EMPTY_CONTEXT


def _source_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)) or any(not isinstance(s, str) or not s for s in value):
        raise ReaderContextError('citation_source_ids_invalid')
    result = tuple(value)
    if len(set(result)) != len(result):
        raise ReaderContextError('citation_source_ids_duplicate')
    return result


def core_reader_context(
    bundle, source_event_map: Mapping[str, str], *,
    token_budget: int = CORE_TOKEN_BUDGET, item_limit: int = CORE_ITEM_LIMIT,
) -> ReaderContext:
    """Validate and deliver every selected native evidence item, without repacking.

    `token_budget` is the same budget passed to recall, not a second selection
    budget. Citation proofs bind occurrence-specific candidate IDs and spans;
    public memory IDs may repeat for different source windows. This is a host
    consistency check, not a replacement for the core's authorization checks.
    """
    from agent_memory.retrieval.candidate_identity import candidate_identity
    from agent_memory.retrieval.packing import ordered_bundle_evidence, source_ids, token_estimate

    if type(token_budget) is not int or token_budget < 64:
        raise ReaderContextError('invalid_core_token_budget')
    if type(item_limit) is not int or not 1 <= item_limit <= 100:
        raise ReaderContextError('invalid_core_item_limit')
    if bundle.current_state:
        raise ReaderContextError('current_state_not_supported_request_include_current_state_false')
    items = ordered_bundle_evidence(bundle)
    if len(items) > item_limit:
        raise ReaderContextError('core_item_limit_exceeded')
    if any(not isinstance(item.text, str) or not item.text for item in items):
        raise ReaderContextError('core_evidence_text_invalid')
    estimated_tokens = sum(token_estimate(item.text) for item in items)
    if type(bundle.token_estimate) is not int or bundle.token_estimate != estimated_tokens:
        raise ReaderContextError('core_token_estimate_mismatch')
    if estimated_tokens > token_budget:
        raise ReaderContextError('core_token_budget_exceeded')
    proofs = bundle.retrieval_metadata.get('citation_spans', ())
    if not isinstance(proofs, (list, tuple)) or len(proofs) != len(items):
        raise ReaderContextError('native_citation_proofs_missing_or_extra')
    identities = [candidate_identity(item) for item in items]
    if len(set(identities)) != len(identities):
        raise ReaderContextError('ambiguous_duplicate_candidate_identity')
    if any(not isinstance(proof, Mapping) for proof in proofs):
        raise ReaderContextError('native_citation_proof_invalid')
    if [proof.get('candidate_id') for proof in proofs] != identities:
        raise ReaderContextError('native_citation_order_or_identity_mismatch')
    expected_citations = []
    candidates = []
    for item, proof, identity in zip(items, proofs, identities, strict=True):
        sources = _source_tuple(proof.get('source_event_ids'))
        if proof.get('memory_id') != item.id or sources != source_ids(item):
            raise ReaderContextError('native_citation_evidence_mismatch')
        if not sources or any(not isinstance(source_event_map.get(sid), str) or not source_event_map[sid] for sid in sources):
            raise ReaderContextError('citation_outside_captured_history')
        expected_citations.append((item.id, sources))
        for name in ('source_span', 'source_revision', 'source_chars'):
            if proof.get(name) != item.metadata.get(name):
                raise ReaderContextError('native_citation_span_mismatch')
        span = proof.get('source_span')
        if span is not None:
            total = proof.get('source_chars')
            if (not isinstance(span, Mapping) or span.get('unit') != 'characters'
                    or type(span.get('start')) is not int or type(span.get('end')) is not int
                    or type(total) is not int or not 0 <= span['start'] < span['end'] <= total
                    or span['end']-span['start'] != len(item.text)):
                raise ReaderContextError('native_citation_span_geometry_invalid')
        candidate = {
            'id': item.id, 'candidate_id': identity, 'text': item.text,
            'kind': item.kind.value, 'score': item.score,
            'source_event_ids': list(sources),
            'source_ids': sorted({source_event_map[sid] for sid in sources}),
        }
        candidate.update({name: dict(proof[name]) if name == 'source_span' else proof[name]
                          for name in ('source_span', 'source_revision', 'source_chars') if name in proof})
        candidates.append(candidate)
    actual_citations = Counter((citation.memory_id, _source_tuple(citation.source_event_ids))
                               for citation in bundle.citations)
    if actual_citations != Counter(expected_citations):
        raise ReaderContextError('bundle_citations_do_not_match_delivered_evidence')
    delivered = tuple({**candidate, 'partial': False} for candidate in candidates)
    context = _render(delivered)
    context_bytes = len(context.encode('utf-8'))
    # Every native estimated token covers <=4 UTF-8 bytes. The 3-byte bullet
    # framing is explicit overhead; a failed bound is an error, never a cut.
    byte_guard = 4*token_budget + 3*item_limit
    if context_bytes > byte_guard:
        raise ReaderContextError('serialization_guard_exceeded')
    return ReaderContext(context, tuple(candidates), delivered, {
        'mode': CORE_READER_MODE, 'budget_owner': 'native_core_packer',
        'evidence_estimated_token_budget': token_budget,
        'evidence_estimated_tokens': estimated_tokens,
        'item_limit': item_limit, 'serialization_utf8_guard': byte_guard,
        'context_utf8_bytes': context_bytes,
        'evidence_utf8_bytes': sum(len(item.text.encode('utf-8')) for item in items),
        'framing_utf8_bytes': 3*len(items), 'empty_context_marker': not bool(items),
        'serialized_context_estimated_tokens': token_estimate(context),
        'candidate_count': len(items), 'delivered_count': len(items),
        'partial_count': 0, 'omitted_count': 0,
        'delivered_candidate_ids': identities, 'selection': 'all_core_selected_evidence',
        'text_preservation': 'verbatim',
        'partial_semantics': 'relative to selected core evidence; core source windows remain explicit',
        'framing_in_core_budget': False, 'cross_arm_budget_equivalence': False,
    })


def legacy_reader_context(
    memories: Sequence[Mapping[str, Any]], *, byte_budget: int = LEGACY_CONTEXT_BYTES,
) -> ReaderContext:
    """Historical byte-prefix baseline, with explicit item/character diagnostics.

    This intentionally reproduces the old clipping behavior for raw baselines
    and offline before/after audits. It must not receive a native packed bundle.
    """
    if type(byte_budget) is not int or byte_budget < len(EMPTY_CONTEXT.encode('utf-8')):
        raise ReaderContextError('invalid_legacy_byte_budget')
    candidates = tuple(dict(memory) for memory in memories)
    if any(not isinstance(memory.get('text'), str) for memory in candidates):
        raise ReaderContextError('legacy_evidence_text_invalid')
    delivered = []
    selected = []
    remaining = byte_budget
    for index, memory in enumerate(candidates):
        available = remaining-3
        if available <= 0:
            break
        body = memory['text'].encode('utf-8')[:available].decode('utf-8', errors='ignore')
        if not body:
            break
        partial = body != memory['text']
        delivered.append({**memory, 'text': body, 'partial': partial})
        selected.append({'input_index': index, 'id': memory.get('id'),
                         'candidate_id': memory.get('candidate_id'), 'partial': partial,
                         'delivered_character_span': {'start': 0, 'end': len(body), 'unit': 'characters'},
                         'source_item_characters': len(memory['text'])})
        remaining -= len(body.encode('utf-8'))+3
        if partial:
            break
    context = _render(delivered)
    return ReaderContext(context, candidates, tuple(delivered), {
        'mode': LEGACY_READER_MODE, 'budget_owner': 'reader_byte_prefix_packer',
        'context_utf8_byte_cap': byte_budget, 'context_utf8_bytes': len(context.encode('utf-8')),
        'candidate_count': len(candidates), 'delivered_count': len(delivered),
        'partial_count': sum(item['partial'] for item in delivered),
        'omitted_count': len(candidates)-len(delivered), 'selected': selected,
        'omitted_input_indices': list(range(len(delivered), len(candidates))),
        'selection': 'ordered_prefix_with_possible_partial_last_item',
        'cross_arm_budget_equivalence': False,
    })


def legacy_pack(memories, *, byte_budget: int = LEGACY_CONTEXT_BYTES):
    """Tuple-compatible legacy serializer for frozen-protocol offline comparisons."""
    packed = legacy_reader_context(memories, byte_budget=byte_budget)
    return packed.context, list(packed.delivered)
