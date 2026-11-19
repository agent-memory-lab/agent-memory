import asyncio
from dataclasses import replace
from datetime import datetime, UTC
import pytest
from agent_memory.domain import MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.retrieval.lossless_packing import LosslessBudget, pack_lossless_evidence
from agent_memory import build_local_kernel, MemoryEvent

# Synthetic deterministic counter: verifies budget semantics, NOT model tokens.
def budget():
    return LosslessBudget(lambda s, i: 20 + sum(len(v.text) for v in (*s,*i)), 'synthetic-characters')
def item(text, id='a'):
    return MemoryItem(id, MemoryKind.EVENT, text, 1., datetime.now(UTC))
def query(n=256):
    return MemoryQuery(MemoryScope('test'), 'received cost date', token_budget=n, include_current_state=False)

def test_no_loss_of_tail_numbers_dates_or_negation():
    a=item('context ' * 12 + 'not received until 2031-03-18; total 300, not 300 more.')
    result=pack_lossless_evidence(query(), (), (a,), budget=budget())
    assert result.items == (a,) and result.items[0] is a
    assert result.tokens == 20 + len(a.text)

def test_overflow_skips_whole_candidate_and_records_reason():
    a=item('x'*300);b=item('not yet; 200 total','b')
    r=pack_lossless_evidence(query(), (), (a,b), budget=budget())
    assert r.items == (b,)
    assert r.metadata['packing_omitted'][0]['reason']=='token_budget'

def test_exact_boundary_and_prompt_overhead():
    a=item('x'*236)
    assert pack_lossless_evidence(query(),(),(a,),budget=budget()).tokens==256
    assert not pack_lossless_evidence(query(255),(),(a,),budget=budget()).items
    with pytest.raises(ValueError,match='overhead'):
        pack_lossless_evidence(query(),(),(),budget=LosslessBudget(lambda *_:257,'fake'))

def test_order_and_same_source_distinct_facts_are_not_collapsed():
    a=replace(item('200 total'),metadata={'source_event_ids':('shared',)})
    b=replace(item('300 needed','b'),metadata={'source_event_ids':('shared',)})
    r=pack_lossless_evidence(query(),(),(a,b,a),budget=budget())
    assert r.items==(a,b)
    assert r.metadata['packing_omitted'][0]['reason']=='duplicate_identity'

def test_invalid_span_is_rejected_without_truncation():
    a=replace(item('abc'),metadata={'source_span':{'start':-1,'end':3}})
    r=pack_lossless_evidence(query(),(),(a,),budget=budget())
    assert not r.items and r.metadata['packing_omitted'][0]['reason']=='invalid_source_span'

@pytest.mark.parametrize('bad',[True,-1,1.5,None])
def test_invalid_counter_is_fail_closed(bad):
    with pytest.raises(ValueError):
        pack_lossless_evidence(query(),(),(),budget=LosslessBudget(lambda *_:bad,'fake'))

def test_counter_exceptions_are_not_swallowed():
    def fail(*_):raise RuntimeError('tokenizer missing')
    with pytest.raises(RuntimeError,match='tokenizer missing'):
        pack_lossless_evidence(query(),(),(),budget=LosslessBudget(fail,'unavailable'))

def test_real_kernel_opt_in_and_default_preserved(tmp_path):
    async def run():
        for enabled in [False,True]:
            k=build_local_kernel(tmp_path / str(enabled),**({'lossless_packing_budget':budget()} if enabled else {}))
            await k.initialize()
            try:
                scope=MemoryScope('test')
                await k.ingest_event(MemoryEvent(scope,'user.message','Archive parcel received March 18, total 300.',id='source'))
                b=await k.retrieve(MemoryQuery(scope,'archive parcel',token_budget=512,include_current_state=False))
                assert b.relevant_memories
                if enabled:
                    from agent_memory.retrieval.packing import ordered_bundle_evidence
                    assert b.token_estimate == budget().measure(b.current_state, ordered_bundle_evidence(b))
                    assert b.retrieval_metadata['packing_final_delivery_recounted'] is True
                assert (b.retrieval_metadata['strategy']=='lossless-ranked-units/1')==enabled
            finally:await k.close()
    asyncio.run(run())

def test_full_source_replaces_derived_claim_but_not_other_source():
    raw=item('The invoice total is 300, not 300 more.', 'source')
    a=MemoryItem('claim',MemoryKind.CLAIM,'300 more',1.,raw.occurred_at,{'source_event_ids':('source',)})
    b=replace(a,id='other',metadata={'source_event_ids':('source','missing')})
    r=pack_lossless_evidence(query(),(),(a,raw,b),budget=budget())
    assert r.items==(raw,b)
    assert r.metadata['packing_omitted'][0]['reason']=='covered_by_complete_source'

def test_partial_source_does_not_replace_claim():
    raw=replace(item('partial','source'),metadata={'excerpt':True})
    a=MemoryItem('claim',MemoryKind.CLAIM,'full meaning',1.,raw.occurred_at,{'source_event_ids':('source',)})
    assert pack_lossless_evidence(query(),(),(a,raw),budget=budget()).items==(a,raw)

def test_preservation_guard_requires_same_source_and_revision():
    from agent_memory.retrieval.lossless_packing import _preserves_event
    old=replace(item('300'),metadata={'source_revision':'rev'})
    full=replace(old,text='not 300 more')
    assert _preserves_event(old,(full,))
    assert not _preserves_event(old,(replace(full,id='other'),))
    assert not _preserves_event(old,(replace(full,metadata={'source_revision':'new'}),))
    assert not _preserves_event(old,(replace(full,kind=MemoryKind.CLAIM),))

def test_guard_fallback_is_explicit_and_budget_checked(monkeypatch):
    import agent_memory.retrieval.lossless_packing as module
    a=item('not 300 more'); b=item('irrelevant','b')
    base=module.pack_lossless_evidence(query(),(),(a,),budget=budget())
    proposal=module.pack_lossless_evidence(query(),(),(b,),budget=budget())
    monkeypatch.setattr(module,'pack_native_evidence',lambda *a,**kw:base)
    monkeypatch.setattr(module,'pack_lossless_evidence',lambda *a,**kw:proposal)
    result=module.pack_preserving_evidence(query(),(),(a,b),budget=budget())
    assert result.items==base.items
    assert result.metadata['packing_fallback']=='baseline_v2'
    assert result.metadata['packing_missing_baseline_positions']==(0,)
    with pytest.raises(ValueError,match='baseline-preserving payload'):
        module.pack_preserving_evidence(query(),(),(a,b),budget=LosslessBudget(lambda *_:257,'test'))

def test_guard_accepts_complete_same_source_expansion(monkeypatch):
    import agent_memory.retrieval.lossless_packing as module
    full=item('300 total, not 300 more')
    base=module.pack_lossless_evidence(query(),(),(replace(full,text='300 total'),),budget=budget())
    monkeypatch.setattr(module,'pack_native_evidence',lambda *a,**kw:base)
    result=module.pack_preserving_evidence(query(),(),(full,),budget=budget())
    assert result.items==(full,)
    assert result.metadata['packing_fallback'] is None

def test_guard_does_not_confuse_repeated_text_at_different_offsets():
    import hashlib, json
    from agent_memory.retrieval.lossless_packing import _preserves_event
    text='300 then 300';revision=hashlib.sha256(text.encode()).hexdigest()
    def span(lo,hi):
        return replace(item(text[lo:hi]),metadata={
            'source_revision':revision,'source_chars':len(text),
            'source_span':{'start':lo,'end':hi,'unit':'characters'},
            'lexical_chunk_id':hashlib.sha256(json.dumps(['events','a',revision,lo,hi]).encode()).hexdigest()})
    assert not _preserves_event(span(0,3),(span(9,12),))
    assert _preserves_event(span(0,3),(span(0,len(text)),))

def test_guard_checks_native_excerpt_offsets():
    import hashlib, json
    from agent_memory.retrieval.lossless_packing import _preserves_event
    text='300 then 300';revision=hashlib.sha256(text.encode()).hexdigest()
    base={'source_revision':revision,'source_chars':len(text)}
    locator=hashlib.sha256(json.dumps(['events','a',revision,0,len(text)]).encode()).hexdigest()
    old=replace(item('300'),metadata={**base,'source_span':{'start':9,'end':12,'unit':'characters'},
        'retrieved_source_span':{'start':0,'end':len(text),'unit':'characters'},'retrieved_lexical_chunk_id':locator})
    full=replace(item(text),metadata={**base,'source_span':{'start':0,'end':len(text),'unit':'characters'},'lexical_chunk_id':locator})
    assert _preserves_event(old,(full,))


def test_final_guard_changed_selection_is_recounted_and_not_restored(tmp_path):
    async def run():
        counting = LosslessBudget(lambda state, items: 20 + sum(len(v.text) for v in (*state, *items)), 'synthetic')
        k = build_local_kernel(tmp_path / 'guard', lossless_packing_budget=counting)
        await k.initialize()
        try:
            scope = MemoryScope('test')
            await k.ingest_event(MemoryEvent(scope, 'user.message', 'Archive parcel cost 300.', id='source'))
            original = k._repository.guard_retrieval_delivery
            async def removing_guard(query, bundle, **kwargs):
                guarded = await original(query, bundle, **kwargs)
                return replace(guarded, relevant_memories=(), current_state=(), episodes=(), procedures=(), citations=(), token_estimate=0)
            k._repository.guard_retrieval_delivery = removing_guard
            q = MemoryQuery(scope, 'archive parcel', token_budget=512, include_current_state=False)
            result = await k.retrieve(q)
            assert not result.relevant_memories
            assert result.token_estimate == 20
            # A synchronous counter can include overhead which changes with the
            # authoritative selection; the final check must not trust old totals.
            async def oversized_guard(query, bundle, **kwargs):
                guarded = await original(query, bundle, **kwargs)
                object.__setattr__(counting, 'count', lambda *_: 513)
                return guarded
            k._repository.guard_retrieval_delivery = oversized_guard
            with pytest.raises(ValueError, match='final delivery payload'):
                await k.retrieve(replace(q, request_id="second-recount"))
        finally:
            await k.close()
    asyncio.run(run())
