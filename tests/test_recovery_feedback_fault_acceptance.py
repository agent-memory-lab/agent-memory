"""Feedback aggregation and cancellation race acceptance with real storage."""
import asyncio

from agent_memory.compression_feedback import CompressionFeedback
from agent_memory.context_compression import CompressionPlan, ContextSegment
from agent_memory.recovery_store import SQLiteRecoveryStore
from test_recovery_specialized_acceptance import asynchronous, prepared, SCOPE


async def proposal_for(memory, state):
    result = await memory.propose_compression(CompressionPlan(
        state.run_id, state.version,
        (ContextSegment(state.source_event_ids[0], 'Read the report. Do not publish.'),),
        4096,
    ))
    assert result.accepted
    return result


@asynchronous
async def test_feedback_units_counters_and_evaluators_are_separate(tmp_path):
    memory, state = await prepared(tmp_path)
    try:
        proposal = await proposal_for(memory, state)
        cases = (
            ('a', 'tokens', 'counter-1', 'host-1'),
            ('b', 'utf8_bytes', 'counter-1', 'host-1'),
            ('c', 'tokens', 'counter-2', 'host-1'),
            ('d', 'tokens', 'counter-1', 'host-2'),
        )
        for identity, unit, counter, evaluator in cases:
            await memory.record_compression_feedback(CompressionFeedback(
                identity, proposal.summary_id, evaluator, 'succeeded', 1000, 500, unit, counter,
            ))
        report = await memory.compression_feedback_report()
        assert report['scanned'] == 4
        assert len(report['groups']) == 4
        assert {(g['unit'], g['counter_id'], g['evaluator_id']) for g in report['groups']} == {
            (unit, counter, evaluator) for _, unit, counter, evaluator in cases
        }
        assert all(g['samples'] == 1 and g['saved_units'] == 500 for g in report['groups'])
        assert report['automatic_promotion'] is False
        assert report['source'] == 'host_reported'
    finally:
        await memory.close()


@asynchronous
async def test_feedback_pagination_and_forget(tmp_path):
    memory, state = await prepared(tmp_path)
    try:
        proposal = await proposal_for(memory, state)
        for index in range(5):
            await memory.record_compression_feedback(CompressionFeedback(
                f'f{index}', proposal.summary_id, 'host', 'succeeded', 1000, 500, 'tokens', 'counter',
            ))
        cursor = None
        pages = []
        for _ in range(4):
            page = await memory.compression_feedback_report(limit=2, after=cursor)
            pages.append(page)
            cursor = page['next_cursor']
            if cursor is None:
                break
        assert cursor is None
        assert [p['scanned'] for p in pages] == [2, 2, 1]
        assert sum(g['samples'] for p in pages for g in p['groups']) == 5
        assert len({p['feedback_snapshot_digest'] for p in pages}) == 3
        assert all(p['page_only'] for p in pages)
        foreign = await SQLiteRecoveryStore(tmp_path / 'recovery.db').feedback_page(
            type(SCOPE)('other-tenant', session_id='session'), limit=2,
        )
        assert foreign['items'] == []
        await memory.forget_sources(state.source_event_ids)
        report = await memory.compression_feedback_report()
        assert report['scanned'] == 0
        assert report['groups'] == []
    finally:
        await memory.close()


@asynchronous
async def test_feedback_report_separates_persisted_strategy_versions(tmp_path):
    memory, state = await prepared(tmp_path)
    try:
        proposal = await proposal_for(memory, state)
        store = SQLiteRecoveryStore(tmp_path / 'recovery.db')
        for version in (1, 2):
            identity = f'version-{version}'
            payload = await memory.record_compression_feedback(CompressionFeedback(
                identity, proposal.summary_id, 'host', 'succeeded', 1000, 500, 'tokens', 'counter',
            ))
            # Seed two persisted snapshots via the storage port; this checks
            # report grouping, not the separately tested promotion lifecycle.
            payload = {**payload, 'strategy': {'strategy_id': 'extractive', 'version': str(version)}}
            await store.write(SCOPE, 'feedback', identity, payload, state.source_event_ids, expected_revision=1)
        report = await memory.compression_feedback_report()
        assert len(report['groups']) == 2
        assert {g['strategy']['version'] for g in report['groups']} == {'1', '2'}
        assert all(g['samples'] == 1 for g in report['groups'])
    finally:
        await memory.close()


@asynchronous
async def test_cancel_during_capture_does_not_undo_completed_ingest(tmp_path, monkeypatch):
    memory, state = await prepared(tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()
    original = SQLiteRecoveryStore.write
    blocked = False

    async def delayed_write(store, scope, kind, *args, **kwargs):
        nonlocal blocked
        if kind == 'receipt' and not blocked:
            blocked = True
            entered.set()
            await release.wait()
        return await original(store, scope, kind, *args, **kwargs)

    tasks = []
    try:
        await memory.enqueue_capture(event_id='racing', role='user', content='Read the second report.', run_id='run')
        with monkeypatch.context() as patch:
            patch.setattr(SQLiteRecoveryStore, 'write', delayed_write)
            processing = asyncio.create_task(memory.process_next_capture())
            tasks.append(processing)
            await asyncio.wait_for(entered.wait(), timeout=2)
            cancelling = asyncio.create_task(memory.cancel_capture('racing'))
            tasks.append(cancelling)
            await asyncio.sleep(0)
            assert not cancelling.done()
            release.set()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)
        receipt = await memory.capture_receipt('racing')
        assert receipt.persisted
        assert receipt.raw_readable
        assert receipt.queue_status == 'done'
        assert await memory.process_next_capture() is None
    finally:
        release.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await memory.close()
