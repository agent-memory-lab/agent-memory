"""Real storage and optional tokenizer acceptance; no vendor quality claims."""
import asyncio
from dataclasses import replace
from functools import wraps
from pathlib import Path
import subprocess
import sys

import pytest

from agent_memory import MemoryScope
from agent_memory.recovery import RecoveryState, ToolExecutionState
from agent_memory.recovery_store import SQLiteRecoveryStore, RecoveryConflict
from agent_memory.recovery_partitions import PartitionedRecoveryStore
from agent_memory.unified_memory import UnifiedMemory
from agent_memory.context_compression import CompressionPlan, ContextSegment
from agent_memory.compression_feedback import CompressionFeedback
from agent_memory.model_token_counter import TiktokenModelCounter


def asynchronous(fn):
    @wraps(fn)
    def run(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))
    return run


SCOPE = MemoryScope('specialized-acceptance', session_id='session')


async def prepared(path):
    memory = UnifiedMemory.local(path / 'memory.db', SCOPE, recovery_path=path / 'recovery.db')
    await memory.initialize()
    receipt = await memory.capture_with_receipt(event_id='event', role='user', content='Read the report. Do not publish.', run_id='run')
    source = receipt.provider_event_id
    state = RecoveryState('run', 1, 'Read report', (source,), tools=(ToolExecutionState('call', 'publish', 'succeeded', True, (source,)),))
    await memory.save_recovery(state)
    return memory, state


@asynchronous
async def test_history_pagination_restore_conflict_and_restart(tmp_path):
    memory, state = await prepared(tmp_path)
    try:
        await memory.save_recovery(replace(state, version=2, goal='Second'), expected_version=1)
        await memory.save_recovery(replace(state, version=3, goal='Third'), expected_version=2)
        first = await memory.recovery_history('run', limit=1)
        assert [item['version'] for item in first['items']] == [2]
        second = await memory.recovery_history('run', limit=1, before=first['next_cursor'])
        assert [item['version'] for item in second['items']] == [1]
        assert second['next_cursor'] is None
        with pytest.raises(RecoveryConflict):
            await memory.restore_recovery('run', 1, expected_version=2)
        restored = await memory.restore_recovery('run', 1, expected_version=3)
        assert restored.version == 4
        assert restored.goal == state.goal
        assert restored.tools[0].status == 'unknown'
    finally:
        await memory.close()
    memory = UnifiedMemory.local(tmp_path / 'memory.db', SCOPE, recovery_path=tmp_path / 'recovery.db')
    await memory.initialize()
    try:
        assert (await memory.load_recovery('run')).version == 4
    finally:
        await memory.close()


@asynchronous
async def test_forget_invalidates_current_and_historical_state(tmp_path):
    memory, state = await prepared(tmp_path)
    try:
        await memory.save_recovery(replace(state, version=2), expected_version=1)
        await memory.forget_sources(state.source_event_ids)
        assert await memory.load_recovery('run') is None
        assert (await memory.recovery_history('run'))['items'] == []
        with pytest.raises((RecoveryConflict, ValueError)):
            await memory.restore_recovery('run', 1, expected_version=2)
    finally:
        await memory.close()


@asynchronous
async def test_history_scope_isolation(tmp_path):
    memory, state = await prepared(tmp_path)
    try:
        await memory.save_recovery(replace(state, version=2), expected_version=1)
        store = SQLiteRecoveryStore(tmp_path / 'recovery.db')
        await store.initialize()
        foreign = MemoryScope('other-tenant', session_id='session')
        assert (await store.history(foreign, 'run'))['items'] == []
        assert await store.historical_state(foreign, 'run', 1) is None
    finally:
        await memory.close()


class Approve:
    async def authorize(self, *args):
        return True


@asynchronous
async def test_retirement_delete_failure_persists_and_retries(tmp_path, monkeypatch):
    directory = tmp_path / 'partitions'
    store = PartitionedRecoveryStore(directory, SCOPE, authorizer=Approve())
    await store.initialize()
    info = await store.partition_info()
    run = info['required_prefix'] + 'run'
    await store.ensure_run(SCOPE, run)
    await store.configure_run(SCOPE, run, completed=True)
    await store.rotate(expected_generation=info['generation'], approval='host-approved')
    unlink = Path.unlink
    def denied(path, *args, **kwargs):
        if path.parent == directory:
            raise PermissionError('injected unlink failure')
        return unlink(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', denied)
        result = await store.retire(info['generation'], approval='host-approved')
        assert result['cleanup_complete'] is False
    reopened = PartitionedRecoveryStore(directory, SCOPE, authorizer=Approve())
    await reopened.initialize()
    result = await reopened.retry_retired_cleanup()
    assert result['completed'] == 1
    assert result['pending'] == 0
    assert (await reopened.retry_retired_cleanup())['attempted'] == 0
    with pytest.raises((RecoveryConflict, ValueError)):
        await reopened.ensure_run(SCOPE, run)


@pytest.mark.parametrize('text', ['hello world', '\u4f60\u597d\uff0c\u8bb0\u5fc6', 'a\n\tb', '<|endoftext|>'])
def test_real_tokenizer_exact_count_and_overhead(text):
    tiktoken = pytest.importorskip('tiktoken')
    counter = TiktokenModelCounter(encoding_name='cl100k_base', model_id='acceptance-model', template_version='v1', framing_tokens=7, reserve_tokens=11)
    expected = len(tiktoken.get_encoding('cl100k_base').encode(text, disallowed_special=())) + 18
    assert counter.count(text) == expected


def test_tokenizer_template_rendering_and_identity():
    tiktoken = pytest.importorskip('tiktoken')
    options = dict(encoding_name='cl100k_base', model_id='acceptance-model', template_version='v1')
    counter = TiktokenModelCounter(**options, render=lambda text: 'USER: ' + text)
    assert counter.count('hello') == len(tiktoken.get_encoding('cl100k_base').encode('USER: hello'))
    other = TiktokenModelCounter(**{**options, 'template_version': 'v2'})
    assert counter.counter_id != other.counter_id


@asynchronous
async def test_failed_receipt_storage_does_not_acknowledge(tmp_path, monkeypatch):
    memory = UnifiedMemory.local(tmp_path / 'memory.db', SCOPE, recovery_path=tmp_path / 'recovery.db')
    await memory.initialize()
    original = SQLiteRecoveryStore.write
    async def fail(self, scope, kind, *args, **kwargs):
        if kind == 'receipt':
            raise OSError('injected disk failure')
        return await original(self, scope, kind, *args, **kwargs)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(SQLiteRecoveryStore, 'write', fail)
            with pytest.raises(OSError):
                await memory.capture_with_receipt(event_id='failed-event', role='user', content='text', run_id='run')
        assert await memory.capture_receipt('failed-event') is None
    finally:
        await memory.close()


@asynchronous
async def test_sqlite_process_crash_rolls_back_transaction(tmp_path):
    memory, state = await prepared(tmp_path)
    await memory.close()
    script = """import sqlite3,sys,os
connection=sqlite3.connect(sys.argv[1])
connection.execute('BEGIN IMMEDIATE')
connection.execute('DELETE FROM recovery_records_v1')
os._exit(17)
"""
    result = subprocess.run([sys.executable, '-c', script, str(tmp_path / 'recovery.db')], timeout=10)
    assert result.returncode == 17
    memory = UnifiedMemory.local(tmp_path / 'memory.db', SCOPE, recovery_path=tmp_path / 'recovery.db')
    await memory.initialize()
    try:
        assert await memory.load_recovery('run') == state
    finally:
        await memory.close()
