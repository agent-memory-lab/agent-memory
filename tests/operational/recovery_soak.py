"""Opt-in bounded recovery soak. Never uses an existing database."""
import argparse
import asyncio
from collections import deque
import json
import os
from pathlib import Path
import resource
import sqlite3
import statistics
import sys
import time
from dataclasses import replace

from agent_memory import MemoryScope
from agent_memory.recovery import RecoveryState
from agent_memory.recovery_partitions import PartitionedRecoveryStore
from agent_memory.unified_memory import UnifiedMemory


class Approval:
    async def authorize(self, *args):
        return True


def rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == 'darwin' else value * 1024


def fd_count():
    return len(os.listdir('/dev/fd'))


async def run(args):
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    data = root / 'data'
    data.mkdir(exist_ok=False)
    scope = MemoryScope('soak-only', session_id='isolated')
    started = time.monotonic()
    timings = deque(maxlen=10000)
    baseline_fd = fd_count()
    peak_fd = baseline_fd
    completed = 0
    generations = 0
    restarts = 0
    next_sample = 0.0
    status = 'failed'
    error = None
    memory = None

    async def open_memory():
        store = PartitionedRecoveryStore(data / 'partitions', scope, authorizer=Approval(), max_records=2048, max_retained_partitions=3)
        memory = UnifiedMemory.local(data / 'memory.db', scope, recovery_store=store)
        await memory.initialize()
        return memory, store

    async def cycle(index, prefix):
        tick = time.monotonic()
        run_id = prefix + f'run-{index}'
        event_id = prefix + f'event-{index}'
        event = dict(event_id=event_id, role='user', content=f'Read report {index}; do not publish.', run_id=run_id)
        if index % 2:
            await memory.enqueue_capture(**event)
            receipt = None
        else:
            receipt = await memory.capture_with_receipt(**event)
        # Shared queue is drained after all concurrent producers finish.
        return run_id, event_id, receipt, tick

    try:
        memory, store = await open_memory()
        while time.monotonic() - started < args.seconds:
            info = await store.partition_info()
            batch = await asyncio.gather(*(cycle(completed + n, info['required_prefix']) for n in range(args.concurrency)))
            while await memory.process_next_capture() is not None:
                pass
            for run_id, event_id, receipt, tick in batch:
                receipt = receipt or await memory.capture_receipt(event_id)
                assert receipt.persisted and receipt.raw_readable
                state = RecoveryState(run_id, 1, 'Read report without publishing', (receipt.provider_event_id,))
                await memory.save_recovery(state)
                await memory.save_recovery(replace(state, version=2, goal='Finish review'), expected_version=1)
                assert (await memory.recovery_history(run_id))['items'][0]['version'] == 1
                restored = await memory.restore_recovery(run_id, 1, expected_version=2)
                assert restored.version == 3 and restored.goal == state.goal
                await memory.complete_recovery(run_id, expected_version=3)
                await memory.forget_sources(state.source_event_ids)
                assert await memory.load_recovery(run_id) is None
                timings.append(time.monotonic() - tick)
                completed += 1
            await memory.cleanup_recovery(limit=100)
            await store.rotate(expected_generation=info['generation'], approval='isolated-soak')
            retired = await store.retire(info['generation'], approval='isolated-soak')
            assert retired['cleanup_complete']
            generations += 1
            if generations % 20 == 0:
                await memory.close()
                memory, store = await open_memory()
                restarts += 1
            elapsed = time.monotonic() - started
            current_fd = fd_count()
            peak_fd = max(peak_fd, current_fd)
            assert current_fd <= baseline_fd + 32, 'file descriptor budget exceeded'
            assert rss_bytes() <= args.max_rss_mib * 1024 ** 2, 'resident memory budget exceeded'
            if elapsed >= next_sample:
                disk = sum(p.stat().st_size for p in data.rglob('*') if p.is_file())
                assert disk <= 512 * 1024 ** 2, 'disk budget exceeded'
                sample = dict(elapsed_seconds=round(elapsed, 2), completed=completed, generations=generations, restarts=restarts,
                              max_rss_bytes=rss_bytes(), fd=current_fd, disk_bytes=disk,
                              cycle_median_ms=round(statistics.median(timings) * 1000, 2))
                print(json.dumps(sample), flush=True)
                next_sample = elapsed + 60
            await asyncio.sleep(0.02)
        await memory.close()
        memory = None
        for database in data.rglob('*.db'):
            connection = sqlite3.connect(database)
            try:
                assert connection.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
            finally:
                connection.close()
        status = 'passed'
    except BaseException as exc:
        error = {'type': type(exc).__name__, 'message': str(exc)}
        raise
    finally:
        if memory is not None:
            await memory.close()
        ordered = sorted(timings)
        report = dict(status=status, error=error, requested_seconds=args.seconds, elapsed_seconds=time.monotonic() - started,
                      concurrency=args.concurrency, completed_cycles=completed, retired_generations=generations, restarts=restarts,
                      max_rss_bytes=rss_bytes(), baseline_fd=baseline_fd, peak_fd=peak_fd,
                      recent_cycle_p95_ms=ordered[int((len(ordered)-1)*.95)]*1000 if ordered else None,
                      boundary='single-host recovery lifecycle; not distributed or saturation throughput')
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--seconds', type=int, default=1800)
    parser.add_argument('--concurrency', type=int, default=4)
    parser.add_argument('--max-rss-mib', type=int, default=256)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 86400 or not 1 <= args.concurrency <= 32:
        parser.error('duration or concurrency outside safe bounds')
    asyncio.run(run(args))
