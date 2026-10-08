import asyncio
from copy import deepcopy
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_purge_restore import restorer

from agent_memory.operations.refresh_schedule_contract import stamp
from agent_memory.operations.retention import RetentionError

store = base.store


@pytest.mark.parametrize(
    "mutation", ["unsigned_floor", "naive_floor", "extra_field", "mixed_version"]
)
def test_invalid_v2_import_cannot_advance_clock(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            repo = engine.repository
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
            operator = restorer(repo, scope, clock)
            snapshot = deepcopy(await operator.export())
            if mutation == "unsigned_floor":
                snapshot["checkpoint"]["scheduler_clock_floor"] = stamp(
                    clock[0] + timedelta(days=5)
                )
            else:
                if mutation == "naive_floor":
                    snapshot["checkpoint"]["scheduler_clock_floor"] = "2026-10-01T00:00:00.000000"
                if mutation == "extra_field":
                    snapshot["checkpoint"]["unexpected"] = 1
                if mutation == "mixed_version":
                    snapshot["checkpoint"]["schema"] = "purge-restore-checkpoint/1"
                snapshot["signature"] = operator._sign(
                    {k: snapshot[k] for k in ("schema", "checkpoint", "entries")}
                )
            with pytest.raises(RetentionError):
                await operator.replay(
                    snapshot,
                    expected_checkpoint=snapshot["checkpoint"],
                    restore_id="bad",
                    reason="bad-import",
                )
            async with repo.unit_of_work() as uow:
                assert await uow.refresh_scheduler_clock(scope) == stamp(clock[0])
                assert await uow.purge_restore_count(scope) == 0

    asyncio.run(run())


def test_legacy_v1_exact_wire_compatibility(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            operator = restorer(engine.repository, scope, clock)
            snapshot = await operator.export()
            from agent_memory.operations.purge_restore import digest

            checkpoint = dict(
                schema="purge-restore-checkpoint/1",
                authority_id="authority",
                scope_key=scope.partition_key(),
                head=0,
                scope_epoch=0,
                entries_sha256=digest([]),
            )
            body = dict(schema="purge-restore-journal/1", checkpoint=checkpoint, entries=[])
            assert snapshot == {**body, "signature": operator._sign(body)}
            await operator.replay(
                snapshot, expected_checkpoint=checkpoint, restore_id="old", reason="legacy-import"
            )
            assert await operator.export() == snapshot

    asyncio.run(run())
