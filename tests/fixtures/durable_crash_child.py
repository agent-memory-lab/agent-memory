"""Real-process recovery fixture. Parent kills us only after the named SQL boundary."""

import asyncio
import json
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import test_atom_admission as base
from test_durable_execution import Generator

from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import MemoryScope
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.retention import DurableReceiver


async def pause():
    print("ready", flush=True)
    await asyncio.Event().wait()


async def main(config):
    if config["backend"] == "postgres":
        from agent_memory_postgres.repository import PostgresMemoryRepository

        repository = PostgresMemoryRepository.from_dsn(config["database"], max_size=2)
    else:
        from agent_memory.sqlite import SQLiteMemoryRepository

        repository = SQLiteMemoryRepository(config["database"])
    await repository.initialize()
    scope = MemoryScope(**config["scope"])
    now = datetime.fromisoformat(config["clock"])
    receiver = DurableReceiver(repository, clock=lambda: now)
    phase = config["phase"]
    unit_type = type(repository.unit_of_work())
    if phase.startswith("receive_"):
        producer = DurableProducer(receiver)
        session = await producer.open(
            scope, producer_id="crash-device", actor="alice", configuration_sha256="a" * 64
        )
        event = replace(
            base.source(scope, identity="crash-receive", idempotency="crash-receive"), actor="alice"
        )
        if phase == "receive_before_commit":
            original = unit_type.producer_put

            async def before_commit(uow, *args):
                await original(uow, *args)
                await pause()

            unit_type.producer_put = before_commit
        await producer.append(event, session, sequence=1, actor="alice")
        await pause()
    elif phase.startswith("publication_"):
        from test_publication_batches import Generator as BatchGenerator
        from test_publication_batches import ReplacementGenerator

        from agent_memory.operations.indexing import CandidateIndexChannel
        from agent_memory.operations.publication_batches import PublicationPolicy

        generator = ReplacementGenerator() if "activation" in phase else BatchGenerator()
        pipeline = AtomExtractionPipeline(generator, generator)
        channel, policy = CandidateIndexChannel("local"), PublicationPolicy(1)
        configuration = processing_configuration_sha256(
            pipeline, base.POLICY, base.SELF, index_channel=channel, publication_policy=policy
        )
        queue = ExtractionQueue(
            repository, scope, configuration, clock=lambda: now, retry_seconds=0
        )
        handler = DurableAtomHandler(
            queue,
            pipeline,
            base.POLICY,
            base.SELF,
            local_only=True,
            index_channel=channel,
            publication_policy=policy,
        )
        lease = await queue.claim("crashed-publication", lease_seconds=5)
        original_generate = generator.generate_atoms

        async def tracked_generation(event):
            with open(config["calls"], "a") as log:
                log.write("generate\n")
            return await original_generate(event)

        generator.generate_atoms = tracked_generation
        original_update, original_exit = unit_type.retention_update, unit_type.__aexit__

        async def update(uow, *args):
            await original_update(uow, *args)
            row = args[-1]
            match = (
                row.get("status") == "running"
                and len(row.get("publication_manifest", {}).get("publications", [])) == 1
                if "batch" in phase
                else row.get("status") == "completed"
            )
            if match:
                if phase.endswith("before_commit"):
                    await pause()
                uow._publication_crash_pause = True

        async def after_exit(uow, *args):
            result = await original_exit(uow, *args)
            if args[0] is None and getattr(uow, "_publication_crash_pause", False):
                await pause()
            return result

        unit_type.retention_update = update
        if phase.endswith("after_commit"):
            unit_type.__aexit__ = after_exit
        await handler(lease.task, lambda value: queue.checkpoint(lease, value))
        await pause()
    elif phase.startswith("purge_restore_"):
        from agent_memory.operations.purge_restore import PurgeRestore

        operator = PurgeRestore(
            repository,
            scope,
            authority_id="authority",
            actor="operator",
            secret=config["test_secret"].encode(),
            clock=lambda: now,
        )
        if phase.endswith("before_commit"):
            original = unit_type.purge_restore_put

            async def before_commit(uow, *args):
                await original(uow, *args)
                await pause()

            unit_type.purge_restore_put = before_commit
        snapshot = config["snapshot"]
        await operator.replay(
            snapshot,
            expected_checkpoint=snapshot["checkpoint"],
            restore_id="restore",
            reason="offline-backup",
        )
        await pause()
    elif phase.startswith("recovery_"):
        from agent_memory.operations.index_recovery import CandidateIndexRecovery
        from agent_memory.operations.indexing import CandidateIndexChannel

        recovery = CandidateIndexRecovery(
            repository, scope, CandidateIndexChannel("local"), actor="operator", clock=lambda: now
        )
        rollover = "rollover" in phase
        if phase.endswith("before_commit"):
            original = unit_type.index_recovery_put

            async def before_commit(uow, scope, channel, epoch, kind, identity, row):
                await original(uow, scope, channel, epoch, kind, identity, row)
                if kind == ("head" if rollover else "repair"):
                    await pause()

            unit_type.index_recovery_put = before_commit
        if rollover:
            await recovery.rollover(recovery_id="crash", expected_generation=0, reason="operator")
        else:
            async with repository.unit_of_work() as uow:
                jobs = await uow.index_jobs(scope, recovery.channel.key, 0)
            publication = jobs[0]["token"]["id"]
            snapshot = await recovery.inspect(publication)
            await recovery.repair(
                publication,
                recovery_id="crash",
                expected_job_sha256=snapshot["job_sha256"],
                stream=snapshot["stream"],
                reason="operator",
            )
        await pause()
    elif phase.startswith("control_"):
        from agent_memory.derived import HostGrantAuthority, ObservationService, QueryDefinition

        service = ObservationService(
            repository,
            scope,
            base.POLICY,
            clock=lambda: now,
            authority_id="local-host",
            authority_min_version=1,
        )
        if phase.endswith("before_commit"):
            original = unit_type.derived_put

            async def before_commit(uow, scope, kind, identity, row):
                await original(uow, scope, kind, identity, row)
                if kind == "definition":  # Control replacement and outbox are still uncommitted.
                    await pause()

            unit_type.derived_put = before_commit
        if phase.startswith("control_query_"):
            await service.register_query(
                QueryDefinition("language-inputs", scope, "alice", ("locale",), version="2"),
                expected_generation=1,
            )
        else:
            await service.set_authority(
                HostGrantAuthority(
                    "local-host", ("alice",), now + timedelta(hours=1), revoked=True
                ),
                expected_version=1,
            )
        await pause()
    elif phase.startswith("coverage_"):
        from agent_memory.derived import (
            FacetContext,
            FacetDefinition,
            ObservationService,
            QueryDefinition,
        )

        if config["backend"] == "postgres":
            import agent_memory_postgres.admission as writer
        else:
            import agent_memory.sqlite as writer
        writer.utc_now = lambda: now
        service = ObservationService(repository, scope, base.POLICY, clock=lambda: now,
            context_token=config.get("context_token"),
            authority_id="local-host", authority_min_version=1, history_mode="published-interval/1")
        if phase.endswith("before_commit"):
            original = unit_type.derived_put

            async def before_commit(uow, scope, kind, identity, row):
                await original(uow, scope, kind, identity, row)
                if kind == "history_interval":
                    await pause()

            unit_type.derived_put = before_commit
        if phase.startswith("coverage_context_"):
            spec = dict(config["definition"])
            spec["context"] = FacetContext.from_payload(spec["context"])
            for key in ("predicates", "readers"):
                spec[key] = tuple(spec[key])
            await service.register(FacetDefinition(**spec), expected_generation=2)
        elif phase.startswith("coverage_query_"):
            await service.register_query(
                QueryDefinition("language-inputs", scope, "alice", ("locale",), version="2"),
                expected_generation=1,
            )
        else:
            async with repository.unit_of_work() as uow:
                atom = next(r for r in await uow.list_admission_records(scope)
                            if r["payload"]["draft"]["predicate"] == "locale")
                payload = dict(atom["payload"], action="REJECT")
                await uow.save_admission_record(
                    scope, atom["id"], atom["event_id"], atom["slot_key"], payload, atom["version"]
                )
        await pause()
    elif phase.startswith(("derived_", "history_", "interval_")):
        from agent_memory.derived import ObservationService
        from agent_memory.operations.facet_refresh import FacetRefreshQueue

        service = ObservationService(
            repository, scope, base.POLICY, clock=lambda: now,
            context_token=config.get("context_token"),
            **(dict(authority_id="local-host", authority_min_version=1,
                    history_mode=("published-interval/1" if phase.startswith("interval_")
                                  else "published-point/1"))
               if phase.startswith(("history_", "interval_")) else {}),
        )
        queue = FacetRefreshQueue(service)
        lease = await queue.claim("crashed-derived-worker", lease_seconds=5)
        if phase in {"derived_before_commit", "history_before_commit", "interval_before_commit"}:
            original = unit_type.derived_put

            async def before_commit(uow, scope, kind, identity, row):
                await original(uow, scope, kind, identity, row)
                if (phase == "history_before_commit" and kind == "history_point") or (
                    phase == "interval_before_commit" and kind == "history_interval"
                ) or (
                    phase == "derived_before_commit"
                    and kind == "job"
                    and row["status"] == "completed"
                ):
                    await pause()

            unit_type.derived_put = before_commit
        await service.apply(lease.task)
        await pause()
    elif phase.startswith("refresh_"):
        from agent_memory.operations.resource_refresh import ResourceRefreshQueue

        queue = ResourceRefreshQueue(repository, scope, clock=lambda: now)
        lease = await queue.claim("crashed-refresh-worker", lease_seconds=5)
        if phase == "refresh_before_commit":
            original = unit_type.refresh_put

            async def before_commit(uow, scope, kind, identity, row):
                await original(uow, scope, kind, identity, row)
                if kind == "resource" and row["completed_through"]:
                    await pause()

            unit_type.refresh_put = before_commit

        async def output(uow, task):
            await uow.delivery_insert(
                scope, "target", "refresh-crash-output", {"units": task.payload["claimed_through"]}
            )
            return task.payload["claimed_through"]

        await queue.commit(lease.task, output)
        await pause()
    elif phase.startswith("index_"):
        from agent_memory.operations.indexing import CandidateIndexChannel, CandidateIndexQueue

        queue = CandidateIndexQueue(
            repository, scope, CandidateIndexChannel("local"), clock=lambda: now
        )
        lease = await queue.claim("crashed-indexer", lease_seconds=5)
        if phase == "index_before_commit":
            original = unit_type.index_job_put

            async def before_commit(uow, scope, row):
                await original(uow, scope, row)
                if row["status"] == "completed":
                    await pause()

            unit_type.index_job_put = before_commit
        await queue.apply(lease.task, None)
        await pause()
    else:
        generator = Generator()
        pipeline = AtomExtractionPipeline(generator, generator)
        configuration = processing_configuration_sha256(pipeline, base.POLICY, base.SELF)
        queue = ExtractionQueue(
            repository, scope, configuration, clock=lambda: now, retry_seconds=0
        )
        handler = DurableAtomHandler(queue, pipeline, base.POLICY, base.SELF, local_only=True)
        lease = await queue.claim("crashed-worker", lease_seconds=5)
        if phase == "worker_after_claim":
            await pause()
        original_generate = generator.generate_atoms

        async def tracked_generation(event):
            with open(config["calls"], "a") as log:
                log.write("generate\n")
                log.flush()
            return await original_generate(event)

        generator.generate_atoms = tracked_generation
        if phase == "worker_before_publication_commit":
            original = unit_type.retention_update

            async def before_commit(uow, scope, identity, row):
                await original(uow, scope, identity, row)
                if row["status"] == "completed":
                    await pause()

            unit_type.retention_update = before_commit

        async def checkpoint(value):
            await queue.checkpoint(lease, value)
            if phase == "worker_after_checkpoint":
                await pause()

        await handler(lease.task, checkpoint)
        await pause()  # Publication committed; worker completion acknowledgment not sent.


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
