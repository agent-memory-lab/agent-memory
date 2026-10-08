"""Run a pinned, separately materialized old source tree in a fresh process.

This fixture never imports current test helpers. Its databases are disposable;
it is not an online downgrade or permission to run old lifecycle writers.
"""

import asyncio
import importlib
import json
import sys
from datetime import datetime
from pathlib import Path


async def main(config):
    root = Path(config["legacy_source"]).resolve()
    sys.path[:0] = [
        str(root / "src"),
        str(root / "tests"),
        *(str(path) for path in sorted((root / "packages").glob("*/src"))),
    ]
    import agent_memory
    import test_atom_admission as base
    from test_derived_observations import build, setup
    from test_durable_purge import source_id

    from agent_memory.derived import DerivedError, ObservationService
    from agent_memory.domain import ForgetMode, ForgetRequest, MemoryScope
    from agent_memory.operations.facet_refresh import FacetRefreshQueue

    assert Path(agent_memory.__file__).resolve().is_relative_to(root / "src")
    clock = [datetime.fromisoformat(config["clock"])]
    modules = [
        "consolidation.admission_runtime", "domain", "kernel",
        "retrieval.temporal_history", "sqlite",
    ]
    for name in modules:
        setattr(importlib.import_module("agent_memory." + name), "utc_now", lambda: clock[0])
    if config["backend"] == "postgres":
        from agent_memory_postgres.repository import PostgresMemoryRepository

        for name in ("repository", "admission", "temporal_history"):
            setattr(importlib.import_module("agent_memory_postgres." + name), "utc_now",
                    lambda: clock[0])
        repository = PostgresMemoryRepository.from_dsn(config["database"], max_size=2)
    else:
        from agent_memory.sqlite import SQLiteMemoryRepository

        repository = SQLiteMemoryRepository(config["database"])
    scope = MemoryScope(**config["scope"])
    kernel = base.MemoryKernel(
        repository, base.MetadataClaimExtractor(), base.TrustedMemoryPolicy(),
        base.ReciprocalRankFusionReranker(),
    )
    await kernel.initialize()
    try:
        if config["mode"] == "seed":
            service, queue, _ = await setup(base.AdmissionEngine(repository), kernel,
                                             scope, clock, inputs=2)
            # Exercise old capture/admission and erasure before the coordinated
            # cutover, while only the old binary owns this disposable database.
            erased_source_id = source_id(scope, "2")
            retained_source_id = source_id(scope, "1")
            await kernel.forget(ForgetRequest(
                scope, (erased_source_id,), mode=ForgetMode.ERASE,
            ))
            receipt = await build(queue)
            view = await service.read("language", actor="alice")
            assert await queue.claim("old-version-drained", lease_seconds=30) is None
            async with repository.unit_of_work() as uow:
                assert await uow.source_erased(scope, erased_source_id)
                assert await uow.get_source_event(scope, erased_source_id) is None
                assert await uow.get_source_event(scope, retained_source_id) is not None
                checkpoint = dict(purge_head=await uow.purge_head(scope),
                                  retention_epoch=await uow.retention_epoch(scope))
            result = dict(receipt=receipt, view=view, clock=clock[0].isoformat(),
                          erased_source_id=erased_source_id,
                          retained_source_id=retained_source_id,
                          deletion_checkpoint=checkpoint)
        elif config["mode"] == "probe":
            service = ObservationService(repository, scope, base.POLICY,
                                         clock=lambda: clock[0])
            queue = FacetRefreshQueue(service)
            legacy = await queue.status(config["legacy_target_id"], actor="alice")
            assert legacy["complete"]
            try:
                claimed = await queue.claim("old-version-probe", lease_seconds=30)
            except KeyError as error:
                # The pinned old worker cannot parse a V7 context. Preserve this
                # incompatibility as evidence, not as a supported downgrade.
                assert error.args == ("query",)
                claimed, worker_error = None, "KeyError:query"
            else:
                worker_error = None
            assert claimed is None, "old worker claimed an incompatible V7 obligation"
            try:
                await service.read(config["instance_id"], actor="host:alice")
            except DerivedError as error:
                read_error = str(error)
            else:
                raise AssertionError("old reader accepted a V7 question as a legacy facet")
            try:
                await queue.status(config["coverage_target_id"], actor="host:alice")
            except DerivedError as error:
                receipt_error = str(error)
            else:
                raise AssertionError("old reader reinterpreted a V7 coverage receipt")
            result = dict(legacy_complete=True, worker_claimed=False, worker_error=worker_error,
                          question_read_error=read_error, coverage_read_error=receipt_error)
        else:
            raise ValueError("unknown legacy drill mode")
        # No database URL, source body, or credential is included in the receipt.
        Path(config["result_path"]).write_text(json.dumps(result, sort_keys=True))
    finally:
        await kernel.close()


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
