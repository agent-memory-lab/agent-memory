"""Opt-in actual-source upgrade and offline rollback probe on both real backends.

Run explicitly with pytest and AGENT_MEMORY_LEGACY_SOURCE. Default test discovery
does not collect this file. Missing legacy source or PostgreSQL is a failure,
never a passing skip. No old and new process runs concurrently on a database.
"""

import asyncio
import importlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_atom_admission as base
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived import ObservationService
from agent_memory.domain import MemoryScope
from agent_memory.operations.facet_refresh import FacetRefreshQueue
from agent_memory.serialization import to_jsonable

CHILD = Path(__file__).resolve().parents[1] / "fixtures/legacy_version_child.py"
KINDS = (
    "definition", "query", "authority", "grant", "barrier", "subscription_index",
    "question_content", "question_certificate", "question_head", "question_registration",
    "subscription", "refresh_demand", "refresh_execution", "refresh_publication",
    "coverage_request", "job",
)


async def old_process(config, tmp_path):
    __tracebackhide__ = True
    identity = uuid4().hex
    path = tmp_path / (identity + ".json")
    result_path = tmp_path / (identity + "-result.json")
    path.write_text(json.dumps(dict(config, result_path=str(result_path))))
    path.chmod(0o600)
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(CHILD), str(path), cwd=tmp_path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=environment,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=90)
        if process.returncode != 0:
            diagnostic = stderr.decode().replace(config["database"], "[test database]")
            pytest.fail("legacy subprocess failed: " + diagnostic)
        assert not stdout, "fixture must not print private database configuration"
        return json.loads(result_path.read_text())
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        path.unlink(missing_ok=True)


async def records(repository, scope):
    async with repository.unit_of_work() as uow:
        return {kind: await uow.derived_records(scope, kind) for kind in KINDS}


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_old_source_upgrade_and_quiescent_rollback_preserve_contracts(
    backend, tmp_path, monkeypatch
):
    legacy = Path(os.environ.get("AGENT_MEMORY_LEGACY_SOURCE", "missing-legacy-source"))
    assert (legacy / "src/agent_memory/derived/service.py").is_file(), (
        "materialize the pinned pre-V7 source and set AGENT_MEMORY_LEGACY_SOURCE"
    )
    assert not (legacy / "src/agent_memory/derived/question_service.py").exists(), (
        "the rollback source must actually predate the V7 runtime"
    )
    clock = [base.at(1)]
    for name in ("consolidation.admission_runtime", "domain", "kernel",
                 "retrieval.temporal_history", "sqlite"):
        monkeypatch.setattr(importlib.import_module("agent_memory." + name),
                            "utc_now", lambda: clock[0])

    async def run():
        scope = MemoryScope("version-drill-test", user_id="alice", session_id="isolated")
        database = str(tmp_path / "upgraded.db")
        schema = None
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
        repository = kernel = None
        if backend == "postgres":
            import psycopg
            from agent_memory_postgres.repository import PostgresMemoryRepository

            assert dsn, "the upgrade drill requires a live disposable PostgreSQL database"
            parsed = urlsplit(dsn)
            assert parsed.scheme in {"postgres", "postgresql"} and "test" in parsed.path
            schema = "v7_upgrade_" + uuid4().hex
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
                await connection.execute(psycopg.sql.SQL("CREATE SCHEMA {}").format(
                    psycopg.sql.Identifier(schema)))
            query = dict(parse_qsl(parsed.query))
            query["options"] = "-csearch_path=" + schema
            database = parsed._replace(query=urlencode(query)).geturl()
            for name in ("repository", "admission", "temporal_history"):
                monkeypatch.setattr(importlib.import_module("agent_memory_postgres." + name),
                                    "utc_now", lambda: clock[0])
        config = dict(legacy_source=str(legacy.resolve()), backend=backend, database=database,
                      scope=to_jsonable(scope), clock=clock[0].isoformat())
        try:
            seeded = await old_process(dict(config, mode="seed"), tmp_path)
            clock[0] = datetime.fromisoformat(seeded["clock"])
            if backend == "postgres":
                repository = PostgresMemoryRepository.from_dsn(database, max_size=3)
            else:
                repository = base.SQLiteMemoryRepository(database)
            kernel = base.MemoryKernel(
                repository, base.MetadataClaimExtractor(), base.TrustedMemoryPolicy(),
                base.ReciprocalRankFusionReranker(),
            )
            # This is the first current-version initialization of the old database.
            await kernel.initialize()
            observation = ObservationService(repository, scope, base.POLICY,
                                             clock=lambda: clock[0])
            old_queue = FacetRefreshQueue(observation)
            status = await old_queue.status(seeded["receipt"]["target_id"], actor="alice")
            assert status["complete"] and status["outcome"] == "applied"
            async with repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "request", seeded["receipt"]["target_id"]) == (
                    seeded["receipt"]
                )
            # A new index may conservatively dirty the old proof; rebuilding is allowed.
            lease = await old_queue.claim("current-version-legacy-drain", lease_seconds=30)
            if lease is not None:
                await observation.apply(lease.task)
                await old_queue.complete(lease)
            view = await observation.read("language", actor="alice")
            assert view["body"] == seeded["view"]["body"]
            service = runtime(base.AdmissionEngine(repository), scope, clock)
            item = await project.stage(service.admission, scope)
            await project.qualify(service.admission, *item)
            registration = await register(service)
            target = await service.request("project-a:owner", actor=ACTOR, dedupe_key="upgrade")
            answer = await fresh(service, clock)
            assert answer["availability_status"] == "valid"
            assert (await service.queue.status(target["target_id"], actor=ACTOR))["complete"]
            await register(service, "risks")
            pending = await service.request("project-a:risks", actor=ACTOR, dedupe_key="pending")
            assert not (await service.queue.status(pending["target_id"], actor=ACTOR))["complete"]
            before = await records(repository, scope)
            # Stop all current writers and close every connection before the old process.
            await kernel.close()
            kernel = None
            result = await old_process(dict(
                config, mode="probe", clock=clock[0].isoformat(),
                legacy_target_id=seeded["receipt"]["target_id"],
                instance_id=registration["instance_id"],
                coverage_target_id=target["target_id"],
            ), tmp_path)
            assert result == {
                "legacy_complete": True, "worker_claimed": False,
                "worker_error": "KeyError:query",
                "question_read_error": "derived_definition_configuration_changed",
                "coverage_read_error": "derived_target_unavailable",
            }
            # Re-upgrade and prove the old probe changed no V7 obligation or artifact.
            if backend == "postgres":
                repository = PostgresMemoryRepository.from_dsn(database, max_size=3)
            else:
                repository = base.SQLiteMemoryRepository(database)
            await repository.initialize()
            assert await records(repository, scope) == before
            resumed = type(service)(
                project.service(base.AdmissionEngine(repository), scope, clock), service.context
            )
            assert await resumed.read("project-a:owner", actor=ACTOR) == answer
            assert (await resumed.queue.status(target["target_id"], actor=ACTOR))["complete"]
            assert not (await resumed.queue.status(pending["target_id"], actor=ACTOR))["complete"]
            recovered = await resumed.answer("project-a:risks", actor=ACTOR, dedupe_key="resume")
            assert recovered["availability_status"] == "valid"
            assert (await resumed.queue.status(pending["target_id"], actor=ACTOR))["complete"]
        finally:
            if kernel is not None:
                await kernel.close()
            elif repository is not None and hasattr(repository, "close"):
                await repository.close()
            if schema:
                async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
                    await connection.execute(psycopg.sql.SQL("DROP SCHEMA {} CASCADE").format(
                        psycopg.sql.Identifier(schema)))

    asyncio.run(run())
