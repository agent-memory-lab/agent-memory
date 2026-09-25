"""Opt-in SQLite synchronization and safe runtime generation replacement."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from .ontology_workspace import OntologyWorkspace

from .ontology_acceptance import validate_ontology_index
from .ontology_backfill import backfill_ontology_memory
from .ontology_checkpoint import SQLiteOntologyCheckpointSink
from .ontology_memory import SQLiteOntologyStore
from .ontology_runtime import open_active_ontology_memory
from .ontology_schema import ontology_schema_digest
from .ontology_source import SQLiteOntologySource, SQLiteOntologySnapshot


class OntologySyncPending(RuntimeError):
    """Synchronization needs another bounded advance before serving queries."""


class LiveOntologyMemory:
    """A RecallPipeline that adopts source and registry changes automatically.

    Each source revision rebuilds a shadow index for the bound exact scope.
    Changes are detected transactionally, including writes by other processes.
    This is scope-level incremental synchronization, not per-assertion delta
    indexing. A query performs at most max_batches of work; if more is needed it
    raises OntologySyncPending. Call refresh() repeatedly to advance larger jobs.

    Reads and replacement share a lock, so retired plugins are never closed
    during an in-flight read. On source/activation changes, failed validation or
    partial backfill, old data is not served. Use as an async context manager.
    Ordinary AgentMemory writes/forget need no wrapper: the next recall observes
    their committed changes. No background thread or daemon is started.
    """

    def __init__(self, source: SQLiteOntologySource, catalog, ontology_id, context, *,
                 work_directory: str | Path, batch_size=32, max_batches=8):
        if type(batch_size) is not int or not 1 <= batch_size <= 256:
            raise ValueError("batch_size must be between 1 and 256")
        if type(max_batches) is not int or not 1 <= max_batches <= 128:
            raise ValueError("max_batches must be between 1 and 128")
        self.source, self.catalog, self.ontology_id, self.context = source, catalog, ontology_id, context
        self.directory = Path(work_directory)
        self.batch_size, self.max_batches = batch_size, max_batches
        self._lock = asyncio.Lock()
        self._active = None
        self._pending = None
        self._opened = False
        self.last_acceptance = None
        self._workspace = OntologyWorkspace(self.directory,
            str(source.path) + ":" + context.scope.partition_key() + ":" + ontology_id)

    async def __aenter__(self):
        if self._opened:
            raise RuntimeError("live ontology runtime is already open")
        await self.source.initialize()
        self.directory.mkdir(parents=True, exist_ok=True)
        self._source_id = await self.source.identity()
        self._workspace = OntologyWorkspace(self.directory,
            str(self.source.path) + ":" + self.context.scope.partition_key() + ":" + self.ontology_id + ":" + self._source_id)
        self._workspace.open()
        self._opened = True
        return self

    async def __aexit__(self, *args):
        async with self._lock:
            self._opened = False
            try:
                if self._active:
                    await self._active["manager"].__aexit__(None, None, None)
            finally:
                self._active = self._pending = None
                self._workspace.close()

    @asynccontextmanager
    async def borrow_store(self, scope, activation):
        """Pin a prepared store through one SDK/MCP query, then check freshness."""
        if scope != self.context.scope:
            raise ValueError("ontology store is outside the runtime scope")
        async with self._lock:
            if not await self._settled_refresh():
                raise OntologySyncPending("ontology index is still being prepared")
            if self._active["key"][0] != activation:
                raise OntologySyncPending("activation changed during store resolution")
            active = self._active
            yield active["store"]
            if (await self.source.identity() != self._source_id
                or await self.source.revision() != active["key"][1]
                or await self.catalog.active(scope, self.ontology_id) != activation):
                raise OntologySyncPending("source or activation changed during ontology query")

    async def refresh(self) -> bool:
        async with self._lock:
            return await self._settled_refresh()

    async def _settled_refresh(self):
        # A cancelled to_thread await does not stop SQLite writes. Keep the lock
        # and generation directory alive until this bounded refresh settles.
        task = asyncio.create_task(self._refresh())
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await task
            except BaseException:
                pass
            raise

    async def _refresh(self):
        if not self._opened or self.context.cancelled or self.context.expired:
            raise RuntimeError("live ontology runtime is inactive or expired")
        if await self.source.identity() != self._source_id:
            raise ValueError("source database identity changed; reopen runtime")
        activation = await self.catalog.active(self.context.scope, self.ontology_id)
        if activation is None:
            raise LookupError("no active ontology schema")
        revision = await self.source.revision()
        key = (activation, revision)
        if self._active and self._active["key"] == key:
            return True
        if self._pending and self._pending["key"] != key:
            self._pending["temporary"].cleanup()
            self._pending = None
        if self._pending is None:
            workspace_key = activation.digest + ":" + str(revision)
            temporary = self._workspace.recover(workspace_key)
            recovered = temporary is not None
            if temporary is None:
                temporary = self._workspace.create(workspace_key)
            root = Path(temporary.name)
            try:
                snapshot = (await SQLiteOntologySnapshot.open(root / "snapshot.db", self.context.scope)
                    if recovered else await self.source.snapshot(self.context.scope, root / "snapshot.db"))
                schema = await self.catalog.get(self.context.scope, self.ontology_id, activation.version)
                if ontology_schema_digest(schema) != activation.digest:
                    raise ValueError("active schema digest mismatch")
                store = SQLiteOntologyStore(root / "index.db")
                if recovered:
                    if not (root / "index.db").is_file():
                        raise ValueError("target index is missing; recovery refused")
                    index_id = await store.index_identity()
                    if index_id != self._workspace.index_id(temporary):
                        raise ValueError("target index identity changed; recovery refused")
                else:
                    await store.initialize()
                    index_id = await store.index_identity()
                    self._workspace.bind(temporary, index_id)
                sink = SQLiteOntologyCheckpointSink(root / "checkpoint.db", scope=self.context.scope,
                    snapshot_id=snapshot.snapshot_id, schema_digest=activation.digest, target_index_id=index_id)
                await sink.initialize()
                self._pending = dict(temporary=temporary, snapshot=snapshot, schema=schema,
                    sink=sink, path=root / "index.db", store=store, index_id=index_id,
                    key=(activation, snapshot.revision))
            except BaseException:
                if not recovered:
                    temporary.cleanup()
                raise
        pending = self._pending
        checkpoint = await backfill_ontology_memory(
            pending["snapshot"], pending["snapshot"].snapshot_id, pending["schema"], pending["store"],
            pending["snapshot"], self.context, checkpoint_sink=pending["sink"],
            resume=await pending["sink"].load(), batch_size=self.batch_size, max_batches=self.max_batches,
            target_index_id=pending["index_id"],
        )
        if not checkpoint.completed:
            return False
        report = await validate_ontology_index(pending["snapshot"], pending["schema"], pending["path"])
        self.last_acceptance = report
        if not report.ready:
            raise ValueError("ontology index acceptance failed: " + ", ".join(report.problems))
        if await self.source.revision() != pending["snapshot"].revision:
            return False
        manager = open_active_ontology_memory(self.catalog, self.ontology_id, self.context,
            pending["store"], pending["snapshot"])
        handle = await manager.__aenter__()
        if handle.activation != pending["key"][0]:
            await manager.__aexit__(None, None, None)
            return False
        previous = self._active
        try:
            self._workspace.promote(pending["temporary"])
        except BaseException:
            await manager.__aexit__(None, None, None)
            raise
        pending.update(manager=manager, handle=handle)
        self._active, self._pending = pending, None
        if previous:
            try:
                await previous["manager"].__aexit__(None, None, None)
            finally:
                self._workspace.collect()
        else:
            self._workspace.collect()
        return True

    async def retrieve(self, query, current_state):
        if query.scope != self.context.scope:
            raise ValueError("query is outside the live ontology scope")
        async with self._lock:
            if not await self._settled_refresh():
                raise OntologySyncPending("ontology synchronization is incomplete; call refresh to continue")
            active = self._active
            bundle = await active["handle"].recall_pipeline.retrieve(query, current_state)
            if (await self.source.identity() != self._source_id
                or await self.source.revision() != active["key"][1]
                or await self.catalog.active(self.context.scope, self.ontology_id) != active["key"][0]):
                raise OntologySyncPending("source or activation changed during recall; retry after refresh")
            return bundle
