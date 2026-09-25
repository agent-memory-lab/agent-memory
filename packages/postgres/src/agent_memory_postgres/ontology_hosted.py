"""Database-native ontology snapshots, checkpoints, catalogs and generations.

One session advisory lock owns a job for the runtime lifetime. All durable state
is PostgreSQL; no local snapshot, index, checkpoint or lock file is created.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from hashlib import sha256
import json
import re
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from agent_memory.ontology_acceptance import OntologyIndexAcceptance
from agent_memory.ontology_backfill import OntologyClaimPage, OntologyBackfillCheckpoint, backfill_ontology_memory
from agent_memory.ontology_live import OntologySyncPending
from agent_memory.ontology_memory import project_claim_to_ontology
from agent_memory.ontology_registry import SQLiteOntologyRegistry
from agent_memory.ontology_runtime import open_active_ontology_memory
from agent_memory.ontology_schema import ontology_schema_digest

from .ontology import PostgresOntologyStore
from .ontology_source import PostgresOntologySource
from .repository import PostgresMemoryRepository


class _Row(dict):
    def __getitem__(self, key):
        return tuple(self.values())[key] if isinstance(key, int) else super().__getitem__(key)


class _Cursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def fetchone(self):
        row = self.cursor.fetchone()
        return None if row is None else _Row(row)

    def fetchall(self):
        return [_Row(row) for row in self.cursor.fetchall()]

    def __iter__(self):
        return (_Row(row) for row in self.cursor)


class _CatalogConnection:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, statement, params=()):
        # The adapter already holds a namespace transaction advisory lock.
        if statement.strip() == "BEGIN IMMEDIATE":
            statement = "SELECT 1"
        return _Cursor(self.connection.execute(statement, params))

    def executescript(self, script):
        self.connection.executescript(script)


class PostgresOntologyRegistry(SQLiteOntologyRegistry):
    """Preserve host-approved version policy and audits with PostgreSQL storage."""

    def __init__(self, dsn, *, namespace="agent_memory_catalog"):
        self._adapter = PostgresOntologyStore(dsn, namespace=namespace)

    @contextmanager
    def _connection(self):
        connection = self._adapter._connect()
        try:
            with connection:
                yield _CatalogConnection(connection)
        finally:
            connection.close()


def _claim(payload):
    row = dict(payload)
    for name in ("valid_from", "valid_to", "created_at"):
        if row[name] is not None and isinstance(row[name], str):
            row[name] = datetime.fromisoformat(row[name])
    return PostgresMemoryRepository._claim_from_row(object.__new__(PostgresMemoryRepository), row)


class _Snapshot:
    def __init__(self, runtime, row):
        self.runtime, self.row = runtime, row
        self.scope = runtime.context.scope
        self.snapshot_id, self.revision = row["id"], row["revision"]

    async def read_page(self, scope, snapshot_id, *, cursor, limit):
        if scope != self.scope or snapshot_id != self.snapshot_id:
            raise ValueError("snapshot identity or scope mismatch")
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("page limit must be between 1 and 256")
        def read():
            with self.runtime._db() as db:
                rows = db.execute("SELECT payload FROM hosted_claims WHERE generation=%s AND id>%s ORDER BY id LIMIT %s",
                    (self.snapshot_id, cursor or "", limit + 1)).fetchall()
            claims = tuple(_claim(row["payload"]) for row in rows[:limit])
            return OntologyClaimPage(claims, claims[-1].id if len(rows) > limit else None)
        return await asyncio.to_thread(read)

    async def verify(self, scope, source_event_ids):
        if scope != self.scope or not source_event_ids or len(source_event_ids) > 256:
            return False
        ids = tuple(set(source_event_ids))
        def check():
            with self.runtime._db() as db:
                count = db.execute("SELECT count(*) AS n FROM hosted_evidence WHERE generation=%s AND id=ANY(%s)",
                    (self.snapshot_id, list(ids))).fetchone()["n"]
            return count == len(ids)
        return await asyncio.to_thread(check)


class _Checkpoint:
    def __init__(self, runtime, row):
        self.runtime, self.row = runtime, row

    async def load(self):
        def read():
            with self.runtime._db() as db:
                return db.execute("SELECT * FROM hosted_generations WHERE id=%s AND job=%s",
                    (self.row["id"], self.runtime.job)).fetchone()
        row = await asyncio.to_thread(read)
        if row is None or row["index_id"] != self.row["index_id"]:
            raise ValueError("checkpoint target index changed")
        return OntologyBackfillCheckpoint(self.runtime.context.scope, row["id"], row["digest"],
            cursor=row["cursor"], processed_claims=row["processed"], completed=row["completed"],
            target_index_id=row["index_id"])

    async def save(self, checkpoint):
        if (checkpoint.scope != self.runtime.context.scope or checkpoint.snapshot_id != self.row["id"]
            or checkpoint.schema_digest != self.row["digest"]
            or checkpoint.target_index_id != self.row["index_id"]):
            raise ValueError("checkpoint does not match hosted generation")
        def write():
            with self.runtime._db() as db:
                self.runtime._fence(db)
                cursor = db.execute("""UPDATE hosted_generations SET cursor=%s, processed=%s, completed=%s
                    WHERE id=%s AND job=%s AND index_id=%s AND processed<=%s""",
                    (checkpoint.cursor, checkpoint.processed_claims, checkpoint.completed,
                     checkpoint.snapshot_id, self.runtime.job, checkpoint.target_index_id,
                     checkpoint.processed_claims))
                if cursor.rowcount != 1:
                    raise ValueError("checkpoint identity changed or progress regressed")
        await asyncio.to_thread(write)


class PostgresHostedOntologyMemory:
    """Optional exact-scope, database-native managed recall and store resolver.

    Initialize core and catalog first. Catalog may be PostgresOntologyRegistry.
    This implementation rebuilds the scope per revision; local SQLite's delta
    optimization is intentionally not claimed here. Keep one owning runtime per
    scope/ontology job. Different scopes may run on different hosts.
    """

    def __init__(self, source, catalog, ontology_id, context, *,
                 namespace="agent_memory_hosted", batch_size=32, max_batches=8):
        if not isinstance(source, PostgresOntologySource):
            raise TypeError("hosted runtime requires a PostgreSQL source")
        PostgresOntologyStore(source._dsn, namespace=namespace)
        if namespace == source.namespace:
            raise ValueError("control and core schemas must be separate")
        if type(batch_size) is not int or not 1 <= batch_size <= 256:
            raise ValueError("batch_size must be between 1 and 256")
        if type(max_batches) is not int or not 1 <= max_batches <= 128:
            raise ValueError("max_batches must be between 1 and 128")
        self.source, self.catalog, self.ontology_id, self.context = source, catalog, ontology_id, context
        self.namespace, self.batch_size, self.max_batches = namespace, batch_size, max_batches
        self.job = sha256((source.storage_key + context.scope.partition_key() + ontology_id).encode()).hexdigest()
        self._owner, self._lease, self._opened = uuid4().hex, None, False
        self._lock = asyncio.Lock()
        self._current = None
        self.last_acceptance = None

    @contextmanager
    def _db(self, *, snapshot=False):
        with psycopg.connect(self.source._dsn, row_factory=dict_row,
                            connect_timeout=5, client_encoding="utf8") as db:
            if snapshot:
                db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            db.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(self.namespace)))
            db.execute("SELECT set_config('statement_timeout',%s,true)", (str(self.source.timeout_ms),))
            db.execute("SELECT set_config('lock_timeout',%s,true)", (str(self.source.timeout_ms),))
            yield db

    def _fence(self, db):
        if self._lease is None or self._lease.closed:
            raise RuntimeError("hosted job lease is closed")
        self._lease.execute("SELECT 1")
        row = db.execute("SELECT owner FROM hosted_jobs WHERE job=%s FOR UPDATE", (self.job,)).fetchone()
        if row is None or row["owner"] != self._owner:
            raise RuntimeError("hosted worker ownership changed")

    async def __aenter__(self):
        if self._opened:
            raise RuntimeError("runtime already open")
        await self.source.initialize()
        self._source_id = await self.source.identity()
        try:
            await self._settle(asyncio.to_thread(self._open))
            self._opened = True
            return self
        except BaseException:
            if self._lease is not None:
                await asyncio.to_thread(self._lease.close)
                self._lease = None
            raise

    def _open(self):
        with psycopg.connect(self.source._dsn, connect_timeout=5) as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self.namespace,))
            db.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.namespace)))
        with self._db() as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self.namespace,))
            db.execute("CREATE TABLE IF NOT EXISTS hosted_jobs(job text PRIMARY KEY, owner text NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS hosted_generations(
                id text PRIMARY KEY, job text NOT NULL, source_id text NOT NULL,
                revision bigint NOT NULL, digest text NOT NULL, version text NOT NULL,
                index_id text, cursor text, processed bigint NOT NULL DEFAULT 0,
                completed boolean NOT NULL DEFAULT false, state text NOT NULL DEFAULT 'building',
                created_at timestamptz NOT NULL DEFAULT clock_timestamp())""")
            db.execute("CREATE INDEX IF NOT EXISTS hosted_generation_job ON hosted_generations(job,created_at)")
            db.execute("""CREATE TABLE IF NOT EXISTS hosted_claims(
                generation text REFERENCES hosted_generations(id) ON DELETE CASCADE,
                id text, payload jsonb NOT NULL, PRIMARY KEY(generation,id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS hosted_evidence(
                generation text REFERENCES hosted_generations(id) ON DELETE CASCADE,
                id text, PRIMARY KEY(generation,id))""")
        self._lease = psycopg.connect(self.source._dsn, autocommit=True, connect_timeout=5)
        if not self._lease.execute("SELECT pg_try_advisory_lock(hashtext(%s))",
                (self.namespace + ":" + self.job,)).fetchone()[0]:
            raise BlockingIOError("another worker owns this hosted ontology job")
        with self._db() as db:
            db.execute("INSERT INTO hosted_jobs VALUES (%s,%s) ON CONFLICT(job) DO UPDATE SET owner=excluded.owner",
                (self.job, self._owner))

    async def __aexit__(self, *args):
        async with self._lock:
            self._opened = False
            self._current = None
            if self._lease is not None:
                await asyncio.to_thread(self._lease.close)
                self._lease = None

    async def _settle(self, operation):
        task = asyncio.ensure_future(operation)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await task
            except BaseException:
                pass
            raise

    def _snapshot(self, activation, revision):
        with self._db(snapshot=True) as db:
            self._fence(db)
            row = db.execute("""SELECT * FROM hosted_generations WHERE job=%s AND source_id=%s
                AND digest=%s AND revision=%s ORDER BY created_at DESC LIMIT 1""",
                (self.job, self._source_id, activation.digest, revision)).fetchone()
            if row is not None:
                return row
            source = sql.Identifier(self.source.namespace)
            state = db.execute(sql.SQL("SELECT source_id,revision FROM {}.agent_memory_ontology_source WHERE id=1").format(source)).fetchone()
            if state["source_id"] != self._source_id:
                raise ValueError("source identity changed")
            generation = uuid4().hex
            db.execute("INSERT INTO hosted_generations(id,job,source_id,revision,digest,version) VALUES (%s,%s,%s,%s,%s,%s)",
                (generation, self.job, self._source_id, state["revision"], activation.digest, activation.version))
            db.execute(sql.SQL("""INSERT INTO hosted_claims SELECT %s,c.id,to_jsonb(c)
                FROM {}.agent_memory_claims c WHERE partition_key=%s AND status='active' AND archived_at IS NULL""").format(source),
                (generation, self.context.scope.partition_key()))
            db.execute(sql.SQL("""INSERT INTO hosted_evidence SELECT %s,id FROM {}.agent_memory_events
                WHERE partition_key=%s AND archived_at IS NULL""").format(source),
                (generation, self.context.scope.partition_key()))
            return db.execute("SELECT * FROM hosted_generations WHERE id=%s", (generation,)).fetchone()

    def _bind(self, row, index_id):
        with self._db() as db:
            self._fence(db)
            changed = db.execute("UPDATE hosted_generations SET index_id=%s WHERE id=%s AND job=%s AND index_id IS NULL",
                (index_id, row["id"], self.job))
            if changed.rowcount != 1:
                raise ValueError("generation was already bound to an index")

    def _collect(self, current, *, promoted):
        with self._db() as db:
            self._fence(db)
            if promoted:
                db.execute("UPDATE hosted_generations SET state='retired' WHERE job=%s AND state='active'", (self.job,))
                db.execute("UPDATE hosted_generations SET state='active' WHERE id=%s AND job=%s", (current, self.job))
            rows = db.execute("SELECT id FROM hosted_generations WHERE job=%s AND id<>%s" +
                ("" if promoted else " AND state<>'active'"), (self.job, current)).fetchall()
            for row in rows:
                generation = row["id"]
                if not re.fullmatch(r"[a-f0-9]{32}", generation):
                    raise ValueError("invalid generation identity; refusing schema deletion")
                db.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier("amg_" + generation)))
                db.execute("DELETE FROM hosted_generations WHERE id=%s AND job=%s", (generation, self.job))

    async def _fresh(self, activation, revision):
        if (await self.source.identity() != self._source_id or await self.source.revision() != revision
                or await self.catalog.active(self.context.scope, self.ontology_id) != activation):
            raise OntologySyncPending("source or activation changed; refresh again")
        def check():
            with self._db() as db:
                self._fence(db)
        await asyncio.to_thread(check)

    async def refresh(self):
        async with self._lock:
            return await self._settle(self._refresh())

    async def _refresh(self):
        if not self._opened or self.context.cancelled or self.context.expired:
            raise RuntimeError("hosted runtime is inactive")
        if await self.source.identity() != self._source_id:
            raise ValueError("source database changed; reopen runtime")
        activation = await self.catalog.active(self.context.scope, self.ontology_id)
        if activation is None:
            raise LookupError("no active schema")
        revision = await self.source.revision()
        if self._current and self._current["activation"] == activation and self._current["row"]["revision"] == revision:
            if await self._current["store"].index_identity() != self._current["row"]["index_id"]:
                raise ValueError("active index identity changed")
            await self._fresh(activation, revision)
            return True
        schema = await self.catalog.get(self.context.scope, self.ontology_id, activation.version)
        if ontology_schema_digest(schema) != activation.digest:
            raise ValueError("active schema digest mismatch")
        row = await asyncio.to_thread(self._snapshot, activation, revision)
        await asyncio.to_thread(self._collect, row["id"], promoted=False)
        if not re.fullmatch(r"[a-f0-9]{32}", row["id"]):
            raise ValueError("invalid generation identity")
        store = PostgresOntologyStore(self.source._dsn, namespace="amg_" + row["id"], timeout_ms=self.source.timeout_ms)
        if row["index_id"] is None:
            await store.initialize()
            row["index_id"] = await store.index_identity()
            await asyncio.to_thread(self._bind, row, row["index_id"])
        elif await store.index_identity() != row["index_id"]:
            raise ValueError("target index identity changed; resume refused")
        snapshot, sink = _Snapshot(self, row), _Checkpoint(self, row)
        checkpoint = await backfill_ontology_memory(snapshot, snapshot.snapshot_id, schema, store,
            snapshot, self.context, checkpoint_sink=sink, resume=await sink.load(),
            batch_size=self.batch_size, max_batches=self.max_batches, target_index_id=row["index_id"])
        if not checkpoint.completed:
            return False
        self.last_acceptance = await asyncio.to_thread(self._validate, row, schema)
        if not self.last_acceptance.ready:
            raise ValueError("hosted index acceptance failed: " + ", ".join(self.last_acceptance.problems))
        await self._fresh(activation, row["revision"])
        await asyncio.to_thread(self._collect, row["id"], promoted=True)
        self._current = dict(row=row, store=store, snapshot=snapshot, activation=activation)
        await self._fresh(activation, row["revision"])
        return True

    @asynccontextmanager
    async def borrow_store(self, scope, activation):
        if scope != self.context.scope:
            raise ValueError("scope differs from hosted job")
        async with self._lock:
            if not await self._settle(self._refresh()):
                raise OntologySyncPending("hosted backfill is incomplete")
            current = self._current
            if current["activation"] != activation:
                raise OntologySyncPending("activation changed")
            yield current["store"]
            await self._fresh(activation, current["row"]["revision"])

    async def retrieve(self, query, current_state):
        activation = await self.catalog.active(self.context.scope, self.ontology_id)
        if activation is None:
            raise LookupError("no active schema")
        async with self.borrow_store(query.scope, activation) as store:
            async with open_active_ontology_memory(self.catalog, self.ontology_id,
                self.context, store, self._current["snapshot"]) as memory:
                return await memory.recall_pipeline.retrieve(query, current_state)

    def _validate(self, generation, schema):
        problems = []
        def problem(message):
            if message not in problems and len(problems) < 20:
                problems.append(message)
        with self._db() as db:
            self._fence(db)
            db.execute("""CREATE TEMP TABLE expected(
                id text PRIMARY KEY, subject text, predicate text, object text,
                literal jsonb, text text, confidence double precision,
                start_at timestamptz, end_at timestamptz, sources text[]) ON COMMIT DROP""")
            db.execute("CREATE TEMP TABLE expected_entities(id text PRIMARY KEY,class text,label text) ON COMMIT DROP")
            with db.cursor(name="hosted_acceptance_claims") as cursor:
                cursor.execute("SELECT payload FROM hosted_claims WHERE generation=%s ORDER BY id", (generation["id"],))
                while rows := cursor.fetchmany(128):
                    for row in rows:
                        projection = project_claim_to_ontology(_claim(row["payload"]), schema)
                        if projection is None:
                            continue
                        a = projection.assertion
                        ids = sorted(set(a.source_event_ids))
                        count = db.execute("SELECT count(*) AS n FROM hosted_evidence WHERE generation=%s AND id=ANY(%s)",
                            (generation["id"], ids)).fetchone()["n"]
                        if not ids or count != len(ids):
                            problem("missing source evidence")
                        db.execute("""INSERT INTO expected VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
                            ON CONFLICT(id) DO UPDATE SET confidence=GREATEST(expected.confidence,excluded.confidence),
                            sources=ARRAY(SELECT DISTINCT value FROM unnest(expected.sources || excluded.sources) AS value ORDER BY value)""",
                            (a.assertion_id, a.subject_entity_id, a.predicate_id, a.object_entity_id,
                             json.dumps(a.literal_value), a.text, a.confidence, a.valid_from, a.valid_to, ids))
                        for entity in projection.entities:
                            existing = db.execute("SELECT class,label FROM expected_entities WHERE id=%s", (entity.entity_id,)).fetchone()
                            if existing and (existing["class"], existing["label"]) != (entity.class_id, entity.label):
                                problem("conflicting entity identity")
                            db.execute("INSERT INTO expected_entities VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                                (entity.entity_id, entity.class_id, entity.label))
            expected = db.execute("SELECT count(*) AS n FROM expected").fetchone()["n"]
            actual = 0
            target = sql.Identifier("amg_" + generation["id"])
            with db.cursor(name="hosted_acceptance_assertions") as cursor:
                cursor.execute(sql.SQL("SELECT * FROM {}.ontology_assertions ORDER BY assertion_id").format(target))
                while rows := cursor.fetchmany(128):
                    for a in rows:
                        actual += 1
                        e = db.execute("SELECT * FROM expected WHERE id=%s", (a["assertion_id"],)).fetchone()
                        if e is None:
                            problem("unexpected assertion")
                            continue
                        if (a["partition_key"] != self.context.scope.partition_key() or a["ontology_id"] != schema.ontology_id
                            or a["ontology_version"] != schema.version or a["status"] != "active" or a["archived_at"] is not None):
                            problem("inactive, conflicting or cross-scope assertion")
                        actual_value = (a["subject_entity_id"], a["predicate_id"], a["object_entity_id"],
                            json.loads(a["literal_json"]) if a["literal_json"] else None, a["text"], a["confidence"],
                            datetime.fromisoformat(a["valid_from"]), datetime.fromisoformat(a["valid_to"]) if a["valid_to"] else None)
                        expected_value = tuple(e[key] for key in ("subject", "predicate", "object", "literal", "text", "confidence", "start_at", "end_at"))
                        if actual_value != expected_value:
                            problem("assertion content mismatch")
                        if set(json.loads(a["source_event_ids_json"])) != set(e["sources"]):
                            problem("assertion evidence mismatch")
            if actual != expected:
                problem("assertion count mismatch")
            if db.execute(sql.SQL("""SELECT 1 FROM expected e LEFT JOIN {}.ontology_assertions a
                ON a.assertion_id=e.id WHERE a.assertion_id IS NULL LIMIT 1""").format(target)).fetchone():
                problem("missing assertion")
            if db.execute(sql.SQL("""SELECT 1 FROM expected_entities e LEFT JOIN {}.ontology_entities a
                ON a.entity_id=e.id AND a.partition_key=%s AND a.ontology_id=%s AND a.ontology_version=%s
                WHERE a.entity_id IS NULL OR a.archived_at IS NOT NULL OR a.class_id<>e.class OR a.label<>e.label LIMIT 1""").format(target),
                (self.context.scope.partition_key(), schema.ontology_id, schema.version)).fetchone():
                problem("missing or inconsistent entity")
        return OntologyIndexAcceptance(not problems, expected, actual, tuple(problems), generation["revision"])
