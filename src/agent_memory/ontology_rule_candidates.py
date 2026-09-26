"""Optional durable inference candidates, never an active-fact store."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Protocol

from .domain import MemoryScope
from .ontology_rules import DerivedMemory, OntologyRuleEngine
from .ontology_schema import ontology_schema_digest
from .serialization import to_jsonable


class RuleCandidateStore(Protocol):
    """Exact-scope storage. Archive is terminal; erase removes the payload.

    Implementations must reject a different payload for an existing identity
    and must not reactivate an archived identity on repeated put(). Reads from
    this low-level port are unverified; use DurableRuleCandidates for use-time
    evidence validation. The host supplies the trusted scope.
    """

    async def initialize(self) -> None: ...
    async def put(self, candidate: DerivedMemory) -> None: ...
    async def get(self, scope: MemoryScope, candidate_id: str) -> DerivedMemory | None: ...
    async def archive(self, scope: MemoryScope, candidate_id: str) -> bool: ...
    async def erase(self, scope: MemoryScope, candidate_id: str) -> bool: ...


def _identity(candidate_id: str) -> None:
    if not isinstance(candidate_id, str) or not 1 <= len(candidate_id) <= 128:
        raise ValueError("candidate ID must contain 1 to 128 characters")


def _decode(payload: str) -> DerivedMemory:
    data = json.loads(payload)
    data["scope"] = MemoryScope(**data["scope"])
    data["valid_from"] = datetime.fromisoformat(data["valid_from"])
    if data["valid_to"] is not None:
        data["valid_to"] = datetime.fromisoformat(data["valid_to"])
    for key in ("premise_ids", "source_event_ids", "rule_versions"):
        data[key] = tuple(data[key])
    return DerivedMemory(**data)


class SQLiteRuleCandidateStore:
    """On-disk, connection-per-operation storage with explicit capacity bounds.

    No worker, vector dependency or resident cache. Archived records count
    toward capacity until explicitly erased. Limits are deployment settings:
    every process sharing a database should use the same values.
    """

    def __init__(self, path: str | Path, *, max_records: int = 10000,
                 max_payload_bytes: int = 65536):
        if str(path) == ":memory:":
            raise ValueError("durable candidate storage requires an on-disk path")
        if type(max_records) is not int or not 1 <= max_records <= 1000000:
            raise ValueError("max_records must be between 1 and 1000000")
        if type(max_payload_bytes) is not int or not 1024 <= max_payload_bytes <= 1048576:
            raise ValueError("max_payload_bytes must be between 1024 and 1048576")
        self.path = Path(path)
        self.max_records = max_records
        self.max_payload_bytes = max_payload_bytes

    @contextmanager
    def _connection(self):
        connection = sqlite3.connect(self.path, timeout=5)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    async def initialize(self) -> None:
        def create():
            with self._connection() as db:
                db.execute("""CREATE TABLE IF NOT EXISTS ontology_rule_candidates_v1 (
                    partition_key TEXT NOT NULL, candidate_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('candidate','archived')),
                    PRIMARY KEY(partition_key,candidate_id)
                )""")
        await asyncio.to_thread(create)

    async def put(self, candidate: DerivedMemory) -> None:
        if not isinstance(candidate, DerivedMemory) or candidate.status != "candidate":
            raise ValueError("only inference candidates can be persisted")
        _identity(candidate.candidate_id)
        payload = json.dumps(to_jsonable(candidate), sort_keys=True,
                             ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if len(payload.encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("candidate payload exceeds byte budget")
        key = (candidate.scope.partition_key(), candidate.candidate_id)

        def write():
            with self._connection() as db:
                db.execute("BEGIN IMMEDIATE")
                previous = db.execute("""SELECT payload,state FROM ontology_rule_candidates_v1
                    WHERE partition_key=? AND candidate_id=?""", key).fetchone()
                if previous is not None:
                    if previous[0] != payload:
                        raise ValueError("candidate identity already has a different payload")
                    if previous[1] != "candidate":
                        raise ValueError("archived candidate cannot be reactivated")
                    return
                if db.execute("SELECT count(*) FROM ontology_rule_candidates_v1").fetchone()[0] >= self.max_records:
                    raise ValueError("candidate store is full; explicitly erase retired records")
                db.execute("INSERT INTO ontology_rule_candidates_v1 VALUES (?,?,?,'candidate')", (*key, payload))
        await asyncio.to_thread(write)

    async def get(self, scope: MemoryScope, candidate_id: str) -> DerivedMemory | None:
        _identity(candidate_id)

        def read():
            with self._connection() as db:
                row = db.execute("""SELECT payload FROM ontology_rule_candidates_v1
                    WHERE partition_key=? AND candidate_id=? AND state='candidate'""",
                    (scope.partition_key(), candidate_id)).fetchone()
            if row is None:
                return None
            if len(row[0].encode("utf-8")) > self.max_payload_bytes:
                raise ValueError("stored candidate exceeds configured byte budget")
            candidate = _decode(row[0])
            if candidate.scope != scope or candidate.candidate_id != candidate_id or candidate.status != "candidate":
                raise ValueError("stored candidate identity does not match its partition")
            return candidate
        return await asyncio.to_thread(read)

    async def archive(self, scope: MemoryScope, candidate_id: str) -> bool:
        _identity(candidate_id)

        def write():
            with self._connection() as db:
                cursor = db.execute("""UPDATE ontology_rule_candidates_v1 SET state='archived'
                    WHERE partition_key=? AND candidate_id=? AND state='candidate'""",
                    (scope.partition_key(), candidate_id))
                return cursor.rowcount == 1
        return await asyncio.to_thread(write)

    async def erase(self, scope: MemoryScope, candidate_id: str) -> bool:
        _identity(candidate_id)

        def write():
            with self._connection() as db:
                cursor = db.execute("DELETE FROM ontology_rule_candidates_v1 WHERE partition_key=? AND candidate_id=?",
                                    (scope.partition_key(), candidate_id))
                return cursor.rowcount == 1
        return await asyncio.to_thread(write)

    async def forget_sources(self, request) -> int:
        """Invalidate the entire proof if any root evidence is forgotten.

        IDs must be source event IDs, not claim or candidate IDs. This is an
        explicit host hook; direct source writes do not automatically call it.
        """
        from .domain import ForgetMode
        if not request.all_in_scope and not 1 <= len(request.memory_ids) <= 256:
            raise ValueError("provide one to 256 source event IDs")
        ids = json.dumps(list(request.memory_ids))
        def write():
            with self._connection() as db:
                condition = "partition_key=?"
                args = [request.scope.partition_key()]
                if not request.all_in_scope:
                    condition += """ AND EXISTS (
                        SELECT 1 FROM json_each(payload, '$.source_event_ids') evidence
                        JOIN json_each(?) removed ON evidence.value=removed.value)"""
                    args.append(ids)
                if request.mode == ForgetMode.ERASE:
                    statement = "DELETE FROM ontology_rule_candidates_v1 WHERE " + condition
                else:
                    statement = "UPDATE ontology_rule_candidates_v1 SET state='archived' WHERE " + condition + " AND state='candidate'"
                return db.execute(statement, args).rowcount
        return await asyncio.to_thread(write)


class DurableRuleCandidates:
    """Host-bound scope and engine; reads rederive candidates from live proof.

    A different schema/rule version returns no candidate without archiving it:
    another pinned engine may still legitimately consume that version.
    Validation is point-in-time, not a transaction with the source store.
    Transient verification errors propagate without permanently archiving data.
    """

    def __init__(self, scope: MemoryScope, engine: OntologyRuleEngine,
                 store: RuleCandidateStore):
        self.scope, self.engine, self.store = scope, engine, store

    def _matches(self, candidate: DerivedMemory) -> bool:
        return (candidate.scope == self.scope and candidate.status == "candidate"
                and candidate.schema_digest == ontology_schema_digest(self.engine.schema)
                and candidate.rules_digest == self.engine.rules_digest)

    async def save(self, candidate: DerivedMemory) -> None:
        if not self._matches(candidate):
            raise ValueError("candidate differs from trusted scope or pinned rule version")
        if not await self.engine.valid(candidate):
            raise ValueError("candidate proof is no longer valid")
        await self.store.put(candidate)

    async def get(self, candidate_id: str) -> DerivedMemory | None:
        candidate = await self.store.get(self.scope, candidate_id)
        if candidate is None or not self._matches(candidate):
            return None
        if not await self.engine.valid(candidate):
            await self.store.archive(self.scope, candidate_id)
            return None
        # Do not return an identity archived or erased during proof validation.
        current = await self.store.get(self.scope, candidate_id)
        return candidate if current == candidate else None

    async def archive(self, candidate_id: str) -> bool:
        return await self.store.archive(self.scope, candidate_id)

    async def erase(self, candidate_id: str) -> bool:
        return await self.store.erase(self.scope, candidate_id)
