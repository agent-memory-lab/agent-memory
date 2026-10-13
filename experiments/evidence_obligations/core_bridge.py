"""Experiment-only exact-quotation bridge to the real memory kernel.

``evidence.quote`` asserts only that the host observed these characters in a
particular source revision. It does not assert that the quoted proposition is
true. Conditions, exceptions, negation and dates stay inside the exact text;
this adapter does not manufacture semantic qualifiers or field evidence.

Every admitted quote is a typed core atom with a persisted source event. Search
uses ``MemoryKernel.retrieve`` first, then applies the host's current-revision
and revocation checks. The extra SQLite tables are provenance/visibility maps,
not a second retrieval index. Give each experiment arm/episode its own file.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote as uri_quote
from uuid import uuid4

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.domain import (
    AtomDraft,
    ForgetMode,
    ForgetRequest,
    MemoryChannel,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
    PredicateSpec,
    SourceAuthority,
)
from agent_memory.kernel import MemoryKernel
from agent_memory.providers import ReciprocalRankFusionReranker
from agent_memory.sqlite import SQLiteMemoryRepository

PREDICATE = "evidence.quote"
MAX_QUOTE_CHARS = 1000


class _NoLegacyAdmission:
    """Close the legacy confidence-based extraction path entirely."""

    async def extract(self, event):
        return ()

    async def should_extract(self, event):
        return False

    async def accept_claim(self, event, claim):
        return False


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identifier(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 4096:
        raise ValueError(f"{name} must be a nonempty string of at most 4096 characters")


def _revision_key(value: str | int) -> str:
    if type(value) is int:
        if value < 0:
            raise ValueError("revision integer must be nonnegative")
    else:
        _identifier(value, "revision")
    # Preserve type: the string "1" and integer 1 are distinct revisions.
    return json.dumps(value, ensure_ascii=False)


class QuoteStore:
    """Small asynchronous, local-only experiment API over SQLite core admission.

    ``admit`` returns the number of newly admitted distinct valid quotations.
    Invalid extraction outputs are skipped rather than trusted or rewritten.
    Source revisions are immutable: reusing one with changed content raises.
    A newly observed revision retires all older quotes even when no new quote
    survives validation. Calling ``admit`` after ``revoke`` is a fresh explicit
    host observation and may reactivate the source.
    """

    def __init__(self, path: str | Path, namespace: str) -> None:
        _identifier(namespace, "namespace")
        if str(path) == ":memory:":
            raise ValueError("QuoteStore requires a file-backed SQLite database")
        self.path = Path(path)
        self.namespace = namespace
        self.scope = MemoryScope(
            "evidence-obligations-experiment", namespace, session_id="quote-store"
        )
        self.repository = SQLiteMemoryRepository(self.path)
        guard = _NoLegacyAdmission()
        self.kernel = MemoryKernel(
            self.repository, guard, guard, ReciprocalRankFusionReranker()
        )
        self.policy = AdmissionPolicy([
            PredicateSpec(PREDICATE, value_type="string", allow_self_report=False)
        ])
        self._lock = asyncio.Lock()
        self._initialized = False

    @contextmanager
    def _metadata(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("call await QuoteStore.initialize() before using the store")

    async def initialize(self) -> QuoteStore:
        async with self._lock:
            if self._initialized:
                return self
            self.path.parent.mkdir(parents=True, exist_ok=True)
            await self.kernel.initialize()
            with self._metadata() as db:
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS experiment_quote_sources (
                        namespace TEXT NOT NULL, source_id TEXT NOT NULL,
                        revision TEXT NOT NULL, source_hash TEXT NOT NULL,
                        source_text TEXT NOT NULL, generation TEXT NOT NULL,
                        active INTEGER NOT NULL,
                        PRIMARY KEY (namespace, source_id)
                    );
                    CREATE TABLE IF NOT EXISTS experiment_quote_revisions (
                        namespace TEXT NOT NULL, source_id TEXT NOT NULL,
                        revision TEXT NOT NULL, source_hash TEXT NOT NULL,
                        PRIMARY KEY (namespace, source_id, revision)
                    );
                    CREATE TABLE IF NOT EXISTS experiment_quotes (
                        namespace TEXT NOT NULL, source_id TEXT NOT NULL,
                        revision TEXT NOT NULL, generation TEXT NOT NULL,
                        quote TEXT NOT NULL, source_start INTEGER NOT NULL,
                        source_end INTEGER NOT NULL, event_id TEXT NOT NULL,
                        atom_id TEXT, candidate_id TEXT,
                        PRIMARY KEY (namespace, source_id, generation, quote)
                    );
                    CREATE INDEX IF NOT EXISTS experiment_quotes_atom
                    ON experiment_quotes (namespace, atom_id);
                """)
            self._initialized = True
        return self

    async def _retire(self, source_id: str) -> None:
        # Commit the host deny before awaiting erasure. If erasure fails or the
        # process stops, old evidence cannot be delivered after reopening.
        with self._metadata() as db:
            db.execute(
                "UPDATE experiment_quote_sources SET active = 0, source_text = '' "
                "WHERE namespace = ? AND source_id = ?",
                (self.namespace, source_id),
            )
            events = tuple(row[0] for row in db.execute(
                "SELECT event_id FROM experiment_quotes WHERE namespace = ? AND source_id = ?",
                (self.namespace, source_id),
            ))
        # All versions and pending event reservations are included. Core forget
        # erases their atom claims, admission history, and dependent artifacts.
        for start in range(0, len(events), 100):
            await self.kernel.forget(ForgetRequest(
                self.scope, events[start:start + 100], mode=ForgetMode.ERASE
            ))
        with self._metadata() as db:
            db.execute(
                "DELETE FROM experiment_quotes WHERE namespace = ? AND source_id = ?",
                (self.namespace, source_id),
            )

    async def admit(
        self, source_id: str, revision: str | int, source_text: str, quotes: list[str]
    ) -> int:
        self._require_initialized()
        _identifier(source_id, "source_id")
        revision_label = str(revision)
        revision = _revision_key(revision)
        if not isinstance(source_text, str):
            raise ValueError("source_text must be a string")
        if not isinstance(quotes, list):
            raise ValueError("quotes must be a list")
        # No whitespace normalization, substring guessing or paraphrase repair.
        valid = list(dict.fromkeys(
            quote for quote in quotes
            if isinstance(quote, str) and quote.strip()
            and len(quote) <= MAX_QUOTE_CHARS and quote in source_text
        ))
        source_hash = _digest(source_text)
        async with self._lock:
            with self._metadata() as db:
                previous = db.execute(
                    "SELECT * FROM experiment_quote_sources WHERE namespace = ? AND source_id = ?",
                    (self.namespace, source_id),
                ).fetchone()
                known = db.execute(
                    "SELECT source_hash FROM experiment_quote_revisions "
                    "WHERE namespace = ? AND source_id = ? AND revision = ?",
                    (self.namespace, source_id, revision),
                ).fetchone()
            if known is not None and known[0] != source_hash:
                await self._retire(source_id)
                raise ValueError("source revision reused with different content")
            unchanged = previous and previous["active"] and previous["revision"] == revision
            if unchanged:
                generation = previous["generation"]
            else:
                await self._retire(source_id)
                generation = uuid4().hex
                with self._metadata() as db:
                    db.execute(
                        "INSERT INTO experiment_quote_sources VALUES (?, ?, ?, ?, ?, ?, 1) "
                        "ON CONFLICT(namespace, source_id) DO UPDATE SET "
                        "revision=excluded.revision, source_hash=excluded.source_hash, "
                        "source_text=excluded.source_text, "
                        "generation=excluded.generation, active=1",
                        (self.namespace, source_id, revision, source_hash, source_text, generation),
                    )
                    db.execute(
                        "INSERT OR IGNORE INTO experiment_quote_revisions VALUES (?, ?, ?, ?)",
                        (self.namespace, source_id, revision, source_hash),
                    )
            admitted = 0
            for quote in valid:
                with self._metadata() as db:
                    existing = db.execute(
                        "SELECT atom_id, event_id FROM experiment_quotes WHERE namespace = ? "
                        "AND source_id = ? AND generation = ? AND quote = ?",
                        (self.namespace, source_id, generation, quote),
                    ).fetchone()
                if existing is not None and existing["atom_id"] is not None:
                    continue
                if existing is not None:
                    # A previous attempt stopped between its reservation and
                    # publication. Erase its event before retrying a fresh one.
                    await self.kernel.forget(ForgetRequest(
                        self.scope, (existing["event_id"],), mode=ForgetMode.ERASE
                    ))
                    with self._metadata() as db:
                        db.execute(
                            "DELETE FROM experiment_quotes WHERE namespace = ? AND event_id = ?",
                            (self.namespace, existing["event_id"]),
                        )
                start = source_text.index(quote)
                event = MemoryEvent(
                    self.scope, "document.observation", quote,
                    id="quote-event:" + uuid4().hex,
                    actor="experiment:host-document-observer",
                    source_uri=(
                        "evidence-quote://source/" + uri_quote(source_id, safe="")
                        + "?revision=" + uri_quote(revision_label, safe="")
                        + f"&start={start}&end={start + len(quote)}"
                    ),
                )
                subject = "quote-span:" + _digest(json.dumps(
                    [self.namespace, source_id, revision, generation, quote], ensure_ascii=False
                ))
                draft = AtomDraft(subject, PREDICATE, quote, quote, quote)
                authority = SourceAuthority(
                    "experiment:host-document-observer", kind="document",
                    subjects=(subject,), predicates=(PREDICATE,),
                )
                # Reserve provenance before core write, so even an interrupted
                # admission's event can be erased by source revocation later.
                with self._metadata() as db:
                    db.execute(
                        "INSERT INTO experiment_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
                        (self.namespace, source_id, revision, generation, quote,
                         start, start + len(quote), event.id),
                    )
                receipt = await self.kernel.admit_event(
                    event, (draft,), authority=authority, policy=self.policy
                )
                if (len(receipt.claim_ids) != 1 or len(receipt.candidate_ids) != 1
                        or any(item.action != "ACCEPT" for item in receipt.decisions)):
                    raise RuntimeError("core admission did not accept the exact quote observation")
                with self._metadata() as db:
                    db.execute(
                        "UPDATE experiment_quotes SET atom_id = ?, candidate_id = ? "
                        "WHERE namespace = ? AND event_id = ?",
                        (receipt.claim_ids[0], receipt.candidate_ids[0], self.namespace, event.id),
                    )
                admitted += 1
            return admitted

    async def search(self, query: str, limit: int = 4) -> list[dict]:
        self._require_initialized()
        if not isinstance(query, str):
            raise ValueError("query must be a string")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        async with self._lock:
            retrieve_limit = min(100, limit * 4)
            bundle = await self.kernel.retrieve(MemoryQuery(
                self.scope, query, limit=retrieve_limit,
                token_budget=retrieve_limit * MAX_QUOTE_CHARS,
                include_current_state=False, channels=(MemoryChannel.SEMANTIC,),
            ))
            citations = {citation.memory_id: citation.source_event_ids
                         for citation in bundle.citations}
            result = []
            # Only candidates the real core returned are eligible. These joins
            # validate delivery; they never generate or rank retrieval matches.
            with self._metadata() as db:
                for item in bundle.relevant_memories:
                    row = db.execute(
                        "SELECT q.*, s.source_text, s.source_hash FROM experiment_quotes q "
                        "JOIN experiment_quote_sources s ON q.namespace = s.namespace "
                        "AND q.source_id = s.source_id AND q.revision = s.revision "
                        "AND q.generation = s.generation WHERE q.namespace = ? "
                        "AND q.atom_id = ? AND s.active = 1",
                        (self.namespace, item.id),
                    ).fetchone()
                    if row is None:
                        continue
                    quote = row["quote"]
                    event_ids = tuple(item.metadata.get("source_event_ids", ()))
                    if (item.text != quote or not quote.strip() or len(quote) > MAX_QUOTE_CHARS
                            or row["source_text"][row["source_start"]:row["source_end"]] != quote
                            or _digest(row["source_text"]) != row["source_hash"]
                            or event_ids != (row["event_id"],)
                            or tuple(citations.get(item.id, ())) != event_ids):
                        continue
                    result.append({
                        "source_id": row["source_id"], "revision": json.loads(row["revision"]),
                        "quote": quote, "atom_id": item.id,
                        "candidate_id": row["candidate_id"],
                        "source_event_ids": list(event_ids),
                        "source_start": row["source_start"], "source_end": row["source_end"],
                    })
                    if len(result) == limit:
                        break
            return result

    async def revoke(self, source_id: str) -> None:
        self._require_initialized()
        _identifier(source_id, "source_id")
        async with self._lock:
            await self._retire(source_id)

    async def close(self) -> None:
        async with self._lock:
            await self.kernel.close()
            self._initialized = False
