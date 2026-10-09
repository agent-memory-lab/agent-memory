"""Transactional SQLite lexical locators, separate from governed locator indexes.

The index stores terms and exact source spans, never generated evidence. SQL
triggers invalidate changed sources even for legacy writers. Repository writers
consume the durable dirty queue before commit; restart/legacy writes are repaired
before retrieval. Full backfill is restricted to schema/analyzer migration.
"""

from __future__ import annotations

import json
from collections import Counter
from hashlib import sha256
from itertools import product

from ..domain import MemoryScope
from .analyzer import LEXICAL_ANALYZER_VERSION, lexical_terms

INDEX_VERSION = "sqlite-lexical/2"
CHUNK_CHARS = 1024
CHUNK_OVERLAP = 128
MAX_QUERY_TERMS = 1024
MAX_CANDIDATES = 2400
SOURCES = {"events": "id", "artifacts": "id", "claim_versions": "revision_id"}

SCHEMA = """
CREATE INDEX IF NOT EXISTS lexical_legacy_claims_idx
ON claim_observations(partition_key,observed_at) WHERE legacy=1;
CREATE TABLE IF NOT EXISTS lexical_index_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), version TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lexical_dirty (
    source_table TEXT NOT NULL, source_id TEXT NOT NULL,
    PRIMARY KEY(source_table,source_id)
);
CREATE TABLE IF NOT EXISTS lexical_chunks (
    chunk_id TEXT PRIMARY KEY, partition_key TEXT NOT NULL,
    source_table TEXT NOT NULL, source_id TEXT NOT NULL, owner_id TEXT NOT NULL,
    source_revision TEXT NOT NULL, span_start INTEGER NOT NULL,
    span_end INTEGER NOT NULL, source_chars INTEGER NOT NULL, term_count INTEGER NOT NULL,
    byte_start INTEGER NOT NULL, byte_end INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS lexical_chunks_source_idx
ON lexical_chunks(source_table,source_id);
CREATE INDEX IF NOT EXISTS lexical_chunks_scope_idx
ON lexical_chunks(partition_key,source_table,owner_id);
CREATE TABLE IF NOT EXISTS lexical_terms (
    partition_key TEXT NOT NULL, term TEXT NOT NULL, chunk_id TEXT NOT NULL,
    frequency INTEGER NOT NULL, PRIMARY KEY(partition_key,term,chunk_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS lexical_terms_chunk_idx ON lexical_terms(chunk_id);
CREATE TRIGGER IF NOT EXISTS lexical_chunk_delete AFTER DELETE ON lexical_chunks BEGIN
    DELETE FROM lexical_terms WHERE chunk_id=OLD.chunk_id;
END;
"""


def initialize(connection):
    connection.executescript(SCHEMA)
    columns = {row["name"] for row in connection.execute("PRAGMA table_info(lexical_chunks)")}
    for column in ("byte_start", "byte_end"):
        if column not in columns:
            connection.execute(
                f"ALTER TABLE lexical_chunks ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
            )
    for table, identity in SOURCES.items():
        for operation in ("INSERT", "UPDATE", "DELETE"):
            old = "" if operation == "INSERT" else (
                f"DELETE FROM lexical_chunks WHERE source_table='{table}' "
                f"AND source_id=OLD.{identity}; "
                f"DELETE FROM lexical_dirty WHERE source_table='{table}' "
                f"AND source_id=OLD.{identity}; "
            )
            new = "" if operation == "DELETE" else (
                "INSERT OR IGNORE INTO lexical_dirty VALUES "
                f"('{table}',NEW.{identity}); "
            )
            # Closing a temporal interval changes eligibility, not its text.
            update = (
                " OF payload_json,partition_key,claim_id" if table == "claim_versions"
                and operation == "UPDATE" else ""
            )
            connection.execute(
                f"CREATE TRIGGER IF NOT EXISTS lexical_{table}_{operation.lower()} "
                f"AFTER {operation}{update} ON {table} BEGIN {old}{new} END"
            )
    connection.execute("BEGIN IMMEDIATE")
    encoding = connection.execute("PRAGMA encoding").fetchone()[0]
    version = (f"{INDEX_VERSION}:{LEXICAL_ANALYZER_VERSION}:{CHUNK_CHARS}:"
               f"{CHUNK_OVERLAP}:encoded-spans:{encoding}")
    row = connection.execute("SELECT version FROM lexical_index_state WHERE singleton=1").fetchone()
    if row is None or row[0] != version:
        connection.execute("DELETE FROM lexical_chunks")
        connection.execute("DELETE FROM lexical_dirty")
        for table, identity in SOURCES.items():
            connection.execute(
                "INSERT INTO lexical_dirty(source_table,source_id) "
                f"SELECT ?,{identity} FROM {table}", (table,),
            )
        synchronize(connection)
        connection.execute("INSERT OR REPLACE INTO lexical_index_state VALUES (1,?)", (version,))
    else:
        synchronize(connection)


def _source(connection, table, identity):
    if table == "events":
        row = connection.execute(
            "SELECT partition_key,id AS owner_id,content AS text FROM events "
            "WHERE id=? AND archived_at IS NULL", (identity,),
        ).fetchone()
    elif table == "artifacts":
        row = connection.execute(
            "SELECT partition_key,id AS owner_id,text FROM artifacts "
            "WHERE id=? AND archived_at IS NULL AND status IN ('active','candidate')", (identity,),
        ).fetchone()
    else:
        row = connection.execute(
            "SELECT v.partition_key,v.claim_id AS owner_id,v.payload_json "
            "FROM claim_versions v JOIN claims c ON c.id=v.claim_id "
            "WHERE v.revision_id=? AND c.archived_at IS NULL", (identity,),
        ).fetchone()
    if row is None:
        return None
    result = dict(row)
    if table == "claim_versions":
        payload = json.loads(result.pop("payload_json"))
        result["text"] = payload["text"]
        result["extra_terms"] = lexical_terms(
            payload["key"] + " " + json.dumps(payload["value"], ensure_ascii=False)
        )
    return result


def synchronize(connection):
    """Consume only changed source IDs in bounded batches on the writer transaction."""
    encoding = None
    while True:
        pending = connection.execute(
            "SELECT source_table,source_id FROM lexical_dirty "
            "ORDER BY source_table,source_id LIMIT 128"
        ).fetchall()
        if not pending:
            return
        if encoding is None:
            encoding = connection.execute("PRAGMA encoding").fetchone()[0]
        for table, identity in pending:
            connection.execute(
                "DELETE FROM lexical_chunks WHERE source_table=? AND source_id=?", (table, identity)
            )
            source = _source(connection, table, identity)
            if source is not None:
                text = source["text"]
                revision = sha256(text.encode("utf-8")).hexdigest()
                byte_start, previous_start = 0, 0
                # Direct storage permits empty Claim.text even though ClaimDraft
                # does not. Keep its metadata discoverable with a truthful empty
                # source span; never synthesize key/value text as quoted evidence.
                starts = range(0, len(text), CHUNK_CHARS - CHUNK_OVERLAP)
                if not text and table == "claim_versions":
                    starts = (0,)
                for start in starts:
                    end = min(len(text), start + CHUNK_CHARS)
                    byte_start += len(text[previous_start:start].encode(encoding))
                    byte_end = byte_start + len(text[start:end].encode(encoding))
                    previous_start = start
                    frequencies = Counter(lexical_terms(text[start:end]))
                    # Metadata is a revision-level ranking signal, not repeated
                    # evidence in every text span. Postings grow additively with
                    # text chunks and structured metadata, never their product.
                    if start == 0:
                        frequencies.update(source.get("extra_terms", ()))
                    chunk_id = sha256(
                        json.dumps([table, identity, revision, start, end]).encode("utf-8")
                    ).hexdigest()
                    connection.execute(
                        "INSERT INTO lexical_chunks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (chunk_id, source["partition_key"], table, identity, source["owner_id"],
                         revision, start, end, len(text), sum(frequencies.values()),
                         byte_start, byte_end),
                    )
                    connection.executemany(
                        "INSERT INTO lexical_terms VALUES (?,?,?,?)",
                        ((source["partition_key"], term, chunk_id, count)
                         for term, count in frequencies.items()),
                    )
                    if end == len(text):
                        break
            connection.execute(
                "DELETE FROM lexical_dirty WHERE source_table=? AND source_id=?", (table, identity)
            )


def prepare_read(connection):
    """Open one consistent snapshot and repair committed legacy writes if needed."""
    connection.execute("BEGIN")
    if connection.execute("SELECT 1 FROM lexical_dirty LIMIT 1").fetchone():
        connection.rollback()
        connection.execute("BEGIN IMMEDIATE")
        synchronize(connection)


def partitions(scope, *, exact=False):
    if exact:
        return (scope.partition_key(),)
    choices = [(None,) if value is None else (None, value) for value in (
        scope.user_id, scope.agent_id, scope.workspace_id, scope.session_id
    )]
    return tuple(sorted({
        MemoryScope(scope.tenant_id, scope.namespace, *values).partition_key()
        for values in product(*choices)
    }))


def candidates(connection, scope, text, *, source_table, limit, eligibility="1", params=(),
               exact=False, offset=0):
    """Rank narrow locators in SQL; LIMIT precedes all evidence/payload hydration.

    Terms use the indexed partition/term primary key, not LIKE or Python scans.
    The best exact span per source survives; owner IDs retain native citations and
    admission guards. Caller-supplied eligibility SQL is trusted repository code.
    """
    if source_table not in SOURCES:
        raise ValueError("unsupported lexical source")
    if type(limit) is not int or not 1 <= limit <= MAX_CANDIDATES:
        raise ValueError(f"limit must be between 1 and {MAX_CANDIDATES}")
    if type(offset) is not int or not 0 <= offset <= MAX_CANDIDATES:
        raise ValueError("invalid lexical candidate offset")
    terms = tuple(dict.fromkeys(lexical_terms(text)))[:MAX_QUERY_TERMS]
    keys = partitions(scope, exact=exact)
    key_marks = ",".join("?" for _ in keys)
    join = (
        f"CROSS JOIN {source_table} ON {source_table}.{SOURCES[source_table]}=c.source_id "
    )
    if terms:
        hits = (
            "SELECT c.chunk_id,c.source_id,c.owner_id,c.span_start,"
            "COUNT(*)*1.0/? AS overlap,"
            "SUM(t.frequency*2.2/(t.frequency+1.2*(0.25+0.75*c.term_count/128.0))) AS rank_score "
            "FROM lexical_terms t CROSS JOIN lexical_chunks c ON c.chunk_id=t.chunk_id " + join +
            f"WHERE t.partition_key IN ({key_marks}) "
            "AND t.term IN (SELECT value FROM json_each(?)) "
            "AND c.source_table=? AND (" + eligibility + ") GROUP BY c.chunk_id"
        )
        values = (len(terms), *keys, json.dumps(terms, ensure_ascii=False), source_table, *params)
    else:
        hits = (
            "SELECT c.chunk_id,c.source_id,c.owner_id,c.span_start,"
            "0.0 AS overlap,0.0 AS rank_score FROM lexical_chunks c " + join +
            f"WHERE c.partition_key IN ({key_marks}) AND c.source_table=? AND ("
            + eligibility + ")"
        )
        values = (*keys, source_table, *params)
    return connection.execute(
        "WITH hits AS (" + hits + "), ranked AS (SELECT *,ROW_NUMBER() OVER ("
        "PARTITION BY owner_id ORDER BY overlap DESC,rank_score DESC,span_start,source_id"
        ") AS position FROM hits) SELECT chunk_id,source_id,owner_id,overlap,rank_score "
        "FROM ranked WHERE position=1 ORDER BY overlap DESC,rank_score DESC,owner_id "
        "LIMIT ? OFFSET ?",
        (*values, limit, offset),
    ).fetchall()


def lineage(row):
    return {
        "lexical_chunk_id": row["chunk_id"],
        "source_revision": row["source_revision"],
        "source_span": {"start": row["span_start"], "end": row["span_end"], "unit": "characters"},
        "source_chars": row["source_chars"],
        "lexical_analyzer": LEXICAL_ANALYZER_VERSION,
        **({"excerpt": True} if row["source_chars"] > row["span_end"] - row["span_start"] else {}),
    }


def hydrate_events(connection, locators):
    """Read only selected source spans. Never pull an entire long event into Python."""
    result = []
    for locator in locators:
        row = connection.execute(
            "SELECT c.*,e.id,e.tenant_id,e.namespace,e.user_id,e.agent_id,e.workspace_id,"
            "e.session_id,e.event_type,e.occurred_at,"
            "CAST(substr(CAST(e.content AS BLOB),c.byte_start+1,c.byte_end-c.byte_start) "
            "AS TEXT) AS content "
            "FROM lexical_chunks c JOIN events e ON e.id=c.source_id "
            "WHERE c.chunk_id=? AND e.archived_at IS NULL", (locator["chunk_id"],),
        ).fetchone()
        if row is not None:
            result.append((row, locator["overlap"]))
    return result
