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
from math import log1p

from ..domain import MemoryScope
from .analyzer import LEXICAL_ANALYZER_VERSION, lexical_terms
from .candidate_identity import MAX_SOURCE_WINDOW_CHARS

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
CREATE INDEX IF NOT EXISTS lexical_chunks_source_span_idx
ON lexical_chunks(source_table,source_id,span_start);
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


def _bm25_statistics(connection, keys, terms, source_table, eligibility, params):
    """Only authorized, temporally eligible locator headers contribute to IDF.

    No evidence text is fetched. The query-term postings and the scope header
    index bound these SQL aggregates independently of evidence hydration.
    """
    marks = ",".join("?" for _ in keys)
    join = f"CROSS JOIN {source_table} ON {source_table}.{SOURCES[source_table]}=c.source_id "
    corpus = connection.execute(
        "SELECT COUNT(*) AS n,AVG(c.term_count) AS average FROM lexical_chunks c " + join
        + f"WHERE c.partition_key IN ({marks}) AND c.source_table=? AND ({eligibility})",
        (*keys, source_table, *params),
    ).fetchone()
    counts = connection.execute(
        "SELECT t.term,COUNT(*) AS df FROM lexical_terms t CROSS JOIN lexical_chunks c "
        "ON c.chunk_id=t.chunk_id " + join
        + f"WHERE t.partition_key IN ({marks}) "
        "AND t.term IN (SELECT value FROM json_each(?)) "
        f"AND c.source_table=? AND ({eligibility}) GROUP BY t.term",
        (*keys, json.dumps(terms, ensure_ascii=False), source_table, *params),
    ).fetchall()
    frequencies = {row["term"]: row["df"] for row in counts}
    weights = {
        term: log1p((corpus["n"] - frequencies.get(term, 0) + 0.5)
                    / (frequencies.get(term, 0) + 0.5))
        for term in terms
    }
    return weights, max(1.0, corpus["average"] or 0.0)


def candidates(connection, scope, text, *, source_table, limit, eligibility="1", params=(),
               exact=False, offset=0, context_chunks=1, anchor_terms=None):
    """Rank narrow locators in SQL; LIMIT precedes all evidence/payload hydration.

    Terms use the indexed partition/term primary key, not LIKE or Python scans.
    Single-span callers retain their existing contract. Native event retrieval
    can reserve a bounded number of source-context spans per matched owner;
    owner IDs retain public citations and admission guards. Eligibility SQL is
    trusted repository code, and expansion stays in the same read transaction.
    Optional native-query anchors gate matched chunks before ranking/limits;
    all original query terms still contribute to BM25. Other callers are unchanged.
    """
    if source_table not in SOURCES:
        raise ValueError("unsupported lexical source")
    if type(limit) is not int or not 1 <= limit <= MAX_CANDIDATES:
        raise ValueError(f"limit must be between 1 and {MAX_CANDIDATES}")
    if type(offset) is not int or not 0 <= offset <= MAX_CANDIDATES:
        raise ValueError("invalid lexical candidate offset")
    if type(context_chunks) is not int or not 1 <= context_chunks <= 4:
        raise ValueError("context_chunks must be between 1 and 4")
    if context_chunks != 1 and (source_table != "events" or offset):
        raise ValueError("event context expansion requires an unpaged event query")
    terms = tuple(dict.fromkeys(lexical_terms(text)))[:MAX_QUERY_TERMS]
    anchors = tuple(term for term in dict.fromkeys(anchor_terms or ()) if term in terms)
    if anchor_terms and not anchors:
        return ()
    keys = partitions(scope, exact=exact)
    key_marks = ",".join("?" for _ in keys)
    join = (
        f"CROSS JOIN {source_table} ON {source_table}.{SOURCES[source_table]}=c.source_id "
    )
    if terms:
        weights, average = _bm25_statistics(
            connection, keys, terms, source_table, eligibility, params,
        )
        hits = (
            "SELECT c.chunk_id,c.source_id,c.owner_id,c.span_start,"
            "SUM(q.value)*1.0/? AS overlap,"
            "SUM(q.value*t.frequency*2.2/"
            "(t.frequency+1.2*(0.25+0.75*c.term_count/?))) AS rank_score "
            "FROM lexical_terms t CROSS JOIN lexical_chunks c ON c.chunk_id=t.chunk_id " + join +
            "CROSS JOIN json_each(?) q ON q.key=t.term "
            f"WHERE t.partition_key IN ({key_marks}) "
            "AND t.term IN (SELECT value FROM json_each(?)) "
            "AND c.source_table=? AND (" + eligibility + ") GROUP BY c.chunk_id"
        )
        values = (sum(weights.values()), average, json.dumps(weights, ensure_ascii=False),
                  *keys, json.dumps(terms, ensure_ascii=False), source_table, *params)
        if anchors:
            hits += (
                " HAVING MAX(CASE WHEN t.term IN (SELECT value FROM json_each(?)) "
                "THEN 1 ELSE 0 END)=1"
            )
            values = (*values, json.dumps(anchors, ensure_ascii=False))
    else:
        hits = (
            "SELECT c.chunk_id,c.source_id,c.owner_id,c.span_start,"
            "0.0 AS overlap,0.0 AS rank_score FROM lexical_chunks c " + join +
            f"WHERE c.partition_key IN ({key_marks}) AND c.source_table=? AND ("
            + eligibility + ")"
        )
        values = (*keys, source_table, *params)
    expanding = context_chunks > 1 and bool(terms)
    if expanding:
        # First bound owner discovery, then retain up to two hits of each
        # selected owner. A weak second hit must not compete against another
        # owner's stronger first hit before the source-context stage.
        selected = connection.execute(
            "WITH hits AS (" + hits + "), ranked AS (SELECT *,ROW_NUMBER() OVER ("
            "PARTITION BY owner_id ORDER BY rank_score DESC,overlap DESC,span_start,source_id"
            ") AS position FROM hits), owners AS (SELECT owner_id,overlap,rank_score "
            "FROM ranked WHERE position=1 ORDER BY rank_score DESC,overlap DESC,owner_id "
            "LIMIT ?) SELECT r.chunk_id,r.source_id,r.owner_id,r.span_start,r.overlap,"
            "r.rank_score FROM owners o JOIN ranked r ON r.owner_id=o.owner_id "
            "WHERE r.position<=2 ORDER BY r.position,o.rank_score DESC,o.overlap DESC,o.owner_id "
            "LIMIT ?",
            (*values, limit, min(MAX_CANDIDATES, limit * 2)),
        ).fetchall()
        return _event_context_locators(connection, selected, limit, context_chunks)
    selected = connection.execute(
        "WITH hits AS (" + hits + "), ranked AS (SELECT *,ROW_NUMBER() OVER ("
        "PARTITION BY owner_id ORDER BY rank_score DESC,overlap DESC,span_start,source_id"
        ") AS position FROM hits) SELECT chunk_id,source_id,owner_id,overlap,rank_score "
        "FROM ranked WHERE position=1 ORDER BY rank_score DESC,overlap DESC,owner_id "
        "LIMIT ? OFFSET ?",
        (*values, limit, offset),
    ).fetchall()
    return selected


def _event_context_locators(connection, selected, limit, per_owner):
    """Reserve each owner's primary hit, then borrow slots for source context.

    A source may be much longer than the fixed candidate budget. These are
    contextual candidates, not a completeness claim or independent dense recall.
    All owner discovery, source count, per-owner and total locator limits stay
    unchanged. Lazy expansion never builds extra windows after the total limit
    is full. Depth within a high-ranked source yields to breadth across owners.
    """
    result = []
    by_owner = {}
    for locator in selected:
        by_owner.setdefault(locator["owner_id"], []).append(locator)
    active = [
        _owner_context_locators(connection, matches, per_owner)
        for matches in by_owner.values()
    ]
    while active:
        next_round = []
        for owner in active:
            locator = next(owner, None)
            if locator is None:
                continue
            result.append(locator)
            if len(result) >= limit:
                return result
            next_round.append(owner)
        active = next_round
    return result


def _owner_context_locators(connection, matches, per_owner):
    """Yield only requested windows; all probes stay within the original owner."""
    stride = CHUNK_CHARS - CHUNK_OVERLAP
    primary = matches[0]
    anchor = connection.execute(
        "SELECT source_revision,span_start,source_chars FROM lexical_chunks WHERE chunk_id=?",
        (primary["chunk_id"],),
    ).fetchone()
    if anchor is None:
        return
    last = ((max(0, anchor["source_chars"] - CHUNK_CHARS) + stride - 1) // stride) * stride
    starts = tuple(dict.fromkeys((
        anchor["span_start"], *(row["span_start"] for row in matches[1:]),
        last, anchor["span_start"] + stride, max(0, anchor["span_start"] - stride),
    )))
    match_by_start = {row["span_start"]: row for row in matches}
    windows = set()
    for start in starts:
        row = connection.execute(
            "SELECT chunk_id,source_id,owner_id,span_start FROM lexical_chunks "
            "WHERE source_table='events' AND source_id=? AND span_start=? "
            "AND source_revision=?",
            (primary["source_id"], start, anchor["source_revision"]),
        ).fetchone()
        if row is None:
            continue
        match = match_by_start.get(start, primary)
        window = _event_window(connection, dict(row))
        window_key = (window["window_start"], window["window_end"])
        if window_key in windows:
            continue
        windows.add(window_key)
        yield {
            **dict(row), **window, "overlap": match["overlap"],
            "rank_score": match["rank_score"],
            "context_anchor_chunk_id": primary["chunk_id"],
            "context_reason": (
                "matched" if row["chunk_id"] == primary["chunk_id"] else
                "additional_match" if start in match_by_start else
                "source_tail" if start == last else "adjacent"
            ),
        }
        if len(windows) >= per_owner:
            return


def _event_window(connection, locator):
    """Locate a bounded parent with lead-in context and a following child.

    References usually inherit entities from preceding text. Four indexed
    children preserve two preceding boundaries plus one following boundary;
    at the source start the unused lead-in budget extends forward instead.
    The parent remains below the shared 4096-character validation bound.
    """
    stride = CHUNK_CHARS - CHUNK_OVERLAP
    parent_start = max(0, locator["span_start"] - 2 * stride)
    row = connection.execute(
        "SELECT MIN(span_start) AS start,MAX(span_end) AS end,"
        "MIN(byte_start) AS byte_start,MAX(byte_end) AS byte_end "
        "FROM lexical_chunks WHERE source_table='events' AND source_id=? "
        "AND span_start BETWEEN ? AND ?",
        (locator["source_id"], parent_start, parent_start + 3 * stride),
    ).fetchone()
    if not 0 < row["end"] - row["start"] <= MAX_SOURCE_WINDOW_CHARS:
        raise ValueError("invalid bounded source window")
    return {"window_start": row["start"], "window_end": row["end"],
            "window_byte_start": row["byte_start"], "window_byte_end": row["byte_end"]}


def lineage(row):
    result = {
        "lexical_chunk_id": row["chunk_id"],
        "source_revision": row["source_revision"],
        "source_span": {"start": row["span_start"], "end": row["span_end"], "unit": "characters"},
        "source_chars": row["source_chars"],
        "lexical_analyzer": LEXICAL_ANALYZER_VERSION,
        **({"excerpt": True} if row["source_chars"] > row["span_end"] - row["span_start"] else {}),
    }
    if "source_table" in row.keys() and row["source_table"] == "events":
        result.update({
            "candidate_identity": "source-span:" + row["chunk_id"],
            "source_owner_id": row["owner_id"],
        })
    if "context_anchor_chunk_id" in row.keys():
        result.update({key: row[key] for key in ("context_anchor_chunk_id", "context_reason")})
    if "matched_source_span" in row.keys():
        result["matched_source_span"] = row["matched_source_span"]
        result["context_window"] = "bounded-parent/2"
    if "rank_score" in row.keys():
        result["lexical_score"] = row["rank_score"]
        result["lexical_ranker"] = "scoped-bm25/1"
    return result


def hydrate_events(connection, locators):
    """Read only selected source spans. Never pull an entire long event into Python."""
    result = []
    for locator in locators:
        window = "window_start" in locator.keys()
        content_sql = (
            "CAST(substr(CAST(e.content AS BLOB),?+1,?-?) AS TEXT) AS content " if window else
            "CAST(substr(CAST(e.content AS BLOB),c.byte_start+1,c.byte_end-c.byte_start) "
            "AS TEXT) AS content "
        )
        params = ((locator["window_byte_start"], locator["window_byte_end"],
                   locator["window_byte_start"], locator["chunk_id"]) if window else
                  (locator["chunk_id"],))
        row = connection.execute(
            "SELECT c.*,e.id,e.tenant_id,e.namespace,e.user_id,e.agent_id,e.workspace_id,"
            "e.session_id,e.event_type,e.occurred_at,"
            + content_sql +
            "FROM lexical_chunks c JOIN events e ON e.id=c.source_id "
            "WHERE c.chunk_id=? AND e.archived_at IS NULL", params,
        ).fetchone()
        if row is not None:
            hydrated = dict(row)
            if "rank_score" in locator.keys():
                hydrated["rank_score"] = locator["rank_score"]
            if window:
                hydrated["matched_source_span"] = {
                    "start": row["span_start"], "end": row["span_end"], "unit": "characters",
                }
                hydrated.update(span_start=locator["window_start"], span_end=locator["window_end"])
                hydrated["chunk_id"] = sha256(json.dumps([
                    "events", row["id"], row["source_revision"],
                    locator["window_start"], locator["window_end"],
                ]).encode()).hexdigest()
            keys = locator.keys()
            if "context_anchor_chunk_id" in keys:
                hydrated["context_anchor_chunk_id"] = locator["context_anchor_chunk_id"]
                hydrated["context_reason"] = locator["context_reason"]
            result.append((hydrated, locator["overlap"]))
    return result
