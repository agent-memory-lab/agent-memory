"""Bounded post-ranking source context, not another scored retrieval branch."""

from datetime import datetime
from hashlib import sha256
from json import dumps

from ..domain import MemoryChannel, MemoryItem, MemoryKind
from . import sqlite_lexical_index as index
from .candidate_identity import candidate_identity
from .source_context import CONTEXT_POLICY, MAX_CONTEXT_CANDIDATES, context_anchors


def expand_source_context(connection, query, ranked, limit, *, scope_clause, scope_params):
    """Revalidate anchor headers, locate at most eight windows, then hydrate.

    Header probes and byte slices share the caller's SQLite read transaction.
    Dirty-index repair precedes this function, so stale caller revisions cannot
    authorize reads after a write, archive or deletion. No full event body is
    loaded into Python, and context inherits relevance without a semantic score.
    """
    if type(limit) is not int or limit < 0:
        raise ValueError("context limit must be a nonnegative integer")
    limit = min(limit, MAX_CONTEXT_CANDIDATES)
    if (
        not limit or query.valid_at is not None or query.known_at is not None
        or MemoryChannel.SEMANTIC not in query.channels
    ):
        return ()
    anchors = context_anchors(query, ranked)
    seen = {candidate_identity(item) for item in ranked}
    locators = []
    anchor_by_identity = {}
    stride = index.CHUNK_CHARS - index.CHUNK_OVERLAP
    for anchor in anchors:
        metadata = anchor.metadata
        span = metadata["source_span"]
        revision = metadata["source_revision"]
        # A packed/narrowed excerpt is not a new retrieval expansion anchor.
        if not metadata.get("lexical_chunk_id"):
            continue
        trusted = connection.execute(
            "SELECT c.source_revision,c.source_chars FROM lexical_chunks c "
            "JOIN events ON events.id=c.source_id "
            "WHERE c.source_table='events' AND c.source_id=? AND c.span_start=? "
            "AND c.source_revision=? AND c.source_chars=? AND events.archived_at IS NULL "
            f"AND {scope_clause} AND EXISTS (SELECT 1 FROM lexical_chunks boundary "
            "WHERE boundary.source_table='events' AND boundary.source_id=c.source_id "
            "AND boundary.source_revision=c.source_revision AND boundary.span_end=?) LIMIT 1",
            (anchor.id, span["start"], revision, metadata["source_chars"],
             *scope_params, span["end"]),
        ).fetchone()
        if trusted is None:
            continue
        for start in (span["start"] - stride, span["start"] + stride):
            if start < 0:
                continue
            # Four child headers bound both returned rows and resulting text.
            headers = connection.execute(
                "SELECT chunk_id,source_id,owner_id,span_start,span_end,byte_start,byte_end "
                "FROM lexical_chunks WHERE source_table='events' AND source_id=? "
                "AND source_revision=? AND span_start BETWEEN ? AND ? "
                "ORDER BY span_start LIMIT 4",
                (anchor.id, revision, start, start + 3 * stride),
            ).fetchall()
            if not headers or headers[0]["span_start"] != start:
                continue
            end = headers[-1]["span_end"]
            if not 0 < end - start <= index.CHUNK_CHARS + 3 * stride:
                continue
            chunk_id = sha256(
                dumps(["events", anchor.id, revision, start, end]).encode()
            ).hexdigest()
            identity = "source-span:" + chunk_id
            if identity in seen:
                continue
            seen.add(identity)
            anchor_by_identity[identity] = anchor
            locators.append({
                **dict(headers[0]), "window_start": start, "window_end": end,
                "window_byte_start": headers[0]["byte_start"],
                "window_byte_end": headers[-1]["byte_end"], "overlap": 0.0,
            })
            if len(locators) >= limit:
                break
        if len(locators) >= limit:
            break
    result = []
    for row, _ in index.hydrate_events(connection, locators):
        identity = "source-span:" + row["chunk_id"]
        anchor = anchor_by_identity[identity]
        # Hydration's usual matched child was not matched/scored in this stage.
        row.pop("matched_source_span", None)
        result.append(MemoryItem(
            id=row["id"], kind=MemoryKind.EVENT, text=row["content"], score=anchor.score,
            occurred_at=datetime.fromisoformat(row["occurred_at"]),
            metadata={
                "channel": MemoryChannel.SEMANTIC, "event_type": row["event_type"],
                "source_event_ids": (row["id"],), **index.lineage(row),
                "context_window": CONTEXT_POLICY, "context_only": True,
                "context_reason": "reranked_adjacent",
                "context_anchor_candidate_id": candidate_identity(anchor),
                "context_anchor_score": anchor.score,
                "relevance_origin": "inherited_context_anchor",
            },
        ))
    return tuple(result)
