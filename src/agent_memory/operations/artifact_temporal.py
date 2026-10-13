"""Current Claim membership for derived blocks, within the caller's transaction.

Use the materialized bitemporal intervals rather than the legacy ingestion head.
The latter can be superseded while still effective today or after an override ends.
"""

from ..retrieval.temporal_history import aware


def _claim_ids(rows):
    return sorted({row["id"] for row in rows if row["table"] == "claims"})


def sqlite_current_claim_ids(connection, rows, *, valid_at, known_at):
    valid_at = aware(valid_at, "valid_at").isoformat()
    known_at = aware(known_at, "known_at").isoformat()
    identities, current = _claim_ids(rows), set()
    for offset in range(0, len(identities), 256):
        batch = identities[offset:offset + 256]
        marks = ",".join("?" for _ in batch)
        current.update(row["claim_id"] for row in connection.execute(
            f"SELECT DISTINCT claim_id FROM claim_versions WHERE claim_id IN ({marks}) "
            "AND system_from <= ? AND (system_to IS NULL OR ? < system_to) "
            "AND valid_from <= ? AND (valid_to IS NULL OR ? < valid_to)",
            (*batch, known_at, known_at, valid_at, valid_at),
        ))
    return current
