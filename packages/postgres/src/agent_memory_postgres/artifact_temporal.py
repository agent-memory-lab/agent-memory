"""Current Claim membership inside a PostgreSQL artifact dependency transaction."""

from agent_memory.retrieval.temporal_history import aware


async def postgres_current_claim_ids(connection, rows, *, valid_at, known_at):
    valid_at, known_at = aware(valid_at, "valid_at"), aware(known_at, "known_at")
    identities = sorted({row["id"] for row in rows if row["table"] == "claims"})
    if not identities:
        return set()
    cursor = await connection.execute(
        "SELECT DISTINCT claim_id FROM agent_memory_claim_versions WHERE claim_id=ANY(%s) "
        "AND system_from <= %s AND (system_to IS NULL OR %s < system_to) "
        "AND valid_from <= %s AND (valid_to IS NULL OR %s < valid_to)",
        (identities, known_at, known_at, valid_at, valid_at),
    )
    return {row["claim_id"] for row in await cursor.fetchall()}
