CREATE TABLE IF NOT EXISTS agent_memory_retention_purge_heads (
    partition_key TEXT PRIMARY KEY, cursor BIGINT NOT NULL CHECK(cursor >= 0)
);
CREATE TABLE IF NOT EXISTS agent_memory_retention_purges (
    partition_key TEXT NOT NULL, cursor BIGINT NOT NULL,
    source_event_id TEXT NOT NULL, epoch BIGINT NOT NULL,
    all_in_scope BOOLEAN NOT NULL, mode TEXT NOT NULL,
    PRIMARY KEY(partition_key,cursor)
);
CREATE INDEX IF NOT EXISTS agent_memory_retention_purge_source_idx
ON agent_memory_retention_purges(partition_key,source_event_id);
