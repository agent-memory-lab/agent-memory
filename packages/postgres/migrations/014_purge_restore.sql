CREATE TABLE IF NOT EXISTS agent_memory_retention_purge_restores (
    partition_key TEXT NOT NULL, identity TEXT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key,identity)
);
