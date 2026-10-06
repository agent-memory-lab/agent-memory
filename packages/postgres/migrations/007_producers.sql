CREATE TABLE IF NOT EXISTS agent_memory_retention_producers (
    partition_key TEXT NOT NULL, producer_id TEXT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key, producer_id)
);
