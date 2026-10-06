CREATE TABLE IF NOT EXISTS agent_memory_retention_heads (
    partition_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('document','interpretation')),
    identity TEXT NOT NULL, generation BIGINT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key, kind, identity)
);
