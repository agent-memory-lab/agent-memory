CREATE TABLE IF NOT EXISTS agent_memory_resource_refresh (
    partition_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('resource','request')),
    identity TEXT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key,kind,identity)
);
