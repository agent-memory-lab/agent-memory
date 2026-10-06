CREATE TABLE IF NOT EXISTS agent_memory_retention_delivery (
    partition_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('target','sequence')),
    identity TEXT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key,kind,identity)
);
