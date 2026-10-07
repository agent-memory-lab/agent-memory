CREATE TABLE IF NOT EXISTS agent_memory_index_recovery (
    partition_key TEXT NOT NULL, channel TEXT NOT NULL, epoch INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('head','stream','rollover','repair')),
    identity TEXT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key,channel,epoch,kind,identity)
);
