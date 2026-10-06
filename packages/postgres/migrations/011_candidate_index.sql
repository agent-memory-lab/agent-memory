CREATE TABLE IF NOT EXISTS agent_memory_index_jobs (
    partition_key TEXT NOT NULL, channel TEXT NOT NULL, epoch INTEGER NOT NULL,
    token_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_id TEXT NOT NULL,
    status TEXT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key,channel,epoch,token_id),
    UNIQUE(partition_key,channel,epoch,sequence)
);
CREATE TABLE IF NOT EXISTS agent_memory_index_documents (
    partition_key TEXT NOT NULL, channel TEXT NOT NULL, candidate_id TEXT NOT NULL,
    slot_key TEXT NOT NULL, event_id TEXT NOT NULL, payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key,channel,candidate_id)
);
CREATE INDEX IF NOT EXISTS agent_memory_index_slot_idx
ON agent_memory_index_documents(partition_key,channel,slot_key);
