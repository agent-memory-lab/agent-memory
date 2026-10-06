-- Admission snapshots remain separate from legacy Claim lifecycle states.
CREATE TABLE IF NOT EXISTS agent_memory_admission_records (
    record_id text PRIMARY KEY,
    partition_key text NOT NULL,
    event_id text NOT NULL,
    slot_key text NOT NULL,
    scope_json jsonb NOT NULL,
    payload_json jsonb NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    recorded_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS agent_memory_admission_scope_slot_idx
ON agent_memory_admission_records(partition_key, slot_key, record_id);
CREATE INDEX IF NOT EXISTS agent_memory_admission_source_idx
ON agent_memory_admission_records(event_id);
CREATE INDEX IF NOT EXISTS agent_memory_admission_payload_idx
ON agent_memory_admission_records USING gin(payload_json);

CREATE TABLE IF NOT EXISTS agent_memory_admission_versions (
    record_id text NOT NULL REFERENCES agent_memory_admission_records(record_id) ON DELETE CASCADE,
    version integer NOT NULL CHECK (version > 0),
    partition_key text NOT NULL,
    payload_json jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY(record_id, version)
);
CREATE INDEX IF NOT EXISTS agent_memory_admission_versions_scope_idx
ON agent_memory_admission_versions(partition_key, record_id, recorded_at);

-- No event FK: erased identities must remain unavailable to late workers.
CREATE TABLE IF NOT EXISTS agent_memory_admission_tombstones (
    partition_key text NOT NULL,
    event_id text NOT NULL,
    idempotency_key text,
    PRIMARY KEY(partition_key, event_id)
);
CREATE INDEX IF NOT EXISTS agent_memory_admission_tombstone_event_idx
ON agent_memory_admission_tombstones(event_id);
CREATE INDEX IF NOT EXISTS agent_memory_admission_tombstone_idempotency_idx
ON agent_memory_admission_tombstones(partition_key, idempotency_key)
WHERE idempotency_key IS NOT NULL;
