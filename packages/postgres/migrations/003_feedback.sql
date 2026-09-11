ALTER TABLE agent_memory_evolution_records
    ADD COLUMN IF NOT EXISTS feedback_status text NOT NULL DEFAULT 'accepted',
    ADD COLUMN IF NOT EXISTS parent_id text,
    ADD COLUMN IF NOT EXISTS idempotency_key text,
    ADD COLUMN IF NOT EXISTS payload_hash text,
    ADD COLUMN IF NOT EXISTS corrects_id text,
    ADD COLUMN IF NOT EXISTS expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS invalidated_at timestamptz;

CREATE UNIQUE INDEX IF NOT EXISTS agent_memory_evolution_idempotency_idx
ON agent_memory_evolution_records(partition_key, record_type, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS agent_memory_evolution_parent_idx
ON agent_memory_evolution_records(partition_key, parent_id, feedback_status);

INSERT INTO agent_memory_schema(schema_version)
VALUES (2)
ON CONFLICT (schema_version) DO NOTHING;
