-- Generic ingestion and its consolidation handoff share the source transaction.
-- No duplicate source content is retained; erasure removes intents by foreign key.
CREATE TABLE IF NOT EXISTS agent_memory_consolidation_outbox (
    event_id TEXT PRIMARY KEY REFERENCES agent_memory_events(id) ON DELETE CASCADE,
    partition_key TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    result_json JSONB NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'delivering', 'delivered', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ,
    cancel_pending BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_generation BIGINT NOT NULL DEFAULT 0
);
ALTER TABLE agent_memory_consolidation_outbox
ADD COLUMN IF NOT EXISTS cancel_pending BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE agent_memory_consolidation_outbox
ADD COLUMN IF NOT EXISTS cancel_generation BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS agent_memory_consolidation_outbox_claim_idx
ON agent_memory_consolidation_outbox(status, created_at, event_id);
CREATE INDEX IF NOT EXISTS agent_memory_consolidation_outbox_scope_idx
ON agent_memory_consolidation_outbox(partition_key, status);
CREATE INDEX IF NOT EXISTS agent_memory_consolidation_outbox_cancel_idx
ON agent_memory_consolidation_outbox(created_at,event_id)
WHERE status='cancelled' AND cancel_pending;
