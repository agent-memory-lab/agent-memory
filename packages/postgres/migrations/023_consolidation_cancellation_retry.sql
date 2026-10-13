-- A failed remote cancellation must not starve other sources or tenants.
-- Existing markers remain immediately eligible; source content is never copied.
ALTER TABLE agent_memory_consolidation_outbox
    ADD COLUMN IF NOT EXISTS cancel_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_memory_consolidation_outbox
    ADD COLUMN IF NOT EXISTS cancel_next_attempt_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS agent_memory_consolidation_outbox_cancel_retry_idx
    ON agent_memory_consolidation_outbox(cancel_next_attempt_at, created_at, event_id)
    WHERE status='cancelled' AND cancel_pending;
