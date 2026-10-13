-- Durable backoff isolates failed scheduler handoffs without discarding them.
-- Existing pending rows (NULL) remain immediately eligible after upgrade.
ALTER TABLE agent_memory_consolidation_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS agent_memory_consolidation_outbox_retry_idx
    ON agent_memory_consolidation_outbox(next_attempt_at, created_at, event_id)
    WHERE status='pending';
