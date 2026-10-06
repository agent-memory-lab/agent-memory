-- Additive durable-receive ledger. Processing requests are transactional outbox
-- records, not claims or worker-completion receipts.
CREATE TABLE IF NOT EXISTS agent_memory_retention_epochs (
    partition_key TEXT PRIMARY KEY, epoch BIGINT NOT NULL CHECK(epoch >= 0)
);
CREATE TABLE IF NOT EXISTS agent_memory_retention_entries (
    partition_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('ticket', 'request')),
    request_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    idempotency_key TEXT,
    status TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    PRIMARY KEY(partition_key, kind, request_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS agent_memory_retention_ticket_event_idx
ON agent_memory_retention_entries(partition_key, event_id) WHERE kind = 'ticket';
CREATE UNIQUE INDEX IF NOT EXISTS agent_memory_retention_ticket_key_idx
ON agent_memory_retention_entries(partition_key, idempotency_key) WHERE kind = 'ticket';
CREATE INDEX IF NOT EXISTS agent_memory_retention_pending_idx
ON agent_memory_retention_entries(partition_key, kind, status);
