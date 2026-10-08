-- Opt-in durable-coalescing/1. All lifecycle writers must upgrade together.
-- Resource units only: no monetary/model-spend budget is provided here.
CREATE TABLE IF NOT EXISTS agent_memory_refresh_schedule_contract (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1),
 limits_json TEXT, turn BIGINT NOT NULL DEFAULT 0, observed_clock TIMESTAMPTZ
);
ALTER TABLE agent_memory_refresh_schedule_contract
 ADD COLUMN IF NOT EXISTS observed_clock TIMESTAMPTZ;
INSERT INTO agent_memory_refresh_schedule_contract(singleton,version) VALUES (1,1)
 ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS agent_memory_refresh_schedule_due (
 partition_key TEXT NOT NULL, identity TEXT NOT NULL, scope_json JSONB NOT NULL,
 tenant_id TEXT NOT NULL, instance_key TEXT NOT NULL, adapter_key TEXT NOT NULL,
 status TEXT NOT NULL, due_at TIMESTAMPTZ, lease_until TIMESTAMPTZ, priority INTEGER NOT NULL,
 aging_seconds INTEGER NOT NULL, created_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(partition_key,identity)
);
CREATE INDEX IF NOT EXISTS agent_memory_refresh_schedule_due_idx
 ON agent_memory_refresh_schedule_due(adapter_key,due_at,tenant_id) WHERE due_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS agent_memory_refresh_schedule_expired_idx
 ON agent_memory_refresh_schedule_due(adapter_key,lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX IF NOT EXISTS agent_memory_refresh_schedule_usage_idx
 ON agent_memory_refresh_schedule_due(status,tenant_id,instance_key);
CREATE TABLE IF NOT EXISTS agent_memory_refresh_schedule_fairness (
 tenant_id TEXT PRIMARY KEY, last_turn BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS agent_memory_refresh_schedule_reservations (
 partition_key TEXT NOT NULL, execution_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
 instance_key TEXT NOT NULL, units INTEGER NOT NULL CHECK(units > 0),
 PRIMARY KEY(partition_key,execution_id)
);
CREATE INDEX IF NOT EXISTS agent_memory_refresh_schedule_reservation_usage_idx
 ON agent_memory_refresh_schedule_reservations(tenant_id,instance_key);
