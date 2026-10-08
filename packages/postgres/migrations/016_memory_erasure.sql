-- Identity-only fences distinguish erased local memory from external provider IDs.
CREATE TABLE IF NOT EXISTS agent_memory_memory_tombstones (
    partition_key text NOT NULL,
    id text NOT NULL,
    memory_table text NOT NULL CHECK (memory_table IN ('events', 'claims', 'artifacts')),
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    PRIMARY KEY (partition_key, memory_table, id)
);
CREATE INDEX IF NOT EXISTS agent_memory_memory_tombstones_scope_idx
ON agent_memory_memory_tombstones(tenant_id, namespace, id);
