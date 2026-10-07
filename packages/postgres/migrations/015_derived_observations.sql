CREATE TABLE IF NOT EXISTS agent_memory_derived_entries (
 partition_key TEXT NOT NULL, kind TEXT NOT NULL, identity TEXT NOT NULL,
 payload_json JSONB NOT NULL, PRIMARY KEY(partition_key,kind,identity)
);
CREATE TABLE IF NOT EXISTS agent_memory_derived_dependencies (
 partition_key TEXT NOT NULL, revision_id TEXT NOT NULL, parent_id TEXT NOT NULL,
 edge_kind TEXT NOT NULL CHECK(edge_kind IN ('support','processing','query')),
 PRIMARY KEY(partition_key,revision_id,parent_id,edge_kind)
);
CREATE INDEX IF NOT EXISTS agent_memory_derived_reverse_idx
 ON agent_memory_derived_dependencies(partition_key,parent_id);
CREATE TABLE IF NOT EXISTS agent_memory_derived_atom_headers (
 partition_key TEXT NOT NULL, identity TEXT NOT NULL, slot_key TEXT NOT NULL,
 payload_json JSONB NOT NULL, PRIMARY KEY(partition_key,identity)
);
CREATE INDEX IF NOT EXISTS agent_memory_derived_atom_slot_idx
 ON agent_memory_derived_atom_headers(partition_key,slot_key);
