-- Coordinated writer upgrade: stop old writers before applying this schema.
-- Exact-scope backfill/cutover occurs under the existing namespace writer lock.
CREATE TABLE IF NOT EXISTS agent_memory_derived_project_routes (
 partition_key TEXT NOT NULL, route_key TEXT NOT NULL, candidate_id TEXT NOT NULL,
 PRIMARY KEY(partition_key,route_key,candidate_id)
);
CREATE INDEX IF NOT EXISTS agent_memory_derived_project_candidate_idx
 ON agent_memory_derived_project_routes(partition_key,candidate_id);
