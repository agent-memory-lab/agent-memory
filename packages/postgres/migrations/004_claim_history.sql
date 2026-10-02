-- Claim transaction-time snapshots are additive; protocol/event schema stays v2.
CREATE TABLE IF NOT EXISTS agent_memory_claim_observations (
    claim_id text PRIMARY KEY REFERENCES agent_memory_claims(id) ON DELETE CASCADE,
    partition_key text NOT NULL, claim_key text NOT NULL,
    observed_at timestamptz NOT NULL, payload_json jsonb NOT NULL,
    retracted boolean NOT NULL DEFAULT false,
    legacy boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS agent_memory_claim_observations_key_idx
ON agent_memory_claim_observations(partition_key, claim_key);
CREATE TABLE IF NOT EXISTS agent_memory_claim_versions (
    revision_id text PRIMARY KEY,
    claim_id text NOT NULL REFERENCES agent_memory_claims(id) ON DELETE CASCADE,
    partition_key text NOT NULL, claim_key text NOT NULL,
    valid_from timestamptz NOT NULL, valid_to timestamptz,
    system_from timestamptz NOT NULL, system_to timestamptz,
    payload_json jsonb NOT NULL,
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (system_to IS NULL OR system_to > system_from)
);
CREATE INDEX IF NOT EXISTS agent_memory_claim_versions_lookup_idx
ON agent_memory_claim_versions(partition_key, system_from, system_to, valid_from);
