CREATE TABLE IF NOT EXISTS agent_memory_schema (
    schema_version integer PRIMARY KEY,
    installed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_memory_events (
    id text PRIMARY KEY,
    partition_key text NOT NULL,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    event_type text NOT NULL,
    content text NOT NULL,
    metadata_json jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL,
    idempotency_key text,
    actor text NOT NULL,
    source_uri text,
    sensitivity text NOT NULL,
    retention_class text NOT NULL,
    schema_version integer NOT NULL,
    content_hash text NOT NULL,
    archived_at timestamptz,
    search_document tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', coalesce(event_type, '') || ' ' || coalesce(content, ''))
    ) STORED
);

CREATE UNIQUE INDEX IF NOT EXISTS agent_memory_events_idempotency_idx
ON agent_memory_events(partition_key, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS agent_memory_events_scope_idx
ON agent_memory_events(tenant_id, namespace, user_id, agent_id, workspace_id, session_id);

CREATE INDEX IF NOT EXISTS agent_memory_events_search_idx
ON agent_memory_events USING gin(search_document);

CREATE TABLE IF NOT EXISTS agent_memory_claims (
    id text PRIMARY KEY,
    partition_key text NOT NULL,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    claim_key text NOT NULL,
    value_json jsonb NOT NULL,
    text text NOT NULL,
    confidence double precision NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    importance double precision NOT NULL CHECK (importance BETWEEN 0 AND 1),
    status text NOT NULL,
    provenance_json jsonb NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    created_at timestamptz NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    supersedes text,
    superseded_by text,
    archived_at timestamptz,
    search_document tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple',
            coalesce(claim_key, '') || ' ' || coalesce(text, '') || ' ' || coalesce(value_json::text, '')
        )
    ) STORED
);

CREATE UNIQUE INDEX IF NOT EXISTS agent_memory_claims_one_active_idx
ON agent_memory_claims(partition_key, claim_key)
WHERE status = 'active' AND archived_at IS NULL;

CREATE INDEX IF NOT EXISTS agent_memory_claims_scope_idx
ON agent_memory_claims(tenant_id, namespace, user_id, agent_id, workspace_id, session_id, status);

CREATE INDEX IF NOT EXISTS agent_memory_claims_search_idx
ON agent_memory_claims USING gin(search_document);

CREATE TABLE IF NOT EXISTS agent_memory_state_deltas (
    id text PRIMARY KEY,
    partition_key text NOT NULL,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    claim_key text NOT NULL,
    operation text NOT NULL,
    source_event_id text NOT NULL REFERENCES agent_memory_events(id) ON DELETE CASCADE,
    current_claim_id text NOT NULL REFERENCES agent_memory_claims(id) ON DELETE CASCADE,
    previous_claim_id text,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_memory_artifacts (
    id text PRIMARY KEY,
    partition_key text NOT NULL,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    kind text NOT NULL,
    text text NOT NULL,
    payload_json jsonb NOT NULL,
    status text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    quality double precision NOT NULL CHECK (quality BETWEEN 0 AND 1),
    provenance_json jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    archived_at timestamptz,
    search_document tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', coalesce(kind, '') || ' ' || coalesce(text, ''))
    ) STORED
);

CREATE INDEX IF NOT EXISTS agent_memory_artifacts_scope_idx
ON agent_memory_artifacts(tenant_id, namespace, user_id, agent_id, workspace_id, session_id, kind);

CREATE INDEX IF NOT EXISTS agent_memory_artifacts_search_idx
ON agent_memory_artifacts USING gin(search_document);

CREATE TABLE IF NOT EXISTS agent_memory_evolution_records (
    id text PRIMARY KEY,
    partition_key text NOT NULL,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    record_type text NOT NULL,
    payload_json jsonb NOT NULL,
    occurred_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS agent_memory_evolution_scope_idx
ON agent_memory_evolution_records(
    tenant_id, namespace, user_id, agent_id, workspace_id, session_id, record_type
);

CREATE TABLE IF NOT EXISTS agent_memory_proposals (
    id text PRIMARY KEY,
    partition_key text NOT NULL,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    payload_json jsonb NOT NULL,
    status text NOT NULL,
    claim_id text,
    state_delta_id text,
    superseded_claim_id text,
    reason text,
    created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS agent_memory_proposals_scope_idx
ON agent_memory_proposals(tenant_id, namespace, user_id, agent_id, workspace_id, session_id);

CREATE TABLE IF NOT EXISTS agent_memory_consolidation_jobs (
    id text PRIMARY KEY,
    job_key text NOT NULL UNIQUE,
    partition_key text NOT NULL,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    user_id text,
    agent_id text,
    workspace_id text,
    session_id text,
    job_type text NOT NULL,
    payload_json jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    leased_by text,
    lease_expires_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS agent_memory_jobs_claim_idx
ON agent_memory_consolidation_jobs(status, next_attempt_at, lease_expires_at, created_at);

INSERT INTO agent_memory_schema(schema_version)
VALUES (1)
ON CONFLICT (schema_version) DO NOTHING;
