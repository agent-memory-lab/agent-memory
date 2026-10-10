-- Additive selectors over existing authoritative rows; no out-of-band backfill.
CREATE INDEX IF NOT EXISTS agent_memory_admission_verification_pending_idx
ON agent_memory_admission_records(partition_key, record_id)
WHERE NOT payload_json @> '{"deleted": true}'::jsonb
AND payload_json ->> 'action' IN ('PENDING_VERIFICATION', 'CONTESTED')
AND COALESCE(payload_json -> 'qualification', 'null'::jsonb) IN ('null'::jsonb, '{}'::jsonb)
AND COALESCE(payload_json #> '{project_candidate,review}', 'null'::jsonb)
    IN ('null'::jsonb, '{}'::jsonb);

CREATE INDEX IF NOT EXISTS agent_memory_derived_verification_active_idx
ON agent_memory_derived_entries(partition_key, kind, identity)
WHERE kind = 'domain_verification_task'
AND payload_json ->> 'state' IN ('pending', 'retry', 'running');
