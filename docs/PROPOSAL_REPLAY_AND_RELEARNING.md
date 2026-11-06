# Proposal replay identity and independent relearning

Native SQLite and PostgreSQL providers bind proposal replay to the original request.
A proposal ID is still globally unique. Reusing an occupied ID with a different
owner scope or payload raises `ValueError` without returning the stored status,
claim/delta IDs, or private conflict reason. It does not replace the original row.
Use a new ID for a different request.

## Identity and compatibility

- All six fields of the original `MemoryScope` must match exactly, even if the
  resulting Claim is projected to a broader user, workspace, or tenant scope.
- Identity includes key, value, text, source IDs (including order), expected version,
  scope level, confidence, importance, actor, operation, valid time, and reason.
  JSON object key order does not matter. `valid_from` is compared as a UTC instant.
- `created_at` is audit metadata and is not part of replay identity. Reconstructing
  the same request later can still return the original accepted, rejected, or
  conflict result. Its original creation time remains stored.
- Existing SQLite and PostgreSQL rows require no migration. Both stored datetime
  spellings are understood. Legacy rows missing `operation`, `valid_from`, or
  `reason` use the original dataclass defaults (`upsert`, `None`, `None`). Missing
  required identity fields fail closed.
- An erased minimal payload such as `{"erased": true}` rejects all replay,
  including same-owner replay. It cannot authorize a new request under the old ID.
- `MemoryProvider`, MCP/SDK arguments, wire protocol `0.1`, domain schema `2`, and
  the legacy `MemoryUnitOfWork.find_proposal_result(id)` signature are unchanged.
  A new optional `ProposalReplayUnitOfWork.find_proposal_result_for(proposal)`
  extension performs owner/payload checks within the active write transaction.
  The old ID-only method is a low-level compatibility lookup, not authorization
  for a public replay response.
- Third-party UoWs without the optional extension still accept first-time proposal
  IDs. If their ID-only lookup returns an existing result, the kernel raises
  `ValueError("provider cannot validate proposal replay identity")` rather than
  returning an unverifiable result. Implement the extension to restore safe replay.
  It must serialize competing uses of the same global ID through commit; the
  native PostgreSQL adapter adds an advisory proposal-ID lock after its existing
  namespace deletion fence. SQLite uses its existing immediate write transaction.

These checks defend core/provider integrations that accept caller-chosen proposal
IDs. Standard MCP and SDK proposal APIs still generate their IDs internally; this
change does not establish or replace a host authentication boundary.

## Independent relearning after forgetting

An accepted proposal has a synthetic audit event in its Claim provenance. When an
original source is archived or erased, that audit event can remain in storage but
is protected by the source-dependency rules. New same-value evidence must not be
corroborated into that invisible Claim.

Native corroboration checks now apply the same protected-source and legacy-Claim
eligibility rules as current-state projection, inside the write transaction. The
check includes the original evidence owners because a broader projected Claim can
be backed by session-scoped events. If the previous Claim is not effective, new
independent evidence creates a new Claim/state delta with clean provenance. It does
not unprotect the old sources or restore the forgotten evidence. Healthy same-value
corroboration still retains its existing Claim ID and adds no state delta.

An explicit `merge` targeting a non-effective Claim returns a conflict rather than
accepting invisible new evidence. To learn from independent evidence, use ordinary
ingestion or submit an `upsert` with the current optimistic version. Existing
bitemporal observations and source-erasure rules remain in place; the fix changes
eligibility for a new write, not historical rows merely because they were read.
