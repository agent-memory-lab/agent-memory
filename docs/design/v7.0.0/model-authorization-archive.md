# B5 host-acknowledged model authorization archival

The live `model_authorization` collection still has a hard 4,096-record cap per
scope. Every dispatch and every successful delivery, including a cache hit, has
its own authorization. Age, cache expiry, financial settlement, and Question GC
never silently delete that evidence. A host running a long-lived service can now
reuse audit capacity through an explicit **export, durable archive, acknowledge**
lifecycle. No external service is contacted by the library.

This is a finite audit-retention extension, not unlimited local storage, financial
ledger rotation, or a new claim about model quality/cost savings.

## Contract and host integration

```python
from agent_memory.operations.model_authorization_archive import (
    ModelAuthorizationArchive,
    ModelAuthorizationArchivePolicy,
    verify_model_authorization_archive,
)

archive = ModelAuthorizationArchive(model_authority)
policy = ModelAuthorizationArchivePolicy(
    policy_id="host:model-audit/1",
    archive_id="my-owned-erase-aware-audit-store",
    minimum_age_seconds=86400,
    retain_recent=256,
    max_batch=256,
)
batch = await archive.export(
    policy, expected_previous_checkpoint=independently_pinned_archive_chain_head,
)

# HOST IMPLEMENTATION, outside the library transaction:
# 1. Store the ENTIRE returned envelope durably, including checkpoint, records,
#    edges, policy binding and the deletion-journal position.
# 2. Pin batch["checkpoint"] independently of the live database/old backups.
# 3. Verify the stored artifact with verify_model_authorization_archive(
#        stored_batch, expected_checkpoint=independently_pinned_batch_checkpoint
#    ) and obtain the store's durable-write receipt SHA-256.
# 4. Confirm the archive enforces current erasure before reads and restores.

result = await archive.acknowledge(
    policy,
    expected_checkpoint=independently_pinned_batch_checkpoint,
    archive_receipt_sha256=durable_archive_receipt_sha256,
)
# Independently persist result["checkpoint"] as the current archive-chain head.
```

The policy is a trusted host decision, after resolving legal/retention obligations
and archive access controls. `archive_id` names the host-owned destination; its
policy fingerprint binds that destination and all retention limits to the batch.
There is no SDK/MCP/model argument route to this API. The host must not fabricate
a receipt or acknowledge an in-memory/uncommitted copy. The library verifies the
checkpoint/receipt **binding**, not a remote system's durability or legal policy.

Defaults keep the newest 256 local authorizations and require 24 hours of age.
`minimum_age_seconds=0` and `retain_recent=0` are supported explicit host choices,
not automatic defaults. An archive batch contains at most 256 selected audit rows
by default, configurable to 4,096. Audit rows erased before export are exported as
minimal tombstones; their absent pre-erasure contents cannot be reconstructed.
Pending first-delivery reservations are explicitly validated and excluded from
archival selection and completed-stage counts, even after lease expiry. Their
separate token-fenced lifecycle owns cleanup. Export does not remove any row or
create provider/budget activity.

Hosts should rotate well before the cap and retain headroom for concurrent
requests/waiters. A cold dispatch reserves one token-fenced first-delivery slot
in the same scope transaction and under the same 4,096-row cap as its dispatch
record. At 4,095 occupied rows it fails before provider dispatch, and unrelated
cache hits cannot consume its held slot. The first authorized caller/waiter for
that exact call consumes the reservation; additional deliveries need their own
slots and may still fail closed. Revocation, lease expiry, invalid serialization
or other authority failures can still prevent delivery while provider cost is
due. Policy age/holds or an unavailable archive can legitimately prevent freeing
capacity. Do not retry provider calls as a substitute for completing archival.

## Exactly what is preserved

An export includes:

- Exact selected authorization records, including delivery IDs, dispatch/delivery
  payload commitments, call IDs, timestamps and source/derived lineage.
- Their complete reachable **derived-ledger** records and typed dependency edges,
  using the same nested string/key, shared identity and `derived:` reference
  interpretation as Question GC. This can include model cache text/metadata,
  immutable QuestionView certificates/content and historical generation proofs.
- Scope, retention epoch, deletion-journal cursor, current host authority digest,
  policy digest, export time and preceding local archive-chain checkpoint.

This is full retained **authorization evidence plus its reachable derived proof**.
It does not fetch or archive L0 source-event bodies, atom/admission rows, or the
financial ledger. Those have separate retention/restore contracts. No missing
raw source can be reconstructed from an authorization digest. An archived row may
refer to a source whose body is independently unavailable or subsequently erased.

The checkpoint is SHA-256 of a canonical compact descriptor: every envelope
field except `records`, `edges`, and `checkpoint`. This descriptor includes exact
selected authorization IDs/row hashes, stage totals and `evidence_sha256`, which
is SHA-256 of the canonical `{records, edges}` evidence object. Acknowledgment
recomputes the descriptor digest against the external pin before trusting the
deletion selection. `verify_model_authorization_archive` verifies both levels
against that pin. The entire envelope must be retained, not just those hashes.
After acknowledgment, one local `model_authorization_archive/1` checkpoint holds
cumulative counts by stage, archived record count, sequence, previous chain head,
last batch checkpoint, policy fingerprint and storage-receipt digest. It contains
no model answer, source ID, prompt, individual call ID or original proof body.
**A local hash/count summary alone cannot recover, inspect or independently prove
an old individual delivery.** The host archive supplies the full historical record.

Archival deletes only the selected local authorization rows; it never deletes
reachable derived proofs itself. Once no other local root refers to a proof,
explicit Question GC may collect it because the archive already contains its
historical copy. Local finite coverage receipts remain permanent roots. Monetary
calls, settlement/invoice receipts and unknown reservations are never modified,
released or archived by this API, and their existing finite caps remain.

## Bounds and atomicity

The census defaults to 32,768 rows, 131,072 stored/computed reference edges and
64 MiB. Hard maxima are 65,536 rows, 262,144 edges and 256 MiB. Both backend census
bytes and the final canonical envelope have byte limits. Unknown kinds, unknown
V7 schemas, malformed audit rows and incomplete/over-budget censuses fail before
publishing a pending batch. The library never exports a partial proof closure.

Every export requires the independently pinned prior chain checkpoint; a stale
restored chain is rejected before creating a new batch. On first setup, the host
may initialize that pin from `status()` only on its authoritative live database,
never by adopting an unverified restored backup as the new authority.

At most one pending export descriptor and one chain checkpoint exist per scope.
The pending descriptor stores opaque authorization IDs/hashes and checkpoint
bindings, not copies of model/source content. A second export is rejected until
the pending one is acknowledged or explicitly cancelled.

Acknowledgment runs in one scope-locked transaction. It checks current authority,
retention epoch, selective-deletion journal cursor, policy, previous chain head,
external checkpoint and the exact hash of every selected local audit row. It then
updates the checkpoint, deletes exactly those audit rows and removes the pending
descriptor atomically. New/concurrent authorization records are untouched.
Failures, stale evidence or a mid-delete exception roll back the entire operation.
The latest identical acknowledgment is idempotent; old checkpoints or a different
receipt/policy cannot advance the chain again.

## Crashes, cancellation, erasure and restore

`await archive.status()` returns the current metadata-only chain checkpoint,
pending checkpoint/count and bounded capacity telemetry: used/available slots,
consumed dispatch/delivery counts, reserved/erased counts and whether two slots
are currently free for a first dispatch/delivery. This inspection never expires
reservations or deletes evidence. Hosts can use it to rotate ahead of backpressure. After a host crash, locate the durable external batch
by this checkpoint and acknowledge it only after verifying the stored envelope
(using `verify_model_authorization_archive`), storage receipt and previously
pinned chain head. If no durable batch exists, call
`await archive.cancel(expected_checkpoint=...)`, which discards only the pending
descriptor. All original local audit rows remain. A stale or wrong cancellation
checkpoint fails. There is no time-based cancellation that guesses whether a
remote write succeeded.

Any intervening selective erase or whole-scope erase makes a pending export stale.
An old external batch must be invalidated/scrubbed under the host archive's current
deletion journal. Acknowledgment cannot make that stale export usable. The host
must apply source/derived erasure transitively to all exported records and purge
in-flight/orphaned archive copies too, before any historical read or restore.
The library has no access to external files/services and cannot perform that purge.

Whole-scope erase revokes the current host authority. Archive checkpoint history
and the content-free pending descriptor remain, so erasure cannot silently reset
accounting of archived authorizations. To resume maintenance, explicitly establish
a current authority in the new epoch, cancel any stale pending descriptor, and
export remaining erased tombstones under the chosen policy. This never resurrects
sources, proofs, cache bodies, processing grants or old authorizations. Checkpoint
history is continued rather than silently reset.

Restore remains an offline host procedure. Replay the independently current
deletion journal before opening either database or archive; preserve/reconcile
the host's independently pinned archive-chain head and current authority floor.
A restored old database/checkpoint alone cannot prove which batches were already
archived or erased. Do not acknowledge from an old backup as a new writable audit
authority. This extension does not provide archive replay/merge or disaster-recovery
orchestration; if chain continuity cannot be established, keep maintenance closed.
The money-ledger replay/pin remains a separate requirement.

## Compatibility and verification

No SQL table, completed authorization, answer or financial-receipt format changes.
The audit kind additionally supports transient `model-delivery-reservation/1` rows
with `consumed=False`; completed delivery replaces that row with the existing
authorization shape. Host audit consumers must require `consumed is True` before
counting a dispatch/delivery as completed; `stage="delivery"` alone also matches
a pending reservation and is not proof that an answer was delivered. Drain
in-flight model work before downgrading to readers that do not understand the
reservation schema. Its separate token-fenced delete operation can remove only
unused reservations, never completed evidence. Updated governed dispatch requires
that backend primitive and fails closed on older adapters.

The providers also expose the optional `model-authorization-archive/1` contract and
a narrow audit/pending-record deletion primitive. Old backends fail explicitly when
this API is requested. Existing applications retain their original audit behavior
unless their host opts into the archival lifecycle; first-delivery capacity
reservation is an independent dispatch safety fix. The two new metadata kinds are
recognized non-collectible roots by Question GC. Older collectors encountering them
fail closed as unsupported kinds; downgrade does not authorize removing them.

The synthetic SQLite and PostgreSQL suite is
`tests/test_model_authorization_archive_v7.py`. It covers 4,096-row capacity
recovery, repeated cache hits, multiple calls, bounded proof closure, receipt and
policy mismatches, concurrent/idempotent acknowledgment, intervening erasure,
restart metadata recovery, real SQLite-backup/PostgreSQL-`pg_dump` rollback pins,
real QuestionView generation proof, retention ages, census bounds, checkpoint
selection/evidence tampering and atomic rollback.
All provider responses and archive-storage receipts in these tests are synthetic;
no paid model or production database is involved.
