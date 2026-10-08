# V7-B0 lifecycle and rollout contract

Baseline: GitHub main `7f824dd49b18935c6b47f5eb32aa32efdbd8cf01`.
B0 adds pure protocol, domain-oracle, and evaluation contracts only. It creates no
persistent V7 records, runs no migration, and advertises no V7 runtime capability.
The package stays 0.1.0. The frozen design and the 44 AM61 task statuses are unchanged.

## Storage inventory required before B1 writes

The following logical records belong in the existing derived UoW and exact scope
partition. Names below reserve lifecycle responsibilities, not a new writable API.

| Logical record | Sensitive data / dependencies | Delete and recovery requirement |
| --- | --- | --- |
| Question definition / instance | Canonical parameters, subject/project binding, audience, trusted context | Disable affected identities and scrub identifying parameters; preserve only opaque tombstone and epoch. Reinstallation requires current authority and a new generation. |
| Content | Actual rendered structure and immutable generation manifest | Erase every affected revision, including unreferenced historical versions and unquoted processing inputs. Never replace generation provenance with later validation evidence. |
| Certificate / head | Source/parent/query frontiers, support, time/policy/security coordinates | Remove sensitive manifests, invalidate heads atomically, retain non-content tombstones. Certificates cannot reopen erased content. |
| Query subscription / index | Predicate/project/entity/member keys, negative-result dependencies | Remove sensitive keys and reverse mappings. Increment the always-maintained scope/query barrier before cleanup. |
| Demand / execution / receipt | Fixed target, definition/context partition, lease and successor | Preserve bounded non-content responsibility where safe; scrub private parameters; fence obsolete work. Exact v6 receipts keep their original unit semantics. |
| Due index / hotness / cost statistics | Timing, usage, tenant activity, estimates | Delete sensitive activity statistics with scope/object erasure; retain only separately authorized aggregate accounting. No identifiers copied to telemetry labels. |
| Exact answer cache | Original input/processing lineage, content, provider configuration | Erase transitive cache bodies and sensitive keys. Cache hits still check current input/dispatch/delivery guards. |

Every new persisted kind must be registered in **both** SQLite and PostgreSQL
live erasure and `PurgeRestore` replay before its writer is enabled. The current
v6 `forget` implementations enumerate kinds explicitly; adding a new ledger kind
without extending those enumerations and shared erasure planning is unsafe.

## Upgrade order and rollback gate

1. Deploy reader/version rejection and a default-off capability gate.
2. Install the scope/query barriers and all old/new membership write hooks,
   including source revisions, admission/retraction, permission changes, deletes,
   reprocessing, and restore paths. Barriers are authoritative before indexes.
3. Install versioned subscription indexes. Mark backfill incomplete and use
   conservative scope invalidation until the atomic cutover verifies coverage.
4. Backfill and validate under the same UoW ordering as writes. A racing write
   must be reflected in the new index or invalidate the publication proof.
5. Register new kinds in erasure, recovery, retention, and bounded GC. Run
   cross-connection races and offline backup replay on both real backends.
6. Only then enable the narrowly verified capability combination. A missing
   migration, incomplete index, unknown schema, or unsupported processor returns
   a stable unavailable result; it cannot fall back to serving an old answer.

Rollback first disables new reads/writes/claims, fences or auditably hands off
active leases, then drains or seals finite responsibilities. It does not remove
current deletion barriers or reinterpret new coverage receipts as old exact ones.
Old binaries cannot claim new task types. Restore keeps the database offline
until the independently pinned current deletion journal has replayed successfully.
A content backup is never itself authoritative about deletions since that backup.

B0 rollback is removal of the additive modules and documentation: there is no
schema transition and no runtime data to roll back. Later batches must replace
this no-write statement with their exact migration and irreversible boundaries.

## Acceptance responsibility

Q7-04 and Q7-30 remain unrun in B0. A lifecycle inventory is not a tested migration.
Q7-29 additionally requires real independent-process SIGKILL recovery; an in-memory
exception is insufficient. Real PostgreSQL and provider/model-data configuration
must be recorded separately from deterministic SQLite or synthetic unit evidence.
