# Governed runtime integration with current main

This integration keeps PR #18's governed extraction/review, typed verification,
MemoryHost, published-point history, persona and bounded read-only reflection.
It merges the shared transaction-local proof/readset, source omission audit and
finite maintained relation-view changes from main. The integration starts from
PR head `244bec8d497a2d75af5b2a2cda7b9340c96127fa` and main
`efb735ff34027837469bd0871b697c1c4fc2f064`. Historical validation records describe
their original trees; they are not evidence for this combined tree.

## Two explicit host entry paths

- `MemoryHost.submit` durably captures a source, runs the configured governed
  model or rule proposal/review pipeline, and discovers pending ordinary or
  contextual candidates for registered domain verification. A source audit may
  recover additional proposals; its findings never constitute domain evidence.
- `MemoryHost.submit_project` accepts drafts reviewed by trusted host code, with
  explicit project membership, source authority, temporal bounds and typed field
  spans. Native authoritative project verification qualifies those fields before
  maintained questions and relation views can use them. A model cannot choose
  project membership or create permission/qualification records.

`test_runtime_host_workflow_v71.py` demonstrates proposal/review followed by an
explicit trusted host handoff, durable project staging, restart, native domain
verification, shared multi-question publication, a maintained dependency/date
risk, history capture, and current grant revocation. The handoff is a real
required host input in this contract. This test does not claim automatic raw-text
routing into project views. Its authored proposals prove orchestration and safety,
not model effectiveness or business accuracy.

## Combined final guards

Model processing proofs and omission-audit authorization are composed in one
publication transaction. Every retained processing source remains an erasure
parent. Model validators return local fences bound to the checked authority,
source-grant deadlines, service registration floor, model configuration and adapter
sources. After all awaited model/audit work, those fences and the complete input
fingerprint are checked synchronously. A recovered audited slot keeps its pending
hold through later ordinary model regeneration until actual host resolution.

Historical publication points retain their recorded business answers while
requiring current source permission, authority, host registration and context.
Question/page delivery and capture recheck these local controls after their final
awaited reads. Shared batch publication retains durable renewed-lease and valid
window guards; failure rolls back every head and history point in the batch.
Durable extraction and verification likewise fence the final completion write
and composed guards, rather than checking time only before the last await.

These are guarded-transaction boundaries. They do not promise that permission
remains valid after the transaction releases its locks or until network bytes
arrive at a remote reader.

## Bounded progress and packaging

The [verification repair](verification-runtime-repair.md) explains exact-scope
indexed pending pagination, restart-safe discovery, active-only queue capacity,
explicit terminal-receipt retention, bounded-page erasure, isolated stages and
body-free metrics. Terminal history cannot permanently block new verification.

The [package gate](../../PACKAGE_VALIDATION.md) derives a single seven-package
inventory from project metadata. The same gate builds every sdist/wheel, installs
outside the checkout, checks imports/entry points/migration bytes, exercises SQLite
write/recall and scans every archive. Ubuntu, macOS and Windows CI each produce
platform-specific evidence. Workflow presence and Linux missing-module simulation
are not substitutes for actual Windows/macOS execution. Unix RSS is optional;
unsupported measurement raises rather than producing a fabricated zero. Model
weights and inference dependencies remain explicit opt-ins.

## Verification and acceptance boundaries

New combined regressions complement the existing suites. They cover audit-held
regeneration and host resolution; final model/audit configuration or time changes;
renewed and expired worker leases; current versus original generation checks;
new relation premises, unknown/negative coverage, source withdrawal and expiry;
question/page history with current grants; scope isolation; restart; and erasure
from both live storage and an older restored backup.

Local PostgreSQL tests require a disposable DSN and report skips when it is absent.
The exact published commit's hosted `postgres-live` job remains the full dual-
backend and actual pre-V7 upgrade/rollback authority. Platform package results are
scoped to their actual runner and smoke contract. Real local-model inference is a
separate explicitly supplied-artifact test, not part of lightweight installation.

The existing real-acceptance wrapper now reuses the A9 whole-workflow evaluator
with a frozen optimized baseline, isolated feature arms and a combined arm.
[Real acceptance preflight](real-acceptance-preflight.md) records exact missing
endpoint, licensed corpus, independent gold, host binding, calibration and full-
cost inputs. Historical nine-group/seven-generation Ollama smoke remains genuine
synthetic-input inference evidence, not a business comparison or integrated rerun.
No model was downloaded or invoked for these repairs. Optional production audit
activation remains off without its existing host-approved real-evidence gate.
No historical task status or real-quality acceptance gate is marked complete.
