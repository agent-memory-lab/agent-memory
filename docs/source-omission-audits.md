# B2: bounded source-level omission audits

## Delivered scope

`AtomReviewer` checks the faithfulness of proposed candidates. It cannot find a
fact the generator never proposed; an empty generation normally makes no reviewer
call. The default-off `SourceOmissionAudit` adds a separate source-first pass over
host-selected questions, exact source ranges and registered target predicates. It
runs even when generation returned zero candidates. The baseline generator,
candidate reviewer, `remember_atoms()` and trusted typed-source paths remain.

The contract and local host adapter are in `consolidation/source_audit.py`:

- `SourceAuditTarget`: question identity/text, target predicates, exact Python
  character offsets, and a `changed`, `unaudited`, or `high_value` trigger.
- `SourceAuditFence`: host-authenticated source version, authorization revision,
  erasure epoch, valid-at/known-at axes and expiry. Neither source text nor a model
  can issue this grant.
- `SourceAuditHost`: bounded target selection and live source authorization.
- `RetainedSourceAuditHost`: adds real repository revision, erasure and known-at
  checks to the application's authorization callback, reusing an enclosing UoW.
- `SourceAuditor`: receives selected text ranges, time axes, source version and
  bounded relevant candidate context. It returns one observation per requested
  target/predicate pair, including on empty generation.

No model transport, task scheduler, global cache, additional table or migration is
introduced. This implementation does not depend on the separately authored
PR #18 governed-extraction work. It does not claim that work's paid-dispatch or
resource-governance capabilities.

## Enable from the trusted host

```python
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.source_audit import (
    RetainedSourceAuditHost,
    SourceAuditTarget,
    SourceOmissionAudit,
)

# application_host implements select_targets(event) and
# authorize_source(event, authority, *, unit_of_work=None).
# Authorization must authenticate the exact event, audience and source version,
# deny erased/superseded/inaccessible sources, and return a SourceAuditFence.
# It must not build an authorization grant from conversation/model metadata.
audit_host = RetainedSourceAuditHost(repository, application_host)
source_audit = SourceOmissionAudit(
    local_source_auditor, audit_host, local_only=True,
    enabled=True, contract_test_only=True,  # Disposable contract tests only.
)
pipeline = AtomExtractionPipeline(
    generator, candidate_reviewer, source_audit=source_audit,
)
```

Use `RetainedSourceAuditHost` with the existing durable receive/worker path.
`local_only=True` is an explicit application assertion, not network isolation.
A host may implement `SourceAuditHost` directly for newly captured sources; then
it owns the live source-presence/revision/erasure/authorization checks. There is no
permissive default host. Access to read a source and `SourceAuthority` permission
to publish a predicate remain different checks.

The stage itself defaults to `enabled=False`; attaching a disabled stage cannot
silently dispatch it. The example explicitly enables **contract-test-only** mode,
which is marked in every receipt and cannot carry production approval. It is not
a production recommendation.

Controlled-real activation instead requires a `SourceAuditApproval` and a trusted
synchronous `verify_approval` callback. The approval binds the stage's exact
`configuration_sha256` (adapter/model/prompt versions and all audit bounds),
frozen measured evidence and evaluation-protocol SHA-256 digests, a rollback ID,
and `controlled-real` qualification. A disabled instance exposes the configuration
digest for preparing the independently reviewed approval. The host verifier must
authenticate the actual report, verify that its scoped quality/cost thresholds and
baseline comparators apply, and honor rollback/revocation. Merely constructing an
approval DTO or returning model confidence is not evidence. Source access grants
remain separate from this quality gate.

Approval, verifier identity, adapter identity, configuration and activation mode
are pinned and checked before dispatch, after awaits, on reuse and before final
publication/delivery. A synchronous verifier that mutates its own approved
configuration also fails closed. These interfaces do not manufacture qualified
measurements; no controlled-real auditor approval is delivered in B2.

Target selection is a deterministic host plan for a particular source version.
For example, select only the changed owner/status paragraph, an unaudited
commitment range, or an explicitly high-value question. Return no targets for
other ranges; this produces `not_selected` with zero auditor calls. Selecting all
source text indiscriminately is not an automatic fallback. Changing selected
questions, predicates, spans or triggers invalidates old coverage even if the
application accidentally leaves its host version unchanged.

## Findings and admission

Every target/predicate pair must return exactly one `SourceAuditObservation`:

| Finding | Meaning and action |
| --- | --- |
| `no_additional_candidate` | The auditor proposed no additional candidate in this bounded scope. This is not proof of completeness or absence. |
| `missing_candidate` | One or more candidate mappings were proposed; they undergo the normal parser, exact-span checks, candidate reviewer and admission policy. |
| `conflict` | Explicit ambiguity/contradiction needing review. A matching otherwise-accepted baseline candidate is held pending; audit proposals cannot resolve the conflict. |
| `unresolved` | This question/range remains unresolved; it contributes no negative evidence. |

Recovered audit proposals **never auto-activate facts**, including when a model
reviewer returns `supported` and the predicate/source are authorized. Otherwise
acceptable recoveries become `PENDING_VERIFICATION` with
`source_audit_recovery_requires_host_verification`. Unsupported recoveries reject;
transient or non-asserted candidates retain the existing gates. Host verification
uses the existing explicit `resolve_atom()` contract and its evidence/version
checks. Later model-only interpretation reprocessing cannot remove audit recovery
or conflict review requirements, including after the optional audit is disabled.
Baseline-generated candidates keep their existing admission behavior, subject to an explicit audit
conflict in a matching range and a persisted same-source audit hold.

A hold uses the bounded identity policy `same-source-subject-predicate-slot/1`:
exact source event/version plus the projected subject/predicate slot. Automatic
extraction cannot clear it by changing candidate identity, quote/span, value,
validity dates, modality or qualifiers. This deliberately holds alternative values
in the same unresolved audited slot rather than guessing semantic equivalence.
Historical initially-pending audit records still enforce the hold after a model
withdraws/rejects them or removes them from the current interpretation head.
Regenerated records inherit the hold; exact duplicates of retained pending rows
are not reminted. Unrelated subjects/predicates, independent new source events and
explicit trusted typed/host-verification paths keep their existing rules. This is
not a global predicate ban or a negative fact about the source.

Every recovery is restricted to its reported target predicate and a matching
source quote fully inside that exact target range. Model-supplied authority,
scope changes, corrections and fabricated offsets cannot grant permission.
Incomplete/duplicated/foreign observation batches, adapter exceptions and timeouts
become explicit unresolved reports with generic failure codes. Invalid proposals
and proposal-budget overflow also make the affected scope unresolved. The original
finding remains recorded separately, so an invalid proposal cannot erase a
conflict signal. Candidate review failure still cannot publish facts.

`processed` means the requested audit scope was processed, **not** that all useful
facts were found. Receipts always carry `recall_proven=False` and
`world_negative_proof=False`. An empty candidate list, an empty answer, a failed
call, or `no_additional_candidate` must never be used as a world-negative fact,
complete source census, or permission to withdraw prior facts.

## Bounds and time/version fences

- At most 8 targets, 4 distinct predicates per target, 8,000 characters per range,
  and 16,000 selected characters total. Overlaps count toward the total.
- At most 32 target/predicate observations, 16 proposals total and 4 per
  observation. Recoveries share the pipeline's existing total candidate limit.
- Existing candidate context is at most 32,000 encoded bytes; only exact quoted
  spans wholly contained in selected ranges are included. Omitted context is
  explicitly counted in both request and receipt. Auditor output is bounded to
  48,000 encoded bytes. Existing extraction/checkpoint metadata limits still apply.
- At most one source-auditor call per fresh prepared batch. It has the pipeline's
  cooperative timeout (default 20 seconds, maximum 30), separately from generation
  and candidate review. Audit calls are reported independently; enabling this
  stage can add one model call. Host callbacks must be bounded, local and safe to
  execute under the repository transaction; do not perform inference in them.
- Immutable inputs include exact event identity, scope, content digest, source URI,
  actor, capture metadata, sensitivity, observation time, source authority,
  admission policy, source-audit schema, adapter versions, target-plan digest and both time axes. Prompts, model bindings
  and host policy changes require adapter-version changes.

Live source authorization is checked before dispatch, after model awaits, on
prepared/checkpoint reuse, and under the admission lock before and after awaited
writes. Cached delivery rechecks current targets, versions, permission and expiry
after reading the stored result. The final clock/config checks occur after the
last authorization await. Retained-source checks use actual current source rows,
document heads, erasure epoch and receive time; an old source revision or source
received after the fence's known-at cannot pass. Deletion/revision during an audit
or before publication cannot restore its findings.

The trusted application's authorization store must synchronize revocation with
its final `unit_of_work` check. For callers owning a larger transaction, call
`pipeline.validate_prepared_source(...)` after all awaited writes and immediately
before exiting the UoW. Existing durable extraction, interpretation activation and
batch/manifest closure do this. A later revocation/deletion is not retroactive
proof that an already-committed transaction never happened; ordinary storage
revocation/erasure still governs subsequent reads.

## Versioned reuse and compatibility

Reuse is intentionally limited to the existing exact idempotent first result and
durable prepared checkpoint. There is no cross-source, cross-revision or
cross-request semantic cache. Identical completed scopes can therefore skip new
model calls on retry, but changed or newly selected ranges require a fresh request
under the existing explicit source-revision/reprocessing APIs. A changed source,
time axis, permission, erasure epoch, target plan, policy or adapter invalidates
reuse. Audited requests require a stable exact source event ID and observation
time; the baseline's relaxed implicit-observation retry does not substitute a
new event identity into old source coverage. Use the durable host path or
`MemoryKernel.extract_event()` with the same explicit `MemoryEvent` for checked
retries. The convenience `AgentMemory.extract_atoms()` creates a new event ID per
call, so it cannot perform exact audited retries in this release.

`AtomExtractionReceipt.source_audit` is an appended optional mapping with default
`None`; legacy receipts and pipelines with no audit stage keep their behavior and
unchanged pipeline configuration payload. Enabled receipts use nested schema
`source-omission-audit/1`. They live in existing extraction metadata and durable
request/checkpoint JSON. Regenerated candidate payloads may append a
`source_audit_hold` origin reference and identity-policy version. No SQL schema
version changes. An audit-enabled pipeline
rejects missing/legacy audit payloads during prepared reuse; it never silently
upgrades a candidate-faithfulness-only report into source coverage.

`extraction_status()` is a historical diagnostic read; it does not invoke a live
host callback or renew authorization. It is not a reusable current coverage
certificate. Use the enabled pipeline's checked retry/publication path for reuse.
Disable the optional stage to roll back new audit dispatch; persisted recovered
candidates keep their host-verification requirement.

## Validation and unmeasured quality gate

`tests/test_source_omission_audit.py` exercises actual repository/admission and
worker paths with deterministic fakes. It covers zero generation, bounded source
selection, missing/conflict/unresolved reports, malformed model output, budget
exhaustion, ordinary review/authority gates, pending-only recovery, explicit host
verification, exact retry, changed target plans, post-model and post-storage
expiry/revocation, prepared reuse, real source erasure/revision, durable closure and
later interpretation reprocessing, including transformed candidates and historical
holds outside the current interpretation head. The shared fixture runs SQLite and can run
PostgreSQL when its disposable DSN/dependencies are configured; a skipped database
is not evidence of a passing database run.

The authored-case evaluator separately reports source-audit calls, missing-candidate
findings and unresolved scopes. Pending recoveries do **not** count as accepted
true positives or recall improvement. Unknown token usage and cost remain unknown.

**Real-model extraction benefit is unmeasured.** No new Qwen, Ollama, paid model,
model download or security configuration was used for B2. Before promoting an
actual auditor, freeze a held-out source/question/span corpus and compare:

1. Baseline generation plus candidate-faithfulness review.
2. The same pipeline plus this bounded source-level audit.
3. Expert-labeled omissions/conflicts/unresolved spans and independently verified
   recovery precision, separately from accepted-fact recall.
4. All audit/review calls, tokens, latency, timeout rate, review burden and unsafe
   or stale delivery counts, including cold/warm exact reuse and no-op cases.

The intended local model is the user's Ollama `Qwen3.5:9B`; its endpoint and model
binding must be supplied and verified by the host. Contract/fake success is not
model-gain evidence. Keep the optional stage off until the scoped quality/cost,
promotion and rollback gates have real measurements.
