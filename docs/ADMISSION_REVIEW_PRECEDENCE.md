# Admission policy and review precedence

Extraction combines a host admission decision with source-review gates. A hold
for uncertain evidence must not reopen a host's terminal `REJECT` or `L0_ONLY`
decision as `PENDING_VERIFICATION`.

The composition preserves these boundaries:

- `REJECT` from either the host policy or the review gate remains `REJECT`.
- A host `L0_ONLY` remains `L0_ONLY` unless the reviewer rejects the candidate.
- Other host decisions retain the existing behavior: a non-`ACCEPT` review gate
  applies, while an `ACCEPT` review gate preserves the host decision.
- Both decisions' reasons remain in the admission receipt and ledger, with
  duplicate reason strings removed. No new ordering between `CONTESTED` and
  `PENDING_VERIFICATION` is introduced.

This is an always-on safety correction, including when the optional attribution
or source-omission guards are absent. It can change formerly pending outcomes to
terminal outcomes. The extraction report records the review gate; the admission
receipt and ledger record the composed decision. Terminal candidates create no
Claim or pending receipt entry and cannot be scheduled for domain verification.

## Compatibility and existing records

There is no schema migration or automatic ledger rewrite. Existing extraction
fingerprints and cached first-result receipts remain stable. Reusing an already
processed input returns its stored outcome; installing this correction does not
retroactively fix earlier pending records or cancel already scheduled work.
Hosts that ran affected pipelines should review those records and any queued
verification work under their existing reconciliation and authorization policy.
New authorized processing must use a new request identity rather than replaying
the old idempotency key. Do not assume a new request deletes the earlier record.

The regressions use real temporary SQLite repositories and verify persisted
outcomes, duplicate processing, missing Claim/pending IDs and rejection by the
existing domain-verification scheduling guard. They do not validate extraction
quality, external witnesses, live PostgreSQL, or model behavior.
