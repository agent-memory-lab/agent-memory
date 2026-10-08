# B6 provider-specific collection correction

The prior complete run recorded **3783 passed, 6 skipped**. Those six skips were
SQLite instantiations of assertions specific to PostgreSQL query plans and
asynchronous PostgreSQL helpers. Their real PostgreSQL counterparts all passed.

This patch explicitly parameterizes only those three test functions with the
PostgreSQL `store` fixture. Their one, three and two PostgreSQL cases keep exactly
the same collected node IDs and substantive assertions. A mistaken non-PostgreSQL
fixture now fails an assertion instead of skipping. The shared fixture still skips
when its optional PostgreSQL package or test DSN is unavailable; therefore a missing
PostgreSQL prerequisite cannot masquerade as zero-skip verification.

SQLite retains its dedicated query-plan test and every previously passing shared
ownership, transaction and lifecycle test. This change neither filters the complete
test command nor modifies any runtime code or release-gate rule.

## Exact collection preservation

The [machine-readable record](validation-b6-zero-skip.json) binds the baseline,
changed test-file hashes, collection hashes, the exact removed node IDs, and the
six retained PostgreSQL counterparts. Comparison with the previous definitive
JUnit (SHA-256 `ab9df2775cce3e3c912e04b66e207e45d31de5c008039e9931c90caf498b2539`)
established:

- Before: 3789 collected nodes; after: 3783.
- Removed: exactly the six prior skipped SQLite nodes; added: none.
- The new collection equals the entire previous passing-node set, including all
  meaningful SQLite coverage and every PostgreSQL counterpart.
- Focused execution of both changed files: **60 passed, 0 skipped, 0 failed or
  errored**, including all six PostgreSQL-specific cases on real PostgreSQL 17.11.

The collection command is unfiltered and covers the repository plus all five
extension test directories. Quiet-mode defaults are reset only to make pytest
emit individual node IDs. Focused execution uses a local virtualenv rebound to
this worktree for all six packages, including child interpreters that override
`PYTHONPATH`; both PostgreSQL test DSN entrypoints target the same disposable
database. No new dependency or model was downloaded and no paid API was called.

## Completed final verification

The separately reviewed GC/A9 work is now integrated at `dc39baf9a0a751a101232f72526c39486d430240`.
The [complete unfiltered final run](validation-b6.json) passed 3931 cases with zero
skips, failures or errors: all 3783 prior passing nodes plus 148 new GC/A9 nodes.
This final execution, rather than the earlier focused run, establishes zero-skip coverage.
The original six skips remain honestly recorded in the historical B6 result.
No skip has been counted as passing or waived at the strict gate.

This correction does not establish actual Ollama inference, licensed held-out gold
quality, whole-cost benefit, default enablement or complete V7 acceptance.
