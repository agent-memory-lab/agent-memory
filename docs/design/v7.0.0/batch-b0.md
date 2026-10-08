# V7-B0 — question contracts and verifiable baseline

Status: implementation, independent review, full aggregate validation, and build/install verification complete. This record does not
certify B1–B6, production model quality, or a completed V7 release.

## Scope and baseline

The source is GitHub main `7f824dd49b18935c6b47f5eb32aa32efdbd8cf01`, including the
actual V7 design, plan and task ledger. All 535 source files were materialized
through the connected repository API and their Git blob hashes verified. No
previous unpublished implementation was available or silently substituted.
The cloud executor uses Python 3.13.5. This is a code/contract change, not a
software-version bump: package version remains 0.1.0.

B0 implements additive, importable contracts only:

- `derived/question_model.py`: strict versioned identity, parameter/schema,
  context, status, immutable generation content, validation certificate, and
  finite exact/coverage target contracts. Refresh policy is not question semantics.
- `derived/project_questions.py`: bounded owner/status/commitment/risk domain
  specification and deterministic full oracle over explicit qualified protocol
  snapshots. It is not yet a bridge from arbitrary source text into trusted facts.
- `evaluation/question_cost.py`: experiment-only full-cost ledger and acceptance
  gates; unknown invoices and pending work are not counted as zero, and foreground
  savings are not claimed without background/drain accounting.
- `acceptance-profile.json`: explicit missing licensed real data, fixed models,
  judges and predeclared calibrated thresholds. Synthetic fixtures do not satisfy
  those gates.
- `acceptance-map.json`: all 98 inherited/new specification responsibilities,
  preserving the earlier evidence status separately from new V7 evidence.
- [Migration inventory](migration-b0.md): every proposed new persistent object,
  erasure/recovery obligations, migration order and rollback requirements.

No new runtime capability is advertised or enabled. No new database kind is
written, migration run, background worker launched by the library, or model called.
New protocol constructors do not authorize use of evidence, membership, sources,
processing providers, or a historical mode. Later runtime adapters must verify
those claims against authoritative state at input, publication and delivery.

## Verification

Baseline command, before the new B0 tests were present:

```sh
.venv/bin/python -m pytest -q tests packages/evolution/tests \
  packages/langgraph/tests packages/python-sdk/tests packages/mcp-server/tests \
  packages/postgres/tests
```

Result: exit 0; 1,618 passed, 926 skipped, 2,544 collected. Most skips require a
real PostgreSQL DSN; a skipped contract is not passed. The final focused suite has 299 passing tests (166 question contracts, 59 project
oracles, 71 cost contracts, 3 inventory guards). Independent review reproduced and
verified fixes for lossy numeric identity, Unicode normalization, unsupported
qualifier/temporal/predicate exclusion, mutable scope, inconsistent model-use
declarations, and late usage settlement. All seven new Python files are Ruff-clean.
Final frozen run: **2,843 passed, zero failed, zero skipped**, including real
PostgreSQL 17.11 / pgvector 0.8.0 and the separately configured ontology PostgreSQL
contracts, plus tiktoken 0.14.0. Both AGENT_MEMORY_TEST_POSTGRES_DSN and
AGENT_MEMORY_ONTOLOGY_TEST_DSN pointed at an isolated disposable database.
Core and all five extension packages built as sdist/wheel, and all six wheels
installed and imported from a fresh Python 3.13 environment. See the
[machine-readable source hashes and commands](validation-b0.json).
The 2,843 tests are regression/contract evidence, not 98-specification or real-model acceptance.

## Task interpretation

T01 is DONE for its strict protocol scope; T02/T13/T14/T15 remain IN_PROGRESS. A B0 contract slice is not completion of all
applicable runtime or real-data acceptance for those tasks. The 44 AM61 entries
are unchanged (DONE 1 / IN_PROGRESS 33 / TODO 10). The frozen V7 design retains
SHA-256 `309104c81b2e89ce74137903312ecbeaa036ba192d47fcfbdd555cdd4dc907b9`.

B1 next supplies indexed subscriptions and same-transaction freshness proofs.
B2 then adds the scheduler; B3 connects trusted project inputs, full views,
qualified parents/pages and read-only SDK/MCP. Delta/proof reuse and governed
models remain behind those dependencies. Missing real data/provider calibration
is reported as insufficient evidence, never converted into a passing result.

## Upgrade and rollback

There is no schema upgrade or data migration in B0. Existing wire formats, IDs,
exact receipts, capability claims, and default installation behavior remain.
Rollback removes these additive modules and restores documentation; there is no
runtime state or irreversible data transition. B1 must implement its new-object
erasure and restore tests before enabling any persistent write path.
