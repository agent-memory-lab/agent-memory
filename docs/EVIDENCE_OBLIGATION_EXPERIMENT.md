# Persistent evidence-obligation diagnostic

This is an opt-in research harness, not a production feature, public benchmark,
novelty claim, or promotion decision. The original core baseline was
`2cd3a74bf0e1c103f934a82028696890976e78b1` (tree
`4a3bd9530371973a07943cea1ab44049aa7d13a3`). Runs use the checked-out core
and bind its actual source hash; the baseline fields record provenance, not
a claim that a future checkout is identical.

## Offline reproduction and platform scope

Python 3.13+ on Linux or macOS is required. The durable ledger and checkpoint
locks use POSIX `fcntl`; native Windows is unsupported (use WSL). From the
repository root, install the development dependencies, then run:

```sh
python -m pip install -e '.[dev]'
python -m pytest -q tests/test_evidence*.py
```

These tests use synthetic fixtures and injected transports, with no model API
calls. The eight arms have no completed real-model comparison. Passing tests
validate implementation contracts, not answer quality or superiority. Private
run receipts and interrupted-run billing state are intentionally not published.

## Question and controls

Does retaining unresolved, source-frontier-bound evidence requirements improve
answers or total work beyond simple retrieval, per-query repair, or caching?
Eight isolated SQLite stores compare:

1. Controlled quote-atom baseline using unchanged core Atom admission/retrieval
2. Wider lexical raw-source recall under the same sequence ceilings
3. Fresh per-query gap detection and raw-source repair
4. The same mechanism, saving repaired quote atoms
5. Saved atoms plus persistent unresolved/resolved/reopened requirements
6. The same state machine without reuse across any query event
7. The same state machine without source-update hint invalidation
8. Ordinary cached gap responses plus exact-query answer reuse

This baseline is not the full production pipeline. Model-generated quotations
assert only exact source-text occurrence, never verified factual truth. The host
checks exact spans, current revision and authorized visibility; real core Atom
admission, native retrieval and source erasure are used. Requirements are routing
hints and never answer evidence. Operational closure requires a non-abstaining
answer and valid quoted evidence; it does not prove semantic completeness.
Source updates reopen requirements. Deletion/revocation purge them in every arm,
including the diagnostic ablation. Thus the no-invalidation arm measures stale
routing work, not authorization bypass or unsafe-source delivery.

## Inputs and accounting

The seeded synthetic corpus has six development and eight source-disjoint
heldout domains, with varied event schedules. Each follows the same broad
FAQ/bulletin/distractor document construction. Repeated/paraphrased questions,
missing qualifiers, no-change/irrelevant-change controls, source updates,
deletion, restoration, revocation and unsupported requests are included. Query
agent labels do not change permissions: no cross-agent causal claim is supported.
Gold has no route into extraction, gap detection, retrieval or answer prompts.

Initial and refresh extraction artifacts are frozen once per source payload and
shared across arms; their full logical cost is charged to each treatment. Global
provider spending counts each receipt only once. All arms have identical source
access, chronological notifications and total sequence ceilings. Actual spending
is not forced equal: compare measured quality/cost frontiers, not an equal-cost
causal effect. Failed calls, extraction, refresh, gaps and answers remain in the
cumulative model ledger. A failed/censored arm is not scored as a normal ranking.

Each manifest freezes baseline identity, Python/runtime source, corpus and gold
hashes, seed, model parameters, prompts, limits, arm/group selection and the
existing A9 `BootstrapProtocol` identity. Provider model alias weights are not
immutable. Results and artifacts are atomic and checksummed; changed manifests
or partial workspaces cannot be silently resumed.

## Spend and launch gate

Only explicitly approved authenticated runs may use the live client. Keys enter
through stdin/getpass, never command arguments, repository files or logs. The
client uses the configured HTTPS proxy, does not retry, and durably reserves a
UTF-8-byte input bound plus protocol overhead and maximum output before dispatch.
The implementation has hard safety ceilings of 1,000,000 combined tokens and
CNY 5, including reruns. These example caps are not permission to spend and do
not grant a new allowance to any existing run. Existing operators must retain
and reconcile their original cumulative ledger, including uncertain charges.
Missing/invalid usage, crashes or transport uncertainty retain the full reservation
and stop further calls. Tariff estimates use CNY 0.8/million input and 2.7/million
output without cache discounts; they are not provider billing confirmations.

First run: one full-lifecycle development group, sequential, at most 220 actual
model dispatches, 60-second call timeout. Start only after explicit operator authorization, independent review and
offline tests. The runtime prints phase/call/usage progress without keys or content.
A separate reviewed pilot gate is required before heldout execution. Never create
a new ledger to bypass exhausted or uncertain accounting.

Example (the operator supplies the key privately):

```sh
PYTHONPATH=src:. python -m experiments.evidence_obligations.runner \
  --output /approved/pilot-output --work /approved/fresh-work \
  --ledger /approved/cumulative-ledger.json --groups 1 --key-stdin
```

## Interpretation and stopping

The evaluator is a strict independent lexical gold/citation diagnostic. It does
not establish unrestricted semantic entailment; human review of pilot traces is
required before method-quality conclusions. Report answer/value, qualifiers,
correct abstention, evidence coverage and stale/unsupported delivery separately.
The reported `whole_seconds` is attempt-local observed wall time; checkpoint
replay does not recover earlier provider latency or full end-to-end elapsed time.
Source groups, not turns, are statistical units; tiny clustered intervals are
unstable descriptive summaries, and a one-group pilot has no interval.

The decisive contrast is persistent obligations versus saved atoms and ordinary
cache, considering quality and whole-sequence tokens/reads, including initial
cost. Reduced gap calls alone are not user benefit. If simple repair/cache is
competitive, abandon extra obligation complexity. No production switch, release,
PR, push or merge follows automatically from this experiment.

## Checkpoint and accounting invariants

This harness includes durable call intents/responses, atomic query
checkpoints and deterministic replay of saved retrieval observations. A run-level
process lock protects the shared extraction artifacts and dispatch counter.
Manifests additionally bind endpoint hash and stable ledger UUID/path hash.
Existing accounting is not migrated or reset. A prior interrupted run must not
be resumed with a fresh ledger; reconcile its original ledger and authorize any
further paid calls separately. No historical allowance is restored by checkout.
