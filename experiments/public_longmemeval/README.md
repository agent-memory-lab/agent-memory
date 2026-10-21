# Public LongMemEval development diagnostics

Optional source-checkout tools for a public-data, four-arm memory diagnostic:
no memory, BM25 raw chunks, recent raw chunks, and this repository's agent-memory.
These are plumbing, safety, retrieval and provenance tests, **not official QA
accuracy results, production acceptance, or evidence of superiority**. Offline
model answers are mocks. This is a code-only release: no benchmark conversational corpus, questions, gold
answers, saved live results or private account history are included.

## Offline checks (Python 3.13+)

From a development checkout with the core and test dependencies installed, without contacting a model:

```sh
python3.13 -m unittest discover -s experiments/public_longmemeval/tests -v
python3.13 -m pytest -q experiments/public_longmemeval/tests
```

Mandatory CI checks run against authored synthetic fixtures and mocked providers.
Two optional checks (public-data integrity and full public-history integration)
are explicitly reported as skipped until the separately prepared sample exists;
a green synthetic run does not establish a public-data integration pass.

After explicit preparation below, the offline preflight verifies public sample
hashes and reports 49 sessions, 527 turns, 78 extraction calls and four answer
calls. The optional integration test executes those 82 calls with a mock provider
against `src/agent_memory`, in a temporary database. It makes no model calls.

Both runners record current core Python/SQL and experiment-helper source hashes,
the checkout revision, and actual corpus/query/gold plus manifest SHA-256 bindings;
they never load a separate frozen/vendored core. The evaluator rejects missing or
changed input bindings before scoring. Source provenance is recorded rather than
requiring the same evaluator implementation forever. A revision alone does not imply
clean source: the recorded file hashes are authoritative for the executed files.

## Prepare optional public data explicitly

From the repository root:

```sh
python3.13 -m experiments.public_longmemeval.download_full
python3.13 -m experiments.public_longmemeval.prepare_smoke
python3.13 -m experiments.public_longmemeval.run_experiment preflight
python3.13 -m unittest experiments.public_longmemeval.tests.test_runner -v
```

The first command explicitly downloads the pinned 277 MB source dataset. The
second verifies its hash, prepares records in temporary storage, selects the
frozen neutral case ID, and verifies all three reconstructed sample hashes before
writing ignored local files under `smoke/`. Allow disk space for the full source
and temporary prepared dataset. No model weights are downloaded and no model
calls are made. Existing outputs are not overwritten. To use an already downloaded
source, pass `prepare_smoke --data /path/to/longmemeval_s_cleaned.json`.

The full-history tests remain callable without a live provider once this explicit
preparation is complete. Do not force-add generated conversational files to git.
Both live runner flows below require this preparation first.

## macOS / local Ollama

`bash experiments/public_longmemeval/run_mac.sh` uses already installed Python
3.13+, a local Ollama server and `qwen3.5:9b`; it never installs software or pulls
models. For explicit choices, run:

```sh
python3.13 -m experiments.public_longmemeval.mac_smoke preflight --model YOUR_LOCAL_MODEL --embedding-mode none
python3.13 -m experiments.public_longmemeval.mac_smoke run --model YOUR_LOCAL_MODEL --embedding-mode none --output experiments/public_longmemeval/runs/local-smoke
```

Unlike the transport preflight, Ollama preflight contacts loopback `/api/tags` and
`/api/version`, but does not generate. Only bare loopback HTTP hosts are accepted;
proxies, redirects and cloud-model metadata are refused. Optional
`nomic-embed-text` reranking requires installed 768-dimensional embeddings and
explicitly disables input truncation. The launcher targets macOS; Python code is
portable where Ollama and Python 3.13 are available. Native macOS behavior and
model quality require an actual local run and are not established by Linux CI.
Default generation bounds are 160 calls / 1,800 seconds; initialization/cleanup
can take additional time. Local cost is not estimated.

## Windows / HTTPS provider

Copy `config.example.json` to ignored `config.local.json` in this directory.
Replace endpoint, model and rate placeholders with your provider values; verify
conservative CNY-per-million input/output rates (including surcharges), then set
`tariff_verified` to true. Explicitly set `structured_output_mode` to
`json_schema` (provider-enforced schema) or `json_object` (JSON mode with the same
schema appended to the prompt). The legacy default is `json_schema`; model names
never select a mode. No failure automatically switches modes, endpoints or models.
Run `RUN_WINDOWS.cmd`, or:

```powershell
py -3.13 -X utf8 experiments/public_longmemeval/run_experiment.py run
```

The Python transport is cross-platform; the `.cmd` launcher targets Windows.
Offline CI validates mocked transport behavior, not native Windows execution or
provider compatibility. Credentials are entered with hidden `getpass` in an
interactive terminal and are never saved. No real endpoint or credential is
shipped. The chosen provider must support this OpenAI-compatible chat shape,
usage fields, structured output and `enable_thinking=false`; compatibility is
not universal.

Both modes use complete local validation of the bounded Claims schema before
ingestion: exact object fields, at most eight claims, nonblank string fields,
explicit finite numeric confidence in [0, 1], and no duplicate JSON fields. JSON
mode does not provide provider-side schema enforcement. A 503 response cannot
establish format compatibility: a successful synthetic response must also pass
local Claims validation before the selected mode is considered verified.

## Source-bound reader (v3)

Packing is selected independently with `--packing-version v2|v3|v4|v5` in the HTTPS,
local smoke and full-comparison runners. **V2 is the stable packing default.**
V3 remains experimental after losing necessary evidence in synthetic competition
tests. Choosing reader v3 does not enable packing v3. Manifests record the exact
packing policy parameters separately from the reader contract. See the
[generalization audit](../../docs/BENCHMARK_PACKING_GENERALIZATION.md).

V4 adds source-bound context units and removal-only dependency checks at final
delivery. It keeps independent same-topic facts and labels narrowed relevance as
an unverified parent bound. It remains opt-in; see the
[v4 design and acceptance limits](../../docs/BENCHMARK_EVIDENCE_UNITS_V4.md).

New HTTPS and local smoke CLI runs default to `--reader-version v3`. Use
`--reader-version v2` explicitly to retain the plain answer protocol described
below. Existing Python call defaults remain v2; the full LongMemEval example
adds the separate `--reader-mode structured-v3` option without changing its
existing default.

V3 sends all selected evidence as JSON blocks with source IDs, available dates,
speaker metadata and exact source spans. Decoding each text field gives the
unchanged selected text. Content and JSON framing have separately recorded byte
limits; framing overflow fails instead of clipping the evidence. Unknown host
metadata stays unknown. Model output binds exact quotes to individual block IDs
and reports missing facts when abstaining. Invalid quotes remain protocol
errors; raw completions and validation diagnostics are retained without retries.
Traceability does not establish factual entailment or completeness.

Freeze new protocol/runtime hashes and write results to a new directory. Do not
rewrite previous scores or resume a paid run with a fresh unseeded ledger. See
[the v3 architecture and acceptance record](../../docs/BENCHMARK_RELIABILITY_V3.md)
for selection changes, compatibility, and development-evaluation limits.

## Reader delivery and isolated repair audits (v2)

The HTTPS and local runners declare a versioned `reader_contract`. Native recall
owns the 2048-estimated-token / eight-item evidence budget. The reader receives
every selected item verbatim, in native selection order, with occurrence-specific
candidate identities and citation spans. A separate 8216-byte serialization guard
includes bullet framing; exceeding any bound or losing citation consistency stops
the answer request, rather than truncating evidence again. Estimated tokens are
the core's heuristic, not a provider tokenizer measurement.

Raw baselines retain the explicitly named `legacy-utf8-prefix/v1` 6000-byte
contract. They can clip the last item. This is **not an equal-context-budget
comparison**; results and evaluator summaries retain that qualification.
`core-evidence-verbatim/v2` changes the experiment protocol and must not overwrite
an old result or be merged into its accuracy score.

`repair_audit.py` supports local replay of all completed cases in an existing
interrupted custom benchmark. Run each variant in a separate process and use a
fresh output directory outside both source checkouts and the original run:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. python3.13 -m experiments.public_longmemeval.repair_audit --original /path/to/original --core /path/to/original/source --output /path/to/audit/baseline --variant baseline
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. python3.13 -m experiments.public_longmemeval.repair_audit --original /path/to/original --core /path/to/original/source --output /path/to/audit/packing --variant packing
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. python3.13 -m experiments.public_longmemeval.repair_audit --original /path/to/original --core /path/to/repaired-checkout --output /path/to/audit/repaired --variant repaired
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. python3.13 -m experiments.public_longmemeval.repair_audit --original /path/to/original --core /path/to/repaired-checkout --output /path/to/audit/facets-experiment --variant repaired --enable-query-facets
```

This tool expects the custom `prepared/queries.jsonl`, `run/manifest.json` and
`run/cases/*/result.json` schema, including original provenance maps. It copies
closed SQLite databases through immutable read-only connections, refuses pending
WAL/journal writes and existing output paths, and verifies source hashes. Only
local recall runs on the copies. There is no extraction, answer generation,
judging, credential access or model call. The old score is retained as reference;
session coverage and evidence preservation are separate diagnostic measures.
Recall stays in the original current-time mode; timestamps are recorded, so runs
across validity boundaries are not strict causal comparisons. One local timing
per case is diagnostic, not a performance benchmark.

The experimental `bounded-query-facets/1` policy adds a small indexed claim-candidate
lane for explicit English personal-advice and action-count requests. It uses at
most 16 extra locators, shares the original hydration/output budgets, and retains
scope, deletion and both time filters. This is a bounded lexical improvement,
not general multilingual semantic recall. Its stage diagnostics expose admitted
counts and route limits; no model is called for query expansion. It is disabled
by default. Hosts must explicitly pass `enable_query_facets=True` to
`build_local_kernel` or `SQLiteMemoryRepository`; benchmark audit invocations must
also declare the flag. Offline public-case replay found no net session-coverage
gain, so the experiment has not passed the quality gate for default activation.
The default retains the original candidate route with the new reader delivery.

The generic schema-3 transport ledger is not the previous paid-run ledger.
Existing paid checkpoints, protocol hashes, reservations and unresolved provider
errors remain authoritative. A future paid continuation needs an explicit new
protocol linked to the frozen parent and must inherit all cumulative costs;
creating fresh generic state is not a way to resume within the old budget.

## Transport accounting and evaluation

`provider_contract.py` provides pure helpers for external budgeted harnesses:
`prepare_structured_output(prompt, schema, mode=...)`, `strict_json_loads(raw)` and
`safe_response_metadata(status, document, headers=..., secrets=...)`.
`extraction.validate_claims_output(raw)` applies the complete Claims contract.
These helpers perform no I/O or retries and do not modify a budget or run state.

The user confirms sending this public history to their configured provider.
The transport then requires explicit acceptance of cumulative limits (1,000,000
tokens / CNY 5 tariff estimate, not an invoice). Fresh local state starts at zero;
this is a generic experiment ledger, not a continuation of any previous account
or run. Reservations persist before requests. Missing usage, interrupted calls,
truncation, secret reflection or inconsistent receipts stop the run without
automatic retries. Tariff estimates and byte-based input reserves are safeguards,
not a provider-enforced billing cap. Keep `state/` across runs; never delete or
copy it to reset accounting. Unresolved state needs manual review. These JSON
files are local evidence, not tamper-proof accounting.

Transport schema 3 separately records `request_outcome`, `http_outcome`,
`usage_status` and `cost_basis`. An HTTP error is a known failed outcome even when
usage is unknown; the full conservative reservation remains counted until explicit
accounting review. Budget summaries separate known-usage tariff estimates from
unknown-usage reservations. Neither is a provider invoice. Validated request IDs
from allowed response headers or `request_id` / `id` response fields and safe
provider error codes are retained on failures; error bodies and arbitrary headers
are not persisted. Error categories are diagnostic labels, not retry permissions.
The selected output mode is bound into ledger configuration. Older ledgers or
changed modes fail closed without conversion, reset, retry or automatic recovery;
retain the prior evidence and use an explicitly reviewed continuation mechanism.

`runs/`, `state/`, local config, downloaded data, prepared sample payloads and logs are ignored. Runs may
contain prompts, model outputs and local accounting; review before sharing.
The HTTPS evaluator is separate and opens gold only after completed blind
answers exist:

```sh
python3.13 -m experiments.public_longmemeval.evaluate prepare PATH_TO_RUN
# Independently fill manual_review.json before:
python3.13 -m experiments.public_longmemeval.evaluate summarize PATH_TO_RUN
```

The macOS runner emits its own smoke diagnostics; this evaluator targets the
HTTPS runner's source-provenance result schema. Source-session coverage does not
prove the delivered text contains an answer. One manually judged development
case is not population accuracy.

## Shared public data and protocol

- Dataset: [LongMemEval-S cleaned](https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/tree/98d7416c24c778c2fee6e6f3006e7a073259d48f), revision `98d7416c24c778c2fee6e6f3006e7a073259d48f`.
- Full source: 277,383,467 bytes; SHA-256 `d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442`.
- The dataset card declares MIT; its pinned copy is in `licenses/DATASET_CARD.md`.
  Upstream [LongMemEval code](https://github.com/xiaowu0162/LongMemEval/tree/9e0b455f4ef0e2ab8f2e582289761153549043fc)
  attribution/license is preserved in `licenses/LONGMEMEVAL_CODE_LICENSE`
  (Copyright 2024 Di Wu). Repository code retains the root project license.
- `smoke/` contains only the manifest and hashes needed to reconstruct one
  optional publicly licensed case. Corpus, query and evaluator-only gold are
  not distributed. The case was selected by smallest UTF-8 history among answerable
  cases in the frozen smoke subset, not as a representative quality sample.
- `protocol/` contains the frozen custom 50-development / 450-holdout case IDs,
  ten-case stratified smoke subset, native-ID split and preparation metadata.
  These are **not an upstream official split**. Do not tune on the holdout.
  Protocol manifest hashes for full corpus/query/gold files describe optional
  generated artifacts, which are intentionally not committed here.
- `public_retrieval.py` uses all-role session retrieval, unlike upstream user-only
  preprocessing. Gold answer, native IDs, type and answer flags stay outside
  indexing. Naive timestamps are serialized as UTC by assumption. Histories are
  preserved without question-date cutoff, so this is not a causal/as-of-time test.

The full-data downloader is explicit and import-safe:
`python3.13 -m experiments.public_longmemeval.download_full`. It contacts only the
pinned dataset URL and verifies size/hash before accepting it. It does not fetch
models. Use `public_retrieval.py --help` for preparation and retrieval commands.

## Experimental v5 passage selection

`--packing-version v5` enables bounded lexical reservations for explicitly
coordinated query topics. It does not configure a remote scoring service. The
Python host may additionally pass `passage_reranker=...` with explicit v5 to the
local/PostgreSQL builders; that provider owns its credentials, model and ledger.
It scores the actual feasible source text in at most one bounded batch. Returned
evidence is checked and final authority filtering remains mandatory.

See [v5 design and development results](../../docs/BENCHMARK_COVERAGE_PASSAGES_V5.md)
for strict/partial ranking protocols, diagnostics and limits. V2 remains the
default. The 24-case evidence gate and 3-case live repair are exposed development
checks, not full LongMemEval accuracy. New protocols and ledgers must retain
frozen results and cumulative costs; no automatic model retry is introduced.

## Opt-in audited reader (v4)

Use `--reader-version v4` for at most two separately accounted calls per arm:
a source-bound draft and support audit. The four-arm preflight reserves eight
answer calls. Reader v3 remains the default. Protocol 4.2 preserves exact
citation positions and temporal precision through audit; inferred source-month
bounds are declared assumptions, not verified event dates.

See [v4 protocol and measured limits](../../docs/BENCHMARK_QA_RELIABILITY_V4.md)
and [4.2 safety corrections](../../docs/BENCHMARK_QA_RELIABILITY_V4_2.md).
Development regressions and offline proof checks do not establish full benchmark
accuracy or quality/cost acceptance.
