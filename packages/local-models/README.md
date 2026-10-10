# Local model adapters

Optional tokenizer and Qwen3 final-logit scoring package. Import performs no
model download, execution or network request. Model artifacts must be installed
explicitly, identified by an exact manifest, and loaded with local_files_only.
No provider processing permission or production promotion is granted by installation.

The base package supports offline contracts without heavy inference dependencies.
Install the inference extra only in a host that explicitly uses its adapters:

```sh
python -m pip install -e 'packages/local-models[inference]'
```

Runtime integration, pinned model records and validation are recorded in the
v7.1 implementation ledger. The core package retains zero third-party runtime dependencies.

`LocalQwenReranker` loads a hash-approved local Qwen3 snapshot and returns actual
final yes/no logits for `FinalTokenPairScorer`. It rejects token overflow and
limits batch work; cancelled callers cannot queue more work behind an unfinished
inference. `LocalTransformerChat` owns rendering and actual local generation;
wrap it in `TokenBudgetPort` with its counter and binding verifier for exact
input/output reservation. These local APIs currently use the approved Qwen3
path, not arbitrary remote runtimes. Importing this package remains inert.

Run the authored inference probe only after provisioning and approving artifacts:

```sh
python -m agent_memory_local_models.smoke \
  --directory /absolute/path/to/approved/snapshot \
  --manifest-sha256 APPROVED_SHA256 \
  --output /tmp/reranker-smoke.json
```

The [publisher's fixed template](https://huggingface.co/Qwen/Qwen3-Reranker-0.6B)
is applied as separately tokenized prefix/body/suffix. Scores are logit margins;
probabilities are telemetry, not calibrated confidence or proof of memory truth.
Installation/probing does not enable the core feature gate or grant source rights.
Real gold, calibration and whole-cost observations are still needed for promotion.

Offline adapter contracts run without model dependencies or weights. Actual
inference tests additionally require an approved `AGENT_MEMORY_LOCAL_RERANKER_DIR`:

```sh
python -m pytest packages/local-models/tests
AGENT_MEMORY_LOCAL_RERANKER_DIR=/absolute/path/to/approved/snapshot \
  python -m pytest packages/local-models/tests
```
