# Optional tokenization, approved strategies and recovery partition retirement

These features do not install tokenizer libraries, download models, train
weights or start a background service. Existing single-store defaults remain.

## Model token counter

```python
from agent_memory.model_token_counter import ModelTokenCounter

counter = ModelTokenCounter(
    encode=host_tokenizer.encode,
    model_id="host-model-version",
    tokenizer_version="host-tokenizer-version",
    template_version="host-chat-template-version",
    render=render_actual_model_prompt,
    reserve_tokens=2048,
)
# Supply token_counter=counter when constructing UnifiedMemory.local(...).
measurement = counter.measure("serialized replacement context")
```

The host supplies a sized token-sequence encoder and the real prompt template.
The render callback must include message delimiters, system instructions and
tool schemas as needed for the target model. Fixed framing_tokens is available
only when the host knows that overhead exactly. Do not double-count special
tokens between the renderer and encoder. Reserve output/tool budget separately.

`count` returns rendered prompt tokens plus framing and reserve. Thus the
compression plan's budget represents the allowed total, not just summary text.
The full replacement is recounted before acceptance. Counts are not a promise
about undocumented provider-side prompt additions or billing. Host callbacks
must be bounded and must not change behavior without a new tokenizer/template
identity. The default extractor still selects segments conservatively by bytes;
an exact tokenizer does not imply an optimal segment packing algorithm.

Counter identity is persisted with proposals. A differently identified counter
cannot load them. Legacy counters without counter_id use their Python class
identity, which cannot detect configuration changes; use pinned counter_id
implementations for strong version binding.

## Strategy versions, reports and host approval

```python
from agent_memory.compression_strategy import (
    CompressionStrategy, StrategyBinding, StrategyApproval,
    CompressionStrategyRegistry,
)
from agent_memory.context_compression import ExtractiveContextCompressor, UTF8ByteCounter

strategy = CompressionStrategy("extractive", "1", "extractive-v1",
                               "builtin-extractive-v1", "utf8-bytes-v1")
registry = CompressionStrategyRegistry(
    "compression-policies.db", scope,
    bindings=[StrategyBinding(strategy, ExtractiveContextCompressor(), None, UTF8ByteCounter())],
    authorizer=host_strategy_policy,
)
# Supply strategy_registry=registry to UnifiedMemory.local(...); initialize it.
await registry.switch("extractive", "1", expected_generation=0,
    approval=StrategyApproval("approval-1", "host-operator", "Approved baseline"))
```

The host authorizer implements
`authorize(scope, action, strategy, expected_generation, approval)` and must
return True. Activation uses a compare-and-swap generation and immutable audit
records. Rollback uses `switch(..., rollback=True)` with a new approval and can
target only a previously activated version. No report automatically promotes a
strategy. Runtime bindings must expose matching compressor_id, validator_id
and counter_id, including prompt/model configuration in those host identities.

The registry is optional and host-administered, not a model-callable MCP tool.
All strategy changes must be coordinated with the exclusive owning host; the
registry does not atomically replace an already returned context in an Agent.
Configure a registry without an approved binding and compression fails closed.

`record_evaluation` stores host-provided held-out dataset digest, model,
evaluator, sample count and metrics. `compare` accepts matching protocols and
reports candidate-minus-baseline deltas, not a winner or significance claim.
The host must pin metric definitions in evaluator_id. Reports should contain
aggregate metrics only, never raw evidence or user text. Registry size is bounded
and full capacity blocks new reports/approvals rather than dropping audit data.

Accepted summaries pin strategy ID/version/generation and counter identity.
Generation changes invalidate their application path; regenerate under the new
strategy. Compression feedback also records the proposal's strategy and counter.
Offline evaluation does not automatically aggregate those feedback records or
change prompts. Model weights and Agent orchestration remain external.

## Optional partitioned recovery store

```python
from agent_memory.recovery_partitions import PartitionedRecoveryStore
from agent_memory.unified_memory import UnifiedMemory

store = PartitionedRecoveryStore("recovery-partitions", scope,
                                authorizer=host_partition_policy)
memory = UnifiedMemory.local("memory.db", scope, recovery_store=store)
await memory.initialize()
info = await memory.recovery_partition_info()
run_id = info["required_prefix"] + "task-1"
event_id = info["required_prefix"] + "message-1"
# Both run IDs and transport event IDs must use this prefix.
```

The partition authorizer implements `authorize(scope, action, generation,
approval)`. Host policy must require quiescence and explicit approval. Rotate
with `memory.rotate_recovery_partition(expected_generation=..., approval=...)`
only after all runs in the current partition complete or expire. Rotation
changes a durable monotonic generation; old run/event prefixes cannot write to
the new partition, even after old per-task tombstones are physically removed.

The default retains at most eight partitions. Retire the oldest inactive one
with `memory.retire_recovery_partition(generation, approval=...)`. Active
partitions cannot retire. The durable catalog fence is committed before file
removal, so physical cleanup failure must be handled operationally but never
reactivates old data. Retirement unlinks managed recovery content; it is not
secure erase of filesystems/backups. Catalog and directory permissions remain
the host's responsibility; never share the directory with unrelated files.

Only the active partition serves ordinary reads. Source deletion visits every
retained partition, including closed ones. Core event memory is not partitioned
or erased by this optional adapter; apply its separate retention policy. The
catalog and generation history must never be reset or reused with the same
source database. Existing unprefixed recovery stores are not auto-migrated.

Use one exclusively owned UnifiedMemory per scope, stop worker activity before
rotation, and never bypass it with direct provider/store calls. Catalog locking
fences individual store operations, not an entire cross-store ingestion. It may
briefly block on SQLite locks; this is not a distributed nonblocking scheduler.

## Validation status

This increment has not been tested. Required acceptance includes exact known
token fixtures, message-overhead budgets, strategy conflicts/rollback/stale
proposals, scope isolation, active-run rotation rejection, stale-prefix writes,
retirement restart, and deletion across retained partitions.
