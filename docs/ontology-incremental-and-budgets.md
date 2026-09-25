# Incremental shadow indexes and precise graph budgets

Both features are opt-in. Default installations keep their current behavior and
do not import a model SDK, download a tokenizer, or start another service.

## Snapshot-difference indexing

Pass `incremental=True` to `LiveOntologyMemory`. The first generation and every
schema change use a full build. Subsequent revisions with the same schema compare
the new snapshot with the active snapshot and reproject only affected connected
components into a copy of the active index. Shared entities cause connected
assertions to be rebuilt together, preserving source and conflict consistency.

```python
async with LiveOntologyMemory(
    source, registry, ontology_id, context,
    work_directory="ontology-work", incremental=True,
) as memory:
    while not await memory.refresh():
        pass
```

The source can be SQLite or the optional PostgreSQL source adapter. The managed
index remains SQLite. The snapshot stores its delta input selection durably;
checkpoint recovery uses that selection after restart. Publication still requires
the full acceptance check against all snapshot Claims. A different index UUID is
assigned to every shadow copy; the serving generation is not changed in place.

This optimizes index writes, not the full scan or disk footprint: new snapshots,
snapshot comparison, shadow copying and full acceptance still scale with scope
size. A large connected component can require a full reprojection. There is no
claimed CDC implementation, constant-time refresh or measured speedup. Preparation
and acceptance are not bounded by the projection batch limit. Restart without an
active in-memory base may fall back to a full rebuild for a new source revision.

## Host-selected tokenizer

```python
from agent_memory.token_budget import TokenCounter
from agent_memory.ontology_api import OntologyAPI

# encoder is supplied by the host. No tokenizer package is required by core.
counter = TokenCounter("host-model/tokenizer-version", lambda text: len(encoder.encode(text)))
api = OntologyAPI(
    None, registry, ontology_id,
    store_resolver=memory.borrow_store, token_counter=counter,
)
```

The same option is accepted by `traverse_ontology`. SDK and MCP graph requests
using this API inherit the host's counter; clients cannot replace it in tool
arguments. Without a counter, the previous character estimate is retained.

Exact mode counts the canonical JSON produced by `serialize_graph_content`:
nodes, edges, paths and the truncation flag. It re-counts each candidate payload,
rather than adding per-edge token counts. Results expose `token_count_kind` and
`tokenizer_id`; `token_estimate` holds the exact count in exact mode for backward
compatibility. The host should inject the canonical serialized content if it
needs that exact token bound. API wrappers, accounting metadata, system prompts
and model-provider framing are not part of this budget. A budget smaller than
the serialized empty graph is rejected instead of returning an oversized payload.

The counter must be deterministic, use a fixed tokenizer version and return a
positive integer for nonempty text. Its correctness is the host's responsibility.

## Remaining work

- Fully PostgreSQL-hosted snapshots, checkpoints and generation coordination.
- Explicit ancestor-scope merge and override policy.
- Bounded rule inference with versioned derivation evidence.
- Optional external training and orchestration integration.

These are not provided by the two features documented here. Regression coverage
is in `tests/test_ontology_delta_budget.py`, including full-build equivalence,
checkpoint recovery, shared components and host-counter budget boundaries.
