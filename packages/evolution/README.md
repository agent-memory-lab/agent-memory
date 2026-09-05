# Agent Memory Controlled Evolution

An independent control plane for memory-driven evolution. It turns high-quality
episodes into immutable Procedure candidates and enforces offline, shadow, canary,
human-approval, activation, and rollback gates.

It does not train in the request path and it never allows a model to write an active
Procedure directly.

```text
episodes -> candidate -> offline evaluation -> shadow -> canary
         -> activating -> active -> rollback/archive when required
```

```python
from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvolutionEngine,
    MemoryProviderProcedureDeployment,
    SQLiteEvolutionRegistry,
)

registry = SQLiteEvolutionRegistry("./evolution.db")
engine = EvolutionEngine(
    registry,
    DeterministicPromotionPolicy(),
    MemoryProviderProcedureDeployment(memory_provider),
)
await engine.initialize()
```

Evaluation metrics are supplied by an external replay or benchmark harness. Model
self-evaluation is deliberately insufficient. The default gates require minimum sample
sizes, positive task-success delta, acceptable task success, zero safety violations,
and zero cross-scope leakage. Active promotion also requires a human approval reference.

The included SQLite registry is intended for local and single-controller deployments.
A production registry can implement `EvolutionRegistry` with PostgreSQL or another
transactional store without changing the engine.

