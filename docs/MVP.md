# Minimum Viable Product

## Included

- Immutable events and evidence-backed claims.
- Current State with supersession and optimistic updates.
- Semantic, episodic, and procedural retrieval channels.
- SQLite persistence and bounded context assembly.
- Zero-config local facade.
- Provider and integration plugin discovery.
- MCP v2 stdio server and Python SDK.
- Generic and LangGraph lifecycle hooks.
- Pluggable automatic claim extraction from user, model, and tool events.
- Decision, outcome, reward, and controlled Procedure evolution records.

## Acceptance checks

- Duplicate idempotency keys do not duplicate events.
- Current State replacement preserves evidence and version lineage.
- Retrieval never exceeds configured item and token budgets.
- Scope cannot be supplied through MCP tool arguments.
- stdio performs capability, ingest, state, and retrieval round trips.
- Procedure candidates cannot skip offline, shadow, canary, or approval gates.
- Default installation has no third-party runtime dependencies.

## Excluded from the MVP

- Distributed evolution control plane.
- Production PostgreSQL certification.
- Remote gateway deployment certification.
- Graph and latent memory.
- Learned write/retrieval policies and online training.
- Web administration console backed by a live provider.

## Evolution roadmap

The broader project includes memory-driven agent training and runtime orchestration as
final-stage, independently installed extensions. The core remains usable without either.
Near-term work prioritizes evidence-linked feedback, memory consolidation, retrieval policy
versions, and host-controlled Procedure promotion. See
[the v3 evolution design addendum](EVOLUTION_DESIGN_V3_ADDENDUM.md) for proposed contracts,
stage dependencies, resource constraints, and acceptance criteria. Roadmap capabilities are
not claims of implemented functionality.
