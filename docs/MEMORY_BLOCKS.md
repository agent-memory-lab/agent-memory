# Memory Blocks

`MemoryBlock` is the small, mutable unit that lets an agent manage durable context without
loading the full memory store. It uses the existing `artifacts` abstraction, so storage plugins
do not need a parallel database model.

## Guarantees

- Every block has one semantic, episodic, or procedural channel.
- Every block cites at least one visible, non-archived source event.
- Forgetting any source atomically invalidates the complete dependent block, even if other
  sources remain. Removing a provenance reference cannot safely redact arbitrary prose.
- Dependency invalidation reaches child scopes within the same tenant and namespace. ERASE
  also removes previously archived dependent artifacts; unrelated scopes remain isolated.
- `token_budget` rejects oversized blocks instead of silently expanding prompts.
- `expected_version` provides optimistic concurrency control; stale updates fail.
- Reads and searches apply the same scope, active-status, and live-evidence checks.
- Archive is the default forget operation; legal erase remains separately authorized.

## Existing data after upgrading

Initialization adds an typed identity-only local-memory tombstone table to fence erased local
references without rejecting unknown external-provider memory IDs. The read guards reject
stale or invalid evidence, and new forget operations invalidate all dependencies that are
still recorded. Upgrade all participating readers and writers for the new guarantees.

SQLite creates `memory_tombstones` during initialization. PostgreSQL applies additive,
idempotent migration `016_memory_erasure.sql` to create `agent_memory_memory_tombstones`.
Both tables contain only the local record ID, storage-table type, and owning scope columns, with no text, payload,
provenance, or embeddings. Existing rows are not backfilled or rewritten. Initialization
requires the same schema-creation permission as other migrations; the public protocol and
core schema versions do not change. New erasures record the fences atomically with deletion.

Typed local episode/procedure/feedback references must remain live and visible when an
artifact is admitted or returned. Generic memory IDs from other recall providers remain
supported; known erased local event, claim, and artifact IDs cannot be republished as dependencies.

An older release may already have stripped a source reference while retaining derived
prose; that lost dependency cannot be reconstructed from the remaining row. If that happened, the operator should erase
or rebuild affected derived artifacts from surviving authorized evidence before reuse. The
upgrade does not automatically scan or rewrite production content.

## Python

```python
from agent_memory import AgentMemory, MemoryBlock, MemoryScope

scope = MemoryScope("tenant", session_id="session")

async with AgentMemory.local(scope=scope) as memory:
    event = await memory.remember("Prefer concise answers", event_type="user.message")
    block = await memory.write_block(
        MemoryBlock(
            scope=scope,
            title="Response style",
            content="Prefer concise answers.",
            event_ids=(event.event_id,),
            token_budget=64,
        )
    )
    matches = await memory.search_blocks("concise")
```

## MCP tools

- `memory_block_read`
- `memory_block_write`
- `memory_block_search`
- `memory_block_forget`

The MCP caller never supplies tenant or user identity. The host derives scope and erase
authority from the authenticated `MCPRequestContext`.
