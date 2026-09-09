# Memory Blocks

`MemoryBlock` is the small, mutable unit that lets an agent manage durable context without
loading the full memory store. It uses the existing `artifacts` abstraction, so storage plugins
do not need a parallel database model.

## Guarantees

- Every block has one semantic, episodic, or procedural channel.
- Every block cites at least one visible, non-archived source event.
- Forgetting a source updates the block atomically; losing the final source removes the block.
- `token_budget` rejects oversized blocks instead of silently expanding prompts.
- `expected_version` provides optimistic concurrency control; stale updates fail.
- Reads and searches apply the provider's scope-visibility rules.
- Archive is the default forget operation; legal erase remains separately authorized.

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
