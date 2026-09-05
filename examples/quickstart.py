from __future__ import annotations

import asyncio
from pathlib import Path

from agent_memory import AgentMemory, MemoryScope


async def main() -> None:
    scope = MemoryScope(
        tenant_id="example",
        user_id="user-1",
        agent_id="assistant",
        session_id="session-1",
    )
    async with AgentMemory.local(Path("example-memory.db"), scope=scope) as memory:
        await memory.remember(
            "The user prefers concise answers.",
            event_type="user.preference.updated",
            idempotency_key="example-preference-1",
            claims=(
                {
                    "key": "answer.style",
                    "value": "concise",
                    "text": "The user prefers concise answers.",
                    "scope": "user",
                    "confidence": 0.98,
                },
            ),
        )
        bundle = await memory.recall("How should the assistant answer?")
        for claim in bundle.current_state:
            print(f"{claim.key}: {claim.value}")


if __name__ == "__main__":
    asyncio.run(main())

