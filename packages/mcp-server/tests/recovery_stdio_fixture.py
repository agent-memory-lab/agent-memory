"""Isolated subprocess fixture, not a production server entry point."""
import asyncio
from pathlib import Path
import sys

from agent_memory.domain import MemoryScope
from agent_memory.mcp import MCPRequestContext
from agent_memory.recovery_transport import RecoveryTransport
from agent_memory.unified_memory import UnifiedMemory
from agent_memory_mcp import StaticIdentityResolver, create_server


class Policy:
    def __init__(self, mode):
        self.mode = mode

    async def authorize(self, context, operation):
        if self.mode == "slow" and operation == "stats":
            await asyncio.sleep(0.5)
        if self.mode == "error" and operation == "stats":
            raise RuntimeError("private-provider-diagnostic")
        return self.mode != "denied"


if __name__ == "__main__":
    directory, mode = Path(sys.argv[1]), sys.argv[2]
    scope = MemoryScope("stdio-test", session_id="session")
    memory = UnifiedMemory.local(directory / "memory.db", scope,
                                  recovery_path=directory / "recovery.db")
    asyncio.run(memory.initialize())
    identity = MCPRequestContext(MemoryScope("foreign") if mode == "foreign" else scope,
                                 actor="host", can_erase=mode != "no-erase")
    server = create_server(memory.provider, StaticIdentityResolver(identity),
                           recovery_tools=RecoveryTransport(memory, Policy(mode)))
    try:
        server.run(transport="stdio")
    finally:
        asyncio.run(memory.close())
