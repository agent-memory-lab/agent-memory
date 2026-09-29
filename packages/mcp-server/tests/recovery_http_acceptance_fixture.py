"""Local-only HTTP process for acceptance tests."""
import asyncio
from pathlib import Path
import sys
from agent_memory import MemoryScope
from agent_memory.unified_memory import UnifiedMemory
from agent_memory.recovery_transport import RecoveryTransport
from agent_memory_mcp.identity import GatewayHeaderIdentityResolver
from agent_memory_mcp.server import create_server

SECRET = b'local-acceptance-only-not-a-real-secret-0000'

class Policy:
    async def authorize(self, context, operation):
        if sys.argv[3] == 'slow':
            await asyncio.sleep(0.4)
        return True

if __name__ == '__main__':
    path = Path(sys.argv[1])
    scope = MemoryScope('http-acceptance', session_id='session')
    memory = UnifiedMemory.local(path / 'memory.db', scope, recovery_path=path / 'recovery.db')
    asyncio.run(memory.initialize())
    server = create_server(memory.provider, GatewayHeaderIdentityResolver(SECRET), recovery_tools=RecoveryTransport(memory, Policy()))
    try:
        server.run(transport='streamable-http', host='127.0.0.1', port=int(sys.argv[2]), stateless_http=True, json_response=True)
    finally:
        asyncio.run(memory.close())
