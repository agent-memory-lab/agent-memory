"""Local TLS termination and bearer-to-signed-identity ASGI test gateway."""
import asyncio
import hashlib
import hmac
import json
from pathlib import Path
import secrets
import sys
import time

import uvicorn
from agent_memory import MemoryScope
from agent_memory.unified_memory import UnifiedMemory
from agent_memory.recovery_transport import RecoveryTransport
from agent_memory_mcp.identity import GatewayHeaderIdentityResolver, canonical_identity_payload
from agent_memory_mcp.server import create_server


class Policy:
    async def authorize(self, context, operation):
        return True


class Gateway:
    def __init__(self, app, secret, bearer):
        self.app, self.secret, self.bearer = app, secret, bearer

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        headers = {k.decode().lower(): v.decode() for k, v in scope['headers']}
        if not secrets.compare_digest(headers.get('authorization', ''), 'Bearer ' + self.bearer):
            await send({'type': 'http.response.start', 'status': 401, 'headers': [(b'www-authenticate', b'Bearer')]})
            return await send({'type': 'http.response.body', 'body': b'Unauthorized'})
        # Never trust client-supplied tenant, signature or forwarding headers.
        clean = [(k, v) for k, v in scope['headers'] if not k.lower().startswith((b'x-agent-memory-', b'x-forwarded-')) and k.lower() not in (b'authorization', b'forwarded')]
        identity = {
            'x-agent-memory-tenant-id': 'tls-acceptance',
            'x-agent-memory-namespace': 'default',
            'x-agent-memory-session-id': 'session',
            'x-agent-memory-actor': 'test-gateway',
            'x-agent-memory-can-erase': 'false',
            'x-agent-memory-timestamp': str(int(time.time())),
        }
        identity['x-agent-memory-signature'] = hmac.new(self.secret, canonical_identity_payload(identity), hashlib.sha256).hexdigest()
        clean.extend((k.encode(), v.encode()) for k, v in identity.items())
        return await self.app({**scope, 'headers': clean}, receive, send)


if __name__ == '__main__':
    directory = Path(sys.argv[1])
    config = json.loads((directory / 'gateway.json').read_text())
    secret = bytes.fromhex(config['signing_secret'])
    memory = UnifiedMemory.local(directory / 'memory.db', MemoryScope('tls-acceptance', session_id='session'), recovery_path=directory / 'recovery.db')
    asyncio.run(memory.initialize())
    server = create_server(memory.provider, GatewayHeaderIdentityResolver(secret), recovery_tools=RecoveryTransport(memory, Policy()))
    app = server.streamable_http_app(stateless_http=True, json_response=True)
    try:
        uvicorn.run(Gateway(app, secret, config['bearer']), host='127.0.0.1', port=int(sys.argv[2]),
                    ssl_certfile=str(directory / 'server.pem'), ssl_keyfile=str(directory / 'server.key'),
                    proxy_headers=False, access_log=False, log_level='warning')
    finally:
        asyncio.run(memory.close())
