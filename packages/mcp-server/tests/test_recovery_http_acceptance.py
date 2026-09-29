"""Real loopback HTTP MCP acceptance, not TLS or production gateway tests."""
import asyncio
from contextlib import contextmanager
import hashlib
import hmac
from pathlib import Path
import socket
import subprocess
import sys
import time

import httpx
import pytest
from agent_memory_mcp.identity import canonical_identity_payload
from agent_memory_sdk import MCPMemoryClient

SECRET = b'local-acceptance-only-not-a-real-secret-0000'


def headers(*, expired=False, invalid=False, foreign=False):
    values = {
        'x-agent-memory-tenant-id': 'other' if foreign else 'http-acceptance',
        'x-agent-memory-namespace': 'default',
        'x-agent-memory-session-id': 'session',
        'x-agent-memory-actor': 'acceptance-host',
        'x-agent-memory-can-erase': 'true',
        'x-agent-memory-timestamp': str(int(time.time()) - (3600 if expired else 0)),
    }
    signature = hmac.new(SECRET, canonical_identity_payload(values), hashlib.sha256).hexdigest()
    values['x-agent-memory-signature'] = '0' * 64 if invalid else signature
    return values


@contextmanager
def server(tmp_path, mode='normal'):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    url = f'http://127.0.0.1:{port}/mcp'
    fixture = Path(__file__).with_name('recovery_http_acceptance_fixture.py')
    with (tmp_path / 'http-server.log').open('w') as log:
        process = subprocess.Popen([sys.executable, str(fixture), str(tmp_path), str(port), mode], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 10
            with httpx.Client(trust_env=False, timeout=0.25) as client:
                while True:
                    assert process.poll() is None, 'HTTP fixture failed to start; see http-server.log'
                    try:
                        with client.stream('GET', url):
                            pass
                        break
                    except httpx.TransportError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('HTTP fixture startup timed out')
                        time.sleep(0.05)
            yield url
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def test_http_authenticated_session_and_reconnect(tmp_path):
    async def exercise(url):
        for _ in range(2):
            async with httpx.AsyncClient(headers=headers(), trust_env=False) as http:
                async with MCPMemoryClient.from_http(url, http_client=http) as client:
                    result = await client.recovery_stats()
                    assert isinstance(result, dict)
                    assert 'result' in result
    with server(tmp_path) as url:
        asyncio.run(exercise(url))


@pytest.mark.parametrize('invalid_options', [{'invalid': True}, {'expired': True}, {'foreign': True}])
def test_http_rejects_untrusted_identity(tmp_path, invalid_options):
    async def exercise(url):
        async with httpx.AsyncClient(headers=headers(**invalid_options), trust_env=False) as http:
            async with MCPMemoryClient.from_http(url, http_client=http) as client:
                with pytest.raises(Exception):
                    await client.recovery_stats()
    with server(tmp_path) as url:
        asyncio.run(exercise(url))


def test_http_timeout_then_session_remains_usable(tmp_path):
    async def exercise(url):
        async with httpx.AsyncClient(headers=headers(), trust_env=False) as http:
            async with MCPMemoryClient.from_http(url, http_client=http) as client:
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(client.recovery_stats(), timeout=0.02)
                result = await asyncio.wait_for(client.recovery_stats(), timeout=3)
                assert 'result' in result
    with server(tmp_path, 'slow') as url:
        asyncio.run(exercise(url))
