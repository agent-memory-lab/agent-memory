"""Execute documented setup with lifecycle probes, without a live database/listener."""

import asyncio
import inspect
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from agent_memory_mcp import GatewayHeaderIdentityResolver
from mcp.server.mcpserver import MCPServer

README = Path(__file__).resolve().parents[1] / "README.md"


def _documented_secret():
    text = README.read_text(encoding="utf-8")
    command = re.search(r'export AGENT_MEMORY_GATEWAY_SECRET="\$\((.+)\)"', text).group(1)
    arguments = shlex.split(command)
    assert arguments[0] == "python"
    return subprocess.check_output([sys.executable, *arguments[1:]], text=True).strip()


def test_documented_secret_generation_passes_resolver_validation():
    secret = _documented_secret()
    assert len(bytes.fromhex(secret)) == 32
    GatewayHeaderIdentityResolver(secret.encode("utf-8"))
    assert _documented_secret() != secret


@pytest.mark.parametrize("failure", [None, "initialize", "serve", "cancel"])
def test_postgres_example_owns_one_loop_and_always_closes(monkeypatch, failure):
    postgres = pytest.importorskip("agent_memory_postgres")
    import agent_memory_mcp

    operations = []
    loops = []
    failure_error = asyncio.CancelledError if failure == "cancel" else RuntimeError

    class Provider:
        async def initialize(self):
            operations.append("initialize")
            loops.append(asyncio.get_running_loop())
            if failure == "initialize":
                raise RuntimeError("startup failed")

        async def close(self):
            operations.append("close")
            loops.append(asyncio.get_running_loop())

    provider = Provider()

    def build_provider(dsn):
        assert dsn == "postgresql://example.invalid/lifecycle-probe"
        loops.append(asyncio.get_running_loop())
        return provider

    class Server:
        async def run_streamable_http_async(self, **options):
            # Validate the README's call against the installed official MCP API.
            inspect.signature(MCPServer.run_streamable_http_async).bind(self, **options)
            assert options == {
                "host": "127.0.0.1", "port": 8000,
                "stateless_http": True, "json_response": True,
            }
            operations.append("serve")
            loops.append(asyncio.get_running_loop())
            if failure in {"serve", "cancel"}:
                raise failure_error("server stopped")

    def create_server(actual_provider, resolver):
        assert actual_provider is provider
        assert isinstance(resolver, GatewayHeaderIdentityResolver)
        assert operations == ["initialize"]
        operations.append("create_server")
        return Server()

    monkeypatch.setenv("AGENT_MEMORY_GATEWAY_SECRET", _documented_secret())
    monkeypatch.setenv("AGENT_MEMORY_POSTGRES_DSN", "postgresql://example.invalid/lifecycle-probe")
    monkeypatch.setattr(postgres, "build_postgres_kernel", build_provider)
    monkeypatch.setattr(agent_memory_mcp, "create_server", create_server)
    section = README.read_text(encoding="utf-8").split("## Another provider", 1)[1]
    code = section.split("```python\n", 1)[1].split("```", 1)[0]
    namespace = {"__name__": "__main__"}
    if failure:
        with pytest.raises(failure_error):
            exec(compile(code, str(README), "exec"), namespace)
    else:
        exec(compile(code, str(README), "exec"), namespace)
    assert operations == (
        ["initialize", "close"] if failure == "initialize"
        else ["initialize", "create_server", "serve", "close"]
    )
    assert all(loop is loops[0] for loop in loops)
    assert loops[0].is_closed()
