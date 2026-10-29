"""Run inside the built image: installed versions, non-root data, and real stdio MCP.

Example: docker run --rm -i --entrypoint python IMAGE -I - \
    --expected-version 0.1.1 < tools/container_smoke.py
This uses deterministic claims and makes no model or external service calls.
"""

import argparse
import asyncio
import importlib.metadata
import json
import os
import sys
import tempfile
from pathlib import Path

from mcp import Client, StdioServerParameters


async def stdio_roundtrip(directory: Path) -> dict:
    parameters = StdioServerParameters(
        command="agent-memory-mcp",
        args=[
            "--transport", "stdio",
            "--database", str(directory / "memory.db"),
            "--tenant-id", "container-smoke",
            "--user-id", "smoke-user",
            "--session-id", "smoke-session",
        ],
    )
    # Exercise the initialize handshake explicitly, even when the SDK supports discover.
    async with asyncio.timeout(45), Client(parameters, mode="legacy") as client:
        assert client.session.initialize_result is not None
        tools = {tool.name for tool in (await client.list_tools()).tools}
        assert {"memory_ingest", "memory_get_state", "memory_retrieve"} <= tools
        written = await client.call_tool("memory_ingest", {
            "event_type": "container.smoke",
            "content": "The user prefers concise answers.",
            "idempotency_key": "container-smoke-1",
            "metadata": {"claims": [{
                "key": "answer.style",
                "value": "concise",
                "text": "The user prefers concise answers.",
                "scope": "user",
            }]},
        })
        assert not written.is_error, written
        state = await client.call_tool("memory_get_state", {})
        assert not state.is_error, state
        assert any(
            claim["key"] == "answer.style" and claim["value"] == "concise"
            for claim in state.structured_content["current_state"]
        ), state
        recalled = await client.call_tool("memory_retrieve", {
            "text": "How should I answer the user?", "token_budget": 600,
        })
        assert not recalled.is_error, recalled
        assert any(
            claim["key"] == "answer.style" and claim["value"] == "concise"
            for claim in recalled.structured_content["current_state"]
        ), recalled
        return {"protocol": client.protocol_version, "tool_count": len(tools)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()
    assert sys.version_info[:2] == (3, 13), sys.version
    assert os.getuid() == 10001 and os.getgid() == 10001
    assert Path.cwd() == Path("/data")
    versions = {
        name: importlib.metadata.version(name)
        for name in ("agent-memory", "agent-memory-mcp")
    }
    assert set(versions.values()) == {args.expected_version}, versions
    assert not any(Path(path).exists() for path in ("/build", "/wheels", "/data/.git"))
    with tempfile.TemporaryDirectory(prefix="container-smoke-", dir="/data") as directory:
        result = asyncio.run(stdio_roundtrip(Path(directory)))
    print(json.dumps({"status": "passed", "versions": versions, "stdio": result}))


if __name__ == "__main__":
    main()
