"""In-image MCP checks. Synthetic data only; no model or external service calls.

The default roundtrip is ephemeral. Lifecycle phases use a caller-owned directory
and are orchestrated across separate containers by container_lifecycle_smoke.py.
"""

import argparse
import asyncio
import hashlib
import hmac
import importlib.metadata
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from mcp import Client, StdioServerParameters

TENANT = "container-smoke"
USER = "smoke-user"
PRIVATE_TEXT = "CONTAINER_SMOKE_PRIVATE: the user prefers concise answers."
QUERY = {"text": "How should I answer the user?", "token_budget": 600}
FORGET = {"all_in_scope": True, "mode": "erase"}


def parameters(directory: Path, *, tenant=TENANT, user=USER, erase=False):
    args = [
        "-m",
        "agent_memory_mcp.cli",
        "--transport",
        "stdio",
        "--database",
        str(directory / "memory.db"),
        "--tenant-id",
        tenant,
        "--user-id",
        user,
        "--session-id",
        "smoke-session",
    ]
    if erase:
        args.append("--can-erase")
    return StdioServerParameters(command=sys.executable, args=args)


async def check_visibility(client, *, present):
    for tool, arguments in (("memory_get_state", {}), ("memory_retrieve", QUERY)):
        result = await client.call_tool(tool, arguments)
        assert not result.is_error, result
        claims = result.structured_content["current_state"]
        found = any(
            claim["key"] == "answer.style" and claim["value"] == "concise" for claim in claims
        )
        assert found is present, (tool, result)
        if not present:
            assert PRIVATE_TEXT not in json.dumps(result.structured_content), result


async def stdio_phase(directory: Path, phase="roundtrip") -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    async with (
        asyncio.timeout(120),
        Client(
            parameters(directory, erase=phase == "erase"),
            mode="legacy",
        ) as client,
    ):
        assert client.session.initialize_result is not None
        tools = {tool.name for tool in (await client.list_tools()).tools}
        assert {"memory_ingest", "memory_get_state", "memory_retrieve", "memory_forget"} <= tools
        if phase in {"roundtrip", "seed", "seed-hold"}:
            written = await client.call_tool(
                "memory_ingest",
                {
                    "event_type": "container.smoke",
                    "content": PRIVATE_TEXT,
                    "idempotency_key": "container-smoke-1",
                    "metadata": {
                        "claims": [
                            {
                                "key": "answer.style",
                                "value": "concise",
                                "text": PRIVATE_TEXT,
                                "scope": "user",
                                "confidence": 0.99,
                            }
                        ]
                    },
                },
            )
            assert not written.is_error, written
            await check_visibility(client, present=True)
            if phase == "seed-hold":
                # The host kills this container only after an acknowledged committed write.
                print(json.dumps({"status": "checkpoint", "phase": phase}), flush=True)
                await asyncio.Event().wait()
        elif phase == "erase":
            await check_visibility(client, present=True)
            result = await client.call_tool("memory_forget", FORGET)
            assert not result.is_error, result
            await check_visibility(client, present=False)
        else:
            await check_visibility(client, present=phase != "erased")
        if phase == "verify":
            result = await client.call_tool(
                "memory_retrieve", {**QUERY, "scope": {"tenant_id": "foreign"}}
            )
            assert result.is_error, "model-provided scope must be rejected"
            result = await client.call_tool("memory_forget", FORGET)
            assert result.is_error, "erase requires host authorization"
            await check_visibility(client, present=True)
        evidence = {"protocol": client.protocol_version, "tool_count": len(tools), "phase": phase}
    if phase == "verify":
        for scope in ({"tenant": "foreign"}, {"user": "foreign"}):
            async with (
                asyncio.timeout(120),
                Client(parameters(directory, **scope), mode="legacy") as client,
            ):
                await check_visibility(client, present=False)
    return evidence


async def http_checks(directory: Path) -> dict:
    """Actual CLI loopback HTTP with gateway signatures, never the insecure override."""
    import httpx2
    from agent_memory_mcp.identity import canonical_identity_payload
    from mcp.client.streamable_http import streamable_http_client

    secret = secrets.token_hex(32)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    url = f"http://127.0.0.1:{port}/mcp"
    env = {**os.environ, "AGENT_MEMORY_GATEWAY_SECRET": secret}
    with (directory / "http.log").open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agent_memory_mcp.cli",
                "--transport",
                "streamable-http",
                "--database",
                str(directory / "memory.db"),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            env=env,
            stdout=log,
            stderr=log,
        )
        try:
            async with httpx2.AsyncClient(trust_env=False, timeout=1) as http:
                async with asyncio.timeout(120):
                    while True:
                        assert process.poll() is None, "HTTP server exited; inspect http.log"
                        try:
                            async with http.stream("GET", url):
                                pass
                            break
                        except httpx2.TransportError:
                            await asyncio.sleep(0.05)
            for mode in ("valid", "missing", "invalid", "expired", "foreign"):
                headers = {
                    "x-agent-memory-tenant-id": "foreign" if mode == "foreign" else TENANT,
                    "x-agent-memory-user-id": USER,
                    "x-agent-memory-session-id": "smoke-session",
                    "x-agent-memory-namespace": "default",
                    "x-agent-memory-timestamp": str(
                        int(time.time()) - (3600 if mode == "expired" else 0)
                    ),
                }
                headers["x-agent-memory-signature"] = hmac.new(
                    secret.encode(),
                    canonical_identity_payload(headers),
                    hashlib.sha256,
                ).hexdigest()
                if mode == "invalid":
                    headers["x-agent-memory-signature"] = "invalid"
                elif mode == "missing":
                    headers = {}
                async with (
                    asyncio.timeout(120),
                    httpx2.AsyncClient(headers=headers, trust_env=False) as http,
                ):
                    async with Client(
                        streamable_http_client(url, http_client=http), mode="legacy"
                    ) as client:
                        if mode in {"valid", "foreign"}:
                            await check_visibility(client, present=mode == "valid")
                        else:
                            result = await client.call_tool("memory_get_state", {})
                            assert result.is_error, (mode, result)
            return {"gateway_identity_cases": 5, "transport": "loopback-http"}
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument(
        "--phase",
        choices=("roundtrip", "seed", "seed-hold", "verify", "erase", "erased"),
        default="roundtrip",
    )
    parser.add_argument("--data-dir", type=Path)
    args = parser.parse_args()
    if args.phase != "roundtrip" and args.data_dir is None:
        parser.error("lifecycle phases require --data-dir")
    assert sys.version_info[:2] == (3, 13), sys.version
    assert os.getuid() == 10001 and os.getgid() == 10001
    assert Path.cwd() == Path("/data")
    versions = {
        name: importlib.metadata.version(name) for name in ("agent-memory", "agent-memory-mcp")
    }
    assert set(versions.values()) == {args.expected_version}, versions
    assert not any(Path(path).exists() for path in ("/build", "/wheels", "/data/.git"))
    if args.data_dir is None:
        with tempfile.TemporaryDirectory(prefix="container-smoke-", dir="/data") as directory:
            result = asyncio.run(stdio_phase(Path(directory)))
    else:
        result = asyncio.run(stdio_phase(args.data_dir, args.phase))
        if args.phase == "verify":
            result["http"] = asyncio.run(http_checks(args.data_dir))
    print(json.dumps({"status": "passed", "versions": versions, "stdio": result}))


if __name__ == "__main__":
    main()
