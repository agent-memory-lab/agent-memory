from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory.mcp import MCPRequestContext

from .identity import GatewayHeaderIdentityResolver, StaticIdentityResolver
from .server import create_server


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Agent Memory MCP v2 server")
    result.add_argument("--transport", choices=("stdio", "streamable-http"), default="stdio")
    result.add_argument("--database", type=Path, default=Path("agent-memory.db"))
    result.add_argument("--host", default="127.0.0.1")
    result.add_argument("--port", type=int, default=8000)
    result.add_argument("--tenant-id")
    result.add_argument("--namespace", default="default")
    result.add_argument("--user-id")
    result.add_argument("--agent-id")
    result.add_argument("--workspace-id")
    result.add_argument("--session-id")
    result.add_argument("--actor", default="agent")
    result.add_argument("--can-erase", action="store_true")
    result.add_argument("--gateway-secret-env", default="AGENT_MEMORY_GATEWAY_SECRET")
    result.add_argument("--allow-insecure-single-tenant-http", action="store_true")
    return result


def _static_resolver(args: argparse.Namespace) -> StaticIdentityResolver:
    if not args.tenant_id:
        raise SystemExit("--tenant-id is required for stdio or fixed-scope HTTP")
    return StaticIdentityResolver(MCPRequestContext(
        scope=MemoryScope(
            tenant_id=args.tenant_id,
            namespace=args.namespace,
            user_id=args.user_id,
            agent_id=args.agent_id,
            workspace_id=args.workspace_id,
            session_id=args.session_id,
        ),
        actor=args.actor,
        can_erase=args.can_erase,
    ))


def main() -> None:
    args = parser().parse_args()
    provider = build_local_kernel(args.database)
    asyncio.run(provider.initialize())
    if args.transport == "stdio":
        resolver = _static_resolver(args)
    else:
        secret = os.environ.get(args.gateway_secret_env, "").encode("utf-8")
        if secret:
            resolver = GatewayHeaderIdentityResolver(secret)
        elif args.allow_insecure_single_tenant_http:
            resolver = _static_resolver(args)
        else:
            raise SystemExit(
                "Streamable HTTP requires a gateway HMAC secret; the fixed-scope "
                "override is for isolated development only"
            )
    server = create_server(provider, resolver)
    if args.transport == "stdio":
        server.run(transport="stdio")
    else:
        server.run(
            transport="streamable-http",
            host=args.host,
            port=args.port,
            stateless_http=True,
            json_response=True,
        )


if __name__ == "__main__":
    main()

