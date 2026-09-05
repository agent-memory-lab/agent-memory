from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from hmac import compare_digest, new as new_hmac
from time import time
from typing import Mapping, Protocol

from agent_memory.domain import MemoryScope
from agent_memory.mcp import MCPRequestContext
from mcp.server.mcpserver import Context

IDENTITY_HEADER_PREFIX = "x-agent-memory-"
IDENTITY_FIELDS = (
    "tenant-id",
    "namespace",
    "user-id",
    "agent-id",
    "workspace-id",
    "session-id",
    "actor",
    "can-erase",
    "timestamp",
)


class IdentityError(PermissionError):
    pass


class IdentityResolver(Protocol):
    async def resolve(self, context: Context) -> MCPRequestContext: ...


@dataclass(frozen=True, slots=True)
class StaticIdentityResolver:
    """Process-bound identity for stdio or an explicitly isolated HTTP server."""

    request_context: MCPRequestContext

    async def resolve(self, context: Context) -> MCPRequestContext:
        return self.request_context


def canonical_identity_payload(headers: Mapping[str, str]) -> bytes:
    normalized = {str(key).lower(): str(value) for key, value in headers.items()}
    lines = [
        f"{field}={normalized.get(IDENTITY_HEADER_PREFIX + field, '')}"
        for field in IDENTITY_FIELDS
    ]
    return "\n".join(lines).encode("utf-8")


@dataclass(frozen=True, slots=True)
class GatewayHeaderIdentityResolver:
    """Verify identity headers written by a trusted authentication gateway."""

    secret: bytes
    max_clock_skew_seconds: int = 60

    def __post_init__(self) -> None:
        if len(self.secret) < 32:
            raise ValueError("gateway HMAC secret must contain at least 32 bytes")
        if self.max_clock_skew_seconds < 1:
            raise ValueError("max_clock_skew_seconds must be positive")

    async def resolve(self, context: Context) -> MCPRequestContext:
        headers = {str(key).lower(): str(value) for key, value in (context.headers or {}).items()}
        timestamp_text = headers.get(IDENTITY_HEADER_PREFIX + "timestamp", "")
        signature = headers.get(IDENTITY_HEADER_PREFIX + "signature", "")
        try:
            timestamp = int(timestamp_text)
        except ValueError as exc:
            raise IdentityError("missing or invalid signed identity timestamp") from exc
        if abs(int(time()) - timestamp) > self.max_clock_skew_seconds:
            raise IdentityError("signed identity has expired")

        expected = new_hmac(
            self.secret,
            canonical_identity_payload(headers),
            sha256,
        ).hexdigest()
        if not signature or not compare_digest(signature, expected):
            raise IdentityError("invalid signed identity")

        tenant_id = headers.get(IDENTITY_HEADER_PREFIX + "tenant-id", "").strip()
        if not tenant_id:
            raise IdentityError("signed identity is missing tenant-id")
        can_erase = headers.get(IDENTITY_HEADER_PREFIX + "can-erase", "false").lower()
        if can_erase not in {"true", "false"}:
            raise IdentityError("signed can-erase must be true or false")

        def optional(name: str) -> str | None:
            return headers.get(IDENTITY_HEADER_PREFIX + name) or None

        return MCPRequestContext(
            scope=MemoryScope(
                tenant_id=tenant_id,
                namespace=headers.get(IDENTITY_HEADER_PREFIX + "namespace", "default"),
                user_id=optional("user-id"),
                agent_id=optional("agent-id"),
                workspace_id=optional("workspace-id"),
                session_id=optional("session-id"),
            ),
            actor=headers.get(IDENTITY_HEADER_PREFIX + "actor", "agent"),
            can_erase=can_erase == "true",
        )
