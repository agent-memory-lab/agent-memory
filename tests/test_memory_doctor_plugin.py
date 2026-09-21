import asyncio
from datetime import UTC, datetime

from agent_memory import (
    MemoryDoctorProvider,
    MemoryDoctorReport,
    MemoryDoctorStatus,
    MemoryScope,
)
from agent_memory.composition import build_local_kernel
from agent_memory.mcp import MCPMemoryTools, MCPRequestContext


class RecordingDoctor:
    def __init__(self) -> None:
        self.scopes: list[MemoryScope | None] = []

    async def inspect(self, scope: MemoryScope | None = None) -> MemoryDoctorReport:
        self.scopes.append(scope)
        return MemoryDoctorReport(
            checked_at=datetime(2026, 1, 1, tzinfo=UTC),
            status=MemoryDoctorStatus.HEALTHY,
            database_fingerprint="123456789abc",
            scope_fingerprint="abcdef123456",
            schema_version=2,
            database_bytes=1,
            counts={"events": 0},
            findings=(),
        )


def test_doctor_is_optional_and_uses_trusted_scope(tmp_path) -> None:
    async def scenario() -> None:
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        scope = MemoryScope(tenant_id="trusted", user_id="user")
        doctor = RecordingDoctor()
        assert isinstance(doctor, MemoryDoctorProvider)
        plain = MCPMemoryTools(provider)
        assert "memory_doctor" not in {tool["name"] for tool in plain.list_tools()}
        tools = MCPMemoryTools(provider, doctor=doctor)
        names = {tool["name"] for tool in tools.list_tools()}
        assert {"memory_doctor", "memory_repair_plan"} <= names
        context = MCPRequestContext(scope=scope)
        report = await tools.call_tool("memory_doctor", {}, context)
        plan = await tools.call_tool("memory_repair_plan", {}, context)
        assert report["report"]["status"] == "healthy"
        assert plan["repair_plan"]["actions"] == []
        assert doctor.scopes == [scope, scope]

    asyncio.run(scenario())
