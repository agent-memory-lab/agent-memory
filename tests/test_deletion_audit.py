import asyncio
import json

from agent_memory import (
    DeletionAuditService,
    DeletionAuditStatus,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
    SQLiteDeletionAuditSink,
)
from agent_memory.composition import build_local_kernel
from agent_memory.serialization import to_jsonable


SECRET = b"test-only-deletion-audit-secret-32-bytes"


def test_audited_forget_persists_signed_privacy_safe_receipt(tmp_path) -> None:
    async def scenario() -> None:
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        scope = MemoryScope(tenant_id="audit-tenant", user_id="audit-user")
        event = MemoryEvent(scope=scope, event_type="audit.test", content="private")
        await provider.ingest_event(event)
        service = DeletionAuditService(
            SQLiteDeletionAuditSink(tmp_path / "audit.db"), secret=SECRET
        )
        receipt = await service.forget(
            provider,
            ForgetRequest(scope=scope, memory_ids=(event.id,), mode=ForgetMode.ERASE),
            actor="trusted-host",
        )
        assert receipt.status is DeletionAuditStatus.SUCCEEDED
        assert receipt.affected_events == 1
        assert service.verify(receipt)
        encoded = json.dumps(to_jsonable(receipt), sort_keys=True)
        assert event.id not in encoded
        assert scope.partition_key() not in encoded
        assert "trusted-host" not in encoded
        report = await service.report(scope)
        assert report.integrity_verified
        assert report.status_counts["succeeded"] == 1
        assert report.receipts == (receipt,)
        assert (await service.report(MemoryScope(tenant_id="other"))).receipts == ()

    asyncio.run(scenario())


def test_provider_failure_is_recorded_without_error_details(tmp_path) -> None:
    class FailingProvider:
        async def forget(self, request):
            raise RuntimeError("secret database path")

    async def scenario() -> None:
        scope = MemoryScope(tenant_id="audit-failure")
        service = DeletionAuditService(
            SQLiteDeletionAuditSink(tmp_path / "audit.db"), secret=SECRET
        )
        try:
            await service.forget(
                FailingProvider(),
                ForgetRequest(scope=scope, all_in_scope=True),
                actor="host",
            )
        except RuntimeError:
            pass
        report = await service.report(scope)
        assert report.receipts[0].status is DeletionAuditStatus.FAILED
        assert report.receipts[0].failure_code == "provider_error"
        assert "secret database path" not in json.dumps(to_jsonable(report))

    asyncio.run(scenario())
