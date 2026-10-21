"""Stable packing defaults and opt-in strategy boundaries through real SQLite."""
import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest

from agent_memory import ForgetMode, ForgetRequest, MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryQuery, MemoryScope
from agent_memory.retrieval.packing import pack_native_evidence
from agent_memory.retrieval.packing_policy import packing_contract


@pytest.mark.parametrize("invalid", ["v1", "V3", "", None, True])
def test_invalid_strategy_is_rejected_even_for_empty_evidence(tmp_path, invalid):
    query = MemoryQuery(MemoryScope("version-test"), "archive")
    with pytest.raises(ValueError, match="packing version"):
        pack_native_evidence(query, (), (), version=invalid)
    with pytest.raises(ValueError, match="packing version"):
        build_local_kernel(tmp_path / "unused.db", packing_version=invalid)
    assert not (tmp_path / "unused.db").exists()


def test_policy_contract_cannot_mutate_runtime_parameters():
    contract = packing_contract("v3")
    contract["cost_weight"] = 999
    assert packing_contract("v3")["cost_weight"] == .10
    assert packing_contract()["experimental"] is False
    assert packing_contract("v3")["experimental"] is True


@pytest.mark.parametrize("version", [None, "v2", "v3", "v4", "v5"])
def test_kernel_and_final_guard_preserve_selected_version_and_erasure(tmp_path, version):
    async def run():
        kwargs = {} if version is None else {"packing_version": version}
        kernel = build_local_kernel(tmp_path / "memory.db", **kwargs)
        await kernel.initialize()
        scope = MemoryScope("version-test", session_id="session")
        try:
            await kernel.ingest_event(MemoryEvent(scope, "user.message",
                "The archive gate is locked.", id="gate"))
            await kernel.ingest_event(MemoryEvent(replace(scope, tenant_id="other"),
                "user.message", "The archive gate is unlocked.", id="foreign"))
            query = MemoryQuery(scope, "archive gate", include_current_state=False)
            before = await kernel.retrieve(query)
            assert before.relevant_memories
            assert all(value.id != "foreign" for value in before.relevant_memories)
            assert before.retrieval_metadata["packing_contract"] == packing_contract(version or "v2")
            await kernel.forget(ForgetRequest(scope, ("gate",), mode=ForgetMode.ERASE))
            after = await kernel.retrieve(replace(query, request_id=str(uuid4())))
            assert not after.relevant_memories
            assert after.retrieval_metadata["packing_contract"] == packing_contract(version or "v2")
        finally:
            await kernel.close()
    asyncio.run(run())
