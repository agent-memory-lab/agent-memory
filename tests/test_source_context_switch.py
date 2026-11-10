"""Explicit context-off configuration through the real SQLite delivery path."""
import asyncio
from dataclasses import replace
from hashlib import sha256
from uuid import uuid4

import pytest

from agent_memory import AgentMemory, ForgetMode, ForgetRequest, MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryChannel, MemoryQuery
from agent_memory.retrieval.packing import pack_native_evidence
from agent_memory.retrieval.packing_policy import packing_contract
from test_ranked_source_context import FOLLOWING, NOW, SCOPE, request, seed


@pytest.mark.parametrize("invalid", [None, 0, 1, "false", "true", [], {}])
@pytest.mark.parametrize("factory", ["kernel", "local", "plugin"])
def test_invalid_switch_fails_before_database_creation(tmp_path, invalid, factory):
    path = tmp_path / "unused.db"
    with pytest.raises(ValueError, match="enable_source_context must be a bool"):
        if factory == "kernel":
            build_local_kernel(path, enable_source_context=invalid)
        elif factory == "local":
            AgentMemory.local(path, scope=SCOPE, enable_source_context=invalid)
        else:
            AgentMemory.from_plugin("sqlite", scope=SCOPE, database_path=path,
                                    enable_source_context=invalid)
    assert not path.exists()


@pytest.mark.parametrize("factory", ["local", "plugin"])
def test_public_factory_preserves_switch_and_role_time_negation_and_span(tmp_path, factory):
    async def run():
        path = tmp_path / "memory.db"
        memory = (AgentMemory.local(path, scope=SCOPE, enable_source_context=False)
                  if factory == "local" else AgentMemory.from_plugin(
                      "sqlite", scope=SCOPE, database_path=path, enable_source_context=False))
        source = "[2026-10-01] assistant: I do not authorize the archive move before Friday."
        async with memory:
            event = MemoryEvent(SCOPE, "assistant.message", source, id="source", occurred_at=NOW)
            await memory.provider.ingest_event(event)
            await memory.provider.ingest_event(MemoryEvent(
                replace(SCOPE, tenant_id="foreign"), "user.message",
                "The archive move is authorized.", id="foreign"))

            async def forbidden(*args, **kwargs):
                pytest.fail("Explicitly disabled context must not be read")

            memory.provider._repository.expand_source_context = forbidden
            bundle = await memory.recall("archive move")
            assert len(bundle.relevant_memories) == 1
            item = bundle.relevant_memories[0]
            assert item.id == event.id and item.text == source and item.occurred_at == NOW
            assert item.metadata["source_revision"] == sha256(source.encode()).hexdigest()
            assert item.metadata["source_span"] == {
                "start": 0, "end": len(source), "unit": "characters"}
            assert bundle.retrieval_metadata["source_context_expansion"] == {
                "policy": "reranked-adjacent-parent/1", "status": "disabled_config",
                "anchor_count": 0, "candidate_count": 0, "semantic_scoring": False}
            assert bundle.retrieval_metadata["packing_contract"] == packing_contract("v2")
            await memory.provider.forget(ForgetRequest(SCOPE, ("source",), mode=ForgetMode.ERASE))
            assert not (await memory.recall("archive move")).relevant_memories
    asyncio.run(run())


@pytest.mark.parametrize("version", ["v2", "v3", "v4", "v5"])
def test_disabled_path_equals_fixed_ranked_only_packing_and_keeps_delivery_guard(tmp_path, version):
    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db", packing_version=version,
                                    enable_source_context=False)
        await kernel.initialize()
        try:
            anchors = await seed(kernel)
            ranked = await kernel._reranker.rerank(request(), anchors)
            expected = pack_native_evidence(request(), (), ranked, version=version)

            async def primary_only(query, limit):
                return anchors

            async def forbidden(*args, **kwargs):
                pytest.fail("Disabled expansion was invoked")

            kernel._repository.search = primary_only
            kernel._repository.expand_source_context = forbidden
            bundle = await kernel.retrieve(request())
            assert [item.text for item in bundle.relevant_memories] == [item.text for item in expected.items]
            assert [item.metadata["source_span"] for item in bundle.relevant_memories] == [
                item.metadata["source_span"] for item in expected.items]
            assert bundle.token_estimate == expected.tokens
            assert bundle.retrieval_metadata["packing_contract"] == packing_contract(version)
            assert bundle.retrieval_metadata["source_context_expansion"]["status"] == "disabled_config"
            stages = bundle.retrieval_metadata["retrieval_stages"]
            assert stages["source_context"]["count"] == 0
            assert stages["packing_input"] == stages["reranked"]
            assert bundle.retrieval_metadata["delivery_guard"]["status"] == "validated"
            # Explicitly disabling adjacent source reads does lose surrounding
            # evidence when that evidence is absent from the ranked pool.
            assert all(FOLLOWING not in item.text for item in bundle.relevant_memories)
        finally:
            await kernel.close()
    asyncio.run(run())


@pytest.mark.parametrize("overrides, status", [
    ({"valid_at": NOW}, "disabled_historical"),
    ({"known_at": NOW}, "disabled_historical"),
    ({"channels": ()}, "disabled_channel"),
    ({"channels": (MemoryChannel.EPISODIC,)}, "disabled_channel"),
])
def test_historical_and_channel_status_precedence_is_preserved(tmp_path, overrides, status):
    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_source_context=False)
        await kernel.initialize()
        try:
            await seed(kernel)
            bundle = await kernel.retrieve(request(**overrides))
            assert bundle.retrieval_metadata["source_context_expansion"]["status"] == status
        finally:
            await kernel.close()
    asyncio.run(run())


def test_default_explicit_true_and_rollback_are_identical(tmp_path):
    async def run():
        path = tmp_path / "memory.db"
        default = build_local_kernel(path)
        enabled = build_local_kernel(path, enable_source_context=True)
        disabled = build_local_kernel(path, enable_source_context=False)
        kernels = (default, enabled, disabled)
        for kernel in kernels:
            await kernel.initialize()
        try:
            anchors = await seed(default)

            async def primary_only(query, limit):
                return anchors

            for kernel in kernels:
                kernel._repository.search = primary_only
            before = await default.retrieve(request())
            off = await disabled.retrieve(request())
            after = await enabled.retrieve(replace(request(), request_id=str(uuid4())))
            assert before.relevant_memories == after.relevant_memories
            assert before.citations == after.citations
            assert {k: v for k, v in before.retrieval_metadata.items() if k != "delivery_guard"} == {
                k: v for k, v in after.retrieval_metadata.items() if k != "delivery_guard"}
            assert {k: v for k, v in before.retrieval_metadata["delivery_guard"].items()
                    if k not in ("valid_at", "known_at")} == {
                k: v for k, v in after.retrieval_metadata["delivery_guard"].items()
                if k not in ("valid_at", "known_at")}
            assert before.token_estimate == after.token_estimate
            assert any(FOLLOWING in item.text for item in before.relevant_memories)
            assert all(FOLLOWING not in item.text for item in off.relevant_memories)
        finally:
            for kernel in kernels:
                await kernel.close()
    asyncio.run(run())


@pytest.mark.parametrize("phase", ["rerank", "trace"])
@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_disabled_context_still_rejects_erase_during_last_await(tmp_path, monkeypatch, phase, mode):
    from test_final_delivery_guard import pause_at
    from agent_memory.retrieval.delivery_guard import bundle_values

    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_source_context=False)
        await kernel.initialize()
        try:
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "The archive meeting is Friday.", id="event"))

            async def erase():
                await kernel.forget(ForgetRequest(SCOPE, ("event",), mode=mode))

            calls = pause_at(kernel, phase, erase, monkeypatch)
            bundle = await kernel.retrieve(request())
            assert calls == [phase]
            assert bundle_values(bundle) == ()
            assert bundle.citations == () and bundle.token_estimate == 0
            assert bundle.retrieval_metadata["citation_spans"] == ()
            assert bundle.retrieval_metadata["source_context_expansion"]["status"] == "disabled_config"
            assert bundle.retrieval_metadata["delivery_guard"]["status"] == "validated"
        finally:
            await kernel.close()
    asyncio.run(run())
