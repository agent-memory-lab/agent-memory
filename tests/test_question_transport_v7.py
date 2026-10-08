"""Exercise real embedded/MCP reads; model arguments never supply authority."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.derived.model import ProcessingGrant
from agent_memory.mcp import MCPMemoryTools, MCPRequestContext

store = project.store


@asynccontextmanager
async def client_for(kernel, scope, service, transport, *, actor=ACTOR):
    sdk = pytest.importorskip("agent_memory_sdk")
    context = MCPRequestContext(scope, actor=actor)
    if transport == "embedded":
        yield sdk.EmbeddedMemoryClient(kernel, context, questions=service)
    else:
        mcp = pytest.importorskip("agent_memory_mcp")
        server = mcp.create_server(
            kernel, mcp.StaticIdentityResolver(context), questions=service,
        )
        async with sdk.MCPMemoryClient(server) as client:
            yield client


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_questions_opt_in_and_zero_model_transport_closed_loop(store, transport):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = runtime(engine, scope, clock)
            item = await project.stage(service.admission, scope)
            await project.qualify(service.admission, *item)
            await register(service, aliases=("Who owns project A?",))
            names = {tool["name"] for tool in MCPMemoryTools(kernel).list_tools()}
            assert "memory_question" not in names
            clock[0] += timedelta(microseconds=100)
            async with client_for(kernel, scope, service, transport) as client:
                capabilities = await client.question_capabilities()
                assert capabilities["templates"] == ["commitments", "owner", "risks", "status"]
                assert capabilities["compute_modes"] == ["full", "delta", "proof_reuse"]
                assert capabilities["historical"] is capabilities["models"] is False
                assert (await client.question_route("Who owns project A?"))["route"] == "question"
                assert (await client.question_route("who owns this?"))["route"] == "abstain"
                conflict = await client.question_route(
                    "Who owns project A?", parameters={"project_id": "other-project"},
                )
                assert conflict["reason"] == "question_parameter_conflict"
                receipt = await client.question_request("project-a:owner", dedupe_key="target")
                assert not (await client.question_status(receipt["target_id"]))["complete"]
                answer = await client.question_answer("Who owns project A?", dedupe_key="answer")
                assert answer["availability_status"] == "valid" and answer["model_calls"] == 0
                assert answer["citations"] and answer["processing_references"]
                assert answer["generation_manifest"] and answer["coverage"]["candidates_complete"]
                assert (await client.question_status(receipt["target_id"]))["complete"]
                assert (await client.question_read("project-a:owner")) == answer
                assert await service.queue.claim("no-duplicate", lease_seconds=30) is None

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_transport_authority_injection_wrong_scope_and_history_fail_closed(store, transport):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = runtime(engine, scope, clock)
            await register(service)
            async with client_for(kernel, scope, service, transport) as client:
                for field, value in {
                    "actor": "host:other", "scope": scope.partition_key(), "readers": [ACTOR],
                    "context": {}, "grant": {}, "purpose": "other", "definition": {},
                }.items():
                    with pytest.raises(sdk.MemoryClientError, match="invalid_question_request"):
                        await client._call("memory_question", {
                            "operation": "read", "payload": {"question_id": "project-a:owner",
                                                               field: value},
                        })
                with pytest.raises(sdk.MemoryClientError, match="question_historical_unsupported"):
                    await client.question_read("project-a:owner", valid_at=clock[0].isoformat())
                with pytest.raises(sdk.MemoryClientError, match="invalid_question_work_budget"):
                    await client.question_answer(
                        "project-a:owner", dedupe_key="bad", max_steps=True,
                    )
                with pytest.raises(sdk.MemoryClientError):
                    await client._call("memory_question", {"operation": "register", "payload": {}})
            async with client_for(kernel, replace(scope, user_id="other"), service, transport) as c:
                with pytest.raises(sdk.MemoryClientError, match="question_scope_mismatch"):
                    await c.question_capabilities()
            async with client_for(kernel, scope, service, transport, actor="other") as client:
                assert (await client.question_route("project-a:owner"))["route"] == "abstain"
                with pytest.raises(sdk.MemoryClientError, match="derived_read_denied"):
                    await client.question_read("project-a:owner")

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
@pytest.mark.parametrize("operation", ["read", "answer"])
def test_transport_rechecks_after_success_before_final_delivery(store, transport, operation,
                                                               monkeypatch):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = runtime(engine, scope, clock)
            item = await project.stage(service.admission, scope)
            await project.qualify(service.admission, *item)
            await register(service)
            clock[0] += timedelta(microseconds=100)
            await service.answer("project-a:owner", actor=ACTOR, dedupe_key="build")
            original = service.read
            calls = []

            async def revoke_after_read(*args, **kwargs):
                result = await original(*args, **kwargs)
                calls.append(result)
                if len(calls) == 1:
                    await service.grant(
                        ProcessingGrant("source", (ACTOR,), revoked=True), expected_version=1,
                    )
                return result

            monkeypatch.setattr(service, "read", revoke_after_read)
            async with client_for(kernel, scope, service, transport) as client:
                with pytest.raises(sdk.MemoryClientError, match="project_processing_denied"):
                    if operation == "answer":
                        await client.question_answer("project-a:owner", dedupe_key="denied")
                    else:
                        await client.question_read("project-a:owner")

    asyncio.run(run())
