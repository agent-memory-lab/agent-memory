"""Exercise registered MCP tools and stdio, rather than the core adapter alone."""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from agent_memory_mcp import StaticIdentityResolver, create_server
from agent_memory_sdk import EmbeddedMemoryClient, MCPMemoryClient, MemoryClientError
from mcp import Client

from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory.mcp import MCPMemoryTools, MCPRequestContext
from agent_memory.ontology.api import OntologyAPI
from agent_memory.operations.deletion_audit import DeletionAuditService, SQLiteDeletionAuditSink
from agent_memory.operations.doctor import SQLiteMemoryDoctor

SCOPE = MemoryScope("mcp-contract", user_id="owner", session_id="session")
CONTEXT = MCPRequestContext(SCOPE, actor="trusted-host")


class CapabilitiesProvider:
    """A provider that deliberately declines selected optional contracts."""

    def __init__(self, delegate, **capabilities):
        self.delegate = delegate
        self.capabilities = capabilities

    def manifest(self):
        manifest = self.delegate.manifest()
        return replace(manifest, capabilities=replace(manifest.capabilities, **self.capabilities))

    def __getattr__(self, name):
        return getattr(self.delegate, name)


@asynccontextmanager
async def connected_clients(tmp_path, transport):
    path = tmp_path / "memory.db"
    provider = build_local_kernel(path)
    embedded = EmbeddedMemoryClient(provider, CONTEXT)
    await embedded.initialize()
    if transport == "stdio":
        root = Path(__file__).resolve().parents[3]
        pythonpath = os.pathsep.join(
            str(root / path)
            for path in (
                "src",
                "packages/mcp-server/src",
                "packages/python-sdk/src",
            )
        )
        remote = MCPMemoryClient.from_stdio(
            sys.executable,
            env={"PYTHONPATH": pythonpath},
            args=[
                "-m",
                "agent_memory_mcp.cli",
                "--database",
                str(path),
                "--tenant-id",
                SCOPE.tenant_id,
                "--user-id",
                SCOPE.user_id,
                "--session-id",
                SCOPE.session_id,
                "--actor",
                CONTEXT.actor,
            ],
        )
    else:
        remote = MCPMemoryClient(create_server(provider, StaticIdentityResolver(CONTEXT)))
    async with remote:
        yield embedded, remote


@pytest.mark.parametrize(
    "disabled",
    [
        {},
        {"bitemporal_claims": False},
        {"memory_blocks": False},
        {"decision_lineage": False},
        {"outcome_feedback": False},
        {
            "bitemporal_claims": False,
            "memory_blocks": False,
            "decision_lineage": False,
            "outcome_feedback": False,
        },
    ],
)
def test_registered_tools_match_capability_filtered_core_contract(tmp_path, disabled):
    async def scenario():
        provider = CapabilitiesProvider(build_local_kernel(tmp_path / "memory.db"), **disabled)
        await provider.initialize()
        expected = {item["name"]: item for item in MCPMemoryTools(provider).list_tools()}
        async with Client(create_server(provider, StaticIdentityResolver(CONTEXT))) as client:
            registered = {item.name: item for item in (await client.list_tools()).tools}
            assert registered.keys() == expected.keys()
            for name, spec in expected.items():
                assert registered[name].input_schema == spec["inputSchema"]
                assert registered[name].description == spec["description"]
            manifest = await client.call_tool("memory_capabilities", {})
            assert manifest.is_error is False
            for name, value in disabled.items():
                assert manifest.structured_content["capabilities"][name] is value
            if not disabled.get("bitemporal_claims", True):
                result = await client.call_tool(
                    "memory_retrieve",
                    {
                        "text": "style",
                        "valid_at": "2000-01-01T00:00:00Z",
                    },
                )
                assert result.is_error is True
                assert "unsupported_capability" in result.content[0].text
            for tool in ("memory_block_search", "memory_record_decision"):
                if tool not in expected:
                    result = await client.call_tool(tool, {"text": "anything", "action": "answer"})
                    assert result.is_error is True

    asyncio.run(scenario())


def test_optional_tools_retain_the_core_schema(tmp_path):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        options = {
            "doctor": SQLiteMemoryDoctor(tmp_path / "memory.db"),
            "deletion_auditor": DeletionAuditService(
                SQLiteDeletionAuditSink(tmp_path / "audit.db"),
                secret=b"test-only-mcp-contract-secret-000000",
            ),
            "ontology": OntologyAPI(None, None, "contract", switch_policy=object()),
            "derived": object(),
        }
        expected = {item["name"]: item for item in MCPMemoryTools(provider, **options).list_tools()}
        async with Client(
            create_server(provider, StaticIdentityResolver(CONTEXT), **options)
        ) as client:
            registered = {item.name: item for item in (await client.list_tools()).tools}
            assert registered.keys() == expected.keys()
            for name, spec in expected.items():
                assert registered[name].input_schema == spec["inputSchema"]

    asyncio.run(scenario())


@pytest.mark.parametrize("transport", ["in-process", "stdio"])
def test_temporal_sdk_queries_preserve_both_axes_and_validation(tmp_path, transport):
    async def scenario():
        async with connected_clients(tmp_path, transport) as (embedded, remote):
            for year, value in ((2001, "concise"), (2010, "detailed")):
                await remote.ingest(
                    "user.message",
                    f"I prefer {value} answers",
                    metadata={
                        "claims": [
                            {
                                "key": "style",
                                "value": value,
                                "text": f"I prefer {value} answers",
                                "valid_from": f"{year}-01-01T00:00:00Z",
                            }
                        ],
                    },
                )
                if year == 2001:
                    known_between = datetime.now(UTC).isoformat()
            now = datetime.now(UTC).isoformat()
            current = await remote.retrieve("style")
            assert [claim["value"] for claim in current["current_state"]] == ["detailed"]
            queries = [
                ({"valid_at": "2000-01-01T00:00:00Z"}, []),
                ({"known_at": "2000-01-01T00:00:00Z"}, []),
                ({"valid_at": "2005-01-01T00:00:00+00:00"}, ["concise"]),
                ({"valid_at": "2005-01-01T08:00:00+08:00", "known_at": now}, ["concise"]),
                ({"valid_at": now, "known_at": "2000-01-01T00:00:00Z"}, []),
                ({"valid_at": now, "known_at": known_between}, ["concise"]),
            ]
            for arguments, values in queries:
                expected = await embedded.retrieve("style", **arguments)
                actual = await remote.retrieve("style", **arguments)
                assert [claim["value"] for claim in actual["current_state"]] == values
                assert actual["current_state"] == expected["current_state"]
                for field in ("relevant_memories", "episodes", "procedures", "citations"):
                    assert actual[field] == expected[field]
            for field in ("valid_at", "known_at"):
                for value in ("not-a-date", "2026-01-01T00:00:00"):
                    errors = []
                    for client in (embedded, remote):
                        with pytest.raises(MemoryClientError) as caught:
                            await client.retrieve("style", **{field: value})
                        errors.append(caught.value.to_dict())
                    assert errors[0] == errors[1]
                    assert errors[0]["code"] == "invalid_request"

    asyncio.run(scenario())


@pytest.mark.parametrize("transport", ["in-process", "stdio"])
def test_block_and_feedback_sdk_operations_round_trip(tmp_path, transport):
    async def scenario():
        async with connected_clients(tmp_path, transport) as (embedded, remote):
            source = await remote.ingest("user.message", "Remember the project outline")
            block = (
                await remote.write_block(
                    title="Project outline",
                    content="The project uses concise notes.",
                    event_ids=[source["event_id"]],
                    token_budget=64,
                )
            )["block"]
            block_id = block["id"]
            assert (await remote.read_block(block_id)) == (await embedded.read_block(block_id))
            assert (await remote.search_blocks("concise"))["blocks"][0]["id"] == block_id
            updated = (
                await remote.write_block(
                    block_id=block_id,
                    title=block["title"],
                    content="The project uses detailed notes.",
                    event_ids=[source["event_id"]],
                    expected_version=block["version"],
                    token_budget=64,
                )
            )["block"]
            assert updated["version"] == block["version"] + 1
            assert (await remote.search_blocks("detailed", channels=["semantic"]))["blocks"]
            assert (await remote.forget_block(block_id))["affected_artifacts"] == 1
            assert (await remote.search_blocks("detailed"))["blocks"] == []
            with pytest.raises(MemoryClientError, match="legal erase"):
                await remote.forget_block(block_id, mode="erase")

            decision = await remote.record_decision(
                "answer",
                record_id="decision",
                memory_usage="none",
                run_id="run",
                policy_version="policy-1",
                context_hash="sha256:context",
                idempotency_key="decision-idempotency",
            )
            outcome = await remote.record_outcome(
                decision["record_id"],
                "accepted",
                True,
                record_id="outcome",
                score=1.0,
                metrics={"task_success": 1.0},
                run_id="run",
                termination_reason="complete",
                idempotency_key="outcome-idempotency",
            )
            evaluation = await remote.record_evaluation(
                outcome["record_id"],
                record_id="evaluation",
                evaluator_id="host",
                evaluator_version="1",
                rubric_id="quality",
                rubric_version="1",
                metrics={"quality": 1.0},
                evidence_digest="sha256:evidence",
                idempotency_key="evaluation-idempotency",
            )
            reward = await remote.record_reward(
                outcome["record_id"],
                1.0,
                "formula-1",
                record_id="reward",
                evaluation_id=evaluation["record_id"],
                reward_definition_id="task-success",
                components={"quality": 1.0},
                idempotency_key="reward-idempotency",
            )
            for kind, result in (
                ("decision", decision),
                ("outcome", outcome),
                ("evaluation", evaluation),
                ("reward", reward),
            ):
                assert result["record_id"] == kind
                status = await remote.feedback_status(result["record_id"], kind)
                assert status == await embedded.feedback_status(result["record_id"], kind)
                assert status["receipt"]["status"] == "accepted"

    asyncio.run(scenario())


def test_wrapper_rejects_unrecognized_arguments_instead_of_discarding_them(tmp_path):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        async with Client(create_server(provider, StaticIdentityResolver(CONTEXT))) as client:
            for arguments in (
                {"text": "style", "valid_as_of": "2000-01-01T00:00:00Z"},
                {"text": "style", "scope": {"tenant_id": "foreign"}},
            ):
                result = await client.call_tool("memory_retrieve", arguments)
                assert result.is_error is True
            result = await client.call_tool(
                "memory_record_decision",
                {
                    "action": "answer",
                    "tenant_id": "foreign",
                },
            )
            assert result.is_error is True

    asyncio.run(scenario())


def test_new_tools_preserve_scope_erase_and_host_evaluator_permissions(tmp_path):
    async def scenario():
        provider = build_local_kernel(
            tmp_path / "memory.db",
            trusted_evaluator_ids={"trusted-evaluator"},
        )
        await provider.initialize()
        async with MCPMemoryClient(
            create_server(provider, StaticIdentityResolver(CONTEXT))
        ) as owner:
            source = await owner.ingest("user.message", "Private project outline")
            block = (
                await owner.write_block(
                    title="Private project",
                    content="Private owner notes",
                    event_ids=[source["event_id"]],
                )
            )["block"]
            decision = await owner.record_decision("answer", memory_usage="none")
            outcome = await owner.record_outcome(decision["record_id"], "accepted", True)
            evaluation = {
                "evaluator_version": "1",
                "rubric_id": "quality",
                "rubric_version": "1",
                "metrics": {"quality": 1.0},
                "evidence_digest": "sha256:host",
            }
            with pytest.raises(MemoryClientError):
                await owner.record_evaluation(
                    outcome["record_id"],
                    evaluator_id="untrusted-model",
                    record_id="rejected-evaluation",
                    **evaluation,
                )
            assert (await owner.feedback_status("rejected-evaluation", "evaluation"))[
                "receipt"
            ] is None
            accepted = await owner.record_evaluation(
                outcome["record_id"],
                evaluator_id="trusted-evaluator",
                **evaluation,
            )
            assert (await owner.feedback_status(accepted["record_id"], "evaluation"))["receipt"]
            with pytest.raises(MemoryClientError, match="legal erase"):
                await owner.forget_block(block["id"], mode="erase")
            assert (await owner.read_block(block["id"]))["block"] is not None

        foreign = MCPRequestContext(
            MemoryScope(
                SCOPE.tenant_id,
                user_id="intruder",
                session_id="other-session",
            ),
            can_erase=True,
        )
        async with MCPMemoryClient(
            create_server(provider, StaticIdentityResolver(foreign))
        ) as intruder:
            assert (await intruder.read_block(block["id"]))["block"] is None
            assert (await intruder.search_blocks("Private"))["blocks"] == []
            assert (await intruder.feedback_status(decision["record_id"], "decision"))[
                "receipt"
            ] is None
            with pytest.raises(MemoryClientError):
                await intruder.record_outcome(decision["record_id"], "stolen", True)
            assert (await intruder.forget_block(block["id"], mode="erase"))[
                "affected_artifacts"
            ] == 0

        authorized = replace(CONTEXT, can_erase=True)
        async with MCPMemoryClient(
            create_server(provider, StaticIdentityResolver(authorized))
        ) as owner:
            assert (await owner.forget_block(block["id"], mode="erase"))["affected_artifacts"] == 1
            assert (await owner.read_block(block["id"]))["block"] is None

    asyncio.run(scenario())


@pytest.mark.parametrize("drift", ["missing_tool", "missing_argument"])
def test_registration_fails_instead_of_silently_drifting_from_core(tmp_path, monkeypatch, drift):
    original = MCPMemoryTools.list_tools

    def changed_contract(tools):
        definitions = original(tools)
        if drift == "missing_tool":
            return (
                *definitions,
                {
                    "name": "memory_unwrapped",
                    "description": "Future core operation",
                    "inputSchema": {"type": "object", "properties": {}, "required": []},
                },
            )
        retrieve = next(item for item in definitions if item["name"] == "memory_retrieve")
        retrieve["inputSchema"]["properties"]["future_filter"] = {"type": "string"}
        return definitions

    monkeypatch.setattr(MCPMemoryTools, "list_tools", changed_contract)
    provider = build_local_kernel(tmp_path / "memory.db")
    with pytest.raises(ValueError, match="MCP wrapper"):
        create_server(provider, StaticIdentityResolver(CONTEXT))


def test_new_mutation_tools_do_not_bypass_recovery_gate(tmp_path):
    from agent_memory.context.transport import RecoveryTransport
    from agent_memory.unified_memory import UnifiedMemory

    class AllowAll:
        async def authorize(self, context, operation):
            return True

    async def scenario():
        memory = UnifiedMemory.local(
            tmp_path / "memory.db",
            SCOPE,
            recovery_path=tmp_path / "recovery.db",
        )
        await memory.initialize()
        try:
            server = create_server(
                memory.provider,
                StaticIdentityResolver(CONTEXT),
                recovery_tools=RecoveryTransport(memory, AllowAll()),
            )
            async with MCPMemoryClient(server) as client:
                with pytest.raises(MemoryClientError) as caught:
                    await client.write_block(
                        title="Blocked", content="Blocked", event_ids=["source"]
                    )
                assert caught.value.code == "recovery_gate_required"
                with pytest.raises(MemoryClientError) as caught:
                    await client.record_decision("blocked")
                assert caught.value.code == "recovery_gate_required"
                assert (await client.search_blocks("Blocked"))["blocks"] == []
        finally:
            await memory.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("transport", ["in-process", "stdio"])
def test_invalid_block_and_feedback_json_types_are_identical_and_never_write(tmp_path, transport):
    import sqlite3
    from contextlib import closing

    def snapshot():
        with closing(sqlite3.connect(tmp_path / "memory.db")) as connection:
            return tuple(connection.iterdump())

    async def scenario():
        async with connected_clients(tmp_path, transport) as (embedded, remote):
            source = await remote.ingest("user.message", "Contract source")
            block = (
                await remote.write_block(
                    title="Contract",
                    content="Private source notes",
                    event_ids=[source["event_id"]],
                )
            )["block"]
            decision = await remote.record_decision("answer", memory_usage="none")
            outcome = await remote.record_outcome(decision["record_id"], "done", True)
            base = {
                "memory_block_read": {"block_id": block["id"]},
                "memory_block_write": {
                    "title": "Contract",
                    "content": "Updated notes",
                    "event_ids": [source["event_id"]],
                    "block_id": block["id"],
                    "expected_version": block["version"],
                },
                "memory_block_search": {"text": "Private"},
                "memory_block_forget": {"block_id": block["id"]},
                "memory_record_decision": {"action": "answer", "record_id": "invalid-decision"},
                "memory_record_outcome": {
                    "decision_id": decision["record_id"],
                    "outcome": "done",
                    "success": True,
                    "record_id": "invalid-outcome",
                },
                "memory_record_evaluation": {
                    "outcome_id": outcome["record_id"],
                    "evaluator_id": "host",
                    "evaluator_version": "1",
                    "rubric_id": "quality",
                    "rubric_version": "1",
                    "metrics": {"quality": 1.0},
                    "evidence_digest": "sha256:evidence",
                    "record_id": "invalid-evaluation",
                },
                "memory_record_reward": {
                    "outcome_id": outcome["record_id"],
                    "value": 1.0,
                    "formula_version": "1",
                    "record_id": "invalid-reward",
                },
                "memory_feedback_status": {
                    "record_id": decision["record_id"],
                    "record_type": "decision",
                },
            }
            mutations = {
                "memory_block_read": {"block_id": [1, [block["id"]]]},
                "memory_block_write": {
                    "title": [1],
                    "event_ids": ['["source"]', [1]],
                    "token_budget": [True, "64", 64.5, 15, 4097],
                    "expected_version": [False, "1", 1.5, -1],
                    "metadata": ["{}", []],
                    "scope_level": ["invalid"],
                },
                "memory_block_search": {
                    "limit": [True, "8", 1.5, 0, 101],
                    "channels": ['["semantic"]', [1]],
                },
                "memory_block_forget": {"block_id": [1], "mode": [["erase"], True]},
                "memory_record_decision": {
                    "action": [1],
                    "memory_ids": ["[]", [1]],
                    "procedure_ids": ["[]", [1]],
                    "context_hash": [False],
                    "record_id": [1],
                },
                "memory_record_outcome": {
                    "success": ["false", "true", "null", 1, 0, [], {}],
                    "score": [True, "0.5", -0.1, 1.1],
                    "metrics": ["{}", []],
                },
                "memory_record_evaluation": {"metrics": ["{}", []], "rubric_version": [1]},
                "memory_record_reward": {
                    "value": [True, "1.0", []],
                    "components": ["{}", []],
                    "formula_version": [1],
                },
                "memory_feedback_status": {"record_type": [1], "record_id": [["decision"]]},
            }
            before = snapshot()
            for name, fields in mutations.items():
                for field, values in fields.items():
                    for value in values:
                        arguments = {**base[name], field: value}
                        errors = []
                        for client in (embedded, remote):
                            with pytest.raises(MemoryClientError) as caught:
                                await client._call(name, arguments)
                            errors.append(caught.value.to_dict())
                        assert errors[0] == errors[1], (name, field, value, errors)
                        assert errors[0]["code"] == "invalid_request"
                        assert errors[0]["field"].startswith(field)
                        assert snapshot() == before, (name, field, value)
            # Real JSON false/null, integer-valued numbers, and negative rewards
            # stay valid; strict input validation must not change those meanings.
            for success in (False, None):
                result = await remote.record_outcome(decision["record_id"], "unknown", success)
                assert (await embedded.feedback_status(result["record_id"], "outcome"))["receipt"]
            result = await remote.record_reward(outcome["record_id"], -1, "formula-1")
            assert (await embedded.feedback_status(result["record_id"], "reward"))["receipt"]

    asyncio.run(scenario())
