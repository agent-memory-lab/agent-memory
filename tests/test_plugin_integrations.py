import asyncio

from agent_memory import (
    AgentLifecycleContext,
    AgentMemoryAdapter,
    MCPMemoryTools,
    MCPRequestContext,
    MCPToolError,
    MemoryEvent,
    MemoryProposal,
    MemoryScope,
    ProposalStatus,
    ScopeLevel,
    build_local_kernel,
)


def run(coroutine):
    return asyncio.run(coroutine)


def test_proposal_requires_evidence_and_optimistic_version(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")
        evidence = MemoryEvent(scope, "user.message", "Please use email.")
        await kernel.ingest_event(evidence)

        proposal = MemoryProposal(
            scope=scope,
            key="contact.preference",
            value="email",
            text="The user prefers email.",
            source_event_ids=(evidence.id,),
            expected_version=0,
            scope_level=ScopeLevel.USER,
        )
        accepted = await kernel.propose(proposal)
        replayed = await kernel.propose(proposal)
        assert accepted.status == ProposalStatus.ACCEPTED
        assert replayed == accepted

        conflict = await kernel.propose(
            MemoryProposal(
                scope=scope,
                key="contact.preference",
                value="sms",
                text="The user prefers SMS.",
                source_event_ids=(evidence.id,),
                expected_version=0,
                scope_level=ScopeLevel.USER,
            )
        )
        assert conflict.status == ProposalStatus.CONFLICT

    run(scenario())


def test_mcp_scope_is_trusted_and_erase_is_authorized(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        tools = MCPMemoryTools(kernel)
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")
        context = MCPRequestContext(scope=scope)

        result = await tools.call_tool(
            "memory_ingest",
            {
                "tenant_id": "attacker-tenant",
                "event_type": "user.message",
                "content": "Scoped by trusted context.",
                "idempotency_key": "message-1",
            },
            context,
        )
        state = await tools.call_tool("memory_get_state", {}, context)
        assert result["duplicate"] is False
        assert state == {"current_state": []}

        try:
            await tools.call_tool(
                "memory_forget",
                {"all_in_scope": True, "mode": "erase"},
                context,
            )
        except MCPToolError:
            pass
        else:
            raise AssertionError("unauthorized erase must fail")

    run(scenario())


def test_generic_agent_lifecycle_records_decision_outcome_and_episode(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        adapter = AgentMemoryAdapter(kernel)
        await adapter.initialize()
        context = AgentLifecycleContext(
            scope=MemoryScope("tenant-a", user_id="user-a", session_id="session-a"),
            run_id="run-1",
            turn_id="turn-1",
        )
        event = await adapter.after_tool(
            context,
            tool_call_id="tool-1",
            tool_name="search",
            arguments={"query": "memory"},
            result={"count": 3},
            success=True,
        )
        bundle = await adapter.before_model(context, "What happened in search?")
        decision = await adapter.record_decision(context, bundle, "summarize")
        outcome = await adapter.record_outcome(
            context,
            decision,
            outcome="Summary accepted.",
            success=True,
            score=1.0,
        )
        reward = await adapter.record_reward(
            context,
            outcome,
            value=1.0,
            formula_version="task-success-v1",
        )
        episode = await adapter.session_end(
            context,
            observation="Search returned three results.",
            action="Summarized the results.",
            outcome="Accepted.",
            lesson="Use concise summaries.",
            source_event_ids=(event.event_id,),
            quality=0.9,
        )
        assert decision.memory_ids
        assert reward.outcome_id == outcome.id
        assert episode.provenance.source_event_ids == (event.event_id,)

    run(scenario())
