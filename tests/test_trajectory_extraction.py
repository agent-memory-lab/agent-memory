import asyncio

from agent_memory import (
    AgentLifecycleContext,
    AgentMemory,
    AgentMemoryAdapter,
    MemoryScope,
    build_local_kernel,
    build_trajectory_extractor,
)


def run(coroutine):
    return asyncio.run(coroutine)


class FakeTrajectoryGenerator:
    async def generate_claims(self, event):
        if event.event_type == "user.message":
            return (
                {
                    "key": "response.language",
                    "value": "zh-CN",
                    "text": "The user prefers Chinese responses.",
                    "scope": "user",
                    "confidence": 0.95,
                },
                {
                    "key": "unsafe.cross-scope",
                    "value": True,
                    "text": "This scope is unavailable.",
                    "scope": "workspace",
                    "confidence": 0.99,
                },
            )
        if event.event_type == "agent.tool.completed":
            assert event.metadata["result"] == {"repository": "agent-memory"}
            return (
                {
                    "key": "project.repository",
                    "value": "agent-memory",
                    "text": "The active repository is agent-memory.",
                    "scope": "session",
                    "confidence": 0.9,
                },
            )
        return ()


class FailingTrajectoryGenerator:
    async def generate_claims(self, event):
        raise RuntimeError("model provider unavailable")


def test_lifecycle_automatically_extracts_evidence_backed_claims(tmp_path) -> None:
    async def scenario() -> None:
        extractor = build_trajectory_extractor(
            FakeTrajectoryGenerator(),
            provider="fake-provider",
            model="fake-model",
        )
        kernel = build_local_kernel(tmp_path / "memory.db", extractor=extractor)
        adapter = AgentMemoryAdapter(kernel)
        await adapter.initialize()
        assert kernel.manifest().capabilities.automatic_extraction is True
        context = AgentLifecycleContext(
            scope=MemoryScope("tenant-a", user_id="user-a", session_id="session-a"),
            run_id="run-1",
            turn_id="turn-1",
        )

        user_result = await adapter.after_user(context, content="请使用中文回答。")
        tool_result = await adapter.after_tool(
            context,
            tool_call_id="tool-1",
            tool_name="repository.current",
            arguments={},
            result={"repository": "agent-memory"},
            success=True,
        )

        assert len(user_result.claim_ids) == 1
        assert len(tool_result.claim_ids) == 1
        state = await kernel.get_state(context.scope)
        assert {claim.key for claim in state} == {
            "response.language",
            "project.repository",
        }
        language = next(claim for claim in state if claim.key == "response.language")
        assert language.provenance.provider == "fake-provider"
        assert language.provenance.model == "fake-model"
        assert language.provenance.source_event_ids == (user_result.event_id,)

    run(scenario())


def test_explicit_claim_wins_over_generated_duplicate(tmp_path) -> None:
    async def scenario() -> None:
        extractor = build_trajectory_extractor(FakeTrajectoryGenerator())
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")
        async with AgentMemory.local(
            tmp_path / "memory.db",
            scope=scope,
            extractor=extractor,
        ) as memory:
            result = await memory.remember(
                "Use English.",
                event_type="user.message",
                claims=(
                    {
                        "key": "response.language",
                        "value": "en",
                        "text": "The user explicitly requested English.",
                        "scope": "user",
                        "confidence": 1.0,
                    },
                ),
            )
            assert len(result.claim_ids) == 1
            state = await memory.state()
            assert len(state) == 1
            assert state[0].value == "en"

    run(scenario())


def test_generator_failure_does_not_block_event_ingestion(tmp_path) -> None:
    async def scenario() -> None:
        extractor = build_trajectory_extractor(FailingTrajectoryGenerator())
        kernel = build_local_kernel(tmp_path / "memory.db", extractor=extractor)
        adapter = AgentMemoryAdapter(kernel)
        await adapter.initialize()
        context = AgentLifecycleContext(
            scope=MemoryScope("tenant-a", user_id="user-a", session_id="session-a"),
            run_id="run-1",
            turn_id="turn-1",
        )

        result = await adapter.after_user(context, content="Remember this event.")

        assert result.duplicate is False
        assert result.claim_ids == ()
        assert await kernel.get_state(context.scope) == ()

    run(scenario())
