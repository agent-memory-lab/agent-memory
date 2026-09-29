"""Regression coverage for candidate truncation and source evidence recall."""

from __future__ import annotations

import asyncio

from agent_memory import MemoryEvent, MemoryKind, MemoryQuery, MemoryScope, build_local_kernel


def test_relevant_claim_beyond_previous_sql_scan_limit_is_retrieved(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            scope = MemoryScope("tenant", session_id="session")
            claims = [
                {"key": f"a{index:03d}", "value": "unrelated", "text": "Unrelated note."}
                for index in range(200)
            ]
            claims.append(
                {
                    "key": "z_family_heirloom",
                    "value": "antique music box",
                    "text": "I inherited an antique music box from my family.",
                }
            )
            await kernel.ingest_event(
                MemoryEvent(
                    scope,
                    "user.message",
                    "Stored family facts.",
                    metadata={"claims": claims},
                )
            )

            candidates = await kernel._repository.search(
                MemoryQuery(scope, "antique music box family", limit=8), 48
            )
            assert any(item.metadata.get("key") == "z_family_heirloom" for item in candidates)
        finally:
            await kernel.close()

    asyncio.run(scenario())


def test_high_ranked_long_event_is_excerpted_and_keeps_citation(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            scope = MemoryScope("tenant", session_id="session")
            event = MemoryEvent(
                scope,
                "user.message",
                "user: I inherited an antique music box from my aunt.\n"
                "assistant: " + "Unrelated maintenance advice. " * 250,
                metadata={
                    "claims": [
                        {"key": f"filler_{index}", "value": "unrelated", "text": "Generic note."}
                        for index in range(8)
                    ]
                },
            )
            await kernel.ingest_event(event)

            for token_budget in (256, 2048):
                bundle = await kernel.retrieve(
                    MemoryQuery(
                        scope,
                        "What antique item did I inherit from my aunt?",
                        limit=8,
                        token_budget=token_budget,
                        include_current_state=False,
                    )
                )
                retrieved_event = next(
                    item for item in bundle.relevant_memories if item.kind == MemoryKind.EVENT
                )
                assert "antique music box" in retrieved_event.text
                assert retrieved_event.metadata["excerpt"] is True
                assert len(retrieved_event.text) <= 1200
                assert bundle.token_estimate <= token_budget
                assert any(
                    citation.memory_id == event.id and event.id in citation.source_event_ids
                    for citation in bundle.citations
                )
        finally:
            await kernel.close()

    asyncio.run(scenario())
