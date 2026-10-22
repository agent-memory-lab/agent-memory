"""Bounded real reader transport adapter, with an offline fake provider only."""

import asyncio
import json

import pytest

from agent_memory import AgentMemory, MemoryScope
from agent_memory.domain import MemoryQuery
from agent_memory.retrieval.evidence_research import EvidenceRequirement, QueryContract
from agent_memory.retrieval.evidence_research_adapters import sqlite_research
from agent_memory.retrieval.evidence_research_reader import (
    BoundedJsonResearchReader,
    ResearchModelReply,
)


def test_json_adapter_calls_once_with_reserved_prompt_and_reports_host_usage(tmp_path):
    async def run():
        scope, path = MemoryScope("reader", session_id="a"), tmp_path / "memory.db"
        async with AgentMemory.local(path, scope=scope) as memory:
            await memory.remember("Orchid owner is Lina.")
        seen = []

        async def transport(prompt, allowance):
            seen.append(allowance)
            payload = json.loads(prompt.split("INPUT_JSON=", 1)[1])
            key = payload["evidence"][0]["key"]
            return ResearchModelReply(
                json.dumps(
                    {
                        "answer": "Lina",
                        "supports": [{"requirement_id": "owner", "evidence_keys": [key]}],
                        "gaps": [],
                    }
                ),
                150,
                50,
            )

        reader = BoundedJsonResearchReader(transport)
        async with sqlite_research(path, reader) as loop:
            result = await loop.answer(
                QueryContract(
                    MemoryQuery(scope, "orchid"), (EvidenceRequirement("owner", "orchid"),)
                )
            )
        assert result.status == "answered" and len(seen) == 1
        assert result.model_calls == 1 and result.unknown_model_usage_calls == 0
        assert result.known_input_tokens == 150 and result.known_output_tokens == 50
        assert result.reservation_basis == "utf8_input_bytes_plus_output_token_cap"

    asyncio.run(run())


@pytest.mark.parametrize("attack", ["scope", "duplicate", "output_tokens", "body", "failure"])
def test_bad_model_response_or_provider_failure_never_retries_and_retains_reservation(
    tmp_path, attack
):
    async def run():
        seen = []

        async def transport(prompt, allowance):
            seen.append(allowance)
            if attack == "failure":
                raise RuntimeError("provider rejected")
            if attack == "duplicate":
                return ResearchModelReply('{"answer":"","answer":"x","supports":[],"gaps":[]}')
            if attack == "output_tokens":
                return ResearchModelReply(
                    '{"answer":"","supports":[],"gaps":[]}', 1, allowance.max_output_tokens + 1
                )
            if attack == "body":
                return ResearchModelReply("x" * (allowance.max_output_tokens * 4 + 1))
            return ResearchModelReply('{"answer":"","supports":[],"gaps":[],"scope":"foreign"}')

        async with sqlite_research(
            tmp_path / "memory.db", BoundedJsonResearchReader(transport)
        ) as loop:
            result = await loop.answer(
                QueryContract(
                    MemoryQuery(MemoryScope("reader"), "empty"),
                    (EvidenceRequirement("fact", "empty"),),
                )
            )
        assert result.status == "partial" and result.answer is None
        assert result.reason == "callback_failed" and len(seen) == 1
        assert result.model_call_reservations == 1 and result.unknown_model_usage_calls == 1
        assert result.reserved_tokens > 0

    asyncio.run(run())
