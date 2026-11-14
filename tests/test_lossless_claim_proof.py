"""The opt-in packer's state proof must survive native final authorization."""

import asyncio

from agent_memory import MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryProposal, MemoryQuery, MemoryScope
from agent_memory.retrieval.lossless_packing import LosslessBudget


def test_current_state_citation_proof_survives_sqlite_delivery_guard(tmp_path):
    async def run():
        budget = LosslessBudget(
            lambda state, items: 100 + sum(len(value.text) for value in (*state, *items)),
            "synthetic-characters",
        )
        kernel = build_local_kernel(tmp_path / "memory.db", lossless_packing_budget=budget)
        await kernel.initialize()
        try:
            scope = MemoryScope("proof-test", session_id="session")
            event = MemoryEvent(scope, "user.message", "Use email for contact.")
            await kernel.ingest_event(event)
            result = await kernel.propose(MemoryProposal(
                scope, "contact", "email", "Use email for contact.", (event.id,), 0,
            ))
            assert result.status == "accepted"
            bundle = await kernel.retrieve(MemoryQuery(scope, "contact", token_budget=1024))
            assert bundle.retrieval_metadata["delivery_guard"]["status"] == "validated"
            assert bundle.retrieval_metadata["strategy"] == "lossless-ranked-units/1"
            assert len(bundle.current_state) == 1
            claim = bundle.current_state[0]
            proofs = bundle.retrieval_metadata["citation_spans"]
            assert any(
                proof["memory_id"] == claim.id
                and proof["candidate_id"] == claim.id
                and tuple(proof["source_event_ids"]) == claim.provenance.source_event_ids
                and event.id in proof["source_event_ids"]
                for proof in proofs
            )
        finally:
            await kernel.close()

    asyncio.run(run())
