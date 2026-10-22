"""Offline executable fixture: one missing fact, one bounded refill, final validation.

Run: PYTHONPATH=src python examples/evidence_research_sqlite.py
The deterministic reader below is a fixture, not evidence of model quality. For
production, BoundedJsonResearchReader accepts a host-authorized/accounted transport.
"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory import AgentMemory, MemoryScope
from agent_memory.domain import MemoryQuery
from agent_memory.retrieval.evidence_research import (
    EvidenceGap,
    EvidenceRequirement,
    EvidenceUse,
    QueryContract,
    ReaderAssessment,
)
from agent_memory.retrieval.evidence_research_adapters import sqlite_research


async def main():
    seen = []

    async def fixture_reader(contract, records, allowance):
        seen.append(len(records))
        supporting = tuple(r.key for r in records if "Mei" in r.text)
        if supporting:
            return ReaderAssessment("Mei.", (EvidenceUse("owner", supporting),))
        return ReaderAssessment(gaps=(EvidenceGap("owner", "native"),))

    with TemporaryDirectory(prefix="evidence-research-") as directory:
        database = Path(directory) / "memory.db"
        scope = MemoryScope("offline-example", session_id="demo")
        async with AgentMemory.local(database, scope=scope) as memory:
            await memory.remember("Deployment briefing references violet.")
            await memory.remember("Violet specialist is Mei.")
        contract = QueryContract(
            MemoryQuery(scope, "deployment", include_current_state=False, token_budget=2048),
            (EvidenceRequirement("owner", "violet specialist"),),
        )
        async with sqlite_research(database, fixture_reader) as research:
            result = await research.answer(contract)
        assert result.status == "answered" and len(seen) == 2
        print(
            json.dumps(
                {
                    "fixture": True,
                    "status": result.status,
                    "answer": result.answer,
                    "reader_rounds": len(seen),
                    "calls": result.calls,
                    "paid_calls": 0,
                    "entailment_verified": result.entailment_verified,
                    "evidence_count": len(result.evidence),
                    "reservation_basis": result.reservation_basis,
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
