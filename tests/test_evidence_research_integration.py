"""Registered views, shared decisions and source authority integration."""

import asyncio
from dataclasses import replace

import pytest
import test_question_runtime_v7 as question_runtime
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory import AgentMemory, MemoryScope
from agent_memory.domain import MemoryQuery
from agent_memory.operations.decision_records import DecisionJournal
from agent_memory.retrieval.evidence_research import (
    EvidenceRequirement,
    EvidenceResearchLoop,
    EvidenceUse,
    QueryContract,
    ReaderAssessment,
)
from agent_memory.retrieval.evidence_research_adapters import (
    RegisteredQuestionResearchView,
    SQLiteCensusPolicy,
    SQLiteResearchBackend,
    sqlite_research,
)
from agent_memory.retrieval.question_router import QuestionRouter

store = question_runtime.store


def test_real_question_view_is_reused_with_no_retrieval_or_reader(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await question_runtime.project.stage(svc.admission, scope)
            await question_runtime.project.qualify(svc.admission, *item)
            await register(svc)
            await question_runtime.fresh(svc, clock, dedupe="prepare")

            async def reader(*args):
                pytest.fail("registered view must not call the generic reader")

            class NoRecall:
                async def search(self, *args):
                    pytest.fail("registered view must not call recall")

            loop = EvidenceResearchLoop(
                NoRecall(), reader, view=RegisteredQuestionResearchView(QuestionRouter(svc))
            )
            contract = QueryContract(
                MemoryQuery(scope, "Who is the owner?"),
                (EvidenceRequirement("owner", "owner"),),
                actor=ACTOR,
                registered_question="project-a:owner",
            )
            result = await loop.answer(contract)
            assert result.status == "view" and result.calls == 1 and result.model_calls == 0
            assert result.view_response["result"]["rows"][0]["fields"][0]["known_values"] == [
                "Alice"
            ]
            result = await loop.answer(replace(contract, registered_question="unregistered"))
            assert (
                result.status == "partial"
                and result.view_response["reason"] == "unregistered_question"
            )

    asyncio.run(run())


def test_shared_decision_journal_is_metadata_only_and_precedes_final_guard(tmp_path):
    async def run():
        scope = MemoryScope("journal", session_id="a")
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope) as memory:
            stored = await memory.remember("Orchid owner is Lina.")
            journal = DecisionJournal(memory.provider._repository, scope)
            written = []

            async def sink(record):
                written.append(record)
                await journal.append(record)

            async def reader(c, records, allowance):
                return ReaderAssessment("Lina", (EvidenceUse("owner", (records[0].key,)),))

            loop = EvidenceResearchLoop(
                SQLiteResearchBackend.from_memory(memory), reader, decision_sink=sink
            )
            result = await loop.answer(
                QueryContract(
                    MemoryQuery(scope, "orchid"), (EvidenceRequirement("owner", "orchid"),)
                )
            )
            assert result.status == "answered" and result.calls == 5
            assert written[-1].action == "research_delivery_candidate"
            assert written[-1].memory_ids == (stored.event_id,)
            assert all("Lina" not in repr(record) for record in written)
            assert (await journal.get(written[-1].id)).fingerprint == written[-1].fingerprint
            assert result.events[-1].stage == "delivery"

    asyncio.run(run())


def test_metadata_cannot_certify_complete_coverage_and_unknown_domain_stays_partial(tmp_path):
    async def run():
        scope = MemoryScope("coverage", session_id="a")
        path = tmp_path / "memory.db"
        async with AgentMemory.local(path, scope=scope) as memory:
            await memory.remember(
                "Orchid is complete", metadata={"coverage_complete": True, "count": 1}
            )

        async def reader(c, records, allowance):
            return ReaderAssessment("One", (EvidenceUse("rows", tuple(r.key for r in records)),))

        requirement = EvidenceRequirement(
            "rows", "orchid", "enumeration", ("native", "census"), "unregistered"
        )
        async with sqlite_research(path, reader) as loop:
            result = await loop.answer(QueryContract(MemoryQuery(scope, "orchid"), (requirement,)))
        assert result.status == "partial" and result.answer is None
        assert any(e.outcome == "unregistered_coverage_domain" for e in result.events)

    asyncio.run(run())


def test_complete_census_cannot_be_cut_to_fit_context(tmp_path):
    async def run():
        scope = MemoryScope("census", session_id="a")
        path = tmp_path / "memory.db"
        async with AgentMemory.local(path, scope=scope) as memory:
            await memory.remember("big " * 5000, event_type="row")

        async def reader(c, records, allowance):
            return ReaderAssessment("One", (EvidenceUse("rows", tuple(r.key for r in records)),))

        requirement = EvidenceRequirement(
            "rows", "census", "enumeration", ("native", "census"), "rows"
        )
        async with sqlite_research(
            path, reader, census_policies=(SQLiteCensusPolicy("rows", "row"),)
        ) as loop:
            result = await loop.answer(
                QueryContract(MemoryQuery(scope, "census", token_budget=64), (requirement,))
            )
        assert (
            result.status == "partial" and result.answer is None and result.coverage_receipts == ()
        )
        assert any(e.outcome == "census_context_budget_exceeded" for e in result.events)

    asyncio.run(run())
