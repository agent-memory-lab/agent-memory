"""Registered views, shared decisions and source authority integration."""

import asyncio
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256

import pytest
import test_question_runtime_v7 as question_runtime
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory import AgentMemory, MemoryScope
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.domain import (
    AtomDraft,
    MemoryChannel,
    MemoryQuery,
    PredicateSpec,
    SourceAuthority,
)
from agent_memory.operations.decision_records import DecisionJournal
from agent_memory.retrieval.evidence_research import (
    EvidenceRequirement,
    EvidenceResearchLoop,
    EvidenceUse,
    QueryContract,
    ReaderAssessment,
)
from agent_memory.retrieval.evidence_research_adapters import (
    FrozenQueryVectors,
    RegisteredQuestionResearchView,
    SQLiteCensusPolicy,
    SQLiteResearchBackend,
    SQLiteVectorResearchRoute,
    VectorSource,
    sqlite_research,
)
from agent_memory.retrieval.evidence_research_contracts import CallAllowance
from agent_memory.retrieval.question_router import QuestionRouter
from agent_memory.retrieval.semantic import SemanticVectorSpec, VectorNormalization

store = question_runtime.store


@pytest.mark.parametrize("budget_limited", [False, True])
@pytest.mark.parametrize("dependent", [False, True])
def test_semantic_candidates_are_admitted_before_using_result_capacity(
    tmp_path, budget_limited, dependent,
):
    async def run():
        scope = MemoryScope("semantic", session_id="one")
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope) as memory:
            pending_text = "Invoice is paid. " * 10
            pending = await memory.remember_atoms(
                pending_text,
                (AtomDraft("invoice", "paid", True, "Invoice is paid.", "Invoice is paid."),),
                authority=SourceAuthority("user", subjects=("invoice",), predicates=("paid",)),
                policy=AdmissionPolicy((
                    PredicateSpec("paid", value_type="boolean", allow_self_report=False),
                )),
            )
            assert pending.pending_ids
            if dependent:
                pending = await memory.remember(
                    pending_text, metadata={"source_event_ids": [pending.event_id]},
                )
            text = "The receipt needs independent verification. " * 4
            visible = await memory.remember(text)
            spec = SemanticVectorSpec("fixture", "vectors", 2, VectorNormalization.L2, "v1")
            route = SQLiteVectorResearchRoute(
                memory.provider._repository,
                FrozenQueryVectors(spec, {"invoice": (1.0, 0.0)}),
                (
                    VectorSource(
                        pending.event_id, sha256(pending_text.encode()).hexdigest(), (1.0, 0.0),
                    ),
                    VectorSource(visible.event_id, sha256(text.encode()).hexdigest(), (0.8, 0.6)),
                ),
            )
            query = MemoryQuery(scope, "invoice", limit=8 if budget_limited else 1,
                                token_budget=64 if budget_limited else 2048)
            bundle = await route.retrieve(query, None)
            assert [item.id for item in bundle.relevant_memories] == [visible.event_id]
            assert bundle.token_estimate <= query.token_budget
            assert [citation.memory_id for citation in bundle.citations] == [visible.event_id]
            assert bundle.retrieval_metadata["delivery_guard"]["status"] == "validated"

    asyncio.run(run())


@pytest.mark.parametrize("route_name", ["semantic", "census"])
def test_research_backend_respects_disabled_channel_before_dispatch(tmp_path, route_name):
    async def run():
        scope = MemoryScope("semantic", session_id="one")
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope) as memory:
            class UnusedRoute:
                model_calls_per_search = 1

                async def retrieve(self, *args):
                    pytest.fail("disabled route must not be dispatched")

            backend = SQLiteResearchBackend.from_memory(memory, semantic_route=UnusedRoute())
            contract = QueryContract(
                MemoryQuery(scope, "invoice", channels=()),
                (EvidenceRequirement("invoice", "invoice"),),
            )
            batch = await backend.search(
                contract, contract.requirements[0], route_name,
                CallAllowance("call", 1000, 64, 1, 1000),
            )
            assert batch.partial_reason == "semantic_channel_disabled"
            assert batch.model_calls == 0 and batch.bundle.relevant_memories == ()

    asyncio.run(run())


def test_semantic_route_only_hydrates_positive_matches(tmp_path, monkeypatch):
    async def run():
        scope = MemoryScope("semantic", session_id="one")
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope) as memory:
            repository = memory.provider._repository
            sources = []
            for index in range(32):
                text = f"Source {index}"
                result = await memory.remember(text)
                sources.append(VectorSource(
                    result.event_id, sha256(text.encode()).hexdigest(),
                    (1.0, 0.0) if index == 0 else (0.0, 1.0),
                ))
            spec = SemanticVectorSpec("fixture", "vectors", 2, VectorNormalization.L2, "v1")
            route = SQLiteVectorResearchRoute(
                repository, FrozenQueryVectors(spec, {"invoice": (1.0, 0.0)}), tuple(sources),
            )
            statements = []
            connection = repository._connection

            @contextmanager
            def traced_connection():
                with connection() as conn:
                    conn.set_trace_callback(statements.append)
                    try:
                        yield conn
                    finally:
                        conn.set_trace_callback(None)

            monkeypatch.setattr(repository, "_connection", traced_connection)
            bundle = await route.retrieve(MemoryQuery(scope, "invoice"), None)
            assert [item.id for item in bundle.relevant_memories] == [sources[0].event_id]
            body_reads = "\n".join(s for s in statements if s.startswith(
                "SELECT id,event_type,content,occurred_at FROM events",
            ))
            assert sources[0].event_id in body_reads
            assert all(source.event_id not in body_reads for source in sources[1:])
            statements.clear()
            empty = await route.retrieve(MemoryQuery(scope, "unknown query"), None)
            assert empty.relevant_memories == ()
            assert not any(s.startswith("SELECT id,event_type,content,occurred_at FROM events")
                           for s in statements)

    asyncio.run(run())


@pytest.mark.parametrize("channels", [(), (MemoryChannel.EPISODIC,)])
def test_disabled_semantic_channel_does_not_call_embedder(tmp_path, channels):
    async def run():
        scope = MemoryScope("semantic", session_id="one")
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope) as memory:
            class UnusedEmbedder:
                spec = SemanticVectorSpec("fixture", "vectors", 2, VectorNormalization.L2, "v1")
                model_calls_per_search = 1

                async def embed_query(self, *args):
                    pytest.fail("disabled semantic channel must not spend an embedding call")

            route = SQLiteVectorResearchRoute(memory.provider._repository, UnusedEmbedder(), ())
            bundle = await route.retrieve(MemoryQuery(scope, "invoice", channels=channels), None)
            assert bundle.relevant_memories == () and bundle.citations == ()

    asyncio.run(run())


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
