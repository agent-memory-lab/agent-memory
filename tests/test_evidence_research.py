"""Real SQLite acquisition and hostile reader boundaries, without model calls."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256

import pytest

from agent_memory import AgentMemory, MemoryScope
from agent_memory.domain import MemoryQuery
from agent_memory.retrieval.evidence_research import (
    EvidenceGap,
    EvidenceRequirement,
    EvidenceResearchLoop,
    EvidenceUse,
    QueryContract,
    ReaderAssessment,
    ResearchLimits,
    ResearchPlan,
    TemporalResolution,
)
from agent_memory.retrieval.evidence_research_adapters import (
    FrozenQueryVectors,
    SQLiteCensusPolicy,
    SQLiteResearchBackend,
    SQLiteVectorResearchRoute,
    VectorSource,
    sqlite_research,
)
from agent_memory.retrieval.semantic import SemanticVectorSpec, VectorNormalization

SCOPE = MemoryScope("research", session_id="one")


def contract(text="deployment", requirements=None, **options):
    return QueryContract(
        MemoryQuery(SCOPE, text, token_budget=2048, include_current_state=False),
        requirements or (EvidenceRequirement("owner", "violet specialist"),),
        **options,
    )


def owner_reader(seen):
    async def read(query, records, allowance):
        seen.append(tuple(r.text for r in records))
        target = tuple(r.key for r in records if "Mei" in r.text)
        if target:
            return ReaderAssessment("Mei.", (EvidenceUse("owner", target),))
        return ReaderAssessment(gaps=(EvidenceGap("owner"),))

    return read


def test_real_sqlite_missing_fact_is_acquired_on_second_wave(tmp_path):
    async def run():
        path, seen = tmp_path / "memory.db", []
        async with AgentMemory.local(path, scope=SCOPE) as memory:
            await memory.remember("Deployment briefing references violet.")
            await memory.remember("Violet specialist is Mei.")
        async with sqlite_research(path, owner_reader(seen)) as loop:
            result = await loop.answer(contract())
        assert result.status == "answered" and result.answer == "Mei."
        assert len(seen) == 2 and all("Mei" not in text for text in seen[0])
        assert result.calls == 5  # Initial search, read, refill, read, final guard.
        assert result.model_calls == 0 and result.model_call_reservations == 2
        assert result.entailment_verified is False
        assert result.events[-1].stage == "delivery"

    asyncio.run(run())


def test_no_new_evidence_stops_without_reader_or_planner_retry(tmp_path):
    async def run():
        calls = []

        async def reader(c, records, allowance):
            calls.append("reader")
            return ReaderAssessment(gaps=(EvidenceGap("owner"),))

        async with sqlite_research(tmp_path / "memory.db", reader) as loop:
            result = await loop.answer(contract())
        assert result.reason == "no_new_evidence" and result.answer is None
        assert result.calls == 3 and calls == ["reader"] and result.evidence == ()

    asyncio.run(run())


@pytest.mark.parametrize("attack", ["requirement", "route", "source"])
def test_reader_cannot_inject_requirement_route_or_evidence(tmp_path, attack):
    async def run():
        async def reader(c, records, allowance):
            if attack == "requirement":
                return ReaderAssessment(gaps=(EvidenceGap("foreign-secret"),))
            if attack == "route":
                return ReaderAssessment(gaps=(EvidenceGap("owner", "semantic"),))
            return ReaderAssessment("forged", (EvidenceUse("owner", ("unknown",)),))

        async with sqlite_research(tmp_path / "memory.db", reader) as loop:
            result = await loop.answer(contract())
        assert result.status == "partial" and result.answer is None
        assert result.reason in ("invalid_gap_plan", "invalid_reader_support")
        assert result.calls == 2

    asyncio.run(run())


def test_scope_and_time_remain_host_owned_and_history_is_explicitly_partial(tmp_path):
    async def run():
        calls = []

        async def reader(c, records, allowance):
            calls.append(c)
            return ReaderAssessment()

        async with AgentMemory.local(tmp_path / "memory.db", scope=SCOPE) as memory:
            backend = SQLiteResearchBackend.from_memory(memory)
            loop = EvidenceResearchLoop(backend, reader)
            historical = replace(
                contract(),
                query=replace(contract().query, valid_at=datetime(2020, 1, 1, tzinfo=UTC)),
            )
            result = await loop.answer(historical)
            assert result.reason == "historical_research_unsupported" and result.calls == 0
            foreign = replace(
                contract(), query=replace(contract().query, scope=MemoryScope("foreign"))
            )
            with pytest.raises(ValueError, match="scope"):
                await loop.answer(foreign)
        assert calls == []

    asyncio.run(run())


def test_deletion_during_reader_blocks_final_answer(tmp_path):
    async def run():
        async with AgentMemory.local(tmp_path / "memory.db", scope=SCOPE) as memory:
            stored = await memory.remember("Deployment specialist is Mei.")

            async def reader(c, records, allowance):
                await memory.forget(memory_ids=(stored.event_id,), erase=True)
                return ReaderAssessment("Mei", (EvidenceUse("owner", (records[0].key,)),))

            result = await EvidenceResearchLoop(
                SQLiteResearchBackend.from_memory(memory), reader
            ).answer(contract())
        assert result.reason == "evidence_changed_before_delivery" and result.answer is None
        assert result.evidence == ()

    asyncio.run(run())


def test_reader_and_planner_calls_share_budget_and_leave_final_slot(tmp_path):
    async def run():
        seen = []

        async def reader(c, records, allowance):
            seen.append("reader")
            return ReaderAssessment()

        async def planner(c, assessment, records, allowance):
            seen.append("planner")
            return ResearchPlan((EvidenceGap("owner"),))

        async with sqlite_research(tmp_path / "memory.db", reader, planner=planner) as loop:
            result = await loop.answer(contract(limits=ResearchLimits(max_calls=4)))
        assert result.reason == "call_budget_exhausted"
        assert result.calls == 3 and seen == ["reader", "planner"]
        assert result.reserved_tokens > 0 and result.model_call_reservations == 2

    asyncio.run(run())


def test_final_round_does_not_call_planner_for_an_unusable_plan(tmp_path):
    async def run():
        seen = []

        async def reader(c, records, allowance):
            seen.append("reader")
            return ReaderAssessment()

        async def planner(c, assessment, records, allowance):
            seen.append("planner")
            return ResearchPlan((EvidenceGap("owner"),), model_calls=1)

        async with sqlite_research(tmp_path / "memory.db", reader, planner=planner) as loop:
            result = await loop.answer(contract(limits=ResearchLimits(max_rounds=1)))

        assert seen == ["reader"]
        assert result.reason == "round_budget_exhausted"
        assert result.calls == 2 and result.model_call_reservations == 1
        assert result.answer is None and result.evidence == ()
        assert result.missing_requirements == ("owner",)

    asyncio.run(run())


def test_planner_can_acquire_missing_evidence_before_the_final_round(tmp_path):
    async def run():
        path, seen, plans = tmp_path / "memory.db", [], []
        async with AgentMemory.local(path, scope=SCOPE) as memory:
            await memory.remember("Deployment briefing references violet.")
            await memory.remember("Violet specialist is Mei.")

        async def planner(c, assessment, records, allowance):
            plans.append(allowance)
            return ResearchPlan((EvidenceGap("owner"),), model_calls=1)

        async with sqlite_research(path, owner_reader(seen), planner=planner) as loop:
            result = await loop.answer(contract(limits=ResearchLimits(max_rounds=2)))

        assert result.status == "answered" and result.answer == "Mei."
        assert len(seen) == 2 and len(plans) == 1
        assert result.calls == 6 and result.model_calls == 1
        assert result.events[-1].stage == "delivery"

    asyncio.run(run())


def test_planner_cannot_dispatch_without_reserving_delivered_evidence_body():
    from agent_memory.domain import MemoryItem, MemoryKind
    from agent_memory.retrieval.evidence_research_adapters import _bundle
    from agent_memory.retrieval.evidence_research_contracts import RetrievalBatch

    async def run():
        seen = []

        class Backend:
            async def search(self, contract, requirement, route, allowance):
                return RetrievalBatch(
                    _bundle(
                        (
                            MemoryItem(
                                "source",
                                MemoryKind.EVENT,
                                "x" * 3000,
                                1.0,
                                datetime.now(UTC),
                                {"source_event_ids": ("source",)},
                            ),
                        )
                    )
                )

        async def reader(c, records, allowance):
            assert len(records[0].text) == 3000
            seen.append("reader")
            return ReaderAssessment()

        async def planner(c, assessment, records, allowance):
            seen.append("planner")
            return ResearchPlan((EvidenceGap("owner"),))

        result = await EvidenceResearchLoop(Backend(), reader, planner=planner).answer(
            contract(limits=ResearchLimits(max_reserved_tokens=10000))
        )
        # The remaining budget can fit the contract/assessment alone, but not
        # the evidence text that the planner would actually receive.
        assert result.reason == "token_budget_exhausted"
        assert result.calls == 2 and seen == ["reader"]
        assert result.model_call_reservations == 1

    asyncio.run(run())


def test_token_limit_and_timeout_do_not_retry_or_disclose_unvalidated_evidence(tmp_path):
    async def run():
        calls = []

        async def reader(c, records, allowance):
            calls.append(allowance)
            await asyncio.sleep(0.1)
            return ReaderAssessment()

        async with sqlite_research(tmp_path / "memory.db", reader) as loop:
            result = await loop.answer(contract(limits=ResearchLimits(max_reserved_tokens=64)))
            assert result.reason == "token_budget_exhausted" and result.calls == 0
            result = await loop.answer(contract(limits=ResearchLimits(max_elapsed_ms=50)))
            assert result.reason == "deadline_exceeded" and result.answer is None
        assert len(calls) == 1 and result.calls == 2

    asyncio.run(run())


def test_cancellation_propagates_without_an_answer(tmp_path):
    async def run():
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def reader(c, records, allowance):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async with sqlite_research(tmp_path / "memory.db", reader) as loop:
            task = asyncio.create_task(loop.answer(contract()))
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert cancelled.is_set()

    asyncio.run(run())


def test_independent_semantic_route_recovers_zero_lexical_overlap(tmp_path):
    async def run():
        path, seen = tmp_path / "memory.db", []
        text = "Mira purchased a scarlet automobile."
        async with AgentMemory.local(path, scope=SCOPE) as memory:
            stored = await memory.remember(text)
        spec = SemanticVectorSpec("fixture", "provided-vectors", 2, VectorNormalization.L2, "v1")
        embedder = FrozenQueryVectors(spec, {"red car owner": (1.0, 0.0)})

        def semantic(repository):
            return SQLiteVectorResearchRoute(
                repository,
                embedder,
                (VectorSource(stored.event_id, sha256(text.encode()).hexdigest(), (1.0, 0.0)),),
            )

        async def reader(c, records, allowance):
            seen.append(records)
            return (
                ReaderAssessment("Mira", (EvidenceUse("owner", (records[0].key,)),))
                if records
                else ReaderAssessment(gaps=(EvidenceGap("owner", "semantic"),))
            )

        req = EvidenceRequirement("owner", "red car owner", allowed_routes=("native", "semantic"))
        async with sqlite_research(path, reader, semantic_factory=semantic) as loop:
            result = await loop.answer(contract("red car owner", (req,)))
        assert seen[0] == () and result.status == "answered" and result.answer == "Mira"
        assert result.model_calls == 0

    asyncio.run(run())


def census_requirement(kind="enumeration"):
    return EvidenceRequirement(
        "rows", "registered collection", kind, ("native", "census"), "commitments"
    )


def test_census_requires_backend_receipt_and_renders_real_count_not_reader_claim(tmp_path):
    async def run():
        path, seen = tmp_path / "memory.db", []
        async with AgentMemory.local(path, scope=SCOPE) as memory:
            await memory.remember("Deliver report", event_type="commitment")
            await memory.remember("Book a room", event_type="commitment")
            await memory.remember("Unrelated", event_type="note")

        async def reader(c, records, allowance):
            seen.append(records)
            return ReaderAssessment(
                "There are 9999 total promises in the world.",
                (EvidenceUse("rows", tuple(r.key for r in records)),),
            )

        async with sqlite_research(
            path, reader, census_policies=(SQLiteCensusPolicy("commitments", "commitment"),)
        ) as loop:
            result = await loop.answer(contract("registered collection", (census_requirement(),)))
        assert len(seen) == 2 and result.status == "answered"
        assert result.answer == "rows: 2 recorded rows in commitments."
        assert len(result.coverage_receipts[0].source_versions) == 2

    asyncio.run(run())


@pytest.mark.parametrize("change", ["insert", "erase", "request"])
def test_census_receipt_cannot_survive_changed_snapshot_or_request(tmp_path, change):
    async def run():
        path = tmp_path / "memory.db"
        async with AgentMemory.local(path, scope=SCOPE) as memory:
            stored = await memory.remember("Deliver report", event_type="commitment")
            n = 0

            async def reader(c, records, allowance):
                nonlocal n
                n += 1
                if n == 2:
                    if change == "insert":
                        await memory.remember("New commitment", event_type="commitment")
                    elif change == "erase":
                        await memory.forget(memory_ids=(stored.event_id,), erase=True)
                return ReaderAssessment(
                    "complete", (EvidenceUse("rows", tuple(r.key for r in records)),)
                )

            backend = SQLiteResearchBackend.from_memory(
                memory, census_policies=(SQLiteCensusPolicy("commitments", "commitment"),)
            )
            if change == "request":
                real_final = backend.finalize

                async def altered(c, bundles, receipts, allowance):
                    other = replace(c, query=replace(c.query, request_id="other"))
                    return await real_final(other, bundles, receipts, allowance)

                backend.finalize = altered
            result = await EvidenceResearchLoop(backend, reader).answer(
                contract("registered collection", (census_requirement(),))
            )
        assert result.status == "partial" and result.answer is None
        assert result.reason in (
            "coverage_changed_before_delivery",
            "evidence_changed_before_delivery",
        )

    asyncio.run(run())


def test_absence_requires_empty_registered_census_and_overbudget_never_complete(tmp_path):
    async def run():
        async def reader(c, records, allowance):
            return ReaderAssessment(
                "Nothing exists.", (EvidenceUse("rows", tuple(r.key for r in records)),)
            )

        path = tmp_path / "memory.db"
        async with sqlite_research(
            path, reader, census_policies=(SQLiteCensusPolicy("commitments", "commitment", 1),)
        ) as loop:
            result = await loop.answer(contract("count", (census_requirement("absence"),)))
            assert result.answer == "rows: No recorded rows in commitments."
        async with AgentMemory.local(path, scope=SCOPE) as memory:
            await memory.remember("First", event_type="commitment")
            await memory.remember("Second", event_type="commitment")
        async with sqlite_research(
            path, reader, census_policies=(SQLiteCensusPolicy("commitments", "commitment", 1),)
        ) as loop:
            result = await loop.answer(contract("count", (census_requirement(),)))
            assert result.status == "partial" and not result.coverage_receipts
            assert any(e.outcome == "census_row_budget_exceeded" for e in result.events)

    asyncio.run(run())


def test_time_requires_trusted_typed_value_and_explicit_approximation_permission(tmp_path):
    async def run():
        path = tmp_path / "memory.db"
        async with AgentMemory.local(path, scope=SCOPE) as memory:
            await memory.remember("Deployment finished around the beginning of May.")

        async def reader(c, records, allowance):
            return ReaderAssessment(
                "Exactly May 1 at noon.", (EvidenceUse("time", (records[0].key,)),)
            )

        async def temporal(c, records, allowance):
            # Simulates source-bound, trusted ingestion/extraction. The reader
            # cannot manufacture this separate callback result or the precision.
            return (TemporalResolution("time", "May 2026", "month", True, (records[0].key,)),)

        req = EvidenceRequirement("time", "deployment", "temporal", precision="month")
        async with sqlite_research(path, reader, temporal_resolver=temporal) as loop:
            strict = await loop.answer(contract(requirements=(req,)))
            result = await loop.answer(contract(requirements=(req,), allow_approximate=True))
        assert strict.reason == "precision_policy_mismatch"
        assert result.answer == "time: Approximately: May 2026"
        async with sqlite_research(path, reader) as loop:
            unsupported = await loop.answer(contract(requirements=(req,), allow_approximate=True))
        assert unsupported.status == "partial"  # Never infer event date from source timestamp.

    asyncio.run(run())
