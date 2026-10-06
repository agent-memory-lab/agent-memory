"""Conditional projection and whole-field temporal proofs on both real providers."""

import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as base

from agent_memory.conditions import Condition, ContextAttribute, ProjectionPolicy, QueryContext
from agent_memory.consolidation.qualification import ContextualMemory, target_fingerprint
from agent_memory.domain import ForgetMode, ForgetRequest, MemoryQuery
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange, intersect, union
from agent_memory.fact_qualification import SourceSpan

store = base.store
POLICY = ProjectionPolicy("city-context/1", "planning")


def context(scope, *, day=5, known=20, project="A", purpose="planning"):
    attrs = () if project is None else (ContextAttribute("project", project, "authenticated-host"),)
    return QueryContext("host:alice", scope, "alice", purpose, base.at(day), base.at(known), attrs)


def link(
    event,
    draft,
    *,
    identity="one",
    start=1,
    end=None,
    family="family-a",
    revision="v1",
    point=False,
):
    fields = ["subject_id", "predicate", "value", "valid_from"]
    if draft.conditions:
        fields.append("conditions")
    if draft.exceptions:
        fields.append("exceptions")
    if draft.valid_to:
        fields.append("valid_to")
    return EvidenceLink(
        identity,
        tuple(fields),
        target_fingerprint(draft),
        SourceSpan(event.id, 0, len(event.content), event.content),
        base.SELF,
        SupportRange(
            base.at(start), base.at(end) if end else None, "point" if point else "interval"
        ),
        family,
        revision,
    )


async def candidate(engine, scope, *, value="Hangzhou", start=1, end=None, exception=False):
    draft = base.atom(
        value,
        conditions=("Only in this project",),
        exceptions=("Except holidays",) if exception else (),
        valid_from=base.at(start),
        valid_to=base.at(end) if end else None,
    )
    source = base.source(scope, draft.text + " Only in this project. Except holidays.", day=start)
    receipt = await engine.admit(source, [draft], authority=base.SELF, policy=base.POLICY)
    assert receipt.pending_ids and not receipt.claim_ids
    return source, draft, receipt.candidate_ids[0]


async def qualify(
    service, identity, draft, links, *, policy=POLICY, project="A", groups=None, expected=1
):
    if groups is None:
        groups = tuple((evidence.id,) for evidence in links)
    fields = {f for evidence in links for f in evidence.fields}
    return await service.qualify(
        identity,
        expected_version=expected,
        admission_policy=base.POLICY,
        projection_policy=policy,
        applicability_id="project-" + project,
        conditions=(Condition("eq", "project", project),),
        exceptions=(Condition("eq", "holiday", True),) if draft.exceptions else (),
        links=links,
        field_support=tuple(FieldSupport(f, groups) for f in sorted(fields)),
    )


async def append_source(engine, scope, text="auxiliary verified observation"):
    source = base.source(scope, text)
    async with engine.repository.unit_of_work() as uow:
        await uow.lock_admission_scope(scope)
        await uow.append_event(source)
    return source


def test_three_valued_conditions_and_strict_types():
    scope = base.MemoryScope("t", session_id="s")
    unknown = context(scope, project=None)
    assert Condition("eq", "project", "A").evaluate(unknown) is None
    assert (
        Condition(
            "and", children=(Condition("eq", "project", "B"), Condition("eq", "missing", 1))
        ).evaluate(context(scope))
        is False
    )
    assert (
        Condition(
            "or", children=(Condition("eq", "project", "A"), Condition("eq", "missing", 1))
        ).evaluate(context(scope))
        is True
    )
    assert Condition("not", children=(Condition("eq", "missing", True),)).evaluate(unknown) is None
    typed = replace(unknown, attributes=(ContextAttribute("paid", 1, "host"),))
    assert Condition("eq", "paid", True).evaluate(typed) is None
    assert Condition("weekday", value=(0,)).evaluate(unknown) is None
    assert (
        Condition("weekday", value=(0,)).evaluate(replace(unknown, timezone="Asia/Shanghai"))
        is True
    )
    with pytest.raises(ValueError):
        Condition("eval", value="__import__('os')")
    with pytest.raises(ValueError):
        ProjectionPolicy("v1", "planning", "ordered_override", (("a", "b"), ("b", "a")))
    assert replace(unknown, purpose="other").context_hash != unknown.context_hash


def test_temporal_sets_preserve_gaps_and_points():
    one, two = SupportRange(base.at(1), base.at(5)), SupportRange(base.at(7), base.at(10))
    assert intersect(one, two) is None
    assert union([two, one]) == (one, two)
    assert intersect(one, SupportRange(base.at(3), base.at(8))) == SupportRange(
        base.at(3), base.at(5)
    )
    assert not one.contains(base.at(5))
    point = SupportRange(base.at(5), kind="point")
    assert intersect(point, one) is None
    assert union([one, point]) == (one, point)


def test_context_is_required_and_conditions_precede_same_slot_composition(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = ContextualMemory(engine, scope, principal="host:alice")
            a, da, ia = await candidate(engine, scope)
            b, db, ib = await candidate(engine, scope, value="Shanghai")
            clock[0] = base.at(2)
            await qualify(service, ia, da, (link(a, da),))
            await qualify(service, ib, db, (link(b, db),), project="B")
            for project, value in [("A", "Hangzhou"), ("B", "Shanghai")]:
                result = await service.query(
                    context(scope, project=project), predicate="city", policy=POLICY
                )
                assert result["status"] == "resolved" and result["value"] == value, result
            assert (
                await service.query(context(scope, project=None), predicate="city", policy=POLICY)
            )["status"] == "unknown"
            assert (
                await service.query(context(scope, project="C"), predicate="city", policy=POLICY)
            )["status"] == "unknown"
            claims, info = await engine.state(scope, valid_at=base.at(5), known_at=base.at(20))
            assert not claims and info["context_required"]
            bundle = await kernel.retrieve(
                MemoryQuery(scope, "city", valid_at=base.at(5), known_at=base.at(20))
            )
            assert not bundle.current_state
            assert not (
                await service.query(context(scope, known=1), predicate="city", policy=POLICY)
            )["interpretations"]
            with pytest.raises(ValueError, match="routing"):
                await service.query(
                    replace(context(scope), principal="forged"), predicate="city", policy=POLICY
                )
            with pytest.raises(ValueError, match="purpose"):
                await service.query(
                    context(scope, purpose="other"), predicate="city", policy=POLICY
                )

    asyncio.run(run())


def test_cross_source_and_or_support_and_erasure(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = ContextualMemory(engine, scope, principal="host:alice")
            source, draft, identity = await candidate(engine, scope)
            a = await append_source(engine, scope, "erased-secret-A")
            b = await append_source(engine, scope, "independent B")
            la, lb = (
                link(a, draft, identity="a", start=1, end=5),
                link(b, draft, identity="b", start=7, end=10, family="family-b"),
            )
            clock[0] = base.at(2)
            await qualify(service, identity, draft, (la, lb))
            for day, state in [
                (3, "resolved"),
                (5, "unknown"),
                (6, "unknown"),
                (7, "resolved"),
                (10, "unknown"),
            ]:
                result = await service.query(
                    context(scope, day=day), predicate="city", policy=POLICY
                )
                assert result["status"] == state, result
            await kernel.forget(ForgetRequest(scope, memory_ids=(a.id,), mode=ForgetMode.ERASE))
            assert (await service.query(context(scope, day=8), predicate="city", policy=POLICY))[
                "status"
            ] == "resolved"
            assert (await service.query(context(scope, day=3), predicate="city", policy=POLICY))[
                "status"
            ] == "unknown"
            versions = await engine.repository.admission_record_versions(scope, identity)
            assert "erased-secret-A" not in str(versions)
            assert "erased-secret-A" not in str(
                await engine.repository.admission_record(scope, identity)
            )
            # Requalify using two jointly required sources. Losing either blocks the field.
            c = await append_source(engine, scope, "joint C")
            lc = link(c, draft, identity="c", start=8, end=12, family="family-c")
            current = await engine.repository.admission_record(scope, identity)
            clock[0] = base.at(3)
            await qualify(
                service,
                identity,
                draft,
                (lb, lc),
                groups=(("b", "c"),),
                expected=current["version"],
            )
            assert (await service.query(context(scope, day=8), predicate="city", policy=POLICY))[
                "status"
            ] == "resolved"
            assert (await service.query(context(scope, day=7), predicate="city", policy=POLICY))[
                "status"
            ] == "unknown"
            await kernel.forget(ForgetRequest(scope, memory_ids=(c.id,), mode=ForgetMode.ERASE))
            assert (await service.query(context(scope, day=8), predicate="city", policy=POLICY))[
                "status"
            ] == "unknown"
            assert await engine.repository.admission_record(scope, identity) is not None

    asyncio.run(run())


def test_business_revision_and_independent_families_are_not_inferred(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            service = ContextualMemory(engine, scope, principal="host:alice")
            source, draft, identity = await candidate(engine, scope)
            a, b = await append_source(engine, scope), await append_source(engine, scope)
            policy = replace(POLICY, require_domain_revision=True, require_independent_sources=True)
            la, lb = (
                link(a, draft, identity="a"),
                link(b, draft, identity="b", revision="v2", family="family-b"),
            )
            clock[0] = base.at(2)
            version = await qualify(
                service, identity, draft, (la, lb), policy=policy, groups=(("a", "b"),)
            )
            assert (await service.query(context(scope), predicate="city", policy=policy))[
                "status"
            ] == "unknown"
            version = await qualify(
                service,
                identity,
                draft,
                (la, replace(lb, domain_revision="v1", source_family="family-a")),
                policy=policy,
                groups=(("a", "b"),),
                expected=version,
            )
            assert (await service.query(context(scope), predicate="city", policy=policy))[
                "status"
            ] == "unknown"
            await qualify(
                service,
                identity,
                draft,
                (la, replace(lb, domain_revision="v1", source_family="family-b")),
                policy=policy,
                groups=(("a", "b"),),
                expected=version,
            )
            assert (await service.query(context(scope), predicate="city", policy=policy))[
                "status"
            ] == "resolved"

    asyncio.run(run())


def test_unknown_exception_blocks_override_and_expiry_does_not_revive(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            await engine.admit(
                base.source(scope), [base.atom()], authority=base.SELF, policy=base.POLICY
            )
            service = ContextualMemory(engine, scope, principal="host:alice")
            policy = replace(POLICY, mode="ordered_override", precedence=(("global", "project-A"),))
            event, draft, identity = await candidate(
                engine, scope, value="Shanghai", exception=True, start=2, end=10
            )
            clock[0] = base.at(3)
            await qualify(
                service, identity, draft, (link(event, draft, start=2, end=10),), policy=policy
            )
            assert (await service.query(context(scope), predicate="city", policy=policy))[
                "status"
            ] == "unknown"
            no_holiday = replace(
                context(scope),
                attributes=(*context(scope).attributes, ContextAttribute("holiday", False, "host")),
            )
            assert (await service.query(no_holiday, predicate="city", policy=policy))[
                "value"
            ] == "Shanghai"
            holiday = replace(
                no_holiday,
                attributes=(
                    ContextAttribute("project", "A", "host"),
                    ContextAttribute("holiday", True, "host"),
                ),
            )
            assert (await service.query(holiday, predicate="city", policy=policy))[
                "value"
            ] == "Hangzhou"
            expired = replace(no_holiday, valid_at=base.at(12))
            assert (await service.query(expired, predicate="city", policy=policy))[
                "status"
            ] == "unknown"
            assert (
                await service.query(
                    no_holiday, predicate="city", policy=replace(policy, revision="different")
                )
            )["status"] == "history_unavailable"

    asyncio.run(run())


def test_missing_fields_target_scope_and_candidate_cas_are_rejected(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            service = ContextualMemory(engine, scope, principal="host:alice")
            source, draft, identity = await candidate(engine, scope)
            good = link(source, draft)
            clock[0] = base.at(2)
            with pytest.raises(ValueError, match="required fields"):
                await service.qualify(
                    identity,
                    expected_version=1,
                    admission_policy=base.POLICY,
                    projection_policy=POLICY,
                    applicability_id="A",
                    conditions=(Condition("eq", "project", "A"),),
                    links=(good,),
                    field_support=(FieldSupport("value", (("one",),)),),
                )
            with pytest.raises(ValueError, match="target binding"):
                await qualify(service, identity, draft, (replace(good, target_sha256="a" * 64),))
            wrong_source = replace(good, span=SourceSpan("foreign-source", 0, 4, "text"))
            with pytest.raises(ValueError, match="source unavailable"):
                await qualify(service, identity, draft, (wrong_source,))
            with pytest.raises(ValueError, match="binding"):
                await service.qualify(
                    identity,
                    expected_version=1,
                    admission_policy=base.POLICY,
                    projection_policy=POLICY,
                    applicability_id="A",
                    links=(good,),
                )
            await qualify(service, identity, draft, (good,))
            with pytest.raises(ValueError, match="version changed"):
                await qualify(service, identity, draft, (good,))

    asyncio.run(run())


def test_point_evidence_never_becomes_continuous_and_unknown_family_is_not_independent(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            service = ContextualMemory(engine, scope, principal="host:alice")
            source, draft, identity = await candidate(engine, scope)
            clock[0] = base.at(2)
            version = await qualify(
                service, identity, draft, (link(source, draft, start=5, point=True),)
            )
            assert (await service.query(context(scope, day=5), predicate="city", policy=POLICY))[
                "status"
            ] == "resolved"
            assert (await service.query(context(scope, day=6), predicate="city", policy=POLICY))[
                "status"
            ] == "unknown"
            other = await append_source(engine, scope)
            policy = replace(POLICY, require_independent_sources=True)
            await qualify(
                service,
                identity,
                draft,
                (
                    link(source, draft, identity="a", family=None),
                    link(other, draft, identity="b", family="family-b"),
                ),
                expected=version,
                policy=policy,
                groups=(("a", "b"),),
            )
            assert (await service.query(context(scope), predicate="city", policy=policy))[
                "status"
            ] == "unknown"

    asyncio.run(run())


def test_same_domain_conflict_and_authorized_precedence(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            service = ContextualMemory(engine, scope, principal="host:alice")
            a, da, ia = await candidate(engine, scope)
            b, db, ib = await candidate(engine, scope, value="Shanghai")
            clock[0] = base.at(2)
            await qualify(service, ia, da, (link(a, da),))
            await qualify(service, ib, db, (link(b, db),))
            result = await service.query(context(scope), predicate="city", policy=POLICY)
            assert result["status"] == "contested" and result["value"] is None

    asyncio.run(run())


def test_auxiliary_erase_during_read_revalidates_surviving_branch(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = ContextualMemory(engine, scope, principal="host:alice")
            source, draft, identity = await candidate(engine, scope)
            a, b = await append_source(engine, scope), await append_source(engine, scope)
            clock[0] = base.at(2)
            await qualify(
                service,
                identity,
                draft,
                (link(a, draft, identity="a"), link(b, draft, identity="b")),
            )
            original = engine.records_at

            async def erase_between(*args):
                rows = await original(*args)
                await kernel.forget(ForgetRequest(scope, memory_ids=(a.id,), mode=ForgetMode.ERASE))
                return rows

            monkeypatch.setattr(engine, "records_at", erase_between)
            result = await service.query(context(scope), predicate="city", policy=POLICY)
            assert result["status"] == "resolved"
            assert result["interpretations"][0]["segments"][0]["evidence_link_ids"] == ["b"]

    asyncio.run(run())


def test_erasure_failure_rolls_back_all_support_snapshots(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            import agent_memory.evidence_support as support

            service = ContextualMemory(engine, scope, principal="host:alice")
            a, da, ia = await candidate(engine, scope)
            b, db, ib = await candidate(engine, scope, value="Shanghai")
            auxiliary = await append_source(engine, scope)
            clock[0] = base.at(2)
            await qualify(service, ia, da, (link(auxiliary, da),))
            await qualify(service, ib, db, (link(auxiliary, db),), project="B")
            before = [await engine.repository.admission_record_versions(scope, i) for i in (ia, ib)]
            original, calls = support.scrub_support, [0]

            def fail_midway(payload, ids):
                result = original(payload, ids)
                calls[0] += 1
                if calls[0] == 4:
                    raise RuntimeError("midway support erase")
                return result

            with monkeypatch.context() as patch:
                patch.setattr(support, "scrub_support", fail_midway)
                with pytest.raises(RuntimeError, match="midway"):
                    await kernel.forget(
                        ForgetRequest(scope, memory_ids=(auxiliary.id,), mode=ForgetMode.ERASE)
                    )
            assert before == [
                await engine.repository.admission_record_versions(scope, i) for i in (ia, ib)
            ]
            assert (await service.query(context(scope), predicate="city", policy=POLICY))[
                "status"
            ] == "resolved"

    asyncio.run(run())


def test_support_source_revision_preserves_historical_knowledge(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            from agent_memory.operations.retention import DurableReceiver

            service = ContextualMemory(engine, scope, principal="host:alice")
            source, draft, identity = await candidate(engine, scope)
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            auxiliary = base.source(scope, "old evidence")
            ticket = await receiver.issue_ticket(
                auxiliary, request_id="evidence", producer_id="host", configuration_sha256="a" * 64
            )
            await receiver.submit(
                auxiliary, ticket=ticket, producer_id="host", configuration_sha256="a" * 64
            )
            clock[0] = base.at(2)
            await qualify(service, identity, draft, (link(auxiliary, draft, family=None),))
            clock[0] = base.at(10)
            await receiver.revise(
                base.source(scope, "new evidence", day=10),
                base_event_id=auxiliary.id,
                expected_revision=1,
                request_id="evidence-revised",
                producer_id="host",
                configuration_sha256="a" * 64,
            )
            assert (await service.query(context(scope, known=9), predicate="city", policy=POLICY))[
                "status"
            ] == "resolved"
            assert (await service.query(context(scope, known=11), predicate="city", policy=POLICY))[
                "status"
            ] == "unknown"

    asyncio.run(run())


def test_support_enumeration_fails_instead_of_truncating():
    from agent_memory.evidence_support import evaluate_support
    from agent_memory.serialization import to_jsonable

    scope = base.MemoryScope("t", session_id="s")
    draft, event = base.atom(), base.source(scope)
    links = [link(event, draft, identity=str(i)) for i in range(8)]
    qualification = {
        "policy": to_jsonable(POLICY),
        "links": to_jsonable(links),
        "field_support": to_jsonable(
            [FieldSupport(f, tuple((evidence.id,) for evidence in links)) for f in links[0].fields]
        ),
    }
    with pytest.raises(ValueError, match="proof budget"):
        evaluate_support(qualification, {event.id})


def test_independence_is_required_per_field_not_across_partial_fields():
    from agent_memory.evidence_support import evaluate_support
    from agent_memory.serialization import to_jsonable

    scope = base.MemoryScope("t", session_id="s")
    draft, a, b = base.atom(), base.source(scope), base.source(scope)
    links = [
        replace(link(a, draft, identity="a"), fields=("subject_id",)),
        replace(link(b, draft, identity="b", family="family-b"), fields=("value",)),
    ]
    qualification = {
        "policy": to_jsonable(replace(POLICY, require_independent_sources=True)),
        "links": to_jsonable(links),
        "field_support": to_jsonable(
            [FieldSupport("subject_id", (("a",),)), FieldSupport("value", (("b",),))]
        ),
    }
    assert evaluate_support(qualification, {a.id, b.id}) == ((), ())


def test_composition_is_order_independent_and_contested_override_never_falls_back():
    from agent_memory.retrieval.contextual_state import compose

    low = {
        "applicability_id": "global",
        "applicable": True,
        "status": "resolved",
        "value": "Chinese",
    }
    high = {
        "applicability_id": "project-A",
        "applicable": True,
        "status": "resolved",
        "value": "English",
    }
    for items in ([low, high], [high, low]):
        assert compose(items, POLICY)[0] == "ambiguous"
    ordered = replace(POLICY, mode="ordered_override", precedence=(("global", "project-A"),))
    assert compose([low, {**high, "status": "contested"}], ordered)[0] == "contested"
    assert compose([low, {**high, "applicable": None}], ordered)[0] == "unknown"


def test_unimplemented_constraint_semantics_are_explicitly_rejected(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            draft = replace(base.atom(), kind="constraint", conditions=("Only in this project",))
            event = base.source(scope)
            receipt = await engine.admit(event, [draft], authority=base.SELF, policy=base.POLICY)
            service = ContextualMemory(engine, scope, principal="host:alice")
            with pytest.raises(ValueError, match="fact/preference"):
                await qualify(service, receipt.candidate_ids[0], draft, (link(event, draft),))

    asyncio.run(run())
