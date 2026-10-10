"""Finite maintained dependency joins on the real SQLite/live-PostgreSQL runtime."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
import test_question_runtime_v7 as runtime

from agent_memory.conditions import Condition
from agent_memory.consolidation.project_admission import ProjectAdmission, ProjectMembership
from agent_memory.consolidation.qualification import target_fingerprint
from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.project_questions import ProjectDomainContract
from agent_memory.derived.question_model import QuestionContext
from agent_memory.derived.question_service import QuestionService
from agent_memory.derived.relation_questions import DependencyRiskPlan
from agent_memory.domain import SourceAuthority
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
from agent_memory.fact_qualification import FieldEvidence, SourceSpan
from agent_memory.ontology.rules import RelationRule
from agent_memory.operations.worker_tasks import WorkerQueueError

store = project.store
CHAIN = RelationRule("project-chain", "1", "project.depends_on", "deliverable.depends_on", "project.depends_on")
PLAN = DependencyRiskPlan("late-dependencies", "1", relation_rules=(CHAIN,))


def service(engine, scope, clock, plan=PLAN):
    contract = replace(project.CONTRACT, relation_plans=(plan,))
    members = (
        *(replace(m, binding_id="project-b") if m.binding_id == "b" else m for m in project.MEMBERSHIPS),
        *(ProjectMembership(key, "registry/1", "project-a", key) for key in ("b", "c", "d")),
        ProjectMembership("foreign-d", "registry/1", "project-b", "d"),
    )
    # Binding IDs are unique even when an entity belongs to multiple projects.
    members = tuple(replace(m, binding_id="member:" + m.binding_id) for m in members)
    authority = SourceAuthority("project-system", "tool_observation",
                                ("project-a", "project-b", "promise-1", "b", "c", "d"),
                                tuple(p.predicate for p in contract.predicate_specs))
    admission = ProjectAdmission(engine, scope, principal=runtime.ACTOR, contract=contract,
                                 authorities=(authority,), memberships=members,
                                 reviewer_version="review/1", clock=lambda: clock[0])
    return QuestionService(admission, QuestionContext("host", "1", {}, clock[0] + timedelta(days=30)))


async def fact(svc, *, identity, subject="project-a", predicate="project.depends_on", value="b",
               valid_from=None, valid_to=None, conditions=(), qualified_conditions=(), review=True,
               membership=None):
    event, draft = project.inputs(svc.scope, identity=identity, subject=subject,
                                  predicate=predicate, value=value, valid_from=valid_from,
                                  conditions=conditions)
    if valid_to:
        draft = replace(draft, valid_to=valid_to, field_evidence=(*draft.field_evidence,
                        FieldEvidence("valid_to", ((SourceSpan(event.id, 0, len(event.content), event.content),),))))
    membership = membership or ("member:a" if subject == "project-a" else "member:" + subject)
    receipt = await svc.admission.stage_source(event, (draft,), source_authority_id="project-system",
                                               request_id="request:" + event.id,
                                               membership_ids=(membership,))
    await project.grant(svc.repository, svc.scope, event.id)
    key = receipt.candidate_ids[0]
    if review:
        fields = tuple(e.field for e in draft.field_evidence)
        evidence = EvidenceLink("evidence", fields, target_fingerprint(draft),
                                SourceSpan(event.id, 0, len(event.content), event.content),
                                svc.admission.authorities["project-system"],
                                SupportRange(draft.valid_from, draft.valid_to))
        await svc.admission.qualify(key, expected_version=2, review_id="review:" + event.id,
                                    applicability_id="explicit", conditions=qualified_conditions,
                                    links=(evidence,),
                                    field_support=tuple(FieldSupport(f, ((evidence.id,),)) for f in fields))
    return key


async def seed(svc, **edge_options):
    edge = await fact(svc, identity="edge", **edge_options)
    launch = await fact(svc, identity="launch", predicate="project.launch_date", value="2026-10-10T00:00:00Z")
    due = await fact(svc, identity="due", subject="b", predicate="deliverable.commitment_date", value="2026-10-12T00:00:00Z")
    return edge, launch, due


def rule(result):
    return next(row for row in result["result"]["rows"] if row["id"] == "rule:late-dependencies")


async def fresh(svc, clock, dedupe="read"):
    return await runtime.fresh(svc, clock, "risks", dedupe=dedupe)


def test_dependency_join_is_maintained_candidate_with_interval_and_full_lineage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            premises = await seed(svc, valid_to=project.base.at(4))
            await runtime.register(svc, "risks")
            result = await fresh(svc, clock)
            row = rule(result)
            assert result["answer_status"] == "resolved"
            assert row["origin"] == "inferred" and row["qualification"] == "rule_candidate"
            conclusion = row["conclusions"][0]
            assert conclusion["matches"] is True
            assert set(conclusion["premise_ids"]) == set(premises)
            assert set(conclusion["source_event_ids"]) == {"edge", "launch", "due"}
            assert conclusion["valid_from"] == project.base.at(1).isoformat()
            assert conclusion["valid_to"] == project.base.at(4).isoformat()
            assert conclusion["lag_microseconds"] == 2 * 86400 * 1_000_000
            assert result["valid_until"] == project.base.at(4).isoformat()
            assert row["aggregates"]["exact"]
            assert row["aggregates"]["dependencies"]["sum"] == 1
            async with engine.repository.unit_of_work() as uow:
                for key in premises:
                    assert result["instance_id"] in await uow.derived_reverse(scope, "atom:" + key)
                assert not (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0]
            assert (await fresh(svc, clock, "again"))["content_revision_id"] == result["content_revision_id"]
    asyncio.run(run())


def test_typed_chain_reuses_ontology_rules_and_deduplicates_dependency_aggregates(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            edge, launch, due = await seed(svc)
            second = await fact(svc, identity="b-c", subject="b", predicate="deliverable.depends_on", value="c")
            c_due = await fact(svc, identity="c-due", subject="c", predicate="deliverable.commitment_date", value="2026-10-13T00:00:00Z")
            await runtime.register(svc, "risks")
            row = rule(await fresh(svc, clock))
            assert len(row["conclusions"]) == 2
            indirect = next(c for c in row["conclusions"] if c["deliverable_id"] == "c")
            assert set(indirect["premise_ids"]) == {edge, launch, second, c_due}
            assert indirect["rule_versions"] == ["late-dependencies@1", "project-chain@1"]
            assert row["aggregates"]["dependencies"]["sum"] == 2
            await fact(svc, identity="direct-c", value="c")
            row = rule(await fresh(svc, clock, "duplicate-path"))
            assert len(row["conclusions"]) == 2 and row["aggregates"]["dependencies"]["sum"] == 2
    asyncio.run(run())


def test_date_edge_retraction_and_new_dependency_invalidate_complete_frontier(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            edge, launch, due = await seed(svc)
            await runtime.register(svc, "risks")
            first = await fresh(svc, clock)
            await svc.admission.withdraw(due, expected_version=4, review_id="replace-date", reasons=("superseded",))
            await fact(svc, identity="earlier", subject="b", predicate="deliverable.commitment_date", value="2026-10-09T00:00:00Z")
            with pytest.raises(DerivedError, match="stale"):
                await svc.read("project-a:risks", actor=runtime.ACTOR)
            changed = await fresh(svc, clock, "changed-date")
            assert changed["answer_status"] == "empty"
            assert rule(changed)["aggregates"]["dependencies"]["sum"] == 0
            await fact(svc, identity="new-edge", value="c")
            pending = await fresh(svc, clock, "new-edge")
            assert pending["answer_status"] == "unknown"
            assert not rule(pending)["aggregates"]["exact"]
            await fact(svc, identity="new-due", subject="c", predicate="deliverable.commitment_date", value="2026-10-14T00:00:00Z")
            added = await fresh(svc, clock, "new-date")
            assert added["answer_status"] == "resolved"
            assert len(rule(added)["conclusions"]) == 2
            await svc.admission.withdraw(edge, expected_version=4, review_id="remove-edge", reasons=("superseded",))
            removed = await fresh(svc, clock, "removed-edge")
            assert [c["deliverable_id"] for c in rule(removed)["conclusions"]] == ["c"]
            assert first["content_revision_id"] != removed["content_revision_id"]
    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["new-premise", "withdraw", "revoke", "expiry", "plan-version", "delete"])
def test_relation_premises_fence_compute_to_publish_and_never_emit_stale_candidate(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            edge, launch, due = await seed(svc)
            await runtime.register(svc, "risks")
            if mutation == "expiry":
                await svc.grant(ProcessingGrant("due", (runtime.ACTOR,), ("project_questions",),
                                                expires_at=clock[0] + timedelta(seconds=2)), expected_version=1)
            _, lease, snapshot, prepared = await runtime.lease_snapshot(svc, clock, "risks")
            if mutation == "new-premise":
                await fact(svc, identity="new-edge", value="c")
            elif mutation == "withdraw":
                await svc.admission.withdraw(edge, expected_version=4, review_id="withdraw", reasons=("removed",))
            elif mutation == "revoke":
                await svc.grant(ProcessingGrant("due", (runtime.ACTOR,), ("project_questions",), revoked=True), expected_version=1)
            elif mutation == "expiry":
                clock[0] += timedelta(seconds=2)
            elif mutation == "plan-version":
                svc.admission.contract = replace(svc.admission.contract, relation_plans=(replace(PLAN, version="2"),))
            else:
                await runtime.erase(kernel, scope, "due")
            with pytest.raises((DerivedError, ValueError, WorkerQueueError)):
                await svc.publish(lease.task, snapshot, prepared)
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "question_content")
    asyncio.run(run())


@pytest.mark.parametrize("case", ["unknown-edge", "unknown-date", "unknown-rule", "missing-date", "contested-date", "unqualified", "truncated-count", "truncated-round", "truncated-candidates"])
def test_unknown_conflicted_and_truncated_premises_never_authorize_absence(store, case):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            cond = Condition("eq", "release", "ready")
            plan = replace(PLAN, impact_conditions=(cond,)) if case == "unknown-rule" else PLAN
            if case == "truncated-count":
                plan = replace(plan, max_dependencies=1)
            if case == "truncated-round":
                plan = replace(plan, max_rounds=1)
            if case == "truncated-candidates":
                plan = replace(plan, max_candidates=1)
            svc = service(engine, scope, clock, plan)
            opts = dict(conditions=("release is ready",), qualified_conditions=(cond,)) if case == "unknown-edge" else {}
            await fact(svc, identity="edge", **opts)
            await fact(svc, identity="launch", predicate="project.launch_date", value="2026-10-10T00:00:00Z")
            if case != "missing-date":
                opts = dict(conditions=("release is ready",), qualified_conditions=(cond,)) if case == "unknown-date" else {}
                await fact(svc, identity="due", subject="b", predicate="deliverable.commitment_date",
                           value="2026-10-12T00:00:00Z", review=case != "unqualified", **opts)
            if case == "contested-date":
                await fact(svc, identity="conflict", subject="b", predicate="deliverable.commitment_date", value="2026-10-09T00:00:00Z")
            if case.startswith("truncated"):
                await fact(svc, identity="chain", subject="b", predicate="deliverable.depends_on", value="c")
                await fact(svc, identity="c-due", subject="c", predicate="deliverable.commitment_date", value="2026-10-12T00:00:00Z")
            if case == "truncated-candidates":
                await fact(svc, identity="second-chain", subject="b", predicate="deliverable.depends_on", value="d")
                await fact(svc, identity="d-due", subject="d", predicate="deliverable.commitment_date", value="2026-10-12T00:00:00Z")
            await runtime.register(svc, "risks")
            result = await fresh(svc, clock)
            expected = "incomplete" if case.startswith("truncated") or case == "unqualified" else "contested" if case == "contested-date" else "unknown"
            assert result["answer_status"] == expected
            assert result["result"]["world_negative"] is False
            assert not rule(result)["aggregates"]["exact"]
            if case.startswith("truncated"):
                assert "relation_frontier_truncated" in result["result"]["reasons"]
    asyncio.run(run())


def test_future_premise_interval_enters_without_write_and_disjoint_premises_do_not_join(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            await seed(svc, valid_to=project.base.at(2))
            await fact(svc, identity="future-edge", value="c", valid_from=project.base.at(2))
            await fact(svc, identity="future-due", subject="c", predicate="deliverable.commitment_date",
                       value="2026-10-14T00:00:00Z", valid_from=project.base.at(2))
            await runtime.register(svc, "risks")
            result = await fresh(svc, clock)
            assert result["valid_until"] == project.base.at(2).isoformat()
            assert [c["deliverable_id"] for c in rule(result)["conclusions"]] == ["b"]
            clock[0] = project.base.at(2)
            with pytest.raises(DerivedError, match="time_coverage_expired"):
                await svc.read("project-a:risks", actor=runtime.ACTOR)
            result = await fresh(svc, clock, "boundary")
            assert [c["deliverable_id"] for c in rule(result)["conclusions"]] == ["c"]
    asyncio.run(run())


def test_plan_validation_is_finite_typed_and_disabled_contracts_keep_identity():
    contract = ProjectDomainContract("projects", "1", "review/1", "accountable", ("active", "paused"))
    assert contract.fingerprint == project.CONTRACT.fingerprint
    assert "project.depends_on" not in {p.predicate for p in contract.predicate_specs}
    for changes in ({"max_rounds": 9}, {"max_dependencies": 0}, {"max_candidates": True}, {"schema": "sql"}):
        with pytest.raises(DerivedError):
            replace(PLAN, **changes)
    with pytest.raises(ValueError, match="type compatible"):
        replace(PLAN, relation_rules=(RelationRule("bad", "1", "deliverable.depends_on", "project.depends_on", "project.depends_on"),))
    with pytest.raises(DerivedError, match="duplicate"):
        replace(contract, relation_plans=(PLAN, PLAN))


def test_relation_delta_replay_matches_full_oracle_with_all_typed_metadata():
    import random
    import test_question_delta_v7 as delta
    import test_project_questions_v7 as p
    from agent_memory.derived.question_delta import evaluate

    contract = replace(project.CONTRACT, qualification_revision="host-reviewed/1", relation_plans=(PLAN,))
    rng, facts, old, saw_delta = random.Random(303), {}, None, False
    choices = (
        ("P", "project.depends_on", ("B", "C")),
        ("B", "deliverable.depends_on", ("C", "D")),
        ("C", "deliverable.depends_on", ("B", "D")),
        ("P", "project.launch_date", (p.at(20).isoformat(), p.at(60).isoformat())),
        ("B", "deliverable.commitment_date", (p.at(10).isoformat(), p.at(80).isoformat())),
        ("C", "deliverable.commitment_date", (p.at(30).isoformat(), p.at(70).isoformat())),
    )
    for step in range(100):
        subject, predicate, values = rng.choice(choices)
        key = subject + predicate + str(rng.randrange(2))
        if rng.random() < 0.2:
            facts.pop(key, None)
        else:
            facts[key] = p.qualified(
                predicate, rng.choice(values), entity=subject, fact_id=key,
                conditions=(Condition("eq", "release", "ready"),) if rng.random() < 0.1 else (),
                valid_from=p.at(rng.choice((0, 15, 45))), valid_to=p.at(75) if rng.random() < 0.2 else None,
            )
        data = delta.work(contract, facts.values(), "risks", old=old, sequence=step,
                          at=10 + step, pending=("pending",) if step % 7 == 0 else ())
        result, old, trace = evaluate(contract, data)
        assert result == delta.oracle(contract, data), step
        assert all("conclusions" in row and "aggregates" in row and "qualification" in row
                   for row in result["rows"])
        saw_delta |= trace["compute_mode"] == "delta"
    assert saw_delta


@pytest.mark.parametrize("question", ["owner", "status"])
def test_relation_predicates_do_not_change_unrelated_answer_semantics(store, question):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            await fact(svc, identity="owner", predicate="project.owner", value="Alice")
            await fact(svc, identity="status", predicate="project.status", value="active")
            await runtime.register(svc, question)
            first = await runtime.fresh(svc, clock, question)
            await seed(svc)
            with pytest.raises(DerivedError, match="stale"):
                await svc.read("project-a:" + question, actor=runtime.ACTOR)
            current = await runtime.fresh(svc, clock, question, dedupe="relations-arrived")
            assert current["result"]["rows"] == first["result"]["rows"]
            assert current["validation_manifest"] != first["validation_manifest"]
            assert {r["id"] for r in current["validation_manifest"]["inputs"] if r["kind"] == "source"} == {"owner", "status", "edge", "launch", "due"}
    asyncio.run(run())


def test_relation_index_upgrade_rebuilds_legacy_wildcard_and_fences_old_head(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            await runtime.register(svc, "risks")
            initial = await fresh(svc, clock)
            assert initial["answer_status"] == "empty"
            key = await fact(svc, identity="legacy-edge", review=False)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.get_admission_record(scope, key)
                row["payload"].pop("project_candidate")
                await svc.admission._save(uow, row)
                await uow.derived_put(scope, "project_index", "scope", dict(
                    schema="derived-project-index/1", state="ready", generation=1))
            # Simulate the prior writer's omission of the newly recognized raw
            # predicate in route/header metadata, while retaining its ledger row.
            if hasattr(engine.repository, "pool"):
                from agent_memory_postgres import project_index as backend
                async with engine.repository.unit_of_work() as uow:
                    await backend.replace(uow.connection, scope, key, None)
            else:
                from agent_memory.operations import sqlite_project_index as backend
                async with engine.repository.unit_of_work() as uow:
                    backend.replace(uow.connection, scope, key, None)
            async with engine.repository.unit_of_work() as uow:
                headers = await uow.derived_project_candidates(scope, svc.admission.contract.fingerprint, "project-a")
                assert [h["id"] for h in headers] == [key]
                assert headers[0]["project"]["unreviewed"]
                assert (await uow.derived_get(scope, "project_index", "scope"))["schema"] == "derived-project-index/2"
            current = await fresh(svc, clock, "migrated")
            assert current["answer_status"] == "incomplete"
            assert not rule(current)["aggregates"]["exact"]
    asyncio.run(run())


@pytest.mark.parametrize("denial", ["revoke", "expiry", "purpose", "reader", "scope", "delete"])
def test_relation_cached_read_fails_closed_before_any_body(store, monkeypatch, denial):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            await seed(svc)
            if denial == "expiry":
                await svc.grant(ProcessingGrant("due", (runtime.ACTOR,), ("project_questions",),
                                                expires_at=clock[0] + timedelta(seconds=3)), expected_version=1)
            await runtime.register(svc, "risks")
            await fresh(svc, clock)
            if denial == "expiry":
                clock[0] += timedelta(seconds=3)
            elif denial == "delete":
                await runtime.erase(kernel, scope, "due")
            elif denial == "scope":
                svc = service(engine, replace(scope, namespace="other"), clock)
            else:
                grant = ProcessingGrant("due", (runtime.ACTOR,) if denial != "reader" else ("other",),
                                        ("project_questions",) if denial != "purpose" else ("other",),
                                        revoked=denial == "revoke")
                await svc.grant(grant, expected_version=1)
            cls = type(engine.repository.unit_of_work())
            original_get = cls.derived_get
            async def checked_get(self, scope, kind, key):
                assert kind not in {"question_content", "question_certificate", "question_delta_state"}
                return await original_get(self, scope, kind, key)
            async def forbidden(*args, **kwargs):
                raise AssertionError("denied relation answer fetched a premise body")
            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_get", checked_get)
                patch.setattr(cls, "get_source_event", forbidden)
                patch.setattr(cls, "get_admission_record", forbidden)
                with pytest.raises(DerivedError):
                    await svc.read("project-a:risks", actor=runtime.ACTOR)
    asyncio.run(run())


def test_join_never_borrows_a_date_from_another_projects_census(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            await fact(svc, identity="edge", value="d")
            await fact(svc, identity="launch", predicate="project.launch_date", value="2026-10-10T00:00:00Z")
            await fact(svc, identity="foreign-date", subject="d", membership="member:foreign-d",
                       predicate="deliverable.commitment_date", value="2026-10-12T00:00:00Z")
            await svc.grant(ProcessingGrant("foreign-date", (runtime.ACTOR,), ("project_questions",), revoked=True), expected_version=1)
            await runtime.register(svc, "risks")
            result = await fresh(svc, clock)
            assert result["answer_status"] == "unknown"
            assert rule(result)["conclusions"][0]["matches"] is None
            assert "foreign-date" not in {r["id"] for r in result["generation_manifest"]["inputs"]}
            assert "foreign-date" not in {r["source_event_id"] for r in result["processing_references"]}
    asyncio.run(run())


def test_relation_host_membership_is_required_and_typed(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            for value in ("unregistered", "project-a", "project-b"):
                with pytest.raises(DerivedError, match="target_membership"):
                    await fact(svc, identity="bad:" + value, value=value)
            with pytest.raises(DerivedError, match="entity_type"):
                await fact(svc, identity="wrong-type", predicate="deliverable.commitment_date",
                           value="2026-10-12T00:00:00Z")
    asyncio.run(run())


def test_equal_offset_dates_do_not_create_false_conflict_or_positive_risk(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            await fact(svc, identity="edge")
            await fact(svc, identity="launch", predicate="project.launch_date", value="2026-10-12T00:00:00Z")
            await fact(svc, identity="due-z", subject="b", predicate="deliverable.commitment_date", value="2026-10-12T00:00:00Z")
            await fact(svc, identity="due-offset", subject="b", predicate="deliverable.commitment_date", value="2026-10-11T20:00:00-04:00")
            await runtime.register(svc, "risks")
            result = await fresh(svc, clock)
            assert result["answer_status"] == "empty"
            conclusion = rule(result)["conclusions"][0]
            assert conclusion["status"] == "resolved" and conclusion["lag_microseconds"] == 0
            assert set(conclusion["source_event_ids"]) == {"edge", "launch", "due-z", "due-offset"}
    asyncio.run(run())


def test_rule_calendar_condition_reuses_current_view_transition_scheduler(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            # October 1, 2026 is Thursday; the next midnight changes applicability.
            svc = service(engine, scope, clock, replace(PLAN, impact_conditions=(Condition("weekday", value=(3,)),)))
            await seed(svc)
            await runtime.register(svc, "risks")
            initial = await fresh(svc, clock)
            assert initial["answer_status"] == "resolved"
            assert initial["valid_until"] == project.base.at(2).isoformat()
            clock[0] = project.base.at(2)
            updated = await fresh(svc, clock, "friday")
            assert updated["answer_status"] == "empty"
            assert updated["valid_until"] == project.base.at(3).isoformat()
    asyncio.run(run())


def test_unproved_census_membership_blocks_aggregate_exactness_even_outside_plan():
    import test_question_delta_v7 as delta
    import test_project_questions_v7 as p
    from agent_memory.derived.question_delta import evaluate

    contract = replace(project.CONTRACT, qualification_revision="host-reviewed/1", relation_plans=(PLAN,))
    facts = [p.qualified("project.depends_on", "B"),
             p.qualified("project.launch_date", p.at(10).isoformat()),
             p.qualified("deliverable.commitment_date", p.at(20).isoformat(), entity="B")]
    data = delta.work(contract, facts, "risks")
    initial, old, _ = evaluate(contract, data)
    assert initial["rows"][0]["aggregates"]["exact"]
    facts.append(p.qualified("project.owner", "Alice", support=("predicate", "value", "valid_from")))
    data = delta.work(contract, facts, "risks", old=old, sequence=1)
    updated, _, trace = evaluate(contract, data)
    assert updated == delta.oracle(contract, data)
    assert trace["compute_mode"] == "delta"
    assert updated["status"] == "incomplete"
    assert updated["rows"][0]["aggregates"]["total_dependencies"] is None
    assert updated["rows"][0]["aggregates"]["unknown_frontier"]


def test_false_impact_condition_does_not_turn_missing_date_into_exact_lag():
    import test_project_questions_v7 as p
    from agent_memory.derived.project_questions import full_project_question

    plan = replace(PLAN, impact_conditions=(Condition("weekday", value=(0,)),))
    contract = replace(project.CONTRACT, qualification_revision="host-reviewed/1", relation_plans=(plan,))
    census = p.snapshot(contract, (p.qualified("project.depends_on", "B"),))
    row = full_project_question(contract, census, "risks").rows[0]
    assert row.matches is False
    assert row.aggregates["lag"]["unknown_count"] == 1
    assert not row.aggregates["exact"]
    assert row.conclusions[0].lag_microseconds is None


def test_protocol_root_overflow_is_incomplete_and_endpoint_join_is_bounded():
    import test_project_questions_v7 as p
    from agent_memory.derived.project_questions import full_project_question

    contract = replace(project.CONTRACT, qualification_revision="host-reviewed/1", relation_plans=(PLAN,))
    facts = tuple(p.qualified("project.depends_on", "D" + str(i)) for i in range(129))
    result = full_project_question(contract, p.snapshot(contract, facts), "risks")
    assert result.status.value == "incomplete"
    assert len(result.rows[0].conclusions) == PLAN.max_dependencies
    assert not result.rows[0].aggregates["exact"]
    assert "relation_frontier_truncated" in result.reasons


def test_relation_readset_declares_exact_plan_predicates_and_conditions():
    import test_question_dependencies_v7 as deps
    from agent_memory.derived.question_dependencies import bound_readset
    from agent_memory.derived.project_questions import ProjectRiskRule
    from agent_memory.serialization import to_jsonable

    plan = replace(PLAN, impact_conditions=(Condition("eq", "region", "US"),))
    contract = replace(project.CONTRACT, relation_plans=(plan,), risk_rules=(
        ProjectRiskRule("paused", "1", "project.status", "paused", "Paused"),
    ))
    definition = deps.definition(contract, question="risks")
    read = bound_readset(definition, deps.p.SCOPE)
    assert read is not None
    assert read["predicates"] == sorted({*plan.predicates, "risk.label", "risk.state", "project.status"})
    declared = read["relation_plans"][0]
    assert declared["plan"]["impact_conditions"] == to_jsonable(plan.impact_conditions)
    assert declared["fingerprint"] == plan.fingerprint
    assert declared["qualifier_scope"] == "all_declared_predicate_conditions_and_exceptions"
    assert declared["context_scope"] == "complete_registered_context"
    # An attribute unknown at registration never becomes a no-op qualifier.
    assert "region" in str(declared)
    for damage in ("omit-predicate", "omit-plan", "condition", "operator", "fingerprint"):
        from copy import deepcopy
        altered = deepcopy(definition)
        value = altered["spec"]["semantic_readset"]
        if damage == "omit-predicate":
            value["predicates"].remove("deliverable.commitment_date")
        elif damage == "omit-plan":
            value.pop("relation_plans")
        elif damage == "condition":
            value["relation_plans"][0]["plan"]["impact_conditions"] = []
        elif damage == "operator":
            value["relation_plans"][0]["plan"]["schema"] = "general-sql/1"
        else:
            value["relation_plans"][0]["fingerprint"] = "f" * 64
        deps.reseal(value)
        assert bound_readset(altered, deps.p.SCOPE) is None


def test_relation_reviewed_effect_is_exact_but_new_entrant_and_proofs_stay_broad(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            _, _, due = await seed(svc)
            await fact(svc, identity="owner", predicate="project.owner", value="Alice")
            await fact(svc, identity="status", predicate="project.status", value="active")
            registered = {}
            for question in ("owner", "status", "risks"):
                registered[question] = await runtime.register(svc, question)
                await runtime.fresh(svc, clock, question, dedupe="initial:" + question)
            async with engine.repository.unit_of_work() as uow:
                before = {q: await uow.derived_get(scope, "definition", r["instance_id"])
                          for q, r in registered.items()}
                header = await uow.derived_header(scope, due)
                assert header["field_effect"]["predicate"] == "deliverable.commitment_date"
                row = await uow.get_admission_record(scope, due)
                await svc.admission._save(uow, row)
                for question, registration in registered.items():
                    current = await uow.derived_get(scope, "definition", registration["instance_id"])
                    assert current["proof_dirty"] and current["dirty"]
                    assert current["proof_dirty_count"] == before[question].get("proof_dirty_count", 0) + 1
                    assert current["semantic_dirty"] is (question == "risks")
                    assert current["last_invalidation"]["classification"] == (
                        "predicate_overlap" if question == "risks" else "predicate_disjoint"
                    )
            for question in registered:
                with pytest.raises(DerivedError, match="stale"):
                    await svc.read("project-a:" + question, actor=runtime.ACTOR)
                current = await runtime.fresh(svc, clock, question, dedupe="review-refresh:" + question)
                if question != "risks":
                    assert current["compute_trace"]["groups_evaluated"] == 0
            await fact(svc, identity="new-edge", value="c", review=False)
            async with engine.repository.unit_of_work() as uow:
                for registration in registered.values():
                    current = await uow.derived_get(scope, "definition", registration["instance_id"])
                    assert current["semantic_dirty"] and current["proof_dirty"]
                    assert current["last_invalidation"]["classification"] == "conservative"
            assert (await fresh(svc, clock, "new-frontier"))["answer_status"] == "incomplete"
    asyncio.run(run())


def test_relation_views_share_batch_census_without_sharing_or_losing_rule_lineage(store):
    from agent_memory.derived.question_inputs import QuestionInputWork
    from agent_memory.derived.project_questions import full_project_question
    from agent_memory.serialization import to_jsonable

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            premises = await seed(svc)
            await fact(svc, identity="owner", predicate="project.owner", value="Alice")
            await fact(svc, identity="status", predicate="project.status", value="active")
            questions, leases = ("owner", "status", "risks"), []
            for question in questions:
                await runtime.register(svc, question)
            clock[0] += timedelta(microseconds=100)
            for question in questions:
                receipt = await svc.request("project-a:" + question, actor=runtime.ACTOR, dedupe_key=question)
                lease = await svc.queue.claim("relation-batch", lease_seconds=30,
                                              target_id=receipt["target_id"])
                assert lease is not None
                leases.append(lease)
            tasks = [lease.task for lease in leases]
            svc.input_work = QuestionInputWork()
            snapshots = await svc.snapshot_many(tasks)
            assert svc.input_work.qualified_census_builds == 1
            assert svc.input_work.qualified_census_reuses == 2
            assert snapshots == [await svc.snapshot(task) for task in tasks]
            prepared = [svc.prepare(snapshot) for snapshot in snapshots]
            svc.input_work = QuestionInputWork()
            await svc.publish_many(tasks, snapshots, prepared)
            assert svc.input_work.qualified_census_builds == 1
            assert svc.input_work.qualified_census_reuses == 2
            for lease in leases:
                await svc.queue.complete(lease)
            answers = await svc.read_many(["project-a:" + question for question in questions],
                                           actor=runtime.ACTOR)
            assert [a["answer_status"] for a in answers] == ["resolved"] * 3
            for answer, snapshot in zip(answers, snapshots):
                expected = full_project_question(svc.admission.contract,
                                                snapshot["census"].snapshot, snapshot["question"])
                assert answer["result"]["rows"] == to_jsonable(expected.rows)
            conclusion = rule(answers[2])["conclusions"][0]
            assert set(conclusion["premise_ids"]) == set(premises)
            assert conclusion["qualification"] == "rule_candidate"
            assert {ref["source_event_id"] for ref in answers[0]["citations"]} == {"owner"}
            await svc.grant(ProcessingGrant("due", (runtime.ACTOR,), ("project_questions",), revoked=True), expected_version=1)
            with pytest.raises(DerivedError, match="processing_denied"):
                await svc.read_many(["project-a:" + question for question in questions], actor=runtime.ACTOR)
    asyncio.run(run())
