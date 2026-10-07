"""Host-bound qualified language facets on SQLite and a real PostgreSQL server."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_durable_indexing import service as capture_service
from test_durable_purge import envelope, source_id

from agent_memory.conditions import Condition, ContextAttribute, ProjectionPolicy, QueryContext
from agent_memory.consolidation.admission import draft_from_payload
from agent_memory.consolidation.qualification import ContextualMemory, target_fingerprint
from agent_memory.derived import (
    DerivedError,
    FacetContext,
    FacetDefinition,
    ObservationService,
    ProcessingGrant,
)
from agent_memory.domain import AtomReview, ForgetMode, ForgetRequest, MemoryQuery
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
from agent_memory.fact_qualification import SourceSpan
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue

store = base.store
POLICY = ProjectionPolicy("locale-context/1", "agent_context")


class Language:
    version = "qualified-language-fixture/1"

    async def generate_atoms(self, event):
        project = "B" if "项目 B" in event.content else "A"
        global_ = "全部项目" in event.content
        return [
            dict(
                subject_id="alice",
                predicate="locale",
                value="en-US" if "英文" in event.content else "zh-CN",
                kind="preference",
                modality="asserted",
                source_quote=event.content,
                valid_from=base.at(1).isoformat(),
                conditions=() if global_ else ("仅限项目 " + project,),
                exceptions=("节假日除外",) if "节假日" in event.content else (),
            )
        ]

    async def review_atoms(self, event, candidates):
        return [
            AtomReview(i, "supported", "durable", ("fixture",)) for i, _ in enumerate(candidates)
        ]


def definition(
    scope,
    clock,
    *,
    project="A",
    holiday=False,
    policy=POLICY,
    token="route-A",
    expiry=3600,
    timezone=None,
):
    attrs = []
    if project is not None:
        attrs.append(ContextAttribute("project", project, "authenticated-host"))
    if holiday is not None:
        attrs.append(ContextAttribute("holiday", holiday, "authenticated-host"))
    query = QueryContext(
        "host:alice",
        scope,
        "alice",
        "agent_context",
        clock[0],
        clock[0],
        tuple(attrs),
        timezone,
        token,
    )
    return FacetDefinition(
        "language",
        "alice",
        template_version="locale-context/1",
        context=FacetContext(query, policy, clock[0] + timedelta(seconds=expiry)),
    )


async def setup(engine, kernel, scope, clock, texts=("项目 A 使用中文。",), **context):
    capture = await capture_service(engine, kernel, scope, clock, generator=Language())
    derived = ObservationService(
        engine.repository,
        scope,
        base.POLICY,
        clock=lambda: clock[0],
        context_token=context.get("token", "route-A"),
    )
    rows = []
    for seq, text in enumerate(texts, 1):
        event = envelope(scope, str(seq), clock)
        event["content"] = text
        await capture[2].durable_append(event, capture[1], seq)
        assert await capture[5].run_once()
        clock[0] += timedelta(seconds=1)
        async with engine.repository.unit_of_work() as uow:
            row = next(
                r
                for r in await uow.list_admission_records(scope)
                if r["event_id"] == source_id(scope, str(seq))
            )
        rows.append(row)
        await derived.grant(ProcessingGrant(row["event_id"], ("alice",)))
    await derived.register(definition(scope, clock, **context))
    return derived, FacetRefreshQueue(derived), capture, rows


async def qualify(
    engine, scope, clock, row, *, policy=POLICY, conditions=None, links=None, groups=None
):
    draft = draft_from_payload(row["payload"]["draft"])
    async with engine.repository.unit_of_work() as uow:
        source = await uow.get_source_event(scope, row["event_id"])
    fields = ["subject_id", "predicate", "value", "valid_from", "conditions"]
    if draft.exceptions:
        fields.append("exceptions")
    if links is None:
        links = (
            EvidenceLink(
                "primary",
                tuple(fields),
                target_fingerprint(draft),
                SourceSpan(source.id, 0, len(source.content), source.content),
                base.SELF,
                SupportRange(base.at(1)),
                source.id,
            ),
        )
    if groups is None:
        groups = tuple((link.id,) for link in links)
    fields = {f for link in links for f in link.fields}
    project = "B" if "项目 B" in source.content else "A"
    await ContextualMemory(engine, scope, principal="host:alice").qualify(
        row["id"],
        expected_version=row["version"],
        admission_policy=base.POLICY,
        projection_policy=policy,
        applicability_id="project-" + project,
        conditions=conditions or (Condition("eq", "project", project),),
        exceptions=(Condition("eq", "holiday", True),) if draft.exceptions else (),
        links=links,
        field_support=tuple(FieldSupport(f, groups) for f in sorted(fields)),
    )
    clock[0] += timedelta(seconds=1)
    async with engine.repository.unit_of_work() as uow:
        return await uow.get_admission_record(scope, row["id"])


async def build(derived, queue, *, key="first"):
    target = await queue.request("language", dedupe_key=key)
    lease = await queue.claim("context", lease_seconds=60)
    assert lease
    await derived.apply(lease.task)
    await queue.complete(lease)
    assert (await queue.status(target["target_id"], actor="alice"))["complete"]
    return await derived.read("language", actor="alice"), target


@pytest.mark.parametrize(
    "project,holiday,expected",
    [
        ("A", False, "resolved"),
        ("B", False, "unknown"),
        (None, False, "unknown"),
        ("A", True, "unknown"),
        ("A", None, "unknown"),
    ],
)
def test_conditions_and_exceptions_remain_qualified(store, project, holiday, expected):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(
                engine,
                kernel,
                scope,
                clock,
                ("项目 A 使用中文，节假日除外。",),
                project=project,
                holiday=holiday,
            )
            await qualify(engine, scope, clock, rows[0])
            view, _ = await build(derived, queue)
            if project == "B" or holiday is True:
                assert view["state"] == "empty"
            else:
                assert view["body"]["projection_status"] == expected
                block = view["body"]["blocks"][0]
                if expected == "resolved":
                    assert block["qualified"] and block["value"] == "zh-CN"
                    assert block["conditions"] and block["exceptions"] and block["field_support"]
                    assert block["support_basis"] == "field_supported"
                else:
                    assert block["kind"] == "context_unknown" and "value" not in block
            assert not (await kernel.retrieve(MemoryQuery(scope, "language"))).current_state

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["or", "and", "point"])
def test_temporal_field_proofs_preserve_gaps_and_point_expiry(store, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock)
            row = rows[0]
            draft = draft_from_payload(row["payload"]["draft"])
            async with engine.repository.unit_of_work() as uow:
                a = await uow.get_source_event(scope, row["event_id"])
                b = base.source(scope, "独立的项目 A 中文偏好证据。")
                await uow.append_event(b)
            await derived.grant(ProcessingGrant(b.id, ("alice",)))
            start = clock[0] + timedelta(seconds=1)

            def link(source, key, support):
                return EvidenceLink(
                    key,
                    ("subject_id", "predicate", "value", "valid_from", "conditions"),
                    target_fingerprint(draft),
                    SourceSpan(source.id, 0, len(source.content), source.content),
                    base.SELF,
                    support,
                    source.id,
                )

            links = (
                link(
                    a,
                    "a",
                    SupportRange(start, kind="point")
                    if mode == "point"
                    else SupportRange(base.at(1), start + timedelta(seconds=4)),
                ),
                link(
                    b,
                    "b",
                    SupportRange(start + timedelta(seconds=8), start + timedelta(seconds=12)),
                ),
            )
            groups = (("a", "b"),) if mode == "and" else (("a",), ("b",))
            await qualify(engine, scope, clock, row, links=links, groups=groups)
            view, _ = await build(derived, queue)
            block = view["body"]["blocks"][0]
            assert block["kind"] == ("context_unknown" if mode == "and" else "source_fact")
            if mode == "and":
                assert "value" not in block
            elif mode == "point":
                assert block["support_kind"] == "point"
                clock[0] += timedelta(microseconds=1)
                assert (await derived.read("language", actor="alice"))["state"] == "stale"
                after, _ = await build(derived, queue, key="after-point")
                assert after["body"]["blocks"][0]["kind"] == "context_unknown"
            else:
                assert block["evidence_link_ids"] == ["a"]
                clock[0] = start + timedelta(seconds=4)
                assert (await derived.read("language", actor="alice"))["state"] == "stale"
                gap, _ = await build(derived, queue, key="gap")
                assert gap["body"]["blocks"][0]["kind"] == "context_unknown"
                clock[0] = start + timedelta(seconds=8)
                assert (await derived.read("language", actor="alice"))["state"] == "stale"
                second, _ = await build(derived, queue, key="second")
                assert second["body"]["blocks"][0]["evidence_link_ids"] == ["b"]

    asyncio.run(run())


@pytest.mark.parametrize("timezone", [None, "Asia/Shanghai"])
def test_weekday_condition_has_current_time_boundary(store, timezone):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            # Keep the host snapshot alive across the first local midnight.
            clock[0] = base.at(1) + timedelta(hours=15)
            derived, queue, _, rows = await setup(
                engine, kernel, scope, clock, expiry=86400, timezone=timezone
            )
            await qualify(
                engine, scope, clock, rows[0], conditions=(Condition("weekday", value=(3,)),)
            )
            view, _ = await build(derived, queue)
            if timezone is None:
                assert view["body"]["blocks"][0]["kind"] == "context_unknown"
            else:
                assert view["body"]["blocks"][0]["value"] == "zh-CN"
                clock[0] = base.at(1) + timedelta(hours=16)
                assert (await derived.read("language", actor="alice"))["state"] == "stale"
                after, _ = await build(derived, queue, key="next-day")
                assert after["state"] == "empty"

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["ambiguous", "override", "unknown", "expired"])
def test_context_composition_preserves_priority_and_does_not_fallback(store, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            policy = (
                POLICY
                if mode == "ambiguous"
                else ProjectionPolicy(
                    "approved-language/1",
                    "agent_context",
                    "ordered_override",
                    (("global", "project-A"),),
                )
            )
            derived, queue, _, rows = await setup(
                engine,
                kernel,
                scope,
                clock,
                ("全部项目使用中文。", "项目 A 使用英文。"),
                policy=policy,
                project=None if mode == "unknown" else "A",
            )
            row = rows[1]
            reviewed = await qualify(engine, scope, clock, row, policy=policy)
            if mode == "expired":
                async with engine.repository.unit_of_work() as uow:
                    payload = reviewed["payload"]
                    for link in payload["qualification"]["links"]:
                        link["support"]["end"] = clock[0].isoformat()
                    await uow.save_admission_record(
                        scope,
                        reviewed["id"],
                        reviewed["event_id"],
                        reviewed["slot_key"],
                        payload,
                        reviewed["version"],
                    )
                clock[0] += timedelta(seconds=1)
            view, _ = await build(derived, queue)
            block = view["body"]["blocks"][0]
            assert block["kind"] == (
                "conflict"
                if mode == "ambiguous"
                else "source_fact"
                if mode == "override"
                else "context_unknown"
            )
            if mode == "override":
                assert block["value"] == "en-US" and block["qualified"]
            elif mode != "ambiguous":
                assert "value" not in block

    asyncio.run(run())


@pytest.mark.parametrize("survivor", [False, True])
def test_auxiliary_input_permissions_and_physical_erasure(store, survivor):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock)
            row = rows[0]
            draft = draft_from_payload(row["payload"]["draft"])
            async with engine.repository.unit_of_work() as uow:
                source = base.source(scope, "private-auxiliary-qualified-proof")
                await uow.append_event(source)
            link = EvidenceLink(
                "aux",
                ("subject_id", "predicate", "value", "valid_from", "conditions"),
                target_fingerprint(draft),
                SourceSpan(source.id, 0, len(source.content), source.content),
                base.SELF,
                SupportRange(base.at(1)),
                source.id,
            )
            links = [link]
            if survivor:
                async with engine.repository.unit_of_work() as uow:
                    remaining = base.source(scope, "independent-surviving-qualified-proof")
                    await uow.append_event(remaining)
                links.append(
                    replace(
                        link,
                        id="survivor",
                        span=SourceSpan(remaining.id, 0, len(remaining.content), remaining.content),
                        source_family=remaining.id,
                    )
                )
                await derived.grant(ProcessingGrant(remaining.id, ("alice",)))
            await qualify(engine, scope, clock, row, links=links)
            target = await queue.request("language", dedupe_key="missing-grant")
            lease = await queue.claim("denied", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_processing_denied"):
                await derived.snapshot(lease.task)
            await queue.fail(lease, DerivedError("derived_processing_denied"))
            await derived.grant(ProcessingGrant(source.id, ("alice",)))
            view, _ = await build(derived, queue, key="granted")
            assert view["body"]["blocks"][0]["evidence"][0]["span"]["quote"] == source.content
            await kernel.forget(ForgetRequest(scope, (source.id,), mode=ForgetMode.ERASE))
            assert (await derived.read("language", actor="alice"))["state"] == "erased"
            async with engine.repository.unit_of_work() as uow:
                revisions = await uow.derived_records(scope, "revision")
                assert all(
                    "body" not in r["payload"] and "manifest" not in r["payload"] for r in revisions
                )
                assert not await uow.derived_reverse(scope, "source:" + source.id)
            after, _ = await build(derived, queue, key="after-delete")
            assert after["body"]["blocks"][0]["kind"] == (
                "source_fact" if survivor else "context_unknown"
            )
            if survivor:
                assert after["body"]["blocks"][0]["evidence_link_ids"] == ["survivor"]
            assert not (await queue.status(target["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["future", "expiry", "attribute", "template", "history"])
def test_trusted_context_contract_rejects_unsupported_bindings(problem):
    scope = base.MemoryScope("t", user_id="alice", session_id="s")
    clock = [base.at(1)]
    value = definition(scope, clock)
    with pytest.raises((DerivedError, ValueError)):
        if problem == "future":
            value.context.current(base.at(1) - timedelta(seconds=1))
        elif problem == "expiry":
            replace(value.context, expires_at=base.at(1) + timedelta(days=2))
        elif problem == "attribute":
            replace(
                value.context,
                query=replace(
                    value.context.query,
                    attributes=(ContextAttribute("secret_fact", "secret", "host"),),
                ),
            )
        elif problem == "template":
            replace(value, template_version="locale-snapshot/1")
        elif problem == "history":
            replace(value.context, query=replace(value.context.query, valid_at=base.at(2)))


def test_same_slot_project_partition_and_context_cas(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(
                engine, kernel, scope, clock, ("项目 A 使用中文。", "项目 B 使用英文。")
            )
            for row in rows:
                await qualify(engine, scope, clock, row)
            first, target = await build(derived, queue)
            assert first["body"]["blocks"][0]["value"] == "zh-CN"
            lease = await queue.claim("none", lease_seconds=60)
            assert lease is None
            updated = definition(scope, clock, project="B")
            with pytest.raises(DerivedError, match="derived_definition_conflict"):
                await derived.register(updated)
            await derived.register(updated, expected_generation=1)
            assert (await derived.read("language", actor="alice"))["state"] == "stale"
            second, _ = await build(derived, queue, key="B")
            assert second["body"]["blocks"][0]["value"] == "en-US"
            assert (await queue.status(target["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize(
    "problem", ["unreviewed", "policy", "field", "target", "quote", "negated", "primary_quote"]
)
def test_incomplete_or_tampered_qualification_never_publishes(store, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock)
            if problem != "unreviewed":
                row = await qualify(engine, scope, clock, rows[0])
                async with engine.repository.unit_of_work() as uow:
                    p = row["payload"]
                    q = p["qualification"]
                    if problem == "policy":
                        q["policy_sha256"] = "0" * 64
                    elif problem == "field":
                        q["field_support"] = [
                            f for f in q["field_support"] if f["field"] != "conditions"
                        ]
                    elif problem == "target":
                        q["target_sha256"] = "0" * 64
                    elif problem == "quote":
                        q["links"][0]["span"]["quote"] = "not the source"
                    elif problem == "negated":
                        p["draft"]["negated"] = True
                    elif problem == "primary_quote":
                        p["draft"]["source_quote"] = "fabricated primary quote"
                    await uow.save_admission_record(
                        scope, row["id"], row["event_id"], row["slot_key"], p, row["version"]
                    )
                clock[0] += timedelta(seconds=1)
            target = await queue.request("language", dedupe_key="bad")
            lease = await queue.claim("bad", lease_seconds=60)
            with pytest.raises(DerivedError):
                await derived.apply(lease.task)
            assert not (await queue.status(target["target_id"], actor="alice"))["complete"]
            assert (await derived.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


def test_context_expiry_and_wrong_host_route_precede_body_reads(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock, expiry=10)
            await qualify(engine, scope, clock, rows[0])
            view, target = await build(derived, queue)
            assert view["state"] == "ready"
            for token in (None, "route-B"):
                wrong = ObservationService(
                    engine.repository,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    context_token=token,
                )
                with pytest.raises(DerivedError, match="derived_context_mismatch"):
                    await wrong.read("language", actor="alice")
                with pytest.raises(DerivedError, match="derived_context_mismatch"):
                    await FacetRefreshQueue(wrong).status(target["target_id"], actor="alice")
                assert await FacetRefreshQueue(wrong).claim("other-route", lease_seconds=60) is None
            clock[0] += timedelta(seconds=10)
            cls = type(engine.repository.unit_of_work())
            original = cls.get_source_event
            calls = []

            async def tracked(self, *args):
                calls.append(args)
                return await original(self, *args)

            monkeypatch.setattr(cls, "get_source_event", tracked)
            assert (await derived.read("language", actor="alice"))[
                "reason"
            ] == "derived_context_expired"
            assert not calls
            lease = await queue.claim("expired", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_context_expired"):
                await derived.snapshot(lease.task)
            assert not calls

    asyncio.run(run())


@pytest.mark.parametrize("change", ["context", "expiry", "grant", "delete"])
def test_change_after_prepare_blocks_atomic_publication(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock, expiry=30)
            await qualify(engine, scope, clock, rows[0])
            target = await queue.request("language", dedupe_key="race")
            lease = await queue.claim("race", lease_seconds=60)
            snap = await derived.snapshot(lease.task)
            prepared = derived.prepare(snap)
            if change == "context":
                await derived.register(definition(scope, clock, project="B"), expected_generation=1)
            elif change == "expiry":
                clock[0] += timedelta(seconds=30)
            elif change == "grant":
                await derived.grant(
                    ProcessingGrant(rows[0]["event_id"], ("alice",), revoked=True),
                    expected_version=1,
                )
            else:
                await kernel.forget(
                    ForgetRequest(scope, (rows[0]["event_id"],), mode=ForgetMode.ERASE)
                )
            with pytest.raises(DerivedError):
                await derived.publish(lease.task, snap, prepared)
            assert not (await queue.status(target["target_id"], actor="alice"))["complete"]
            assert (await derived.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_readonly_transport_cannot_supply_context_attributes(store, transport):
    async def run():
        sdk = pytest.importorskip("agent_memory_sdk")

        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock)
            await qualify(engine, scope, clock, rows[0])
            await build(derived, queue)
            host = MCPRequestContext(scope, actor="alice")

            async def exercise(client):
                assert (await client.derived_capabilities())["qualified_inputs"]
                result = await client.derived_context("language")
                assert result["observations"][0]["body"]["blocks"][0]["qualified"]
                with pytest.raises(DerivedError, match="invalid_derived_request"):
                    await derived.call(
                        "read", {"facet_id": "language", "context": {"project": "B"}}, host
                    )

            if transport == "embedded":
                await exercise(sdk.EmbeddedMemoryClient(kernel, host, derived=derived))
            else:
                mcp = pytest.importorskip("agent_memory_mcp")
                server = mcp.create_server(
                    kernel, mcp.StaticIdentityResolver(host), derived=derived
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


def test_host_routing_and_worker_ownership_do_not_mutate_other_context(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock)
            await qualify(engine, scope, clock, rows[0])
            target = await queue.request("language", dedupe_key="owned")
            other = ObservationService(
                engine.repository,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                context_token="route-B",
            )
            assert await FacetRefreshQueue(other).claim("foreign", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                job = await uow.derived_get(scope, "job", target["unit_id"])
                assert job["status"] == "pending" and job["attempts"] == 0
            bad = definition(scope, clock)
            bad = replace(
                bad,
                context=replace(
                    bad.context,
                    query=replace(
                        bad.context.query, scope=base.MemoryScope("other", user_id="alice")
                    ),
                ),
            )
            with pytest.raises(DerivedError, match="derived_context_mismatch"):
                await derived.register(bad, expected_generation=1)
            view, _ = await build(derived, queue, key="owner")
            assert view["body"]["blocks"][0]["value"] == "zh-CN"

    asyncio.run(run())


def test_final_delivery_rechecks_context_and_scope_erase_scrubs_binding(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _, rows = await setup(engine, kernel, scope, clock)
            await qualify(engine, scope, clock, rows[0])
            await build(derived, queue)
            original = derived.read
            calls = []

            async def changed(*args, **kwargs):
                result = await original(*args, **kwargs)
                calls.append(result)
                if len(calls) == 1:
                    await derived.register(
                        definition(scope, clock, project="B"), expected_generation=1
                    )
                return result

            monkeypatch.setattr(derived, "read", changed)
            result = await derived.call(
                "derived_context", {"facet_id": "language"}, MCPRequestContext(scope, actor="alice")
            )
            assert result["state"] == "stale" and not result["observations"] and len(calls) == 2
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                definition_row = await uow.derived_get(scope, "definition", "language")
                assert definition_row["disabled"] and "context" not in definition_row["spec"]
                revisions = await uow.derived_records(scope, "revision")
                assert all("body" not in r["payload"] for r in revisions)

    asyncio.run(run())
