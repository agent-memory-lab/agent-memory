"""ST16: explicitly opted-in, current qualified graphs on both real stores."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_contextual import (
    build,
    qualify,
    setup,
)
from test_derived_contextual import (
    definition as context_definition,
)
from test_derived_parents import publish

from agent_memory.conditions import ContextAttribute, ProjectionPolicy
from agent_memory.derived import (
    DerivedError,
    FacetDefinition,
    ObservationService,
    PageDefinition,
    ProcessingGrant,
    ScenarioDefinition,
)
from agent_memory.derived.model import digest
from agent_memory.derived.qualified import (
    QUALIFIED_PAGE_TEMPLATE,
    QUALIFIED_PARENT_TEMPLATE,
    manifest_fields,
    route_proof,
    validate_route_edge,
)
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue

store = base.store


def test_never_published_qualified_routes_are_retired_on_source_query_erasure():
    from agent_memory.derived.model import erase_rows

    scope = base.MemoryScope("route-tests", user_id="alice", session_id="s")
    leaf = context_definition(scope, [base.at(1)])
    specs = [
        leaf.payload(),
        parent_definition(leaf.context).payload(),
        page_definition(scope, leaf.context).payload(),
    ]
    rows = [
        dict(
            kind="definition",
            identity=spec["id"],
            payload=dict(
                spec=spec,
                slots=["locale-slot"] if spec["id"] == "language" else [],
                dirty=False,
                disabled=False,
                safety_generation=0,
            ),
        )
        for spec in specs
    ]
    rows.append(
        dict(
            kind="request",
            identity="old-target",
            payload=dict(
                facet_id="qualified-page", context_token="route-A", unit={"parents": "private"}
            ),
        )
    )
    changes, affected = erase_rows(rows, {"source:removed-before-build"}, False, ("locale-slot",))
    assert affected == {"qualified-parent", "qualified-page"}
    values = {(kind, key): value for kind, key, value in changes}
    assert values["definition", "language"]["spec"]["context"] == leaf.context.payload()
    assert not values["definition", "language"]["disabled"]
    for key in affected:
        assert values["definition", key]["disabled"]
        assert "context" not in values["definition", key]["spec"]
        assert "parent_facets" not in values["definition", key]["spec"]
    assert values["request", "old-target"] == {"id": "old-target", "invalidated": True}


def test_empty_scope_erasure_scrubs_never_published_qualified_parent_metadata():
    from agent_memory.derived.model import erase_rows

    scope = base.MemoryScope("route-tests", user_id="alice", session_id="s")
    leaf = context_definition(scope, [base.at(1)])
    spec = parent_definition(leaf.context).payload()
    rows = [
        dict(
            kind="definition",
            identity=spec["id"],
            payload=dict(
                spec=spec,
                slots=[],
                dirty=False,
                disabled=False,
                safety_generation=0,
            ),
        )
    ]
    changes, _ = erase_rows(rows, set(), True)
    assert changes[0][2]["disabled"]
    assert "context" not in changes[0][2]["spec"]
    assert "parent_facets" not in changes[0][2]["spec"]


def parent_definition(binding, *, key="qualified-parent", parents=("language",), **changes):
    return FacetDefinition(
        key,
        "alice",
        template_version=QUALIFIED_PARENT_TEMPLATE,
        parent_facets=parents,
        context=binding,
        **changes,
    )


def page_definition(scope, binding, *, parents=("qualified-parent",), **changes):
    return PageDefinition(
        "qualified-page",
        ScenarioDefinition("scenario", scope, "alice", "Qualified language"),
        parents,
        template_version=QUALIFIED_PAGE_TEMPLATE,
        context=binding,
        **changes,
    )


def clone_service(service, clock, **changes):
    return ObservationService(
        service.repository,
        service.scope,
        base.POLICY,
        clock=lambda: clock[0],
        context_token=service.context_token,
        qualified_current=True,
        **changes,
    )


async def ready(
    engine, kernel, scope, clock, *, holiday=False, project="A", conflict=False, authority=False
):
    texts = ("项目 A 使用中文，节假日除外。",)
    if conflict:
        texts += ("项目 A 使用英文，节假日除外。",)
    legacy, _, capture, rows = await setup(
        engine, kernel, scope, clock, texts, holiday=holiday, project=project
    )
    for row in rows:
        await qualify(engine, scope, clock, row)
    async with engine.repository.unit_of_work() as uow:
        leaf = await uow.derived_get(scope, "definition", "language")
    from agent_memory.derived import FacetContext

    binding = FacetContext.from_payload(leaf["spec"]["context"])
    controls = (
        dict(authority_id="qualified-authority", authority_min_version=0) if authority else {}
    )
    service = clone_service(legacy, clock, **controls)
    if authority:
        from agent_memory.derived.contracts import HostGrantAuthority

        await service.set_authority(
            HostGrantAuthority(service.authority_id, ("alice",), clock[0] + timedelta(minutes=30))
        )
        for row in rows:
            await service.grant(ProcessingGrant(row["event_id"], ("alice",)), expected_version=1)
    await service.register(
        FacetDefinition(
            "language",
            "alice",
            context=binding,
            template_version="locale-context/1",
            authority_id=service.authority_id,
        ),
        expected_generation=1,
    )
    queue = FacetRefreshQueue(service)
    await build(service, queue)
    control = dict(authority_id=service.authority_id) if authority else {}
    child = replace(binding, expires_at=binding.expires_at - timedelta(seconds=10))
    await service.register(parent_definition(child, **control))
    await publish(service, queue, "qualified-parent")
    page = replace(child, expires_at=child.expires_at - timedelta(seconds=10))
    await service.register_page(page_definition(scope, page, **control))
    receipt = await publish(service, queue, "qualified-page")
    return service, queue, capture, rows, child, page, receipt


@pytest.mark.parametrize(
    "field",
    [
        "principal",
        "scope",
        "token",
        "attribute",
        "attribute_authority",
        "timezone",
        "policy",
        "early",
        "late",
        "authority",
        "history",
        "unconditional",
    ],
)
def test_qualified_route_compatibility_is_explicit_and_lifetime_contained(field):
    scope = base.MemoryScope("route-tests", user_id="alice", session_id="s")
    clock = [base.at(1)]
    leaf = context_definition(scope, clock)
    child = parent_definition(leaf.context).payload()
    parent = leaf.payload()
    if field == "authority":
        parent["authority_id"] = "different"
    elif field == "history":
        parent["history_mode"] = "published-point/1"
    elif field == "unconditional":
        parent = FacetDefinition("language", "alice").payload()
    else:
        binding = leaf.context
        query = binding.query
        if field == "principal":
            query = replace(query, principal="another-host")
        elif field == "scope":
            query = replace(query, scope=replace(scope, session_id="another"))
        elif field == "token":
            query = replace(query, snapshot_token="other-route")
        elif field == "attribute":
            query = replace(query, attributes=(ContextAttribute("project", "B", "host"),))
        elif field == "attribute_authority":
            query = replace(
                query,
                attributes=tuple(replace(a, authority="different-host") for a in query.attributes),
            )
        elif field == "timezone":
            query = replace(query, timezone="Asia/Shanghai")
        elif field == "early":
            query = replace(
                query,
                known_at=clock[0] - timedelta(seconds=1),
                valid_at=clock[0] - timedelta(seconds=1),
            )
        binding = replace(binding, query=query)
        if field == "late":
            binding = replace(binding, expires_at=binding.expires_at + timedelta(seconds=1))
        if field == "policy":
            binding = replace(binding, policy=ProjectionPolicy("different", "agent_context"))
        child["context"] = binding.payload()
    with pytest.raises(DerivedError):
        validate_route_edge(child, parent)


def test_legacy_templates_and_fingerprints_do_not_silently_expand():
    scope = base.MemoryScope("route-tests", user_id="alice", session_id="s")
    leaf = context_definition(scope, [base.at(1)])
    child = parent_definition(leaf.context)
    validate_route_edge(child.payload(), leaf.payload())
    assert route_proof(child.payload())["context_sha256"] == digest(leaf.context.payload())
    assert "context" not in FacetDefinition("ordinary", "alice").payload()
    ordinary = PageDefinition(
        "page", ScenarioDefinition("s", scope, "alice", "Title"), ("language",)
    )
    assert "context" not in ordinary.payload()
    with pytest.raises(DerivedError):
        replace(child, template_version="locale-parents/1")
    with pytest.raises(DerivedError):
        replace(ordinary, context=leaf.context)
    with pytest.raises(DerivedError):
        replace(child, history_mode="published-point/1")
    with pytest.raises(DerivedError):
        validate_route_edge(
            FacetDefinition(
                "ordinary",
                "alice",
                template_version="locale-parents/1",
                parent_facets=("language",),
            ).payload(),
            leaf.payload(),
        )


@pytest.mark.parametrize(
    "holiday,project,conflict,kind",
    [
        (False, "A", False, "source_fact"),
        (None, "A", False, "context_unknown"),
        (False, None, False, "context_unknown"),
        (False, "A", True, "conflict"),
        (True, "A", False, None),
        (False, "B", False, None),
    ],
)
def test_complete_qualified_graph_preserves_unknown_exception_conflict_and_proof(
    store, holiday, project, conflict, kind
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, receipt = await ready(
                engine, kernel, scope, clock, holiday=holiday, project=project, conflict=conflict
            )
            leaf = await service.read("language", actor="alice")
            parent = await service.read("qualified-parent", actor="alice")
            page = await service.pages.read("qualified-page", actor="alice")
            assert page["state"] == ("ready" if kind else "empty")
            status = await service.pages.status(receipt["target_id"], actor="alice")
            assert status["page_complete"] and status["page_ready"] == bool(kind)
            if kind:
                assert parent["body"]["blocks"][0]["observation"] == leaf["body"]
                assert page["body"]["blocks"][0]["body"]["observation"] == parent["body"]
                block = leaf["body"]["blocks"][0]
                assert block["kind"] == kind
                if kind == "source_fact":
                    assert block["qualified"] and block["conditions"] and block["exceptions"]
                    assert block["field_support"] and block["evidence_link_ids"]
                    assert block["valid_from"] and block["support_basis"] == "field_supported"
                else:
                    assert "value" not in block
            async with engine.repository.unit_of_work() as uow:
                for key in ("language", "qualified-parent", "qualified-page"):
                    definition = await uow.derived_get(scope, "definition", key)
                    head = await uow.derived_get(scope, "head", key)
                    header = await uow.derived_get(
                        scope, "revision_header", head["audit_revision_id"]
                    )
                    assert header["manifest"]["schema"] == "derived-input-manifest/3"
                    assert header["qualification"] == route_proof(definition["spec"])
                    assert header["manifest"]["qualification"] == header["qualification"]
                    assert digest(header) == head["input_header_sha256"]
                assert set(header["manifest"]["lineage"]) == {"language", "qualified-parent"}
            capabilities = await service.pages.call(
                "page_capabilities", {}, MCPRequestContext(scope)
            )
            assert capabilities["templates"] == [QUALIFIED_PAGE_TEMPLATE]
            assert capabilities["qualified_current"] and not capabilities["historical"]
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_expired_host_authority_rejects_before_qualified_body_access(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await ready(
                engine, kernel, scope, clock, authority=True
            )
            await queue.request("qualified-page", dedupe_key="authority", force=True)
            lease = await queue.claim("authority", lease_seconds=60)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "authority", service.authority_id)
                row["spec"]["expires_at"] = clock[0].isoformat()
                row["fingerprint"] = digest(row["spec"])
                await uow.derived_put(scope, "authority", service.authority_id, row)
            cls, original = type(engine.repository.unit_of_work()), None
            original = cls.derived_get

            async def metadata_only(self, *args):
                assert args[1] not in {"revision", "page_block"}
                return await original(self, *args)

            monkeypatch.setattr(cls, "derived_get", metadata_only)
            with pytest.raises(DerivedError, match="derived_authority_expired"):
                await service.snapshot(lease.task)
            with pytest.raises(DerivedError, match="derived_authority_expired"):
                await service.pages.read("qualified-page", actor="alice")

    asyncio.run(run())


@pytest.mark.parametrize("fault", ["block", "head", "certificate", "capacity"])
def test_qualified_block_head_certificate_publication_is_atomic(store, monkeypatch, fault):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, first = await ready(engine, kernel, scope, clock)
            before = await service.pages.read("qualified-page", actor="alice")
            receipt = await queue.request("qualified-page", dedupe_key="atomic", force=True)
            lease = await queue.claim("atomic", lease_seconds=60)
            cls = type(engine.repository.unit_of_work())
            original, records = cls.derived_put, cls.derived_records

            async def fail(self, *args):
                await original(self, *args)
                kind, row = args[1], args[3]
                if (
                    (fault == "block" and kind == "page_block")
                    or (fault == "head" and kind == "head")
                    or (fault == "certificate" and kind == "job" and row["status"] == "completed")
                ):
                    raise RuntimeError("qualified publication fault")

            async def full(self, scope, kind):
                rows = await records(self, scope, kind)
                return rows * 4096 if kind == "page_block" else rows

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_put", fail)
                if fault == "capacity":
                    patch.setattr(cls, "derived_records", full)
                with pytest.raises((RuntimeError, DerivedError)):
                    await service.apply(lease.task)
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "qualified-page")
                assert head["revision_id"] == before["revision_id"]
                assert len(await uow.derived_records(scope, "page_block")) == 1
                versions = [
                    r
                    for r in await uow.derived_records(scope, "revision")
                    if r["payload"]["facet_id"] == "qualified-page"
                ]
                assert len(versions) == 1
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert (await queue.status(first["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_qualified_full_rebuild_budget_counts_entire_materialized_observation():
    from agent_memory.derived.pages import compose_page

    scope = base.MemoryScope("route-tests", user_id="alice", session_id="s")
    clock = [base.at(1)]
    binding = context_definition(scope, clock).context
    spec = page_definition(scope, binding).payload()
    snapshot = dict(
        definition={"spec": spec},
        at=clock[0],
        manifest=manifest_fields(spec),
        parents={
            "qualified-parent": dict(
                id="actual-parent",
                state="ready",
                body=dict(blocks=[dict(kind="context_unknown", reasons=["未知" * 6000])]),
            )
        },
    )
    with pytest.raises(DerivedError, match="page_output_capacity"):
        compose_page(snapshot, scope)


def test_qualified_source_race_fences_publication_and_refreshes_parents_first(store):
    async def run():
        from test_durable_purge import envelope, source_id

        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, _, _, _, first = await ready(engine, kernel, scope, clock)
            await queue.request("qualified-page", dedupe_key="source-race", force=True)
            lease = await queue.claim("old-page", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            _, session, client, _, _, worker, _, _ = capture
            event = envelope(scope, "2", clock)
            event["content"] = "项目 A 使用英文，节假日除外。"
            await client.durable_append(event, session, 2)
            assert await worker.run_once()
            await service.grant(ProcessingGrant(source_id(scope, "2"), ("alice",)))
            async with engine.repository.unit_of_work() as uow:
                row = next(
                    r
                    for r in await uow.list_admission_records(scope)
                    if r["event_id"] == source_id(scope, "2")
                )
            await qualify(engine, scope, clock, row)
            with pytest.raises(DerivedError):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            assert (await service.pages.read("qualified-page", actor="alice"))["body"] is None
            assert not (await service.pages.status(first["target_id"], actor="alice"))["page_ready"]
            clock[0] += timedelta(seconds=61)
            order = []
            for _ in range(3):
                next_lease = await queue.claim("recover", lease_seconds=60)
                assert next_lease
                order.append(next_lease.task.payload["unit"]["facet_id"])
                await service.apply(next_lease.task)
                await queue.complete(next_lease)
            assert order == ["language", "qualified-parent", "qualified-page"]
            assert (await service.pages.read("qualified-page", actor="alice"))["state"] == "ready"

    asyncio.run(run())


def test_source_erase_requires_fresh_graph_registration_but_keeps_independent_leaf_route(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, rows, child, page, _ = await ready(
                engine, kernel, scope, clock, conflict=True
            )
            await kernel.forget(ForgetRequest(scope, (rows[0]["event_id"],), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                leaf = await uow.derived_get(scope, "definition", "language")
                assert not leaf["disabled"] and leaf["spec"]["context"]
            # Primary erasure conservatively retires same-slot interpreted rows.
            # Fresh independent evidence is captured and qualified after deletion.
            from test_durable_purge import envelope, source_id

            _, session, client, _, _, worker, _, _ = capture
            event = envelope(scope, "3", clock)
            event["content"] = "项目 A 使用英文，节假日除外。"
            await client.durable_append(event, session, 3)
            assert await worker.run_once()
            await service.grant(ProcessingGrant(source_id(scope, "3"), ("alice",)))
            async with engine.repository.unit_of_work() as uow:
                fresh = next(
                    r
                    for r in await uow.list_admission_records(scope)
                    if r["event_id"] == source_id(scope, "3")
                )
            await qualify(engine, scope, clock, fresh)
            await publish(service, queue, "language", "surviving-leaf")
            clock[0] += timedelta(seconds=1)
            query = replace(child.query, known_at=clock[0], valid_at=clock[0])
            new_child = replace(child, query=query)
            new_page = replace(page, query=query)
            await service.register(parent_definition(new_child), expected_generation=1)
            await publish(service, queue, "qualified-parent", "fresh-host")
            await service.register_page(page_definition(scope, new_page), expected_generation=1)
            await publish(service, queue, "qualified-page", "fresh-host")
            view = await service.pages.read("qualified-page", actor="alice")
            assert view["state"] == "ready"
            assert rows[0]["event_id"] not in str(view)
            assert view["body"]["qualification"]["context"] == new_page.payload()
            async with engine.repository.unit_of_work() as uow:
                versions = await uow.derived_records(scope, "page_block")
                assert sorted(r["payload"]["state"] for r in versions) == ["erased", "ready"]

    asyncio.run(run())


def test_existing_context_leaf_requires_opt_in_and_actual_republication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            legacy, legacy_queue, _, rows = await setup(engine, kernel, scope, clock)
            await qualify(engine, scope, clock, rows[0])
            await build(legacy, legacy_queue)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                head = await uow.derived_get(scope, "head", "language")
                old = await uow.derived_get(scope, "revision_header", head["audit_revision_id"])
            from agent_memory.derived import FacetContext

            binding = FacetContext.from_payload(definition["spec"]["context"])
            assert old["manifest"]["schema"] == "derived-input-manifest/1"
            assert "qualification" not in old
            with pytest.raises(DerivedError, match="derived_qualified_context_unsupported"):
                await legacy.register(parent_definition(binding))
            service = clone_service(legacy, clock)
            queue = FacetRefreshQueue(service)
            with pytest.raises(DerivedError, match="derived_definition_configuration_changed"):
                await service.read("language", actor="alice")
            await service.register(
                FacetDefinition(
                    "language", "alice", context=binding, template_version="locale-context/1"
                ),
                expected_generation=1,
            )
            await service.register(parent_definition(binding))
            with pytest.raises(DerivedError, match="derived_parent_(route_proof_invalid|stale)"):
                await queue.request("qualified-parent", dedupe_key="old-leaf")
            await publish(service, queue, "language", "route-republication", force=True)
            await publish(service, queue, "qualified-parent")
            assert (await service.read("qualified-parent", actor="alice"))["state"] == "ready"

    asyncio.run(run())


def test_qualified_opt_in_cannot_share_legacy_definition_jobs_or_processor_proofs(store):
    async def run():
        from agent_memory.derived import FacetContext
        from agent_memory.operations.refresh_processor import ObservationRefreshProcessor

        async with store() as (engine, kernel, scope, clock):
            legacy, legacy_queue, _, rows = await setup(engine, kernel, scope, clock)
            await qualify(engine, scope, clock, rows[0])
            await build(legacy, legacy_queue)
            async with engine.repository.unit_of_work() as uow:
                before = await uow.derived_get(scope, "definition", "language")
            qualified = clone_service(legacy, clock)
            left, right = (
                ObservationRefreshProcessor(legacy),
                ObservationRefreshProcessor(qualified),
            )
            assert left.key != right.key
            assert left.target_metadata(before) != right.target_metadata(before)
            assert left.key == "observation-refresh/1:" + digest(
                [
                    scope.partition_key(),
                    legacy.policy,
                    legacy.context_token,
                    legacy.authority_id,
                    legacy.history_mode,
                ]
            )
            definition = FacetDefinition(
                "language",
                "alice",
                context=FacetContext.from_payload(before["spec"]["context"]),
                template_version="locale-context/1",
            )
            after = await qualified.register(definition, expected_generation=1)
            assert after["fingerprint"] != before["fingerprint"]
            assert after["generation"] == before["generation"] + 1
            assert not legacy.accepts_definition(after)
            assert qualified.accepts_definition(after)
            with pytest.raises(DerivedError, match="derived_definition_configuration_changed"):
                await legacy.read("language", actor="alice")
            with pytest.raises(DerivedError, match="derived_definition_configuration_changed"):
                await legacy_queue.request("language", dedupe_key="forbidden-mode-downgrade")
            with pytest.raises(DerivedError, match="derived_definition_configuration_changed"):
                await legacy.register(definition, expected_generation=after["generation"])
            queue = FacetRefreshQueue(qualified)
            await publish(qualified, queue, "language", "opted-in-generation", force=True)
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "language")
                header = await uow.derived_get(scope, "revision_header", head["audit_revision_id"])
                assert header["manifest"]["schema"] == "derived-input-manifest/3"
            assert await legacy_queue.claim("ordinary-worker", lease_seconds=60) is None

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["route_proof", "grant", "route_expiry"])
@pytest.mark.parametrize("boundary", ["snapshot", "delivery"])
def test_current_qualified_safety_and_route_metadata_checked_before_bodies(
    store, monkeypatch, problem, boundary
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, page_context, _ = await ready(engine, kernel, scope, clock)
            if boundary == "snapshot":
                await queue.request("qualified-page", dedupe_key="guard", force=True)
                lease = await queue.claim("guard", lease_seconds=3600)
            if problem == "route_expiry":
                clock[0] = page_context.expires_at
            else:
                async with engine.repository.unit_of_work() as uow:
                    if problem == "grant":
                        row = await uow.derived_get(scope, "grant", rows[0]["event_id"])
                        row["revoked"] = True
                        await uow.derived_put(scope, "grant", rows[0]["event_id"], row)
                    else:
                        head = await uow.derived_get(scope, "head", "language")
                        header = await uow.derived_get(
                            scope, "revision_header", head["audit_revision_id"]
                        )
                        header["manifest"]["qualification"]["context"]["query"][
                            "snapshot_token"
                        ] = "forged"
                        header["qualification"] = deepcopy(header["manifest"]["qualification"])
                        header["manifest_sha256"] = digest(header["manifest"])
                        head["input_header_sha256"] = digest(header)
                        await uow.derived_put(scope, "revision_header", header["id"], header)
                        await uow.derived_put(scope, "head", "language", head)
            cls, seen = type(engine.repository.unit_of_work()), []
            original = cls.derived_get

            async def metadata_only(self, *args):
                if args[1] in {"revision", "page_block"}:
                    seen.append(args[1])
                return await original(self, *args)

            async def forbidden(*args):
                pytest.fail("Source/candidate body fetched before route and whole-chain permission")

            monkeypatch.setattr(cls, "derived_get", metadata_only)
            monkeypatch.setattr(cls, "get_source_event", forbidden)
            monkeypatch.setattr(cls, "get_admission_record", forbidden)
            if boundary == "snapshot":
                with pytest.raises(DerivedError):
                    await service.snapshot(lease.task)
            else:
                assert (await service.pages.read("qualified-page", actor="alice"))["body"] is None
            assert seen == []

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["source", "scope", "page", "parent", "block"])
def test_qualified_route_and_all_block_versions_erase_in_live_and_real_backup(
    store, tmp_path, mode
):
    async def run():
        from test_purge_restore import backup_copy, replay, restorer

        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _, _ = await ready(engine, kernel, scope, clock)
            await publish(service, queue, "qualified-page", "second", force=True)
            page = await service.pages.read("qualified-page", actor="alice")
            keys = dict(
                source=rows[0]["event_id"],
                page="qualified-page",
                parent="qualified-parent",
                block=page["body"]["blocks"][0]["block_id"],
            )
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        () if mode == "scope" else (keys[mode],),
                        all_in_scope=mode == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
                journal = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), journal)
                for repository in (engine.repository, backup):
                    clone = ObservationService(
                        repository,
                        scope,
                        base.POLICY,
                        clock=lambda: clock[0],
                        context_token="route-A",
                        qualified_current=True,
                    )
                    with pytest.raises(DerivedError, match="derived_definition_unavailable"):
                        await clone.pages.read("qualified-page", actor="alice")
                    async with repository.unit_of_work() as uow:
                        definition = await uow.derived_get(scope, "definition", "qualified-page")
                        assert definition["disabled"] and "context" not in definition["spec"]
                        assert "parent_facets" not in definition["spec"]
                        if mode in {"source", "scope", "parent"}:
                            parent = await uow.derived_get(scope, "definition", "qualified-parent")
                            assert parent["disabled"] and "context" not in parent["spec"]
                        for kind in ("revision", "revision_header", "page_block"):
                            versions = [
                                r["payload"]
                                for r in await uow.derived_records(scope, kind)
                                if r["payload"].get("facet_id") == "qualified-page"
                            ]
                            assert len(versions) == 2
                            assert all(
                                v["state"] == "erased"
                                and "manifest" not in v
                                and "body" not in v
                                and "qualification" not in v
                                for v in versions
                            )
                        for r in await uow.derived_records(scope, "request"):
                            if r["payload"].get("invalidated"):
                                assert "context_token" not in r["payload"]
                                assert "unit" not in r["payload"]

    asyncio.run(run())


@pytest.mark.parametrize("change", ["revoke", "erase"])
def test_independent_connection_qualified_publication_serializes_with_privacy_change(
    store, monkeypatch, change
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _, _ = await ready(engine, kernel, scope, clock)
            await queue.request("qualified-page", dedupe_key="cross-connection", force=True)
            lease = await queue.claim("publishing", lease_seconds=60)
            pg = hasattr(engine.repository, "pool")
            if pg:
                from agent_memory_postgres.repository import PostgresMemoryRepository

                other = PostgresMemoryRepository.from_dsn(
                    engine.repository.pool.conninfo, max_size=2
                )
            else:
                from agent_memory.sqlite import SQLiteMemoryRepository

                other = SQLiteMemoryRepository(engine.repository._path)
            await other.initialize()
            clone = ObservationService(
                other,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                context_token="route-A",
                qualified_current=True,
            )
            entered, release, changing = asyncio.Event(), asyncio.Event(), asyncio.Event()
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def paused(self, *args):
                await original(self, *args)
                if self._repository is engine.repository and args[1] == "page_block":
                    entered.set()
                    await release.wait()

            async def commit():
                if change == "revoke":
                    return await clone.grant(
                        ProcessingGrant(rows[0]["event_id"], ("alice",), revoked=True),
                        expected_version=1,
                    )
                from agent_memory.kernel import MemoryKernel
                from agent_memory.providers import (
                    MetadataClaimExtractor,
                    ReciprocalRankFusionReranker,
                    TrustedMemoryPolicy,
                )

                other_kernel = MemoryKernel(
                    other,
                    MetadataClaimExtractor(),
                    TrustedMemoryPolicy(),
                    ReciprocalRankFusionReranker(),
                )
                return await other_kernel.forget(
                    ForgetRequest(scope, (rows[0]["event_id"],), mode=ForgetMode.ERASE)
                )

            async def privacy_change():
                if pg:
                    changing.set()
                    return await commit()
                loop = asyncio.get_running_loop()

                def independent_thread():
                    loop.call_soon_threadsafe(changing.set)
                    return asyncio.run(commit())

                return await asyncio.to_thread(independent_thread)

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(service.apply(lease.task))
                    await asyncio.wait_for(entered.wait(), 10)
                    privacy = asyncio.create_task(privacy_change())
                    await asyncio.wait_for(changing.wait(), 10)
                    await asyncio.sleep(0.03)
                    assert not privacy.done()
                    release.set()
                    await asyncio.wait_for(publication, 10)
                    await asyncio.wait_for(privacy, 10)
                if change == "erase":
                    with pytest.raises(DerivedError, match="derived_definition_unavailable"):
                        await clone.pages.read("qualified-page", actor="alice")
                else:
                    assert (await clone.pages.read("qualified-page", actor="alice"))["body"] is None
                async with other.unit_of_work() as uow:
                    definition = await uow.derived_get(scope, "definition", "qualified-page")
                    assert definition["dirty"]
                    if change == "erase":
                        assert definition["disabled"] and "context" not in definition["spec"]
                        assert all(
                            r["payload"]["state"] == "erased"
                            for r in await uow.derived_records(scope, "page_block")
                        )
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_qualified_page_readonly_transport_and_final_delivery_recheck(
    store, monkeypatch, transport
):
    async def run():
        import agent_memory_sdk as sdk

        async with store() as (engine, kernel, scope, clock):
            service, _, _, rows, _, _, receipt = await ready(engine, kernel, scope, clock)
            context = MCPRequestContext(scope, actor="alice")
            original, calls = service.pages.read, []

            async def revoke_between_reads(*args, **kwargs):
                result = await original(*args, **kwargs)
                calls.append(result)
                if len(calls) == 1:
                    await service.grant(
                        ProcessingGrant(rows[0]["event_id"], ("alice",), revoked=True),
                        expected_version=1,
                    )
                return result

            async def exercise(client):
                capabilities = await client.page_capabilities()
                assert capabilities["templates"] == [QUALIFIED_PAGE_TEMPLATE]
                assert (await client.derived_capabilities())["pages"]
                assert (await client.page_status(receipt["target_id"]))["page_ready"]
                monkeypatch.setattr(service.pages, "read", revoke_between_reads)
                assert (await client.page_context("qualified-page"))["pages"] == []
                assert len(calls) == 2 and calls[0]["state"] == "ready"
                with pytest.raises(DerivedError, match="unsupported_derived_operation"):
                    await service.call("page_register", {}, context)

            if transport == "embedded":
                await exercise(sdk.EmbeddedMemoryClient(kernel, context, derived=service))
            else:
                import agent_memory_mcp as mcp

                server = mcp.create_server(
                    kernel, mcp.StaticIdentityResolver(context), derived=service
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


@pytest.mark.parametrize("contract", ["derived_qualified_contract", "refresh_scheduler_contract"])
def test_qualified_templates_require_explicit_backend_attestation(store, monkeypatch, contract):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            legacy, _, _, _ = await setup(engine, kernel, scope, clock)
            service = clone_service(legacy, clock)
            definition = context_definition(scope, clock)
            cls = type(engine.repository.unit_of_work())
            monkeypatch.setattr(cls, contract, "old-backend")
            with pytest.raises(DerivedError, match="derived_qualified_backend_unsupported"):
                await service.register(parent_definition(definition.context))
            with pytest.raises(DerivedError, match="derived_qualified_backend_unsupported"):
                await service.register_page(page_definition(scope, definition.context))
            capabilities = await service.pages.call(
                "page_capabilities", {}, MCPRequestContext(scope)
            )
            assert not capabilities["enabled"] and capabilities["templates"] == []

    asyncio.run(run())


def test_qualified_full_rebuild_keeps_block_identity_and_entire_generation_lineage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, binding, _ = await ready(engine, kernel, scope, clock)
            before = await service.pages.read("qualified-page", actor="alice")
            await service.register_page(
                replace(page_definition(scope, binding), version="2"), expected_generation=1
            )
            await publish(service, queue, "qualified-page", "second-generation")
            after = await service.pages.read("qualified-page", actor="alice")
            old, new = before["body"]["blocks"][0], after["body"]["blocks"][0]
            assert old["block_id"] == new["block_id"] and old["body"] == new["body"]
            assert old["revision_id"] != new["revision_id"]
            async with engine.repository.unit_of_work() as uow:
                for key in (before["revision_id"], after["revision_id"]):
                    revision = await uow.derived_get(scope, "revision", key)
                    assert set(revision["manifest"]["lineage"]) == {"language", "qualified-parent"}
                    block = await uow.derived_get(
                        scope, "page_block", revision["body"]["blocks"][0]["revision_id"]
                    )
                    assert block["manifest"] == revision["manifest"]
                    assert block["manifest"]["qualification"] == route_proof(
                        page_definition(
                            scope, binding, version="1" if key == before["revision_id"] else "2"
                        ).payload()
                    )

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_actual_sigkill_qualified_page_blocks_route_header_and_receipt(store, tmp_path, boundary):
    async def run():
        from test_durable_process_recovery import kill_at_boundary

        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await ready(engine, kernel, scope, clock)
            before = await service.pages.read("qualified-page", actor="alice")
            receipt = await queue.request("qualified-page", dedupe_key="actual-crash", force=True)
            await kill_at_boundary(
                engine,
                scope,
                clock,
                tmp_path,
                "derived_" + boundary,
                extra={"context_token": "route-A", "qualified_current": True},
            )
            committed = boundary == "after_commit"
            status = await queue.status(receipt["target_id"], actor="alice")
            assert status["complete"] == committed
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "qualified-page")
                assert (head["revision_id"] != before["revision_id"]) == committed
                assert len(await uow.derived_records(scope, "page_block")) == 1 + int(committed)
                header = await uow.derived_get(scope, "revision_header", head["audit_revision_id"])
                assert digest(header) == head["input_header_sha256"]
                definition = await uow.derived_get(scope, "definition", "qualified-page")
                assert header["qualification"] == route_proof(definition["spec"])
                assert header["manifest"]["qualification"] == header["qualification"]
            if not committed:
                clock[0] += timedelta(seconds=6)
                lease = await queue.claim("recover", lease_seconds=60)
                assert lease
                await service.apply(lease.task)
                await queue.complete(lease)
            assert (await service.pages.status(receipt["target_id"], actor="alice"))["page_ready"]

    asyncio.run(run())


def test_shared_scheduler_refreshes_qualified_graph_parents_first_and_certifies_page(store):
    async def run():
        from test_refresh_demand import finish, scheduler

        from agent_memory.derived import FacetContext
        from agent_memory.operations.refresh_policy import RefreshPolicy

        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, _, _, _ = await ready(engine, kernel, scope, clock)
            queue = scheduler(service, clock)
            for key in ("language", "qualified-parent", "qualified-page"):
                await queue.configure(key, RefreshPolicy(mode="on_demand"))
            async with engine.repository.unit_of_work() as uow:
                leaf = await uow.derived_get(scope, "definition", "language")
            await service.register(
                FacetDefinition(
                    "language",
                    "alice",
                    context=FacetContext.from_payload(leaf["spec"]["context"]),
                    template_version="locale-context/1",
                    version="2",
                ),
                expected_generation=leaf["generation"],
            )
            receipt = await queue.request("qualified-page", dedupe_key="governed", actor="alice")
            order = []
            for _ in range(12):
                lease = await queue.claim("governed", lease_seconds=60)
                if lease:
                    order.append(lease.task.payload["unit"]["facet_id"])
                    await finish(queue, lease)
                if (await queue.status(receipt["target_id"], actor="alice"))["complete"]:
                    break
                clock[0] += timedelta(seconds=3)
            assert order == ["language", "qualified-parent", "qualified-page"]
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert (await service.pages.read("qualified-page", actor="alice"))["state"] == "ready"
            async with engine.repository.unit_of_work() as uow:
                publication = [
                    r["payload"]
                    for r in await uow.derived_records(scope, "refresh_publication")
                    if r["payload"].get("facet_id") == "qualified-page"
                ]
                assert publication and all(p["state"] == "committed" for p in publication)

    asyncio.run(run())


def test_managed_legacy_facet_cannot_silently_change_proof_processor(store):
    async def run():
        from test_refresh_demand import finish, scheduler

        from agent_memory.derived import FacetContext
        from agent_memory.operations.refresh_policy import RefreshPolicy

        async with store() as (engine, kernel, scope, clock):
            legacy, legacy_queue, _, _ = await setup(engine, kernel, scope, clock, texts=())
            await build(legacy, legacy_queue)
            queue = scheduler(legacy, clock)
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            old_target = await queue.request("language", dedupe_key="old-contract", actor="alice")
            kinds = (
                "definition",
                "head",
                "job",
                "request",
                "revision",
                "revision_header",
                "refresh_policy",
                "refresh_demand",
                "coverage_request",
                "refresh_execution",
            )
            async with engine.repository.unit_of_work() as uow:
                before = await uow.derived_get(scope, "definition", "language")
                records = {kind: await uow.derived_records(scope, kind) for kind in kinds}
            opted = clone_service(legacy, clock)
            with pytest.raises(DerivedError, match="derived_definition_configuration_changed"):
                await opted.register(
                    FacetDefinition(
                        "language",
                        "alice",
                        template_version="locale-context/1",
                        context=FacetContext.from_payload(before["spec"]["context"]),
                    ),
                    expected_generation=before["generation"],
                )
            async with engine.repository.unit_of_work() as uow:
                assert {kind: await uow.derived_records(scope, kind) for kind in kinds} == records
            await opted.register(
                FacetDefinition(
                    "qualified-leaf",
                    "alice",
                    template_version="locale-context/1",
                    context=FacetContext.from_payload(before["spec"]["context"]),
                )
            )
            next_queue = scheduler(opted, clock)
            await next_queue.configure("qualified-leaf", RefreshPolicy(mode="on_demand"))
            target = await next_queue.request(
                "qualified-leaf", dedupe_key="new-contract", actor="alice"
            )
            lease = await next_queue.claim("qualified-worker", lease_seconds=60)
            assert lease.task.payload["unit"]["facet_id"] == "qualified-leaf"
            await finish(next_queue, lease)
            assert (await next_queue.status(target["target_id"], actor="alice"))["complete"]
            assert not (await queue.status(old_target["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize("all_scope", [False, True])
def test_qualified_governed_publication_and_receipts_cannot_revive_from_backup(
    store, tmp_path, all_scope
):
    async def run():
        from test_purge_restore import backup_copy, replay, restorer
        from test_refresh_demand import finish, scheduler

        from agent_memory.operations.refresh_policy import RefreshPolicy

        async with store() as (engine, kernel, scope, clock):
            service, _, _, rows, _, _, _ = await ready(engine, kernel, scope, clock)
            queue = scheduler(service, clock)
            await queue.configure("qualified-page", RefreshPolicy(mode="on_demand"))
            target = await queue.request(
                "qualified-page", dedupe_key="governed-page", actor="alice"
            )
            lease = await queue.claim("governed", lease_seconds=60)
            assert lease
            await finish(queue, lease)
            assert (await queue.status(target["target_id"], actor="alice"))["complete"]
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        () if all_scope else (rows[0]["event_id"],),
                        all_in_scope=all_scope,
                        mode=ForgetMode.ERASE,
                    )
                )
                journal = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), journal)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for kind in (
                            "refresh_demand",
                            "refresh_execution",
                            "refresh_policy",
                            "refresh_publication",
                        ):
                            assert await uow.derived_records(scope, kind) == ()
                        receipts = await uow.derived_records(scope, "coverage_request")
                        assert receipts and all(
                            r["payload"] == {"schema": "coverage-receipt/1", "state": "erased"}
                            for r in receipts
                        )
                        blocks = await uow.derived_records(scope, "page_block")
                        assert all(
                            r["payload"]["state"] == "erased" and "manifest" not in r["payload"]
                            for r in blocks
                        )

    asyncio.run(run())


@pytest.mark.parametrize("difference", ["principal", "attribute", "policy", "lifetime"])
def test_incompatible_qualified_registration_is_atomic_before_any_body(
    store, monkeypatch, difference
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, child, _, _ = await ready(engine, kernel, scope, clock)
            if difference == "principal":
                child = replace(child, query=replace(child.query, principal="other-host"))
            elif difference == "attribute":
                child = replace(
                    child,
                    query=replace(
                        child.query,
                        attributes=tuple(
                            replace(a, value="B") if a.name == "project" else a
                            for a in child.query.attributes
                        ),
                    ),
                )
            elif difference == "policy":
                child = replace(child, policy=ProjectionPolicy("other-policy", "agent_context"))
            else:
                child = replace(child, expires_at=child.expires_at + timedelta(seconds=20))
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_get

            async def metadata_only(self, *args):
                assert args[1] not in {"revision", "page_block"}
                return await original(self, *args)

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_get", metadata_only)
                with pytest.raises(DerivedError, match="derived_parent_(route|qualification)"):
                    await service.register(parent_definition(child, key="incompatible"))
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "definition", "incompatible") is None
            assert (await service.pages.read("qualified-page", actor="alice"))["state"] == "ready"

    asyncio.run(run())


@pytest.mark.parametrize("target", ["language", "qualified-parent", "qualified-page"])
@pytest.mark.parametrize("change", ["route_expiry", "authority_expiry", "host_route"])
def test_qualified_delivery_rechecks_controls_after_last_body_load(
    store, monkeypatch, target, change
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, _, _, _ = await ready(
                engine, kernel, scope, clock, authority=change == "authority_expiry"
            )
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", target)
                expiry = definition["spec"]["context"]["expires_at"]
                if change == "authority_expiry":
                    authority = await uow.derived_get(
                        scope, "authority", service.authority_id
                    )
                    expiry = authority["spec"]["expires_at"]
            cls = type(engine.repository.unit_of_work())
            original, changed = cls.derived_get, []

            async def interrupted(self, *args):
                value = await original(self, *args)
                final_body = (
                    args[1] == "page_block" and target == "qualified-page"
                    or args[1] == "revision"
                    and target != "qualified-page"
                    and value is not None
                    and value.get("facet_id") == target
                )
                if final_body and not changed:
                    changed.append(True)
                    if change == "host_route":
                        service.context_token = "route-replaced-during-delivery"
                    else:
                        from datetime import datetime

                        clock[0] = datetime.fromisoformat(expiry)
                return value

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_get", interrupted)
                if target == "qualified-page":
                    result = await service.pages.read(target, actor="alice")
                else:
                    result = await service.read(target, actor="alice")
            assert changed, "The fault must occur after the selected body was loaded."
            assert result["body"] is None and result["state"] != "ready"

    asyncio.run(run())


@pytest.mark.parametrize(
    "change", ["route_expiry", "authority_expiry", "host_route", "lease_expiry", "job_expiry"]
)
@pytest.mark.parametrize("boundary", ["page_block", "head", "completion"])
def test_qualified_publication_rechecks_controls_before_transaction_commit(
    store, monkeypatch, change, boundary
):
    async def run():
        from datetime import datetime

        from agent_memory.operations.worker_tasks import WorkerQueueError

        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, page, _ = await ready(
                engine, kernel, scope, clock, authority=change == "authority_expiry"
            )
            if change == "job_expiry":
                queue = FacetRefreshQueue(service, max_age_seconds=5)
            await queue.request("qualified-page", dedupe_key="late-control", force=True)
            lease = await queue.claim("late-control", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            prepared = service.prepare(snapshot)
            kinds = ("revision", "revision_header", "page_block", "head", "definition", "job")
            async with engine.repository.unit_of_work() as uow:
                before = {kind: await uow.derived_records(scope, kind) for kind in kinds}
                job = await uow.derived_get(scope, "job", lease.task.id)
                expiry = page.expires_at
                if change == "authority_expiry":
                    authority = await uow.derived_get(scope, "authority", service.authority_id)
                    expiry = datetime.fromisoformat(authority["spec"]["expires_at"])
                elif change in {"lease_expiry", "job_expiry"}:
                    expiry = datetime.fromisoformat(
                        job["lease_until" if change == "lease_expiry" else "expires_at"]
                    )
            cls = type(engine.repository.unit_of_work())
            original, changed = cls.derived_put, []

            async def interrupted(self, *args):
                result = await original(self, *args)
                hit = (
                    args[1] == boundary
                    or boundary == "completion" and args[1] == "job"
                    and args[3].get("status") == "completed"
                )
                if hit and not changed:
                    changed.append(True)
                    if change == "host_route":
                        service.context_token = "route-replaced-during-publication"
                    else:
                        clock[0] = expiry
                return result

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_put", interrupted)
                with pytest.raises((DerivedError, WorkerQueueError)):
                    await service.publish(lease.task, snapshot, prepared)
            assert changed, "The fault must occur inside the publication transaction."
            async with engine.repository.unit_of_work() as uow:
                assert {kind: await uow.derived_records(scope, kind) for kind in kinds} == before

    asyncio.run(run())


@pytest.mark.parametrize("target", ["language", "qualified-page"])
@pytest.mark.parametrize("boundary", ["before_read", "after_body"])
def test_qualified_expiry_cannot_be_undone_by_clock_rollback_after_restart(
    store, monkeypatch, target, boundary
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, _, _, _ = await ready(engine, kernel, scope, clock)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", target)
            from datetime import datetime

            original_time = clock[0]
            expires = datetime.fromisoformat(definition["spec"]["context"]["expires_at"])
            if boundary == "before_read":
                clock[0] = expires
            cls = type(engine.repository.unit_of_work())
            original_get, changed = cls.derived_get, []

            async def interrupted(self, *args):
                value = await original_get(self, *args)
                if boundary == "after_body" and not changed and (
                    args[1] == "page_block" and target == "qualified-page"
                    or args[1] == "revision" and target == "language"
                    and value is not None and value.get("facet_id") == target
                ):
                    changed.append(True)
                    clock[0] = expires
                return value

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_get", interrupted)
                if target == "qualified-page":
                    expired = await service.pages.read(target, actor="alice")
                else:
                    expired = await service.read(target, actor="alice")
            assert expired["body"] is None
            assert changed or boundary == "before_read"
            clock[0] = original_time
            restarted = clone_service(service, clock)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                if target == "qualified-page":
                    await restarted.pages.read(target, actor="alice")
                else:
                    await restarted.read(target, actor="alice")

    asyncio.run(run())


@pytest.mark.parametrize("target", ["language", "qualified-page"])
def test_disabling_qualified_opt_in_during_final_clock_delivery_denies_body(
    store, monkeypatch, target
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, _, _, _ = await ready(engine, kernel, scope, clock)
            cls = type(engine.repository.unit_of_work())
            original_get = cls.derived_get
            original_lineage = service._managed_lineage
            body_seen, disabled = [], []

            async def get(uow, *args):
                value = await original_get(uow, *args)
                if args[1] == ("page_block" if target == "qualified-page" else "revision"):
                    body_seen.append(True)
                return value

            async def lineage(uow, facet_id):
                result = await original_lineage(uow, facet_id)
                if body_seen and facet_id == target and not disabled:
                    service.qualified_current = False
                    disabled.append(True)
                return result

            monkeypatch.setattr(cls, "derived_get", get)
            monkeypatch.setattr(service, "_managed_lineage", lineage)
            result = (await service.pages.read(target, actor="alice")
                      if target == "qualified-page" else await service.read(target, actor="alice"))
            assert disabled
            assert result["body"] is None and result["state"] == "invalid"

    asyncio.run(run())
