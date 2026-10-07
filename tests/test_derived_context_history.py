"""Frozen qualified history with independent fact time and current access guards."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
import test_derived_contextual as qualified
from test_derived_history import archive_spy, checkpoint, historical, refresh

from agent_memory.conditions import Condition, ProjectionPolicy
from agent_memory.consolidation.admission import draft_from_payload
from agent_memory.consolidation.qualification import target_fingerprint
from agent_memory.derived import (
    DerivedError,
    HostGrantAuthority,
    ObservationService,
    ProcessingGrant,
    QueryDefinition,
)
from agent_memory.derived.model import HISTORY_INTERVAL, HISTORY_POINT, digest
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.evidence_support import EvidenceLink, SupportRange
from agent_memory.fact_qualification import SourceSpan
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue

store = base.store
MODES = (HISTORY_POINT, HISTORY_INTERVAL)


async def configured(
    engine, kernel, scope, clock, *, mode=HISTORY_INTERVAL, texts=("项目 A 使用中文。",), **context
):
    _, _, capture, rows = await qualified.setup(engine, kernel, scope, clock, texts, **context)
    service = ObservationService(
        engine.repository,
        scope,
        base.POLICY,
        clock=lambda: clock[0],
        context_token=context.get("token", "route-A"),
        history_mode=mode,
        authority_id="local-host",
        authority_min_version=0,
    )
    authority = HostGrantAuthority("local-host", ("alice",), clock[0] + timedelta(hours=24))
    await service.set_authority(authority)
    await service.register_query(QueryDefinition("language-inputs", scope, "alice", ("locale",)))
    spec = replace(
        qualified.definition(scope, clock, **context),
        query_id="language-inputs",
        authority_id="local-host",
        history_mode=mode,
    )
    await service.register(spec, expected_generation=1)
    for row in rows:
        await service.grant(ProcessingGrant(row["event_id"], ("alice",)), expected_version=1)
    return service, FacetRefreshQueue(service), capture, rows, spec, authority


def bound_definition(scope, clock, old, **context):
    return replace(
        qualified.definition(scope, clock, **context),
        query_id=old.query_id,
        authority_id=old.authority_id,
        history_mode=old.history_mode,
    )


def clone(repository, scope, clock, *, mode=HISTORY_INTERVAL, token="route-A", floor=1):
    return ObservationService(
        repository,
        scope,
        base.POLICY,
        clock=lambda: clock[0],
        context_token=token,
        history_mode=mode,
        authority_id="local-host",
        authority_min_version=floor,
    )


async def auxiliary(engine, scope, service, row, *, support=None, identity="aux"):
    async with engine.repository.unit_of_work() as uow:
        source = base.source(
            scope, "qualified-private-" + identity, identity=identity, idempotency=identity
        )
        await uow.append_event(source)
    await service.grant(ProcessingGrant(source.id, ("alice",)))
    link = EvidenceLink(
        identity,
        ("subject_id", "predicate", "value", "valid_from", "conditions"),
        target_fingerprint(draft_from_payload(row["payload"]["draft"])),
        SourceSpan(source.id, 0, len(source.content), source.content),
        base.SELF,
        support or SupportRange(base.at(1)),
        source.id,
    )
    return source, link


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    "project,holiday,state,kind",
    [
        ("A", False, "ready", "source_fact"),
        ("B", False, "empty", None),
        (None, False, "ready", "context_unknown"),
        ("A", True, "empty", None),
        ("A", None, "ready", "context_unknown"),
    ],
)
def test_frozen_conditions_exceptions_and_unknown_keep_their_qualification(
    store, mode, project, holiday, state, kind
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(
                engine,
                kernel,
                scope,
                clock,
                mode=mode,
                texts=("项目 A 使用中文，节假日除外。",),
                project=project,
                holiday=holiday,
            )
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "qualified")
            known = await checkpoint(service)
            clock[0] += timedelta(seconds=5)
            request = clock[0] if mode == HISTORY_INTERVAL else known
            view = await historical(service, request, base.at(1))
            assert view["state"] == state
            if state == "ready":
                body, block = view["body"], view["body"]["blocks"][0]
                assert block["kind"] == kind
                assert body["context"] == spec.context.payload()
                assert body["context_sha256"] == digest(spec.context.payload())
                assert (
                    body["projection_context_sha256"]
                    == spec.context.historical(
                        base.datetime.fromisoformat(known) if isinstance(request, str) else request,
                        base.at(1),
                    ).context_hash
                )
                if kind == "source_fact":
                    assert block["qualified"] and block["conditions"] and block["exceptions"]
                    assert block["field_support"] and block["value"] == "zh-CN"
                else:
                    assert "value" not in block

    asyncio.run(run())


@pytest.mark.parametrize("mode", MODES)
def test_past_context_remains_readable_after_expiry_rotation_and_different_valid_time(store, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(
                engine,
                kernel,
                scope,
                clock,
                mode=mode,
                expiry=10,
                texts=("项目 A 使用中文。", "项目 B 使用英文。"),
            )
            for row in rows:
                await qualified.qualify(engine, scope, clock, row)
            await refresh(service, queue, "A")
            known = await checkpoint(service)
            start = clock[0]
            clock[0] = spec.context.expires_at + timedelta(seconds=5)
            assert (await service.read("language", actor="alice"))[
                "reason"
            ] == "derived_context_expired"
            old = await historical(service, known, clock[0])
            assert old["body"]["blocks"][0]["value"] == "zh-CN"
            assert old["body"]["context"]["query"]["snapshot_token"] == "route-A"
            if mode == HISTORY_INTERVAL:
                interior = await historical(service, start + timedelta(seconds=1), clock[0])
                assert interior["coverage"]["known_to"] == spec.context.expires_at.isoformat()
                with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                    await historical(service, spec.context.expires_at, base.at(1))
            rotated = clone(engine.repository, scope, clock, mode=mode, token="route-B")
            updated = bound_definition(scope, clock, spec, project="B", token="route-B")
            await rotated.register(updated, expected_generation=2)
            await refresh(rotated, FacetRefreshQueue(rotated), "B")
            newest = await checkpoint(rotated)
            assert (await historical(rotated, newest, base.at(1)))["body"]["blocks"][0][
                "value"
            ] == "en-US"
            assert (await historical(rotated, known, clock[0]))["body"] == old["body"]
            with pytest.raises(DerivedError, match="derived_context_mismatch"):
                await historical(service, known, base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("change", ["none", "query", "candidate", "context"])
def test_context_expiry_caps_interval_and_later_mutation_never_extends_it(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(
                engine, kernel, scope, clock, expiry=10
            )
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "first")
            first = clock[0]
            clock[0] = spec.context.expires_at + timedelta(seconds=3)
            if change == "query":
                await service.register_query(
                    QueryDefinition("language-inputs", scope, "alice", ("locale",), version="2"),
                    expected_generation=1,
                )
            elif change == "context":
                await service.register(bound_definition(scope, clock, spec), expected_generation=2)
            elif change == "candidate":
                async with engine.repository.unit_of_work() as uow:
                    row = await uow.get_admission_record(scope, rows[0]["id"])
                    await uow.save_admission_record(
                        scope,
                        row["id"],
                        row["event_id"],
                        row["slot_key"],
                        dict(row["payload"], action="REJECT"),
                        row["version"],
                    )
            view = await historical(service, first + timedelta(seconds=1), base.at(1))
            assert view["coverage"]["known_to"] == spec.context.expires_at.isoformat()
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, spec.context.expires_at, base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("support", ["or", "and", "point"])
def test_field_support_and_gaps_are_reprojected_in_valid_time_not_context_lifetime(
    store, mode, support
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _ = await configured(
                engine, kernel, scope, clock, mode=mode
            )
            source, a = await auxiliary(
                engine,
                scope,
                service,
                rows[0],
                support=SupportRange(base.at(1), base.at(1) + timedelta(seconds=4)),
            )
            _, b = await auxiliary(
                engine,
                scope,
                service,
                rows[0],
                identity="second",
                support=SupportRange(
                    base.at(1) + timedelta(seconds=8), base.at(1) + timedelta(seconds=12)
                ),
            )
            if support == "point":
                a = replace(a, support=SupportRange(base.at(1), kind="point"))
            groups = (("aux", "second"),) if support == "and" else (("aux",), ("second",))
            await qualified.qualify(engine, scope, clock, rows[0], links=(a, b), groups=groups)
            clock[0] += timedelta(seconds=20)
            await refresh(service, queue, "late-review")
            known = await checkpoint(service)
            for offset, expected in [(0, support != "and"), (6, False), (9, support != "and")]:
                view = await historical(service, known, base.at(1) + timedelta(seconds=offset))
                block = view["body"]["blocks"][0]
                assert block["kind"] == ("source_fact" if expected else "context_unknown")
                if expected:
                    assert block["evidence_link_ids"] == (["aux"] if offset == 0 else ["second"])
                    assert block["support_kind"] == (
                        "point" if support == "point" and offset == 0 else "interval"
                    )
            # Both inputs are processing dependencies, even during a gap or with no citation.
            await service.grant(
                ProcessingGrant(source.id, ("alice",), revoked=True), expected_version=1
            )
            with pytest.raises(DerivedError, match="derived_processing_denied"):
                await historical(service, known, base.at(1) + timedelta(seconds=9))

    asyncio.run(run())


@pytest.mark.parametrize("mode", MODES)
def test_frozen_precedence_policy_and_qualification_versions_survive_migration(store, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(
                engine,
                kernel,
                scope,
                clock,
                mode=mode,
                texts=("全部项目使用中文。", "项目 A 使用英文。"),
            )
            reviewed = await qualified.qualify(engine, scope, clock, rows[1])
            await refresh(service, queue, "exclusive")
            first = await checkpoint(service)
            assert (await historical(service, first, base.at(1)))["body"]["blocks"][0][
                "kind"
            ] == "conflict"
            clock[0] += timedelta(seconds=5)
            policy = ProjectionPolicy(
                "approved-language/2",
                "agent_context",
                "ordered_override",
                (("global", "project-A"),),
            )
            await service.register(
                bound_definition(scope, clock, spec, policy=policy), expected_generation=2
            )
            await qualified.qualify(engine, scope, clock, reviewed, policy=policy)
            await refresh(service, queue, "override")
            current = await historical(service, await checkpoint(service), base.at(1))
            assert current["body"]["blocks"][0]["value"] == "en-US"
            old = await historical(service, first, base.at(1))
            assert old["body"]["blocks"][0]["kind"] == "conflict"
            assert old["body"]["policy_sha256"] == qualified.POLICY.fingerprint
            assert current["body"]["policy_sha256"] == policy.fingerprint

    asyncio.run(run())


@pytest.mark.parametrize("mode", MODES)
def test_admission_policy_migration_keeps_old_review_and_rejects_unreviewed_new_policy(store, mode):
    from agent_memory.consolidation.admission import AdmissionPolicy

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(
                engine, kernel, scope, clock, mode=mode
            )
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "old-policy")
            known = await checkpoint(service)
            clock[0] += timedelta(seconds=10)
            policy = AdmissionPolicy([base.PredicateSpec("locale", allow_self_report=False)])
            policy.version = "new-admission/2"
            newer = ObservationService(
                engine.repository,
                scope,
                policy,
                clock=lambda: clock[0],
                context_token="route-A",
                history_mode=mode,
                authority_id="local-host",
                authority_min_version=1,
            )
            await newer.register_query(
                QueryDefinition("language-inputs", scope, "alice", ("locale",), version="2"),
                expected_generation=1,
            )
            await newer.register(replace(spec, version="2"), expected_generation=2)
            assert (await historical(newer, known, base.at(1)))["body"]["blocks"][0]["qualified"]
            lease = await FacetRefreshQueue(newer).claim("new-policy", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_qualification_invalid"):
                await newer.apply(lease.task)
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(newer, clock[0], base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("timezone", [None, "Asia/Shanghai"])
def test_weekday_uses_requested_valid_time_and_frozen_timezone(store, timezone):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            clock[0] = base.at(1) + timedelta(hours=15)
            service, queue, _, rows, spec, _ = await configured(
                engine,
                kernel,
                scope,
                clock,
                expiry=86400,
                timezone=timezone,
            )
            await qualified.qualify(
                engine, scope, clock, rows[0], conditions=(Condition("weekday", value=(3,)),)
            )
            await refresh(service, queue, "weekday")
            known = await checkpoint(service)
            clock[0] = base.at(1) + timedelta(hours=17)
            before = await historical(service, known, base.at(1) + timedelta(hours=15))
            after = await historical(service, known, base.at(1) + timedelta(hours=16))
            if timezone:
                assert before["body"]["blocks"][0]["value"] == "zh-CN"
                assert after["state"] == "empty"
            else:
                assert before["body"]["blocks"][0]["kind"] == "context_unknown"
                assert after["body"]["blocks"][0]["kind"] == "context_unknown"
            assert spec.context.query.timezone == timezone

    asyncio.run(run())


def test_unreviewed_history_never_borrows_later_qualification_or_backfills_a_gap(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _ = await configured(engine, kernel, scope, clock)
            earlier = clock[0]
            lease = await queue.claim("unreviewed", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_qualification_incomplete"):
                await service.apply(lease.task)
            await queue.fail(lease, DerivedError("derived_qualification_incomplete"))
            clock[0] += timedelta(seconds=5)
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "reviewed")
            assert (await historical(service, await checkpoint(service), base.at(1)))[
                "state"
            ] == "ready"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, earlier, base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize(
    "problem",
    [
        "auxiliary",
        "authority",
        "authority_expiry",
        "grant_expiry",
        "floor",
        "route",
        "reader",
        "purpose",
    ],
)
def test_current_security_precedes_qualified_archive_and_evidence(store, monkeypatch, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, authority = await configured(engine, kernel, scope, clock)
            source, link = await auxiliary(engine, scope, service, rows[0])
            await qualified.qualify(engine, scope, clock, rows[0], links=(link,))
            await refresh(service, queue, "secure")
            known = await checkpoint(service)
            if problem == "auxiliary":
                await service.grant(
                    ProcessingGrant(source.id, ("alice",), revoked=True), expected_version=1
                )
            elif problem == "authority":
                await service.set_authority(replace(authority, revoked=True), expected_version=1)
            elif problem == "authority_expiry":
                clock[0] = authority.expires_at
            elif problem == "grant_expiry":
                await service.grant(
                    ProcessingGrant(source.id, ("alice",), expires_at=clock[0]), expected_version=1
                )
            elif problem == "floor":
                service = clone(engine.repository, scope, clock, floor=2)
            elif problem == "route":
                service = clone(engine.repository, scope, clock, token="wrong-route")
            calls = archive_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError):
                await service.read(
                    "language",
                    actor="bob" if problem == "reader" else "alice",
                    purpose="other" if problem == "purpose" else "agent_context",
                    known_at=known,
                    valid_at=base.at(1),
                )
            assert not calls

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["context_certificate", "projection_policy", "qualification"])
def test_missing_or_changed_historical_semantic_proof_never_uses_current_versions(store, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _ = await configured(engine, kernel, scope, clock)
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "intact")
            known = await checkpoint(service)
            async with engine.repository.unit_of_work() as uow:
                item = (await uow.derived_records(scope, "history_point"))[0]
                point = item["payload"]
                if problem == "context_certificate":
                    point.pop("context")
                else:
                    revision = await uow.derived_get(scope, "revision", point["revision_id"])
                    archive = revision["history"]
                    if problem == "projection_policy":
                        archive["definition"]["context"]["policy"]["revision"] = (
                            "missing-old-policy"
                        )
                    else:
                        archive["records"][0]["payload"]["qualification"]["policy_sha256"] = (
                            "0" * 64
                        )
                    revision["history_sha256"] = point["history_sha256"] = digest(archive)
                    await uow.derived_put(scope, "revision", revision["id"], revision)
                point["sha256"] = digest({k: v for k, v in point.items() if k != "sha256"})
                await uow.derived_put(scope, "history_point", item["identity"], point)
            with pytest.raises(DerivedError):
                await historical(service, known, base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("change", ["context", "expiry", "qualification", "grant", "delete"])
def test_qualified_history_publication_rechecks_the_original_context_and_input_versions(
    store, change
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(
                engine, kernel, scope, clock, expiry=30
            )
            reviewed = await qualified.qualify(engine, scope, clock, rows[0])
            lease = await queue.claim("race", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            prepared = service.prepare(snapshot)
            if change == "context":
                await service.register(
                    bound_definition(scope, clock, spec, project="B"), expected_generation=2
                )
            elif change == "expiry":
                clock[0] = spec.context.expires_at
            elif change == "qualification":
                await qualified.qualify(engine, scope, clock, reviewed)
            elif change == "grant":
                await service.grant(
                    ProcessingGrant(rows[0]["event_id"], ("alice",), revoked=True),
                    expected_version=2,
                )
            else:
                await kernel.forget(
                    ForgetRequest(scope, (rows[0]["event_id"],), mode=ForgetMode.ERASE)
                )
            with pytest.raises(DerivedError):
                await service.publish(lease.task, snapshot, prepared)
            assert not (await service.history_points("language", actor="alice"))["points"]
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "revision")

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["auxiliary", "scope"])
def test_real_backup_replay_erases_frozen_context_qualification_and_all_proofs(
    store, tmp_path, mode
):
    from test_purge_restore import backup_copy, replay, restorer

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _ = await configured(engine, kernel, scope, clock)
            source, link = await auxiliary(engine, scope, service, rows[0])
            await qualified.qualify(engine, scope, clock, rows[0], links=(link,))
            await refresh(service, queue, "backup")
            known = await checkpoint(service)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                assert (await historical(clone(backup, scope, clock), known, base.at(1)))[
                    "state"
                ] == "ready"
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        (source.id,) if mode == "auxiliary" else (),
                        all_in_scope=mode == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
                snapshot = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), snapshot)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        assert all(
                            "history" not in r["payload"] and "manifest" not in r["payload"]
                            for r in await uow.derived_records(scope, "revision")
                        )
                        assert all(
                            "context" not in r["payload"]
                            for r in await uow.derived_records(scope, "history_point")
                        )
                        assert (await uow.derived_get(scope, "history_interval", "language"))[
                            "state"
                        ] == "erased"
                with pytest.raises(DerivedError):
                    await historical(clone(backup, scope, clock), known, base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
@pytest.mark.parametrize("change", ["expiry", "context", "revoke"])
def test_sdk_final_delivery_keeps_frozen_context_and_time_but_rechecks_access(
    store, monkeypatch, transport, change
):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, authority = await configured(
                engine, kernel, scope, clock, expiry=20
            )
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "sdk")
            start = clock[0]
            clock[0] += timedelta(seconds=5)
            known, valid = (start + timedelta(seconds=1)).isoformat(), base.at(1).isoformat()
            original, calls = service.read, []

            async def intervening(*args, **kwargs):
                calls.append(kwargs)
                value = await original(*args, **kwargs)
                if len(calls) == 1:
                    if change == "revoke":
                        await service.set_authority(
                            replace(authority, revoked=True), expected_version=1
                        )
                    elif change == "expiry":
                        clock[0] = spec.context.expires_at
                    else:
                        await service.register(
                            bound_definition(scope, clock, spec, project="B"), expected_generation=2
                        )
                return value

            monkeypatch.setattr(service, "read", intervening)
            host = MCPRequestContext(scope, actor="alice")

            async def exercise(client):
                caps = await client.derived_capabilities()
                assert "locale-context/1" in caps["historical_templates"]
                assert caps["historical_context"] == "frozen-host-route/1"
                if change == "revoke":
                    with pytest.raises(
                        sdk.MemoryClientError, match="derived_authority_unavailable"
                    ):
                        await client.derived_context("language", known_at=known, valid_at=valid)
                else:
                    result = await client.derived_context(
                        "language", known_at=known, valid_at=valid
                    )
                    assert result["observations"][0]["body"]["blocks"][0]["value"] == "zh-CN"
                assert len(calls) == 2 and all(
                    c["known_at"] == known and c["valid_at"] == valid for c in calls
                )
                with pytest.raises(DerivedError, match="invalid_derived_request"):
                    await service.call(
                        "read",
                        dict(
                            facet_id="language",
                            known_at=known,
                            valid_at=valid,
                            context={"project": "B"},
                        ),
                        host,
                    )

            if transport == "embedded":
                await exercise(sdk.EmbeddedMemoryClient(kernel, host, derived=service))
            else:
                mcp = pytest.importorskip("agent_memory_mcp")
                server = mcp.create_server(
                    kernel, mcp.StaticIdentityResolver(host), derived=service
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_actual_sigkill_keeps_qualified_archive_and_context_certificate_atomic(
    store, tmp_path, mode, boundary
):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _ = await configured(
                engine, kernel, scope, clock, mode=mode
            )
            await qualified.qualify(engine, scope, clock, rows[0])
            phase = ("interval_" if mode == HISTORY_INTERVAL else "history_") + boundary
            await kill_at_boundary(
                engine, scope, clock, tmp_path, phase, extra={"context_token": "route-A"}
            )
            async with engine.repository.unit_of_work() as uow:
                points = await uow.derived_records(scope, "history_point")
                revisions = await uow.derived_records(scope, "revision")
                head = await uow.derived_get(scope, "head", "language")
                assert bool(points) == bool(revisions) == bool(head) == (boundary == "after_commit")
                if points:
                    assert points[0]["payload"]["context"]["sha256"]
            clock[0] += timedelta(seconds=6)
            if boundary == "before_commit":
                await refresh(service, queue, "recover")
            assert (await historical(service, await checkpoint(service), base.at(1)))["body"][
                "blocks"
            ][0]["qualified"]

    asyncio.run(run())


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("state", ["contested", "unknown", "expired"])
def test_historical_high_priority_uncertainty_never_falls_back_to_global(store, mode, state):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            policy = ProjectionPolicy(
                "approved/1", "agent_context", "ordered_override", (("global", "project-A"),)
            )
            texts = ("全部项目使用中文。", "项目 A 使用英文。")
            if state == "contested":
                texts += ("项目 A 使用中文。",)
            service, queue, _, rows, _, _ = await configured(
                engine,
                kernel,
                scope,
                clock,
                mode=mode,
                texts=texts,
                policy=policy,
                project=None if state == "unknown" else "A",
            )
            for row in rows[1:]:
                if state == "expired":
                    _, link = await auxiliary(
                        engine, scope, service, row, support=SupportRange(base.at(1), clock[0])
                    )
                    await qualified.qualify(engine, scope, clock, row, policy=policy, links=(link,))
                else:
                    await qualified.qualify(engine, scope, clock, row, policy=policy)
            await refresh(service, queue, "uncertainty")
            view = await historical(service, await checkpoint(service), clock[0])
            block = view["body"]["blocks"][0]
            assert block["kind"] == ("conflict" if state == "contested" else "context_unknown")
            assert "value" not in block

    asyncio.run(run())


@pytest.mark.parametrize("mode", MODES)
def test_actual_source_withdrawal_keeps_old_qualification_and_closes_current_coverage(store, mode):
    from agent_memory.operations.source_revisions import withdraw_revision

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, _, _ = await configured(
                engine, kernel, scope, clock, mode=mode
            )
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "original")
            known = await checkpoint(service)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            async with engine.repository.unit_of_work() as uow:
                source = await uow.get_source_event(scope, rows[0]["event_id"])
                await withdraw_revision(uow, source, "withdraw-qualified")
            assert (await historical(service, known, base.at(1)))["body"]["blocks"][0]["qualified"]
            if mode == HISTORY_INTERVAL:
                assert (await historical(service, start + timedelta(seconds=1), base.at(1)))[
                    "coverage"
                ]["known_to"] == clock[0].isoformat()
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, clock[0], base.at(1))
            clock[0] += timedelta(seconds=1)
            await refresh(service, queue, "withdrawn")
            assert (await historical(service, await checkpoint(service), base.at(1)))[
                "state"
            ] == "empty"

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_actual_sigkill_context_rotation_and_coverage_boundary_are_atomic(
    store, tmp_path, boundary
):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(engine, kernel, scope, clock)
            await qualified.qualify(engine, scope, clock, rows[0])
            await refresh(service, queue, "original")
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            changed_at = clock[0]
            updated = bound_definition(scope, clock, spec, project="B")
            await kill_at_boundary(
                engine,
                scope,
                clock,
                tmp_path,
                "coverage_context_" + boundary,
                extra={"context_token": "route-A", "definition": updated.payload()},
            )
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                coverage = await uow.derived_get(scope, "history_interval", "language")
                committed = boundary == "after_commit"
                assert definition["generation"] == (3 if committed else 2)
                assert coverage["spans"][0]["state"] == ("sealed" if committed else "open")
                assert coverage["spans"][0]["known_to"] == (
                    changed_at.isoformat() if committed else spec.context.expires_at.isoformat()
                )
            clock[0] += timedelta(seconds=1)
            assert (await historical(service, start + timedelta(seconds=1), base.at(1)))["body"][
                "blocks"
            ][0]["value"] == "zh-CN"
            if committed:
                with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                    await historical(service, changed_at, base.at(1))

    asyncio.run(run())


def test_publication_serializes_with_independent_context_rotation_connection(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, rows, spec, _ = await configured(engine, kernel, scope, clock)
            await qualified.qualify(engine, scope, clock, rows[0])
            lease = await queue.claim("publication", lease_seconds=60)
            snap = await service.snapshot(lease.task)
            prepared = service.prepare(snap)
            pg = hasattr(engine.repository, "pool")
            if pg:
                from agent_memory_postgres.repository import PostgresMemoryRepository

                other = PostgresMemoryRepository.from_dsn(
                    engine.repository.pool.conninfo, max_size=2
                )
                await other.initialize()
            else:
                from agent_memory.sqlite import SQLiteMemoryRepository

                other = SQLiteMemoryRepository(engine.repository._path)
            entered, release = asyncio.Event(), asyncio.Event()
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def paused(self, scope, kind, key, row):
                await original(self, scope, kind, key, row)
                if kind == "history_point":
                    entered.set()
                    await release.wait()

            async def rotate():
                remote = clone(other, scope, clock)
                await remote.register(
                    bound_definition(scope, clock, spec, project="B"), expected_generation=2
                )

            start = clock[0]
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(service.publish(lease.task, snap, prepared))
                    await asyncio.wait_for(entered.wait(), timeout=10)
                    clock[0] += timedelta(seconds=5)
                    rotation = asyncio.create_task(
                        rotate() if pg else asyncio.to_thread(lambda: asyncio.run(rotate()))
                    )
                    await asyncio.sleep(0.05)
                    assert not rotation.done()
                    release.set()
                    await asyncio.wait_for(publication, timeout=10)
                    await asyncio.wait_for(rotation, timeout=10)
                view = await historical(service, start + timedelta(seconds=1), base.at(1))
                assert view["body"]["blocks"][0]["value"] == "zh-CN"
                assert view["coverage"]["known_to"] == clock[0].isoformat()
                with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                    await historical(service, clock[0], base.at(1))
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())
