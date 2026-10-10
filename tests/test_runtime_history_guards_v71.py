"""Immutable historical bodies retain fresh host and expiry delivery guards."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived.contracts import HostGrantAuthority
from agent_memory.derived.model import DerivedError, ProcessingGrant, digest
from agent_memory.derived.question_history import KINDS

store = project.store


async def published(engine, scope, clock, change, kind):
    svc = runtime(engine, scope, clock, history_points=True)
    expiry = clock[0] + timedelta(seconds=10)
    if change == "context":
        svc.context = replace(svc.context, expires_at=expiry)
    if change in {"authority", "authority_floor"}:
        svc.admission.authority_id = "history-host"
        svc.admission.authority_min_version = 1
        authority = HostGrantAuthority(
            "history-host", (ACTOR,), expiry, purposes=(svc.admission.purpose,)
        )
        async with engine.repository.unit_of_work() as uow:
            await uow.derived_put(scope, "authority", authority.id, dict(
                spec=authority.payload(), version=1, epoch=await uow.retention_epoch(scope),
                fingerprint=digest(authority.payload()),
            ))
    item = await project.stage(svc.admission, scope)
    if change in {"grant", "authority", "authority_floor"}:
        await svc.grant(ProcessingGrant(
            item[0].id, (ACTOR,), (svc.admission.purpose,),
            expires_at=expiry if change == "grant" else None,
        ), expected_version=1)
    await project.qualify(svc.admission, *item)
    await register(svc)
    await fresh(svc, clock)
    label = "project-a:owner"
    if kind == "page":
        label = "overview"
        await svc.pages.register(label, ["project-a:owner"], readers=(ACTOR,))
        await svc.pages.publish(label, actor=ACTOR)
        await svc.history.capture(label, actor=ACTOR, kind=kind)
    return svc, expiry, label


@pytest.mark.parametrize("kind", ["question", "page"])
@pytest.mark.parametrize("change", ["grant", "context", "host", "authority", "authority_floor"])
@pytest.mark.parametrize(
    "boundary", ["first_source", "final_source", "body", "final_header", "final_clock"]
)
def test_historical_delivery_rechecks_every_awaited_boundary(
    store, monkeypatch, change, kind, boundary
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, expiry, label = await published(engine, scope, clock, change, kind)
            known = clock[0]
            cls = type(engine.repository.unit_of_work())
            source_proof, get = cls.derived_project_source_proof, cls.derived_get
            observe = cls.refresh_scheduler_observe_clock
            calls, bodies, fired = [], [], []

            def changed():
                if fired:
                    return
                fired.append(True)
                if change == "host":
                    membership = svc.admission.memberships["a"]
                    svc.admission.memberships["a"] = replace(
                        membership, registry_revision="revoked"
                    )
                elif change == "authority_floor":
                    svc.admission.authority_min_version = 2
                else:
                    clock[0] = expiry

            async def source(uow, *args):
                result = await source_proof(uow, *args)
                calls.append(True)
                if ((boundary == "first_source" and len(calls) == 1)
                        or (boundary == "final_source" and len(calls) == 2)):
                    changed()
                return result

            async def read(uow, item_scope, row_kind, key):
                result = await get(uow, item_scope, row_kind, key)
                if row_kind == KINDS[1]:
                    bodies.append(True)
                    if boundary == "first_source":
                        pytest.fail("Historical body loaded after its input permission changed")
                    if boundary == "body":
                        changed()
                if boundary == "final_header" and row_kind == KINDS[0] and bodies:
                    changed()
                return result

            async def observed(uow, *args, **kwargs):
                result = await observe(uow, *args, **kwargs)
                if boundary == "final_clock" and len(calls) == 2:
                    changed()
                return result

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_project_source_proof", source)
                patch.setattr(cls, "derived_get", read)
                patch.setattr(cls, "refresh_scheduler_observe_clock", observed)
                with pytest.raises(DerivedError, match="expired|registration_changed|rollback"):
                    await svc.history.read(label, actor=ACTOR, kind=kind,
                                           known_at=known, valid_at=known)
            assert fired
            if change not in {"host", "authority_floor"}:
                # Failed delivery remembers its later observation across a new service.
                clock[0] = known + timedelta(seconds=1)
                restarted = runtime(engine, scope, clock, history_points=True)
                with pytest.raises(DerivedError, match="clock_discontinuity"):
                    await restarted.read("project-a:owner", actor=ACTOR)
    asyncio.run(run())


@pytest.mark.parametrize("change", ["grant", "context", "host", "authority"])
def test_idempotent_history_capture_checks_final_header_boundary(store, monkeypatch, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, expiry, label = await published(engine, scope, clock, change, "question")
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_get
            fired = []

            async def late(uow, item_scope, kind, key):
                result = await original(uow, item_scope, kind, key)
                if kind == KINDS[0]:
                    fired.append(True)
                    if change == "host":
                        membership = svc.admission.memberships["a"]
                        svc.admission.memberships["a"] = replace(
                            membership, registry_revision="revoked"
                        )
                    else:
                        clock[0] = expiry
                return result

            monkeypatch.setattr(cls, "derived_get", late)
            with pytest.raises(DerivedError, match="expired|registration_changed"):
                await svc.history.capture(label, actor=ACTOR)
            assert fired
            if change != "host":
                clock[0] = expiry - timedelta(seconds=1)
                restarted = runtime(engine, scope, clock, history_points=True)
                with pytest.raises(DerivedError, match="clock_discontinuity"):
                    await restarted.read("project-a:owner", actor=ACTOR)
    asyncio.run(run())
