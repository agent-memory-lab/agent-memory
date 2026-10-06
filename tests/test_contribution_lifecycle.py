"""Contribution operations and erasure semantics against both real adapters."""

import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as base
from test_atom_admission import POLICY, SELF, at, atom, source, state

from agent_memory.consolidation.contributions import ContributionMemory
from agent_memory.domain import ForgetMode, ForgetRequest

store = base.store


async def add(engine, scope, value="Hangzhou", day=1):
    event = source(scope, f"Alice lives in {value}", day=day)
    receipt = await engine.admit(
        event, [atom(value, valid_from=at(day))], authority=SELF, policy=POLICY
    )
    return event, receipt.candidate_ids[0]


async def withdraw(service, scope, identity, **changes):
    args = dict(
        event=source(scope, "The contribution is withdrawn", day=10),
        expected_versions=(
            changes["expected_versions"]
            if "expected_versions" in changes
            else await service.snapshot(identity)
        ),
        authority=SELF,
        policy=POLICY,
        source_quote="The contribution is withdrawn",
    )
    args.update(changes)
    return await service.withdraw(identity, **args)


def test_withdraw_one_support_preserves_independent_peer_and_history(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, first = await add(engine, scope)
            clock[0] = at(2)
            _, second = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            clock[0] = at(10)
            await withdraw(service, scope, first)
            claims, info = await state(engine, scope)
            assert [c.value for c in claims] == ["Hangzhou"]
            assert info["atom_support"][claims[0].id]["candidate_id"] == second
            past = await engine.repository.admission_record_versions(scope, first)
            assert past[0]["payload"]["action"] == "ACCEPT"
            assert past[-1]["payload"]["action"] == "WITHDRAWN"

    asyncio.run(run())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_erasure_keeps_independent_support_and_temporal_barrier(store, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            a_event, a = await add(engine, scope)
            clock[0] = at(5)
            b_event, b = await add(engine, scope, "Shanghai", 5)
            _, b2 = await add(engine, scope, "Shanghai", 5)
            service = ContributionMemory(engine, scope, principal="host")
            clock[0] = at(10)
            await withdraw(service, scope, b2)  # Enroll the closed slot; B is still supported.
            assert [c.value for c in (await state(engine, scope, valid=6))[0]] == ["Shanghai"]
            await kernel.forget(ForgetRequest(scope, memory_ids=(b_event.id,), mode=mode))
            assert not (await state(engine, scope, valid=6))[0]
            assert [c.value for c in (await state(engine, scope, valid=2))[0]] == ["Hangzhou"]
            assert [c.value for c in (await state(engine, scope, valid=6, known=6))[0]] == [
                "Shanghai"
            ]
            assert await engine.repository.admission_record(scope, a) is not None
            assert await engine.repository.admission_record(scope, b) is None
            assert await engine.repository.admission_record_versions(scope, b) == ()
            with pytest.raises(ValueError):
                await engine.admit(
                    b_event, [atom("Shanghai", valid_from=at(5))], authority=SELF, policy=POLICY
                )

    asyncio.run(run())


def test_erasing_same_value_duplicate_does_not_end_independent_support(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, first = await add(engine, scope)
            event, second = await add(engine, scope, day=2)
            _, third = await add(engine, scope, day=3)
            service = ContributionMemory(engine, scope, principal="host")
            await withdraw(service, scope, third)
            await kernel.forget(ForgetRequest(scope, memory_ids=(event.id,), mode=ForgetMode.ERASE))
            claims, info = await state(engine, scope, valid=6)
            assert [c.value for c in claims] == ["Hangzhou"]
            assert info["atom_support"][claims[0].id]["candidate_id"] == first

    asyncio.run(run())


def test_transition_end_basis_survives_new_support_loss_and_its_own_erasure(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            clock[0] = at(5)
            b_event, b = await add(engine, scope, "Shanghai", 5)
            service = ContributionMemory(engine, scope, principal="host")
            ending = source(scope, "Alice left Hangzhou on October 5", day=5)
            clock[0] = at(10)
            result = await service.transition(
                b,
                predecessor_ids=[a],
                valid_from=at(5),
                event=ending,
                expected_versions=await service.snapshot(b),
                authority=SELF,
                policy=POLICY,
                source_quote=ending.content,
            )
            assert result["candidate_ids"] == [a, b]
            row = await engine.repository.admission_record(scope, a)
            assert row["payload"]["transitions"][0]["end_support"]["source_event_id"] == ending.id
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(b_event.id,), mode=ForgetMode.ERASE)
            )
            assert not (await state(engine, scope, valid=6))[0]
            assert [c.value for c in (await state(engine, scope, valid=2))[0]] == ["Hangzhou"]
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(ending.id,), mode=ForgetMode.ERASE)
            )
            assert not (await state(engine, scope, valid=6))[0]
            versions = await engine.repository.admission_record_versions(scope, a)
            assert all(ending.content not in str(v) for v in versions)
            row = await engine.repository.admission_record(scope, a)
            transition = row["payload"]["transitions"][0]
            assert transition["end_support"] is None and transition["start_support"] is None
            assert row["payload"]["valid_to"] == at(5).isoformat()

    asyncio.run(run())


def test_correct_does_not_remove_independent_old_evidence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, first = await add(engine, scope)
            _, second = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            clock[0] = at(10)
            result = await service.correct(
                first,
                event=source(scope, "Correction"),
                expected_versions=await service.snapshot(first),
                authority=SELF,
                policy=POLICY,
                source_quote="Correction",
                replacement_event=source(scope, "Alice lives in Shanghai"),
                replacement=atom("Shanghai"),
                replacement_authority=SELF,
            )
            corrected = await engine.repository.admission_record(scope, first)
            independent = await engine.repository.admission_record(scope, second)
            fresh = await engine.repository.admission_record(scope, result["candidate_ids"][1])
            assert corrected["payload"]["action"] == "WITHDRAWN"
            assert independent["payload"]["action"] == "ACCEPT"
            assert fresh["payload"]["action"] == "CONTESTED"
            assert not (await state(engine, scope))[0]
            assert [c.value for c in (await state(engine, scope, known=2))[0]] == ["Hangzhou"]

    asyncio.run(run())


def test_version_fence_duplicate_and_input_identity(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, first = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            versions = await service.snapshot(first)
            _, second = await add(engine, scope)
            with pytest.raises(ValueError, match="versions changed"):
                await withdraw(service, scope, first, expected_versions=versions)
            event = source(scope, "Withdraw", idempotency="withdraw-once")
            args = dict(
                event=event,
                expected_versions=await service.snapshot(first),
                source_quote="Withdraw",
            )
            result = await withdraw(service, scope, first, **args)
            replay = await withdraw(service, scope, first, **args)
            assert result["operation_id"] == replay["operation_id"] and replay["duplicate"]
            with pytest.raises(ValueError, match="identity conflict"):
                await withdraw(service, scope, second, **args)
            await kernel.forget(ForgetRequest(scope, memory_ids=(first,), mode=ForgetMode.ERASE))
            with pytest.raises(ValueError, match="erased"):
                await withdraw(service, scope, first, **args)

    asyncio.run(run())


def test_cross_slot_correction_rolls_back_old_withdrawal(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, first = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            versions = await service.snapshot(first)
            event = source(scope, "Correction")
            with pytest.raises(ValueError, match="cross-slot"):
                await service.correct(
                    first,
                    event=event,
                    expected_versions=versions,
                    authority=SELF,
                    policy=POLICY,
                    source_quote="Correction",
                    replacement_event=source(scope, "English"),
                    replacement=atom("English", predicate="locale", source_quote="English"),
                    replacement_authority=SELF,
                )
            assert await service.snapshot(first) == versions
            assert [c.value for c in (await state(engine, scope))[0]] == ["Hangzhou"]
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, event.id) is None

    asyncio.run(run())


def test_conflict_resolves_only_after_all_opposing_contributions_are_withdrawn(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, first = await add(engine, scope)
            _, second = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            result = await service.correct(
                first,
                event=source(scope, "Correction"),
                expected_versions=await service.snapshot(first),
                authority=SELF,
                policy=POLICY,
                source_quote="Correction",
                replacement_event=source(scope, "Alice lives in Shanghai"),
                replacement=atom("Shanghai"),
                replacement_authority=SELF,
            )
            await withdraw(service, scope, second)
            claims, info = await state(engine, scope)
            assert [c.value for c in claims] == ["Shanghai"]
            assert info["atom_support"][claims[0].id]["candidate_id"] == result["candidate_ids"][1]

    asyncio.run(run())


def test_managed_slot_rejects_legacy_write_and_accepts_fenced_fresh_evidence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            b_event, b = await add(engine, scope, "Shanghai", 5)
            service = ContributionMemory(engine, scope, principal="host")
            await withdraw(service, scope, b)
            with pytest.raises(ValueError, match="managed slot"):
                await add(engine, scope, "Beijing", 7)
            new_event = source(scope, "Alice lives in Beijing", day=7)
            result = await service.add(
                a,
                event=source(scope, "New independent evidence"),
                expected_versions=await service.snapshot(a),
                authority=SELF,
                policy=POLICY,
                source_quote="New independent evidence",
                replacement_event=new_event,
                replacement=atom("Beijing", valid_from=at(7)),
                replacement_authority=SELF,
            )
            assert [c.value for c in (await state(engine, scope, valid=8))[0]] == ["Beijing"]
            assert not (await state(engine, scope, valid=6))[0]
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(b_event.id,), mode=ForgetMode.ERASE)
            )
            versions = await service.snapshot(a)
            with pytest.raises(ValueError, match="backfill"):
                await service.add(
                    a,
                    event=source(scope, "Backfill"),
                    expected_versions=versions,
                    authority=SELF,
                    policy=POLICY,
                    source_quote="Backfill",
                    replacement_event=source(scope, "Alice lives in Hangzhou"),
                    replacement=atom(),
                    replacement_authority=SELF,
                )
            assert await service.snapshot(a) == versions
            assert result["candidate_ids"][1] in versions

    asyncio.run(run())


def test_transition_requires_all_independent_predecessors_and_qualified_authority(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            _, a2 = await add(engine, scope)
            _, b = await add(engine, scope, "Shanghai", 5)
            service = ContributionMemory(engine, scope, principal="host")
            versions = await service.snapshot(b)
            ending = source(scope, "Alice left Hangzhou")
            args = dict(
                valid_from=at(5),
                event=ending,
                expected_versions=versions,
                authority=SELF,
                policy=POLICY,
                source_quote=ending.content,
            )
            with pytest.raises(ValueError, match="all independent"):
                await service.transition(b, predecessor_ids=[a], **args)
            with pytest.raises(ValueError, match="failed admission"):
                await service.transition(
                    b,
                    predecessor_ids=[a, a2],
                    **{
                        **args,
                        "authority": replace(SELF, subjects=("mallory",)),
                    },
                )
            assert await service.snapshot(b) == versions
            result = await service.transition(b, predecessor_ids=[a, a2], **args)
            assert not result["duplicate"]
            assert (await service.transition(b, predecessor_ids=[a2, a], **args))["duplicate"]
            await withdraw(service, scope, b)
            assert not (await state(engine, scope, valid=6))[0]

    asyncio.run(run())


def test_contribution_scope_and_grounding_fail_closed(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            versions = await service.snapshot(a)
            with pytest.raises(ValueError, match="failed admission"):
                await withdraw(service, scope, a, source_quote="not in the source")
            with pytest.raises(ValueError, match="exact.scope"):
                await withdraw(service, scope, a, event=source(replace(scope, user_id="mallory")))
            other = ContributionMemory(engine, replace(scope, session_id="other"), principal="host")
            with pytest.raises(ValueError, match="exact scope"):
                await other.snapshot(a)
            assert await service.snapshot(a) == versions

    asyncio.run(run())


def test_failure_after_replacement_publication_rolls_back_entire_operation(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            versions = await service.snapshot(a)
            command = source(scope, "Correction")
            replacement = source(scope, "Alice lives in Shanghai")
            unit_type = type(engine.repository.unit_of_work())
            original = unit_type.append_event

            async def fail_command(uow, event):
                if event.id == command.id:
                    raise RuntimeError("injected after replacement publication")
                return await original(uow, event)

            with monkeypatch.context() as patch:
                patch.setattr(unit_type, "append_event", fail_command)
                with pytest.raises(RuntimeError, match="injected"):
                    await service.correct(
                        a,
                        event=command,
                        expected_versions=versions,
                        authority=SELF,
                        policy=POLICY,
                        source_quote=command.content,
                        replacement_event=replacement,
                        replacement=atom("Shanghai"),
                        replacement_authority=SELF,
                    )
            assert await service.snapshot(a) == versions
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, command.id) is None
                assert await uow.get_source_event(scope, replacement.id) is None
            assert [c.value for c in (await state(engine, scope))[0]] == ["Hangzhou"]

    asyncio.run(run())


def test_competing_operations_only_one_closed_version_set_can_commit(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            _, a2 = await add(engine, scope)
            service = ContributionMemory(engine, scope, principal="host")
            versions = await service.snapshot(a)
            results = await asyncio.gather(
                withdraw(service, scope, a, expected_versions=versions),
                withdraw(service, scope, a2, expected_versions=versions),
                return_exceptions=True,
            )
            assert sum(isinstance(r, dict) for r in results) == 1
            assert sum(isinstance(r, ValueError) for r in results) == 1
            assert [c.value for c in (await state(engine, scope))[0]] == ["Hangzhou"]

    asyncio.run(run())


def test_erased_barrier_applies_to_historical_scalar_and_contextual_reads(store):
    from agent_memory.conditions import ProjectionPolicy, QueryContext
    from agent_memory.consolidation.qualification import ContextualMemory
    from agent_memory.domain import MemoryQuery

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            clock[0] = at(5)
            b_event, b = await add(engine, scope, "Shanghai", 5)
            clock[0] = at(10)
            service = ContributionMemory(engine, scope, principal="host")
            await withdraw(service, scope, b)
            assert [c.value for c in (await state(engine, scope, valid=6, known=6))[0]] == [
                "Shanghai"
            ]
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(b_event.id,), mode=ForgetMode.ERASE)
            )
            assert not (await state(engine, scope, valid=6, known=6))[0]
            assert [c.value for c in (await state(engine, scope, valid=2, known=2))[0]] == [
                "Hangzhou"
            ]
            contextual = ContextualMemory(engine, scope, principal="host")
            context = QueryContext("host", scope, "alice", "planning", at(6), at(6))
            answer = await contextual.query(
                context, predicate="city", policy=ProjectionPolicy("v1", "planning")
            )
            assert answer["status"] == "unknown"
            result = await kernel.retrieve(MemoryQuery(scope, "Alice city", token_budget=2048))
            assert not result.current_state
            assert all(
                "Shanghai" not in i.text and "Hangzhou" not in i.text
                for i in result.relevant_memories
            )

    asyncio.run(run())


@pytest.mark.parametrize("erase_successor", [False, True])
def test_only_explicit_qualified_transition_correction_restores_continuity(store, erase_successor):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            clock[0] = at(5)
            b_event, b = await add(engine, scope, "Shanghai", 5)
            service = ContributionMemory(engine, scope, principal="host")
            ending = source(scope, "Alice left Hangzhou")
            clock[0] = at(8)
            await service.transition(
                b,
                predecessor_ids=[a],
                valid_from=at(5),
                event=ending,
                expected_versions=await service.snapshot(b),
                authority=SELF,
                policy=POLICY,
                source_quote=ending.content,
            )
            correction = source(scope, "The departure was incorrect; Alice remained in Hangzhou")
            versions = await service.snapshot(a)
            args = dict(
                transition_id=ending.id,
                valid_to=None,
                event=correction,
                expected_versions=versions,
                authority=SELF,
                policy=POLICY,
                source_quote=correction.content,
            )
            with pytest.raises(ValueError, match="opposed"):
                await service.correct_transition(a, **args)
            assert await service.snapshot(a) == versions
            clock[0] = at(10)
            if erase_successor:
                await kernel.forget(
                    ForgetRequest(scope, memory_ids=(b_event.id,), mode=ForgetMode.ERASE)
                )
            else:
                await withdraw(service, scope, b)
            assert not (await state(engine, scope, valid=6))[0]
            args["expected_versions"] = await service.snapshot(a)
            clock[0] = at(15)
            await service.correct_transition(a, **args)
            assert [c.value for c in (await state(engine, scope, valid=6))[0]] == ["Hangzhou"]
            assert not (await state(engine, scope, valid=6, known=12))[0]
            assert (await service.correct_transition(a, **args))["duplicate"]
            # Erasing the continuity proof revokes its waiver even for prior knowledge.
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(correction.id,), mode=ForgetMode.ERASE)
            )
            assert not (await state(engine, scope, valid=6))[0]
            assert not (await state(engine, scope, valid=6, known=16))[0]
            versions = await engine.repository.admission_record_versions(scope, a)
            assert all(correction.content not in str(v) for v in versions)
            assert [c.value for c in (await state(engine, scope, valid=2))[0]] == ["Hangzhou"]

    asyncio.run(run())


def test_continuity_correction_respects_bounded_end_and_version_fence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            _, b = await add(engine, scope, "Shanghai", 5)
            service = ContributionMemory(engine, scope, principal="host")
            ending = source(scope, "Alice left Hangzhou")
            await service.transition(
                b,
                predecessor_ids=[a],
                valid_from=at(5),
                event=ending,
                expected_versions=await service.snapshot(b),
                authority=SELF,
                policy=POLICY,
                source_quote=ending.content,
            )
            stale = await service.snapshot(a)
            await withdraw(service, scope, b)
            args = dict(
                transition_id=ending.id,
                valid_to=at(8),
                event=source(scope, "Actually stayed until October 8"),
                expected_versions=stale,
                authority=SELF,
                policy=POLICY,
                source_quote="Actually stayed until October 8",
            )
            with pytest.raises(ValueError, match="versions changed"):
                await service.correct_transition(a, **args)
            args["expected_versions"] = await service.snapshot(a)
            with pytest.raises(ValueError, match="failed admission"):
                await service.correct_transition(
                    a, **{**args, "authority": replace(SELF, subjects=("mallory",))}
                )
            await service.correct_transition(a, **args)
            assert [c.value for c in (await state(engine, scope, valid=7))[0]] == ["Hangzhou"]
            assert not (await state(engine, scope, valid=8))[0]

    asyncio.run(run())


def test_contextual_read_rechecks_continuity_proof_erased_after_snapshot(store, monkeypatch):
    from agent_memory.conditions import ProjectionPolicy, QueryContext
    from agent_memory.consolidation.qualification import ContextualMemory

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, a = await add(engine, scope)
            _, b = await add(engine, scope, "Shanghai", 5)
            service = ContributionMemory(engine, scope, principal="host")
            ending = source(scope, "Alice left Hangzhou")
            await service.transition(
                b,
                predecessor_ids=[a],
                valid_from=at(5),
                event=ending,
                expected_versions=await service.snapshot(b),
                authority=SELF,
                policy=POLICY,
                source_quote=ending.content,
            )
            await withdraw(service, scope, b)
            correction = source(scope, "Alice remained in Hangzhou")
            await service.correct_transition(
                a,
                transition_id=ending.id,
                valid_to=None,
                event=correction,
                expected_versions=await service.snapshot(a),
                authority=SELF,
                policy=POLICY,
                source_quote=correction.content,
            )
            original = engine.records_at

            async def erase_after_snapshot(*args, **kwargs):
                rows = await original(*args, **kwargs)
                await kernel.forget(
                    ForgetRequest(scope, memory_ids=(correction.id,), mode=ForgetMode.ERASE)
                )
                return rows

            monkeypatch.setattr(engine, "records_at", erase_after_snapshot)
            with pytest.raises(ValueError, match="snapshot invalidated"):
                await ContextualMemory(engine, scope, principal="host").query(
                    QueryContext("host", scope, "alice", "planning", at(6), at(30)),
                    predicate="city",
                    policy=ProjectionPolicy("v1", "planning"),
                )

    asyncio.run(run())


def test_new_erasure_barrier_invalidates_a_snapshot_of_earlier_knowledge(store, monkeypatch):
    from agent_memory.conditions import ProjectionPolicy, QueryContext
    from agent_memory.consolidation.qualification import ContextualMemory

    async def run():
        async with store() as (engine, kernel, scope, clock):
            await add(engine, scope)
            clock[0] = at(5)
            b_event, _ = await add(engine, scope, "Shanghai", 5)
            _, b2 = await add(engine, scope, "Shanghai", 5)
            clock[0] = at(10)
            await withdraw(ContributionMemory(engine, scope, principal="host"), scope, b2)
            original = engine.records_at

            async def erase_after_snapshot(*args, **kwargs):
                rows = await original(*args, **kwargs)
                await kernel.forget(
                    ForgetRequest(scope, memory_ids=(b_event.id,), mode=ForgetMode.ERASE)
                )
                return rows

            monkeypatch.setattr(engine, "records_at", erase_after_snapshot)
            with pytest.raises(ValueError, match="snapshot invalidated"):
                await ContextualMemory(engine, scope, principal="host").query(
                    QueryContext("host", scope, "alice", "planning", at(6), at(2)),
                    predicate="city",
                    policy=ProjectionPolicy("v1", "planning"),
                )

    asyncio.run(run())
