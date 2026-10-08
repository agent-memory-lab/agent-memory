"""Deletion and read-validity contract against SQLite and real PostgreSQL."""

import asyncio
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import replace
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from agent_memory import (
    AgentMemory,
    DecisionRecord,
    Episode,
    EvaluationRecord,
    ForgetMode,
    ForgetRequest,
    MemoryBlock,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
    MemoryUsage,
    OutcomeEvent,
    Procedure,
    Provenance,
    RewardSignal,
    build_local_kernel,
)


@pytest.fixture(params=["sqlite", "postgres"])
def artifact_store(request, tmp_path):
    scope = MemoryScope(f"artifact-erasure-{uuid4().hex}", user_id="alice")
    if request.param == "postgres":
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
        if not dsn:
            pytest.skip("real PostgreSQL artifact erasure tests require a test DSN")
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path.casefold():
            pytest.fail("artifact erasure PostgreSQL tests require a database named test")
        pytest.importorskip("agent_memory_postgres")
        from agent_memory_postgres import build_postgres_kernel

        kernel = build_postgres_kernel(dsn)
    else:
        kernel = build_local_kernel(tmp_path / "artifacts.db")
    repository = kernel._repository

    async def execute(query, params=()):
        if request.param == "postgres":
            query = re.sub(
                r"\b(FROM|JOIN|UPDATE|INTO|TABLE)\s+"
                r"(memory_tombstones|artifacts|events|claims|evolution_records)\b",
                lambda match: match[1] + " agent_memory_" + match[2].lower(),
                query,
                flags=re.IGNORECASE,
            )
            query = query.replace("?", "%s")
            async with repository.pool.connection() as connection:
                cursor = await connection.execute(query, params)
                return list(await cursor.fetchall()) if cursor.description else []
        with repository._connection() as connection:
            cursor = connection.execute(query, params)
            return [dict(row) for row in cursor.fetchall()] if cursor.description else []

    @asynccontextmanager
    async def open_store():
        memory = AgentMemory(kernel, scope)
        await memory.initialize()
        try:
            yield kernel, scope, execute
        finally:
            await memory.__aexit__(None, None, None)

    return open_store


async def write_artifacts(kernel, scope, source_ids):
    provenance = Provenance(source_event_ids=source_ids)
    block = await kernel.write_block(
        MemoryBlock(
            scope,
            "Private profile",
            "Secret BANANA729; likes coffee",
            source_ids,
        )
    )
    episode = Episode(
        scope,
        "Secret BANANA729",
        "learn",
        "remembered",
        "retain evidence",
        provenance=provenance,
    )
    procedure = Procedure(
        scope,
        "Secret BANANA729",
        "question",
        ("use BANANA729",),
        ("answer",),
        provenance=provenance,
    )
    await kernel.record_episode(episode)
    await kernel.publish_procedure(procedure)
    return block, episode, procedure


async def assert_unreadable(kernel, scope, identities):
    assert await kernel.read_block(scope, identities[0]) is None
    assert not {item.id for item in await kernel.search_blocks(scope, "BANANA729")} & set(
        identities
    )
    assert not {
        item.id for item in await kernel._repository.search(MemoryQuery(scope, "BANANA729"), 100)
    } & set(identities)
    bundle = await kernel.retrieve(MemoryQuery(scope, "BANANA729"))
    assert not {
        item.id for item in (*bundle.relevant_memories, *bundle.episodes, *bundle.procedures)
    } & set(identities)


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("all_in_scope", [False, True])
@pytest.mark.parametrize("multi_source", [False, True])
def test_source_withdrawal_invalidates_whole_artifact(
    artifact_store,
    mode,
    child,
    all_in_scope,
    multi_source,
):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            scope = replace(parent, session_id="child") if child else parent
            secret = MemoryEvent(parent, "user.message", "Secret BANANA729")
            independent = MemoryEvent(parent, "user.message", "Likes coffee")
            await kernel.ingest_event(secret)
            await kernel.ingest_event(independent)
            sources = (secret.id, independent.id) if multi_source else (secret.id,)
            artifacts = await write_artifacts(kernel, scope, sources)
            identities = tuple(item.id for item in artifacts)
            result = await kernel.forget(
                ForgetRequest(
                    parent,
                    () if all_in_scope else (secret.id,),
                    mode=mode,
                    all_in_scope=all_in_scope,
                )
            )
            assert result.affected_artifacts == len(artifacts)
            await assert_unreadable(kernel, scope, identities)
            rows = await execute("SELECT * FROM artifacts WHERE tenant_id=?", (parent.tenant_id,))
            if mode == ForgetMode.ERASE:
                assert rows == []  # No payload, text or provenance containing erased information.
            else:
                assert len(rows) == len(artifacts)
                assert all(row["status"] == "archived" and row["archived_at"] for row in rows)

    asyncio.run(scenario())


def test_archive_then_erase_removes_retained_artifacts_and_child_feedback(artifact_store):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            child = replace(parent, session_id="child")
            secret = MemoryEvent(parent, "user.message", "Secret BANANA729")
            independent = MemoryEvent(parent, "user.message", "Likes coffee")
            await kernel.ingest_event(secret)
            await kernel.ingest_event(independent)
            artifacts = await write_artifacts(kernel, child, (secret.id, independent.id))
            bundle = await kernel.retrieve(MemoryQuery(child, "BANANA729"))
            decision = DecisionRecord(
                child,
                "Repeat BANANA729",
                memory_ids=(artifacts[0].id,),
                memory_usage=MemoryUsage.CONFIRMED,
                bundle_id=bundle.bundle_id,
            )
            await kernel.record_decision(decision)
            outcome = OutcomeEvent(child, decision.id, "BANANA729 accepted", True)
            await kernel.record_outcome(outcome)
            for mode in (ForgetMode.ARCHIVE, ForgetMode.ERASE):
                result = await kernel.forget(ForgetRequest(parent, (secret.id,), mode=mode))
                assert result.affected_artifacts == len(artifacts)
                await assert_unreadable(kernel, child, tuple(item.id for item in artifacts))
            assert (
                await execute("SELECT * FROM artifacts WHERE tenant_id=?", (parent.tenant_id,))
                == []
            )
            rows = await execute(
                "SELECT id,payload_json FROM evolution_records WHERE id IN (?,?,?)",
                (bundle.bundle_id, decision.id, outcome.id),
            )
            assert len(rows) == 3
            for row in rows:
                payload = row["payload_json"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                assert payload["redacted"] is True
                assert "BANANA729" not in json.dumps(payload)

    asyncio.run(scenario())


@pytest.mark.parametrize("target", ["source", "block"])
def test_transitive_dependencies_and_unrelated_scope_isolation(artifact_store, target):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            child = replace(parent, session_id="child")
            secret = MemoryEvent(parent, "user.message", "Secret BANANA729")
            independent = MemoryEvent(child, "user.message", "Independent evidence")
            await kernel.ingest_event(secret)
            await kernel.ingest_event(independent)
            block = await kernel.write_block(
                MemoryBlock(child, "Profile", "BANANA729", (secret.id,))
            )
            episode = Episode(
                child,
                "BANANA729",
                "learn",
                "remembered",
                "retained",
                used_memory_ids=(block.id,),
                provenance=Provenance((independent.id,)),
            )
            await kernel.record_episode(episode)
            procedure = Procedure(
                child,
                "BANANA729",
                "question",
                ("use BANANA729",),
                ("answer",),
                source_episode_ids=(episode.id,),
                provenance=Provenance((independent.id,)),
            )
            await kernel.publish_procedure(procedure)
            safe_blocks = []
            for other in (
                replace(parent, user_id="bob"),
                replace(parent, tenant_id=parent.tenant_id + "-other"),
                replace(parent, namespace="other"),
            ):
                event = MemoryEvent(other, "user.message", "Independent BANANA729")
                await kernel.ingest_event(event)
                safe_blocks.append(
                    await kernel.write_block(
                        MemoryBlock(
                            other,
                            "Independent profile",
                            "Independent BANANA729",
                            (event.id,),
                        )
                    )
                )
            # Supplying another scope's source ID cannot seed a dependency purge.
            wrong = await kernel.forget(
                ForgetRequest(
                    safe_blocks[0].scope,
                    (secret.id,),
                    mode=ForgetMode.ERASE,
                )
            )
            assert wrong.affected_events == wrong.affected_artifacts == 0
            assert await kernel.read_block(child, block.id) is not None
            request = ForgetRequest(
                parent if target == "source" else child,
                (secret.id if target == "source" else block.id,),
                mode=ForgetMode.ERASE,
            )
            result = await kernel.forget(request)
            assert result.affected_artifacts == 3
            await assert_unreadable(kernel, child, (block.id, episode.id, procedure.id))
            for safe in safe_blocks:
                assert await kernel.read_block(safe.scope, safe.id) is not None
            assert await execute("SELECT id FROM events WHERE id=?", (independent.id,))

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "archived",
        "foreign_tenant",
        "foreign_namespace",
        "sibling_scope",
        "empty_provenance",
        "scalar_provenance",
        "object_provenance",
        "invalid_source_type",
        "scalar_block_sources",
        "missing_block_source",
        "candidate",
        "superseded",
        "retired",
        "status_archived",
    ],
)
def test_all_block_reads_fail_closed_for_invalid_stored_evidence(artifact_store, damage):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            first = MemoryEvent(parent, "user.message", "Secret BANANA729")
            second = MemoryEvent(parent, "user.message", "Independent evidence")
            await kernel.ingest_event(first)
            await kernel.ingest_event(second)
            block = await kernel.write_block(
                MemoryBlock(
                    parent,
                    "Profile",
                    "BANANA729",
                    (first.id, second.id),
                )
            )
            if damage == "missing":
                await execute("DELETE FROM events WHERE id=?", (first.id,))
            elif damage == "archived":
                await execute("UPDATE events SET archived_at=occurred_at WHERE id=?", (first.id,))
            elif damage.startswith("foreign_") or damage == "sibling_scope":
                column = {
                    "foreign_tenant": "tenant_id",
                    "foreign_namespace": "namespace",
                    "sibling_scope": "session_id",
                }[damage]
                await execute(f"UPDATE events SET {column}=? WHERE id=?", ("elsewhere", first.id))
            elif damage in {"candidate", "superseded", "retired", "status_archived"}:
                await execute(
                    "UPDATE artifacts SET status=? WHERE id=?",
                    (damage.removeprefix("status_"), block.id),
                )
            else:
                row = (
                    await execute(
                        "SELECT payload_json,provenance_json FROM artifacts WHERE id=?", (block.id,)
                    )
                )[0]
                column = "payload_json" if "block_source" in damage else "provenance_json"
                data = row[column]
                if isinstance(data, str):
                    data = json.loads(data)
                field = "event_ids" if column == "payload_json" else "source_event_ids"
                data[field] = {
                    "empty_provenance": [],
                    "scalar_provenance": first.id,
                    "object_provenance": {"id": first.id},
                    "invalid_source_type": [42],
                    "scalar_block_sources": first.id,
                    "missing_block_source": [first.id, "absent"],
                }[damage]
                await execute(
                    f"UPDATE artifacts SET {column}=? WHERE id=?", (json.dumps(data), block.id)
                )
            await assert_unreadable(kernel, parent, (block.id,))

    asyncio.run(scenario())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_archive_then_erase_finds_projected_claim_dependencies(artifact_store, all_in_scope):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            child = replace(parent, session_id="child")
            event = MemoryEvent(
                child,
                "user.message",
                "Secret BANANA729",
                metadata={
                    "claims": [
                        {
                            "key": "private.token",
                            "value": "BANANA729",
                            "text": "Secret BANANA729",
                            "scope": "user",
                        }
                    ]
                },
            )
            receipt = await kernel.ingest_event(event)
            assert len(receipt.claim_ids) == 1
            for mode in (ForgetMode.ARCHIVE, ForgetMode.ERASE):
                result = await kernel.forget(
                    ForgetRequest(
                        child,
                        () if all_in_scope else (event.id,),
                        all_in_scope=all_in_scope,
                        mode=mode,
                    )
                )
                assert result.affected_claims == 1
            assert (
                await execute("SELECT id FROM claims WHERE tenant_id=?", (parent.tenant_id,)) == []
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("link", ["decision_ids", "outcome_ids", "retrieval_trace_ids"])
@pytest.mark.parametrize("archive_first", [False, True])
def test_joint_feedback_artifact_closure(artifact_store, link, archive_first):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            child = replace(parent, session_id="child")
            secret = MemoryEvent(parent, "user.message", "Secret BANANA729")
            independent = MemoryEvent(child, "user.message", "Independent evidence")
            await kernel.ingest_event(secret)
            await kernel.ingest_event(independent)
            trace = await kernel.retrieve(MemoryQuery(parent, "BANANA729"))
            decision = DecisionRecord(
                parent,
                "BANANA729 action",
                (secret.id,),
                memory_usage=MemoryUsage.CONFIRMED,
            )
            await kernel.record_decision(decision)
            outcome = OutcomeEvent(parent, decision.id, "BANANA729 outcome", True)
            await kernel.record_outcome(outcome)
            link_id = {
                "decision_ids": decision.id,
                "outcome_ids": outcome.id,
                "retrieval_trace_ids": trace.bundle_id,
            }[link]
            episode = Episode(
                child,
                "BANANA729",
                "act",
                "done",
                "BANANA729 lesson",
                provenance=Provenance((independent.id,)),
                **{link: (link_id,)},
            )
            await kernel.record_episode(episode)
            procedure = Procedure(
                child,
                "BANANA729",
                "act",
                ("BANANA729 step",),
                ("done",),
                provenance=Provenance((independent.id,)),
                source_episode_ids=(episode.id,),
            )
            await kernel.publish_procedure(procedure)
            next_decision = DecisionRecord(
                child,
                "BANANA729 next action",
                (procedure.id,),
                memory_usage=MemoryUsage.CONFIRMED,
            )
            await kernel.record_decision(next_decision)
            next_outcome = OutcomeEvent(child, next_decision.id, "BANANA729 next outcome", True)
            await kernel.record_outcome(next_outcome)
            final_episode = Episode(
                child,
                "BANANA729 last observation",
                "act",
                "done",
                "BANANA729 last lesson",
                provenance=Provenance((independent.id,)),
                outcome_ids=(next_outcome.id,),
            )
            await kernel.record_episode(final_episode)
            modes = (ForgetMode.ARCHIVE, ForgetMode.ERASE) if archive_first else (ForgetMode.ERASE,)
            for mode in modes:
                result = await kernel.forget(ForgetRequest(parent, (secret.id,), mode=mode))
                assert result.affected_artifacts == 3
            assert (
                await execute("SELECT id FROM artifacts WHERE tenant_id=?", (parent.tenant_id,))
                == []
            )
            found = await kernel._repository.search(MemoryQuery(child, "BANANA729"), 100)
            assert not {episode.id, procedure.id, final_episode.id} & {item.id for item in found}
            rows = await execute(
                "SELECT payload_json FROM evolution_records WHERE tenant_id=?",
                (parent.tenant_id,),
            )
            assert len(rows) == 5
            for row in rows:
                payload = row["payload_json"]
                payload = json.loads(payload) if isinstance(payload, str) else payload
                assert payload["redacted"] is True
                assert "BANANA729" not in json.dumps(payload)

    asyncio.run(scenario())


@pytest.mark.parametrize("colliding_table", ["event", "block", "feedback"])
def test_cross_user_table_identity_collisions_do_not_seed_erasure(artifact_store, colliding_table):
    async def scenario():
        async with artifact_store() as (kernel, alice, execute):
            bob = replace(alice, user_id="bob")
            source = MemoryEvent(bob, "user.message", "Bob independent source")
            await kernel.ingest_event(source)
            safe = await write_artifacts(kernel, bob, (source.id,))
            decision = DecisionRecord(
                bob,
                "Bob private action",
                (safe[0].id,),
                memory_usage=MemoryUsage.CONFIRMED,
            )
            await kernel.record_decision(decision)
            chosen_id = {"event": safe[0].id, "block": decision.id, "feedback": source.id}[
                colliding_table
            ]
            alice_source = MemoryEvent(
                alice,
                "user.message",
                "Alice independent source",
                **({"id": chosen_id} if colliding_table == "event" else {}),
            )
            await kernel.ingest_event(alice_source)
            if colliding_table == "block":
                await kernel.write_block(
                    MemoryBlock(
                        alice,
                        "Alice block",
                        "Alice content",
                        (alice_source.id,),
                        id=chosen_id,
                    )
                )
            elif colliding_table == "feedback":
                await kernel.record_decision(
                    DecisionRecord(
                        alice,
                        "Alice action",
                        (alice_source.id,),
                        id=chosen_id,
                        memory_usage=MemoryUsage.CONFIRMED,
                    )
                )
            result = await kernel.forget(ForgetRequest(alice, (chosen_id,), mode=ForgetMode.ERASE))
            assert result.affected_artifacts == (1 if colliding_table == "block" else 0)
            assert await kernel.read_block(bob, safe[0].id) is not None
            rows = await execute(
                "SELECT id FROM artifacts WHERE partition_key=?", (bob.partition_key(),)
            )
            assert {row["id"] for row in rows} == {item.id for item in safe}
            row = (
                await execute(
                    "SELECT payload_json FROM evolution_records WHERE id=?", (decision.id,)
                )
            )[0]
            payload = row["payload_json"]
            payload = json.loads(payload) if isinstance(payload, str) else payload
            assert payload.get("redacted") is not True
            assert payload["action"] == "Bob private action"

    asyncio.run(scenario())


def test_explicit_episode_reference_does_not_alias_event_id(artifact_store):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            child = replace(parent, session_id="child")
            source = MemoryEvent(parent, "user.message", "Source to erase")
            independent = MemoryEvent(child, "user.message", "Independent child source")
            await kernel.ingest_event(source)
            await kernel.ingest_event(independent)
            episode = Episode(
                child,
                "independent",
                "act",
                "done",
                "independent lesson",
                id=source.id,
                provenance=Provenance((independent.id,)),
            )
            await kernel.record_episode(episode)
            procedure = Procedure(
                child,
                "independent",
                "act",
                ("independent step",),
                ("done",),
                source_episode_ids=(episode.id,),
                provenance=Provenance((independent.id,)),
            )
            await kernel.publish_procedure(procedure)
            result = await kernel.forget(ForgetRequest(parent, (source.id,), mode=ForgetMode.ERASE))
            assert result.affected_artifacts == 0
            rows = await execute("SELECT id FROM artifacts WHERE tenant_id=?", (parent.tenant_id,))
            assert {row["id"] for row in rows} == {episode.id, procedure.id}

    asyncio.run(scenario())


def test_corrected_feedback_remains_in_erasure_lineage(artifact_store):
    async def scenario():
        async with artifact_store() as (kernel, scope, execute):
            secret = MemoryEvent(scope, "user.message", "Secret BANANA729")
            independent = MemoryEvent(scope, "user.message", "Independent evidence")
            await kernel.ingest_event(secret)
            await kernel.ingest_event(independent)
            original = DecisionRecord(
                scope,
                "BANANA729 original",
                (secret.id,),
                memory_usage=MemoryUsage.CONFIRMED,
            )
            await kernel.record_decision(original)
            correction = DecisionRecord(
                scope,
                "BANANA729 revised",
                (),
                corrects_id=original.id,
                memory_usage=MemoryUsage.NONE,
            )
            await kernel.record_decision(correction)
            episode = Episode(
                scope,
                "BANANA729 revised",
                "act",
                "done",
                "BANANA729 revised lesson",
                decision_ids=(correction.id,),
                provenance=Provenance((independent.id,)),
            )
            await kernel.record_episode(episode)
            result = await kernel.forget(ForgetRequest(scope, (secret.id,), mode=ForgetMode.ERASE))
            assert result.affected_artifacts == 1
            assert await execute("SELECT id FROM artifacts WHERE id=?", (episode.id,)) == []
            row = (
                await execute(
                    "SELECT payload_json FROM evolution_records WHERE id=?",
                    (correction.id,),
                )
            )[0]
            payload = row["payload_json"]
            payload = json.loads(payload) if isinstance(payload, str) else payload
            assert payload["redacted"] is True

    asyncio.run(scenario())


def test_all_scope_feedback_roots_invalidate_child_artifacts(artifact_store):
    async def scenario():
        async with artifact_store() as (kernel, parent, execute):
            child = replace(parent, session_id="child")
            independent = MemoryEvent(child, "user.message", "Independent source")
            await kernel.ingest_event(independent)
            decision = DecisionRecord(parent, "BANANA729 private action", ())
            await kernel.record_decision(decision)
            episode = Episode(
                child,
                "BANANA729 copied observation",
                "act",
                "done",
                "BANANA729 lesson",
                decision_ids=(decision.id,),
                provenance=Provenance((independent.id,)),
            )
            await kernel.record_episode(episode)
            result = await kernel.forget(
                ForgetRequest(parent, all_in_scope=True, mode=ForgetMode.ERASE)
            )
            assert result.affected_artifacts == 1
            assert await execute("SELECT id FROM artifacts WHERE id=?", (episode.id,)) == []
            assert await execute("SELECT id FROM events WHERE id=?", (independent.id,))

    asyncio.run(scenario())


@pytest.mark.parametrize("record_type", ["outcome", "evaluation", "reward"])
def test_corrected_feedback_with_independent_parent_is_erased(artifact_store, record_type):
    async def scenario():
        async with artifact_store() as (kernel, scope, execute):
            source = MemoryEvent(scope, "user.message", "BANANA729 source")
            independent = MemoryEvent(scope, "user.message", "Independent source")
            await kernel.ingest_event(source)
            await kernel.ingest_event(independent)
            chains = []
            for event in (source, independent):
                decision = DecisionRecord(
                    scope,
                    "action",
                    (event.id,),
                    memory_usage=MemoryUsage.CONFIRMED,
                )
                await kernel.record_decision(decision)
                outcome = OutcomeEvent(scope, decision.id, "done", True)
                await kernel.record_outcome(outcome)
                evaluation = EvaluationRecord(
                    scope,
                    outcome.id,
                    "host",
                    "1",
                    "quality",
                    "1",
                    {"quality": 1},
                    "sha256:test",
                )
                await kernel.record_evaluation(evaluation)
                reward = RewardSignal(scope, outcome.id, 1, "1", evaluation_id=evaluation.id)
                await kernel.record_reward(reward)
                chains.append({"outcome": outcome, "evaluation": evaluation, "reward": reward})
            original, safe = chains
            correction = replace(
                safe[record_type],
                id=str(uuid4()),
                corrects_id=original[record_type].id,
            )
            await getattr(kernel, "record_" + record_type)(correction)
            await kernel.forget(ForgetRequest(scope, (source.id,), mode=ForgetMode.ERASE))
            row = (
                await execute(
                    "SELECT payload_json FROM evolution_records WHERE id=?",
                    (correction.id,),
                )
            )[0]
            payload = row["payload_json"]
            payload = json.loads(payload) if isinstance(payload, str) else payload
            assert payload["redacted"] is True
            # Correction lineage does not invalidate its independent supporting parent.
            for item in safe.values():
                row = (
                    await execute(
                        "SELECT payload_json FROM evolution_records WHERE id=?",
                        (item.id,),
                    )
                )[0]
                payload = row["payload_json"]
                payload = json.loads(payload) if isinstance(payload, str) else payload
                assert payload.get("redacted") is not True

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_stale_artifact_publication_cannot_restore_erased_dependencies(artifact_store, mode):
    async def scenario():
        async with artifact_store() as (kernel, scope, execute):
            source = MemoryEvent(scope, "user.message", "BANANA729 source")
            independent = MemoryEvent(scope, "user.message", "Independent source")
            await kernel.ingest_event(source)
            await kernel.ingest_event(independent)
            block, episode, _ = await write_artifacts(kernel, scope, (source.id,))
            decision = DecisionRecord(
                scope, "BANANA729", (source.id,), memory_usage=MemoryUsage.CONFIRMED
            )
            await kernel.record_decision(decision)
            outcome = OutcomeEvent(scope, decision.id, "BANANA729", True)
            await kernel.record_outcome(outcome)
            await kernel.forget(ForgetRequest(scope, (source.id,), mode=mode))
            attempts = [
                Episode(
                    scope,
                    "BANANA729",
                    "act",
                    "done",
                    "BANANA729",
                    used_memory_ids=(block.id,),
                    provenance=Provenance((independent.id,)),
                ),
                Episode(
                    scope,
                    "BANANA729",
                    "act",
                    "done",
                    "BANANA729",
                    outcome_ids=(outcome.id,),
                    provenance=Provenance((independent.id,)),
                ),
                Procedure(
                    scope,
                    "BANANA729",
                    "act",
                    ("BANANA729",),
                    ("done",),
                    source_episode_ids=(episode.id,),
                    provenance=Provenance((independent.id,)),
                ),
                replace(
                    block, event_ids=(independent.id,), provenance=Provenance((independent.id,))
                ),
            ]
            for item in attempts:
                method = (
                    kernel.record_episode
                    if isinstance(item, Episode)
                    else kernel.publish_procedure
                    if isinstance(item, Procedure)
                    else kernel.write_block
                )
                with pytest.raises((ValueError, RuntimeError), match="dependencies|conflicts"):
                    await method(item)
            if mode == ForgetMode.ERASE:
                rows = await execute(
                    "SELECT * FROM memory_tombstones "
                    "WHERE tenant_id=? AND memory_table='artifacts'",
                    (scope.tenant_id,),
                )
                assert len(rows) == 3
                assert all(
                    set(row)
                    == {
                        "id",
                        "partition_key",
                        "memory_table",
                        "tenant_id",
                        "namespace",
                        "user_id",
                        "agent_id",
                        "workspace_id",
                        "session_id",
                    }
                    for row in rows
                )
                assert "BANANA729" not in json.dumps(rows)

    asyncio.run(scenario())


def test_external_memory_ids_and_tombstone_identity_scopes_are_preserved(artifact_store):
    async def scenario():
        async with artifact_store() as (kernel, alice, execute):
            bob = replace(alice, user_id="bob")
            bob_source = MemoryEvent(bob, "user.message", "Bob source")
            await kernel.ingest_event(bob_source)
            erased = await kernel.write_block(
                MemoryBlock(bob, "Bob", "Bob block", (bob_source.id,))
            )
            await kernel.forget(ForgetRequest(bob, (bob_source.id,), mode=ForgetMode.ERASE))
            alice_source = MemoryEvent(alice, "user.message", "Alice source", id=erased.id)
            await kernel.ingest_event(alice_source)
            external = Episode(
                alice,
                "external memory",
                "act",
                "done",
                "external lesson",
                used_memory_ids=("ontology:external-node", erased.id),
                provenance=Provenance((alice_source.id,)),
            )
            await kernel.record_episode(external)
            # A Bob artifact fence cannot block Alice's own artifact ID or an event ID.
            block = await kernel.write_block(
                MemoryBlock(
                    alice,
                    "Alice",
                    "Alice block",
                    (alice_source.id,),
                    id=erased.id,
                )
            )
            assert await kernel.read_block(alice, block.id) is not None
            results = await kernel._repository.search(MemoryQuery(alice, "external"), 100)
            assert external.id in {item.id for item in results}

    asyncio.run(scenario())


def test_legacy_stale_rows_fail_read_validation_before_result_limit(artifact_store):
    async def scenario():
        async with artifact_store() as (kernel, scope, execute):
            source = MemoryEvent(scope, "user.message", "BANANA729 source")
            independent = MemoryEvent(scope, "user.message", "Independent source")
            await kernel.ingest_event(source)
            await kernel.ingest_event(independent)
            erased = await kernel.write_block(
                MemoryBlock(scope, "private", "BANANA729", (source.id,))
            )
            stale = Episode(
                scope,
                "BANANA729",
                "act",
                "done",
                "BANANA729",
                provenance=Provenance((independent.id,)),
            )
            await kernel.record_episode(stale)
            await kernel.forget(ForgetRequest(scope, (source.id,), mode=ForgetMode.ERASE))
            # Emulate a pre-fix worker that inserted a stale typed reference directly.
            row = (await execute("SELECT payload_json FROM artifacts WHERE id=?", (stale.id,)))[0]
            payload = row["payload_json"]
            payload = json.loads(payload) if isinstance(payload, str) else payload
            payload["used_memory_ids"] = [erased.id]
            await execute(
                "UPDATE artifacts SET payload_json=? WHERE id=?", (json.dumps(payload), stale.id)
            )
            live = await kernel.write_block(
                MemoryBlock(scope, "valid", "BANANA729", (independent.id,))
            )
            results = await kernel._repository.search(MemoryQuery(scope, "BANANA729"), 1)
            assert [item.id for item in results] == [live.id]
            assert stale.id not in {
                item.id
                for item in await kernel._repository.search(MemoryQuery(scope, "BANANA729"), 100)
            }

    asyncio.run(scenario())


def test_sqlite_upgrade_adds_identity_only_artifact_fences(tmp_path):
    async def scenario():
        path = tmp_path / "legacy.db"
        kernel = build_local_kernel(path)
        await kernel.initialize()
        scope = MemoryScope("upgrade-test")
        event = MemoryEvent(scope, "user.message", "Existing evidence")
        await kernel.ingest_event(event)
        block = await kernel.write_block(
            MemoryBlock(scope, "Existing", "Existing block", (event.id,))
        )
        with kernel._repository._connection() as connection:
            connection.execute("DROP TABLE memory_tombstones")
        # Reopening an existing pre-fence schema adds the table without rewriting content.
        reopened = build_local_kernel(path)
        await reopened.initialize()
        assert await reopened.read_block(scope, block.id) == block
        await reopened.forget(ForgetRequest(scope, (event.id,), mode=ForgetMode.ERASE))
        with reopened._repository._connection() as connection:
            row = connection.execute(
                "SELECT * FROM memory_tombstones WHERE memory_table='artifacts'"
            ).fetchone()
            assert row["id"] == block.id
            assert "content" not in row.keys() and "payload_json" not in row.keys()
        await reopened.close()
        await kernel.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("local_kind", ["events", "claims", "artifacts"])
@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_stale_generic_reference_to_each_local_kind_is_rejected(artifact_store, local_kind, mode):
    async def scenario():
        async with artifact_store() as (kernel, scope, execute):
            source = MemoryEvent(
                scope,
                "user.message",
                "BANANA729",
                metadata={
                    "claims": [
                        {
                            "key": "secret",
                            "value": "BANANA729",
                            "text": "BANANA729",
                            "scope": "user",
                        }
                    ]
                },
            )
            receipt = await kernel.ingest_event(source)
            independent = MemoryEvent(scope, "user.message", "Independent source")
            await kernel.ingest_event(independent)
            block = await kernel.write_block(
                MemoryBlock(scope, "Secret", "BANANA729", (source.id,))
            )
            identity = {"events": source.id, "claims": receipt.claim_ids[0], "artifacts": block.id}[
                local_kind
            ]
            stale = Episode(
                scope,
                "BANANA729 stale",
                "act",
                "done",
                "BANANA729 stale lesson",
                used_memory_ids=(identity,),
                provenance=Provenance((independent.id,)),
            )
            await kernel.forget(ForgetRequest(scope, (identity,), mode=mode))
            with pytest.raises(ValueError, match="dependencies"):
                await kernel.record_episode(stale)
            if mode == ForgetMode.ERASE:
                rows = await execute(
                    "SELECT memory_table FROM memory_tombstones WHERE partition_key=? AND id=?",
                    (scope.partition_key(), identity),
                )
                assert local_kind in {row["memory_table"] for row in rows}

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_surviving_multi_source_claim_remains_valid_artifact_support(artifact_store, mode):
    async def scenario():
        async with artifact_store() as (kernel, scope, execute):
            metadata = {
                "claims": [
                    {
                        "key": "preference",
                        "value": "concise",
                        "text": "Concise replies",
                        "scope": "user",
                    }
                ]
            }
            first = MemoryEvent(scope, "user.message", "Concise replies", metadata=metadata)
            second = MemoryEvent(scope, "user.message", "Concise replies too", metadata=metadata)
            receipt = await kernel.ingest_event(first)
            await kernel.ingest_event(second)
            await kernel.forget(ForgetRequest(scope, (first.id,), mode=mode))
            episode = Episode(
                scope,
                "Concise replies",
                "act",
                "done",
                "Supported by independent evidence",
                used_memory_ids=(receipt.claim_ids[0],),
                provenance=Provenance((second.id,)),
            )
            await kernel.record_episode(episode)
            results = await kernel._repository.search(MemoryQuery(scope, "Concise"), 100)
            assert episode.id in {item.id for item in results}
            assert (
                await execute(
                    "SELECT id FROM memory_tombstones WHERE memory_table='claims' AND id=?",
                    (receipt.claim_ids[0],),
                )
                == []
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_late_feedback_correction_cannot_revive_revoked_predecessor(artifact_store, mode):
    async def scenario():
        async with artifact_store() as (kernel, scope, execute):
            source = MemoryEvent(scope, "user.message", "BANANA729")
            independent = MemoryEvent(scope, "user.message", "Independent source")
            await kernel.ingest_event(source)
            await kernel.ingest_event(independent)
            original = DecisionRecord(
                scope, "BANANA729", (source.id,), memory_usage=MemoryUsage.CONFIRMED
            )
            await kernel.record_decision(original)
            await kernel.forget(ForgetRequest(scope, (source.id,), mode=mode))
            correction = DecisionRecord(scope, "BANANA729 revised", (), corrects_id=original.id)
            with pytest.raises(ValueError, match="no longer valid"):
                await kernel.record_decision(correction)
            assert (
                await execute("SELECT id FROM evolution_records WHERE id=?", (correction.id,)) == []
            )
            # Old software could overwrite a revoked predecessor's status during correction.
            # Simulate that stored lineage and prove it cannot support new or existing artifacts.
            legacy = DecisionRecord(scope, "Legacy correction", ())
            await kernel.record_decision(legacy)
            episode = Episode(
                scope,
                "BANANA729 stale",
                "act",
                "done",
                "BANANA729 lesson",
                decision_ids=(legacy.id,),
                provenance=Provenance((independent.id,)),
            )
            await kernel.record_episode(episode)
            row = (
                await execute("SELECT payload_json FROM evolution_records WHERE id=?", (legacy.id,))
            )[0]
            payload = row["payload_json"]
            payload = json.loads(payload) if isinstance(payload, str) else payload
            payload["corrects_id"] = original.id
            await execute(
                "UPDATE evolution_records SET payload_json=? WHERE id=?",
                (json.dumps(payload), legacy.id),
            )
            await execute(
                "UPDATE evolution_records SET feedback_status='superseded', invalidated_at=NULL "
                "WHERE id=?",
                (original.id,),
            )
            with pytest.raises(ValueError, match="dependencies"):
                await kernel.record_episode(replace(episode, id=str(uuid4())))
            results = await kernel._repository.search(MemoryQuery(scope, "BANANA729"), 100)
            assert episode.id not in {item.id for item in results}

    asyncio.run(scenario())
