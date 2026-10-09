"""Indexed candidate selection, exact evidence lineage and transactional lifecycle."""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from agent_memory import (
    ForgetMode, ForgetRequest, MemoryBlock, MemoryEvent, MemoryQuery, MemoryScope,
    build_local_kernel,
)
from agent_memory.domain import MemoryChannel
from agent_memory.retrieval import sqlite_lexical_index as index
from agent_memory.retrieval.lexical_plugin import LexicalCandidatePlugin
from agent_memory.sqlite import SQLiteMemoryRepository


SCOPE = MemoryScope("indexed-test", user_id="alice", session_id="session")
NOW = datetime(2026, 10, 1, tzinfo=UTC)


async def repository(tmp_path):
    repo = SQLiteMemoryRepository(tmp_path / "index.db")
    await repo.initialize()
    return repo


async def append(repo, text, *, identity="source", scope=SCOPE, when=NOW):
    event = MemoryEvent(scope, "message", text, id=identity, occurred_at=when)
    async with repo.unit_of_work() as uow:
        await uow.append_event(event)
    return event


def evidence(repo, query, scope=SCOPE, limit=8):
    return repo.load_indexed_event_evidence(scope, query, limit=limit)


def sql(repo, query, params=()):
    with closing(sqlite3.connect(repo._path)) as connection:
        connection.row_factory = sqlite3.Row
        with connection:
            return connection.execute(query, params).fetchall()


def test_old_and_long_sources_recalled_before_bounded_hydration(tmp_path, monkeypatch):
    async def run():
        repo = await repository(tmp_path)
        content = "routine filler. " * 350 + "杭州档案 orchidpassport exact tail evidence."
        target = await append(repo, content)
        async with repo.unit_of_work() as uow:
            for number in range(600):
                await uow.append_event(MemoryEvent(
                    SCOPE, "message", f"newer unrelated record {number}", id=f"new-{number}",
                    occurred_at=NOW + timedelta(days=1, seconds=number),
                ))
        hydrated = []
        original = index.hydrate_events

        def bounded(connection, locators):
            assert len(locators) <= 2
            hydrated.extend(value["source_id"] for value in locators)
            return original(connection, locators)

        monkeypatch.setattr(index, "hydrate_events", bounded)
        plugin = LexicalCandidatePlugin.from_sqlite(repo, max_items=2, limit=2)
        found = await plugin.candidates("orchidpassport 杭州", SCOPE)
        assert [item.item.id for item in found.candidates] == [target.id]
        candidate = found.candidates[0]
        assert candidate.source_event_ids == (target.id,)
        span = candidate.item.metadata["source_span"]
        assert span["start"] > 2048
        assert candidate.item.text == content[span["start"]:span["end"]]
        assert candidate.item.metadata["source_revision"] == sha256(content.encode()).hexdigest()
        assert candidate.item.metadata["source_chars"] == len(content)
        assert len(candidate.item.text) <= index.CHUNK_CHARS
        assert found.trace.input_count == 1 and hydrated == [target.id]

    asyncio.run(run())


def test_native_events_artifacts_claims_use_ranked_ids_before_hydration(tmp_path, monkeypatch):
    async def run():
        kernel = build_local_kernel(tmp_path / "index.db")
        await kernel.initialize()
        event = MemoryEvent(SCOPE, "message", "saffronrocket source", metadata={"claims": [
            {"key": "a-target", "value": "saffronrocket", "text": "saffronrocket fact"}
        ]})
        await kernel.ingest_event(event)
        block = await kernel.write_block(MemoryBlock(
            SCOPE, "saffronrocket", "saffronrocket instruction", (event.id,),
        ))
        async with kernel._repository.unit_of_work() as uow:
            for number in range(80):
                await uow.append_event(MemoryEvent(
                    SCOPE, "message", f"unrelated distractor {number}", id=f"filler-{number}",
                ))
        repo = kernel._repository
        statements = []
        connect = repo._connect

        def traced():
            connection = connect()
            connection.set_trace_callback(statements.append)
            return connection

        monkeypatch.setattr(repo, "_connect", traced)
        monkeypatch.setattr(index, "_source", lambda *args: pytest.fail("read rebuilt the corpus"))
        result = await repo.search(MemoryQuery(SCOPE, "saffronrocket"), 3)
        assert {item.kind.value for item in result} == {"event", "claim", "block"}
        assert {event.id, block.id} <= {item.id for item in result}
        ranked = [query for query in statements if query.startswith("WITH hits AS")]
        assert len(ranked) == 3 and all("LIMIT 32" in query for query in ranked)
        assert all("lexical_terms t" in query for query in ranked)
        for query in ranked:
            plan = [row["detail"] for row in sql(repo, "EXPLAIN QUERY PLAN " + query)]
            postings = next(i for i, detail in enumerate(plan) if "SEARCH t USING PRIMARY KEY" in detail)
            chunks = next(i for i, detail in enumerate(plan) if "SEARCH c USING INDEX" in detail)
            assert postings < chunks
            assert not any("SCAN claims" in detail or "SCAN artifacts" in detail for detail in plan)
        assert not any(query.startswith("SELECT * FROM events") for query in statements)
        assert not any(query.startswith("SELECT * FROM artifacts") for query in statements)
        assert not any(query.startswith("SELECT * FROM claim_versions") for query in statements)
        await kernel.close()

    asyncio.run(run())


def test_term_lookup_uses_partition_term_index(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        await append(repo, "orchidpassport")
        plan = sql(repo,
                   "EXPLAIN QUERY PLAN SELECT chunk_id FROM lexical_terms "
                   "WHERE partition_key=? AND term IN (?,?)",
                   (SCOPE.partition_key(), "orchidpassport", "杭州"))
        assert any("SEARCH lexical_terms USING PRIMARY KEY" in row["detail"] for row in plan)

    asyncio.run(run())


def test_migration_backfills_existing_sources_once_and_analyzer_changes_rebuild(tmp_path, monkeypatch):
    async def run():
        repo = await repository(tmp_path)
        await append(repo, "existing saffronrocket")
        with closing(sqlite3.connect(repo._path)) as connection:
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'lexical_%'"
            ).fetchall():
                connection.execute(f'DROP TRIGGER "{name}"')
            for table in ("lexical_terms", "lexical_chunks", "lexical_dirty", "lexical_index_state"):
                connection.execute(f"DROP TABLE {table}")
            connection.commit()
        reopened = SQLiteMemoryRepository(repo._path)
        await reopened.initialize()
        assert evidence(reopened, "saffronrocket")[0].evidence.item.id == "source"
        first = [tuple(row) for row in sql(repo, "SELECT * FROM lexical_chunks")]
        original = index._source
        with monkeypatch.context() as patch:
            patch.setattr(index, "_source", lambda *args: pytest.fail("repeat backfill"))
            await reopened.initialize()
        assert [tuple(row) for row in sql(repo, "SELECT * FROM lexical_chunks")] == first
        calls = []

        def count(*args):
            calls.append(args[2])
            return original(*args)

        monkeypatch.setattr(index, "_source", count)
        monkeypatch.setattr(index, "LEXICAL_ANALYZER_VERSION", "test-next-version")
        await reopened.initialize()
        assert calls == ["source"]
        assert "test-next-version" in sql(repo, "SELECT version FROM lexical_index_state")[0][0]
        assert evidence(reopened, "saffronrocket")

    asyncio.run(run())


def test_source_spans_cover_every_character_and_chunk_boundary(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        content = "x " * 505 + "orchidpassport杭州档案" + " y" * 1600
        await append(repo, content)
        chunks = sql(repo, "SELECT span_start,span_end FROM lexical_chunks ORDER BY span_start")
        assert chunks[0][0] == 0 and chunks[-1][1] == len(content)
        assert all(left[1] >= right[0] for left, right in zip(chunks, chunks[1:]))
        item = evidence(repo, "orchidpassport杭州档案")[0].evidence.item
        span = item.metadata["source_span"]
        assert item.text == content[span["start"]:span["end"]]
        assert "orchidpassport杭州档案" in item.text

    asyncio.run(run())


def test_raw_writer_update_invalidates_old_terms_and_repair_survives_restart(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        await append(repo, "oldmarker evidence")
        before = evidence(repo, "oldmarker")[0].evidence.item.metadata["source_revision"]
        sql(repo, "UPDATE events SET content=? WHERE id='source'", ("newmarker revised evidence",))
        assert not sql(repo, "SELECT 1 FROM lexical_terms WHERE term='oldmarker'")
        assert len(sql(repo, "SELECT 1 FROM lexical_dirty")) == 1
        reopened = SQLiteMemoryRepository(repo._path)
        after = evidence(reopened, "newmarker")[0].evidence.item
        assert before != after.metadata["source_revision"]
        assert not evidence(reopened, "oldmarker")
        assert not sql(repo, "SELECT 1 FROM lexical_dirty")
        sql(repo, "DELETE FROM events WHERE id='source'")
        assert not sql(repo, "SELECT 1 FROM lexical_chunks")
        assert not sql(repo, "SELECT 1 FROM lexical_terms")
        assert not evidence(reopened, "newmarker")

    asyncio.run(run())


def test_artifact_update_and_source_erasure_remove_stale_locators(tmp_path):
    async def run():
        kernel = build_local_kernel(tmp_path / "index.db")
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "message", "source proof")
        await kernel.ingest_event(source)
        block = await kernel.write_block(MemoryBlock(SCOPE, "title", "oldmarker", (source.id,)))
        updated = await kernel.write_block(replace(block, content="newmarker"), expected_version=1)
        assert not await kernel.search_blocks(SCOPE, "oldmarker")
        assert [item.id for item in await kernel.search_blocks(SCOPE, "newmarker")] == [updated.id]
        await kernel.forget(ForgetRequest(SCOPE, (source.id,), mode=ForgetMode.ERASE))
        assert not await kernel.search_blocks(SCOPE, "newmarker")
        assert not sql(kernel._repository, "SELECT 1 FROM lexical_terms WHERE term IN ('oldmarker','newmarker')")
        await kernel.close()

    asyncio.run(run())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_forget_rollback_then_commit_and_restart(tmp_path, mode):
    async def run():
        repo = await repository(tmp_path)
        event = await append(repo, "secretmarker proof")
        request = ForgetRequest(SCOPE, (event.id,), mode=mode)
        with pytest.raises(RuntimeError, match="rollback"):
            async with repo.unit_of_work() as uow:
                await uow.forget_for_restore(request)
                assert not uow.connection.execute(
                    "SELECT 1 FROM lexical_terms WHERE term='secretmarker'"
                ).fetchone()
                raise RuntimeError("rollback")
        assert evidence(repo, "secretmarker")
        await repo.forget(request)
        assert not sql(repo, "SELECT 1 FROM lexical_terms WHERE term='secretmarker'")
        reopened = SQLiteMemoryRepository(repo._path)
        await reopened.initialize()
        assert not evidence(reopened, "secretmarker")
        with pytest.raises(ValueError, match="forgotten"):
            async with reopened.unit_of_work() as uow:
                await uow.append_event(event)

    asyncio.run(run())


def test_failed_index_publication_rolls_back_source_write(tmp_path, monkeypatch):
    async def run():
        repo = await repository(tmp_path)
        with monkeypatch.context() as patch:
            patch.setattr(index, "_source", lambda *args: (_ for _ in ()).throw(RuntimeError("fail")))
            with pytest.raises(RuntimeError, match="fail"):
                await append(repo, "atomicmarker")
        assert not sql(repo, "SELECT 1 FROM events")
        assert not sql(repo, "SELECT 1 FROM lexical_dirty")
        assert not sql(repo, "SELECT 1 FROM lexical_terms")

    asyncio.run(run())


def test_exact_optional_scope_and_native_inheritance_are_preserved(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        parent = replace(SCOPE, session_id=None)
        sibling = replace(SCOPE, user_id="bob")
        foreign = replace(SCOPE, tenant_id="foreign")
        for identity, scope in (("local", SCOPE), ("parent", parent), ("sibling", sibling), ("foreign", foreign)):
            await append(repo, "scopemarker", identity=identity, scope=scope)
        assert {item.evidence.item.id for item in evidence(repo, "scopemarker")} == {"local"}
        native = await repo.search(MemoryQuery(SCOPE, "scopemarker"), 10)
        assert {item.id for item in native} == {"local", "parent"}
        assert not evidence(repo, "scopemarker", scope=replace(SCOPE, namespace="different"))

    asyncio.run(run())


def test_native_admission_guard_still_blocks_indexed_disallowed_sources(tmp_path):
    async def run():
        kernel = build_local_kernel(tmp_path / "index.db")
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "memory.atom.verification", "admissionsecret " * 900)
        async with kernel._repository.unit_of_work() as uow:
            await uow.append_event(source)
        # The candidate index is not an admission decision: the native final
        # guard must still reject even a top-ranked chunk of governed evidence.
        assert evidence(kernel._repository, "admissionsecret")
        bundle = await kernel.retrieve(MemoryQuery(
            SCOPE, "admissionsecret", include_current_state=False,
        ))
        assert not bundle.relevant_memories and not bundle.citations
        await kernel.close()

    asyncio.run(run())


def test_channel_and_empty_query_boundaries(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        await append(repo, "orchidpassport")
        assert not await repo.search(MemoryQuery(SCOPE, "orchidpassport", channels=()), 1)
        assert not await repo.search(MemoryQuery(SCOPE, "orchidpassport", channels=(MemoryChannel.PROCEDURAL,)), 1)
        assert len(evidence(repo, "", limit=1)) == 1
        with pytest.raises(ValueError, match="limit"):
            evidence(repo, "orchidpassport", limit=513)

    asyncio.run(run())


def test_backup_purge_replay_erases_lexical_terms_and_does_not_resurrect_on_restart(tmp_path):
    from agent_memory.operations.purge_restore import PurgeRestore

    async def run():
        repo = await repository(tmp_path)
        event = await append(repo, "restoresecret proof")
        await append(repo, "survivingmarker proof", identity="survivor")
        backup_path = tmp_path / "backup.db"
        with closing(repo._connect()) as source, closing(sqlite3.connect(backup_path)) as target:
            source.backup(target)
        backup = SQLiteMemoryRepository(backup_path)
        await backup.initialize()
        assert evidence(backup, "restoresecret")
        await repo.forget(ForgetRequest(SCOPE, (event.id,), mode=ForgetMode.ERASE))
        options = dict(authority_id="test-authority", secret=b"test-integrity-key-32-bytes-long!!", actor="operator")
        snapshot = await PurgeRestore(repo, SCOPE, **options).export()
        restore = PurgeRestore(backup, SCOPE, **options)
        receipt = await restore.replay(
            snapshot, expected_checkpoint=snapshot["checkpoint"], restore_id="restore",
            reason="offline-backup",
        )
        assert receipt["state"] == "replayed"
        assert not evidence(backup, "restoresecret")
        assert not sql(backup, "SELECT 1 FROM lexical_terms WHERE term='restoresecret'")
        restarted = SQLiteMemoryRepository(backup_path)
        await restarted.initialize()
        assert not evidence(restarted, "restoresecret")
        assert [item.evidence.item.id for item in evidence(restarted, "survivingmarker")] == ["survivor"]

    asyncio.run(run())


def test_transitive_invalid_candidates_do_not_consume_evidence_hydration_budget(tmp_path):
    import json

    async def run():
        kernel = build_local_kernel(tmp_path / "index.db")
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "message", "source proof")
        await kernel.ingest_event(source)
        for number in range(40):
            block = await kernel.write_block(MemoryBlock(
                SCOPE, "targetmarker", "targetmarker", (source.id,), id=f"a-{number:03d}",
            ))
            row = sql(kernel._repository, "SELECT payload_json FROM artifacts WHERE id=?", (block.id,))[0]
            payload = json.loads(row[0])
            payload["source_episode_ids"] = ["missing-artifact"]
            sql(kernel._repository, "UPDATE artifacts SET payload_json=? WHERE id=?", (json.dumps(payload), block.id))
        live = await kernel.write_block(MemoryBlock(
            SCOPE, "targetmarker", "targetmarker", (source.id,), id="z-live",
        ))
        statements = []
        connect = kernel._repository._connect

        def trace():
            connection = connect()
            connection.set_trace_callback(statements.append)
            return connection

        kernel._repository._connect = trace
        result = await kernel.search_blocks(SCOPE, "targetmarker", limit=1)
        assert [item.id for item in result] == [live.id]
        hydration = [query for query in statements if query.startswith("SELECT a.*,c.chunk_id")]
        assert len(hydration) == 1
        dependency_reads = [query for query in statements if "FROM artifacts WHERE tenant_id" in query]
        assert dependency_reads and all("json_object(" in query for query in dependency_reads)
        await kernel.close()

    asyncio.run(run())


@pytest.mark.parametrize("additional", [0, 1])
def test_transitive_validation_cap_is_explicit_and_exact_exhaustion_is_known(tmp_path, monkeypatch, additional):
    import json

    async def run():
        kernel = build_local_kernel(tmp_path / "index.db")
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "message", "source proof")
        await kernel.ingest_event(source)
        monkeypatch.setattr(index, "MAX_CANDIDATES", 32)
        for number in range(32 + additional):
            block = await kernel.write_block(MemoryBlock(
                SCOPE, "targetmarker", "targetmarker", (source.id,), id=f"a-{number:03d}",
            ))
            row = sql(kernel._repository, "SELECT payload_json FROM artifacts WHERE id=?", (block.id,))[0]
            payload = json.loads(row[0])
            payload["source_episode_ids"] = ["missing-artifact"]
            sql(kernel._repository, "UPDATE artifacts SET payload_json=? WHERE id=?", (json.dumps(payload), block.id))
        if additional:
            with pytest.raises(ValueError, match="validation capacity"):
                await kernel.search_blocks(SCOPE, "targetmarker", limit=1)
        else:
            assert not await kernel.search_blocks(SCOPE, "targetmarker", limit=1)
        await kernel.close()

    asyncio.run(run())


def test_nul_and_unicode_before_match_preserve_exact_source_span(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        content = "中文😀\x00" + "routine filler " * 350 + "orchidpassport 杭州😀"
        await append(repo, content)
        optional = evidence(repo, "orchidpassport")[0].evidence.item
        native = (await repo.search(MemoryQuery(SCOPE, "orchidpassport"), 1))[0]
        for item in (optional, native):
            span = item.metadata["source_span"]
            assert "orchidpassport" in item.text
            assert item.text == content[span["start"]:span["end"]]
        # The NUL can also be inside the selected span rather than before it.
        sql(repo, "UPDATE events SET content=? WHERE id='source'", ("中文😀\x00orchidpassport suffix",))
        found = await LexicalCandidatePlugin.from_sqlite(repo).candidates("orchidpassport", SCOPE)
        assert found.candidates[0].item.text == "中文😀\x00orchidpassport suffix"

    asyncio.run(run())


def test_legacy_block_missing_channel_remains_semantic(tmp_path):
    import json

    async def run():
        kernel = build_local_kernel(tmp_path / "index.db")
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "message", "source proof")
        await kernel.ingest_event(source)
        block = await kernel.write_block(MemoryBlock(SCOPE, "legacytarget", "legacytarget", (source.id,)))
        row = sql(kernel._repository, "SELECT payload_json FROM artifacts WHERE id=?", (block.id,))[0]
        payload = json.loads(row[0])
        payload.pop("channel")
        sql(kernel._repository, "UPDATE artifacts SET payload_json=? WHERE id=?", (json.dumps(payload), block.id))
        assert [item.id for item in await kernel.search_blocks(SCOPE, "legacytarget")] == [block.id]
        assert [item.id for item in await kernel._repository.search(MemoryQuery(SCOPE, "legacytarget"), 1)] == [block.id]
        await kernel.close()

    asyncio.run(run())


def test_header_projection_does_not_accept_non_object_artifact_payload(tmp_path):
    from agent_memory.domain import Episode, Provenance

    async def run():
        kernel = build_local_kernel(tmp_path / "index.db")
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "message", "source proof")
        await kernel.ingest_event(source)
        episode = Episode(SCOPE, "invalidpayloadtarget", "act", "done", "lesson",
                          provenance=Provenance((source.id,)))
        await kernel.record_episode(episode)
        sql(kernel._repository, "UPDATE artifacts SET payload_json='[]' WHERE id=?", (episode.id,))
        result = await kernel._repository.search(MemoryQuery(SCOPE, "invalidpayloadtarget"), 1)
        assert not result
        await kernel.close()

    asyncio.run(run())


def test_full_optional_han_query_capacity_keeps_final_character(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        query = "".join(chr(0x4E00 + offset) for offset in range(512))
        assert len(index.lexical_terms(query)) == 1023
        await append(repo, query[-1])
        result = await LexicalCandidatePlugin.from_sqlite(repo).candidates(query, SCOPE)
        assert [candidate.item.id for candidate in result.candidates] == ["source"]

    asyncio.run(run())


@pytest.mark.parametrize("encoding", ["UTF-16le", "UTF-16be"])
def test_existing_utf16_database_keeps_exact_unicode_and_nul_spans(tmp_path, encoding):
    async def run():
        path = tmp_path / "index.db"
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(f"PRAGMA encoding='{encoding}'")
            connection.execute("CREATE TABLE legacy_marker(value TEXT)")
        repo = await repository(tmp_path)
        content = "prefix 中文😀\x00orchidpassport suffix"
        await append(repo, content)
        item = evidence(repo, "orchidpassport")[0].evidence.item
        assert item.text == content
        assert item.metadata["source_span"] == {"start": 0, "end": len(content), "unit": "characters"}
        assert item.metadata["source_revision"] == sha256(content.encode("utf-8")).hexdigest()

    asyncio.run(run())


def test_legacy_load_keeps_recency_order_while_search_finds_older_sources(tmp_path):
    from agent_memory.retrieval.sqlite_source import SQLiteRecentEventEvidenceSource

    async def run():
        repo = await repository(tmp_path)
        await append(repo, "older targetmarker", identity="a-old", when=NOW)
        await append(repo, "middle source", identity="m-middle", when=NOW + timedelta(days=1))
        await append(repo, "newest source " * 300, identity="z-new", when=NOW + timedelta(days=2))
        archived = await append(repo, "archived source", identity="y-archived", when=NOW + timedelta(days=3))
        await repo.forget(ForgetRequest(SCOPE, (archived.id,), mode=ForgetMode.ARCHIVE))
        source = SQLiteRecentEventEvidenceSource(repo)
        records = source.load(SCOPE, limit=2)
        assert [record.evidence.item.id for record in records] == ["z-new", "m-middle"]
        assert len(records[0].evidence.item.text) <= index.CHUNK_CHARS
        assert [record.evidence.item.id for record in source.search(SCOPE, "targetmarker", limit=1)] == ["a-old"]

    asyncio.run(run())


async def stored_claim(repo, text, value, *, identity="claim", key="structuredmetadatakey"):
    from agent_memory.domain import Claim, ClaimStatus, Provenance

    event = MemoryEvent(SCOPE, "message", "Authoritative claim evidence", id=f"event-{identity}")
    claim = Claim(identity, SCOPE, key, value, text, 1.0, 0.5, ClaimStatus.ACTIVE,
                  Provenance((event.id,)), NOW, NOW)
    async with repo.unit_of_work() as uow:
        await uow.append_event(event)
        await uow.save_claim(claim)
    return claim


def test_large_claim_value_and_long_text_postings_grow_additively(tmp_path):
    import json

    async def run():
        repo = await repository(tmp_path)
        value = {"entries": [f"metadataword{number:05d}" for number in range(2000)]}
        text = "firsttextmarker " + "plain body filler " * 2000 + " distanttailmarker"
        claim = await stored_claim(repo, text, value)
        chunks = sql(repo, "SELECT * FROM lexical_chunks WHERE source_table='claim_versions' ORDER BY span_start")
        assert len(chunks) > 30
        text_postings = sum(len(set(index.lexical_terms(text[row["span_start"]:row["span_end"]])))
                            for row in chunks)
        metadata_terms = set(index.lexical_terms(
            claim.key + " " + json.dumps(value, ensure_ascii=False)
        ))
        postings = sql(repo, "SELECT t.term,c.span_start FROM lexical_terms t "
                       "JOIN lexical_chunks c ON c.chunk_id=t.chunk_id "
                       "WHERE c.source_table='claim_versions'")
        assert len(postings) <= text_postings + len(metadata_terms)
        metadata_postings = [row for row in postings if row["term"] in metadata_terms]
        assert len(metadata_postings) == len(metadata_terms)
        assert {row["span_start"] for row in metadata_postings} == {0}
        for keyword in ("metadataword01999", "structuredmetadatakey", "firsttextmarker", "distanttailmarker"):
            found = await repo.search(MemoryQuery(SCOPE, keyword), 1)
            assert [item.id for item in found] == [claim.id]
            item = found[0]
            span = item.metadata["source_span"]
            assert item.text == text[span["start"]:span["end"]]
            assert item.metadata["source_event_ids"] == ("event-claim",)
            if keyword.startswith("metadata") or keyword == claim.key:
                assert span["start"] == 0
                assert keyword not in item.text  # Ranking fields are not invented quoted evidence.
            elif keyword == "distanttailmarker":
                assert span["start"] > 2048 and keyword in item.text

    asyncio.run(run())


def test_empty_stored_claim_text_retains_metadata_lookup_with_empty_exact_span(tmp_path):
    async def run():
        repo = await repository(tmp_path)
        claim = await stored_claim(repo, "", {"name": "emptytextvaluemarker"})
        for keyword in ("emptytextvaluemarker", claim.key):
            result = await repo.search(MemoryQuery(SCOPE, keyword), 1)
            assert [item.id for item in result] == [claim.id]
            assert result[0].text == ""
            assert result[0].metadata["source_span"] == {"start": 0, "end": 0, "unit": "characters"}
            assert result[0].metadata["source_chars"] == 0
            assert result[0].metadata["source_revision"] == sha256(b"").hexdigest()
        chunks = sql(repo, "SELECT span_start,span_end,byte_start,byte_end FROM lexical_chunks "
                     "WHERE source_table='claim_versions'")
        assert [tuple(row) for row in chunks] == [(0, 0, 0, 0)]

    asyncio.run(run())


def test_prior_metadata_layout_rebuilds_atomically_without_changing_claim_evidence(tmp_path, monkeypatch):
    async def run():
        repo = await repository(tmp_path)
        await stored_claim(repo, "revision body " * 500, {"value": "upgrademetadatamarker"})
        await stored_claim(repo, "", {"value": "emptyupgrademarker"}, identity="empty-claim", key="emptykey")
        before_sources = [tuple(row) for row in sql(repo, "SELECT * FROM claim_versions ORDER BY revision_id")]
        # Emulate the prior B2 layout: metadata repeated on all spans, and no
        # locator for an empty-text revision. The stored identity must trigger
        # rebuilding even when no source writer has dirtied any record.
        sql(repo, "INSERT OR IGNORE INTO lexical_terms(partition_key,term,chunk_id,frequency) "
                  "SELECT partition_key,'upgrademetadatamarker',chunk_id,1 FROM lexical_chunks "
                  "WHERE source_table='claim_versions' AND owner_id='claim'")
        sql(repo, "DELETE FROM lexical_chunks WHERE source_table='claim_versions' AND owner_id='empty-claim'")
        sql(repo, "UPDATE lexical_index_state SET version=replace(version,?,?)",
            (index.INDEX_VERSION, "sqlite-lexical/1"))
        before_chunks = [tuple(row) for row in sql(repo, "SELECT * FROM lexical_chunks ORDER BY chunk_id")]
        before_terms = [tuple(row) for row in sql(repo, "SELECT * FROM lexical_terms ORDER BY chunk_id,term")]
        assert len(sql(repo, "SELECT 1 FROM lexical_terms WHERE term='upgrademetadatamarker'")) > 1
        with monkeypatch.context() as patch:
            patch.setattr(index, "_source", lambda *args: (_ for _ in ()).throw(RuntimeError("migration interruption")))
            with pytest.raises(RuntimeError, match="migration interruption"):
                await SQLiteMemoryRepository(repo._path).initialize()
        assert sql(repo, "SELECT version FROM lexical_index_state")[0][0].startswith("sqlite-lexical/1:")
        assert [tuple(row) for row in sql(repo, "SELECT * FROM lexical_chunks ORDER BY chunk_id")] == before_chunks
        assert [tuple(row) for row in sql(repo, "SELECT * FROM lexical_terms ORDER BY chunk_id,term")] == before_terms
        reopened = SQLiteMemoryRepository(repo._path)
        await reopened.initialize()
        assert sql(repo, "SELECT version FROM lexical_index_state")[0][0].startswith(index.INDEX_VERSION + ":")
        assert len(sql(repo, "SELECT 1 FROM lexical_terms WHERE term='upgrademetadatamarker'")) == 1
        assert [tuple(row) for row in sql(repo, "SELECT * FROM claim_versions ORDER BY revision_id")] == before_sources
        assert [item.id for item in await reopened.search(MemoryQuery(SCOPE, "upgrademetadatamarker"), 1)] == ["claim"]
        assert [item.id for item in await reopened.search(MemoryQuery(SCOPE, "emptyupgrademarker"), 1)] == ["empty-claim"]

    asyncio.run(run())
