"""Real SQLite/kernel integration tests for the experiment quote bridge."""

import asyncio
from dataclasses import replace

import pytest

from agent_memory.domain import MemoryQuery
from experiments.evidence_obligations.core_bridge import QuoteStore


def test_exact_quotes_use_typed_core_admission_and_real_provenance(tmp_path, monkeypatch):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "arm-one").initialize()
        try:
            async def forbidden_legacy_write(*args, **kwargs):
                pytest.fail("legacy confidence ingestion must not run")

            monkeypatch.setattr(store.kernel, "ingest_event", forbidden_legacy_write)
            quote = "Access is not permitted unless approved; contractors are excepted."
            source = "Policy header\n" + quote + "\nPolicy footer"
            assert await store.admit("policy", "r1", source, [quote]) == 1
            found = await store.search("access contractors")
            assert len(found) == 1
            item = found[0]
            assert item["source_id"] == "policy" and item["revision"] == "r1"
            assert item["quote"] == quote
            assert item["atom_id"].startswith("atom-claim:")
            assert source[item["source_start"]:item["source_end"]] == quote
            record = await store.kernel.admission_status(store.scope, item["candidate_id"])
            payload = record["payload"]
            assert payload["action"] == "ACCEPT"
            assert payload["draft"]["predicate"] == "evidence.quote"
            for key in ("value", "text", "source_quote"):
                assert payload["draft"][key] == quote
            assert not payload["draft"].get("conditions")
            assert not payload["draft"].get("exceptions")
            assert not payload["draft"].get("negated")
            assert not payload["draft"].get("field_evidence")
            assert payload["authority"]["kind"] == "document"
            assert payload["authority"]["source_id"] == "experiment:host-document-observer"
            assert payload["authority"]["predicates"] == ["evidence.quote"]
            assert payload["claim_id"] == item["atom_id"]
            assert payload["source_event_ids"] == item["source_event_ids"]
            async with store.repository.unit_of_work() as uow:
                event = await uow.get_source_event(store.scope, item["source_event_ids"][0])
            assert event.content == quote
            assert event.event_type == "memory.atom"
            assert "policy?revision=r1" in event.source_uri
        finally:
            await store.close()

    asyncio.run(run())


def test_invalid_outputs_are_skipped_without_normalizing_quotes(tmp_path):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "exact").initialize()
        try:
            exact = "  Unicode café — only if approved.  "
            boundary = "b" * 1000
            oversized = "c" * 1001
            source = exact + boundary + oversized
            assert await store.admit("doc", "v1", source, [
                "", "   ", "hallucinated", "Unicode cafe", exact, exact,
                boundary, oversized, None, {"quote": exact},
            ]) == 2
            found = await store.search("Unicode approved", limit=10)
            assert {item["quote"] for item in found} == {exact, boundary}
            assert await store.admit("doc", "v1", source, [exact, boundary]) == 0
            assert len(await store.search("Unicode", limit=10)) == 2
        finally:
            await store.close()

    asyncio.run(run())


def test_multiple_quotes_and_sources_do_not_conflict_or_merge(tmp_path):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "distinct").initialize()
        try:
            first, second = "The option is enabled.", "The option is not enabled."
            assert await store.admit("one", "r1", first + " " + second, [first, second]) == 2
            assert await store.admit("two", "r1", first, [first]) == 1
            found = await store.search("option", limit=10)
            assert len(found) == 3
            assert len({item["atom_id"] for item in found}) == 3
            await store.revoke("one")
            assert [item["source_id"] for item in await store.search("option")] == ["two"]
        finally:
            await store.close()

    asyncio.run(run())


def test_revision_change_erases_old_atoms_and_empty_extraction_still_retires(tmp_path):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "revisions").initialize()
        try:
            await store.admit("doc", "r1", "Access is open.", ["Access is open."])
            old = (await store.search("Access"))[0]
            assert await store.admit("doc", "r2", "Access is closed.", ["Access is closed."]) == 1
            found = await store.search("Access")
            assert [(item["revision"], item["quote"]) for item in found] == [
                ("r2", "Access is closed.")
            ]
            assert await store.kernel.admission_history(store.scope, old["candidate_id"]) == ()
            async with store.repository.unit_of_work() as uow:
                assert not await uow.events_exist(store.scope, old["source_event_ids"])
            assert await store.admit("doc", "r3", "Access is under review.", ["made up"]) == 0
            assert await store.search("Access") == []
        finally:
            await store.close()

    asyncio.run(run())


def test_reused_revision_with_changed_text_fails_closed(tmp_path):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "immutable").initialize()
        try:
            await store.admit("doc", "r1", "Old text", ["Old text"])
            with pytest.raises(ValueError, match="revision reused"):
                await store.admit("doc", "r1", "New text", ["New text"])
            assert await store.search("text") == []
        finally:
            await store.close()

    asyncio.run(run())


def test_revoke_erases_core_and_persists_across_reopen(tmp_path):
    async def run():
        path = tmp_path / "quotes.db"
        store = await QuoteStore(path, "revoke").initialize()
        try:
            await store.admit("doc", "r1", "Sensitive quote", ["Sensitive quote"])
            old = (await store.search("Sensitive"))[0]
            await store.revoke("doc")
            await store.revoke("doc")
            await store.revoke("unknown")
            assert await store.search("Sensitive") == []
            assert await store.kernel.admission_history(store.scope, old["candidate_id"]) == ()
            bundle = await store.kernel.retrieve(MemoryQuery(store.scope, "Sensitive"))
            assert bundle.current_state == () and bundle.relevant_memories == ()
        finally:
            await store.close()
        reopened = await QuoteStore(path, "revoke").initialize()
        try:
            assert await reopened.search("Sensitive") == []
            assert await reopened.admit("doc", "r1", "Sensitive quote", ["Sensitive quote"]) == 1
            new = (await reopened.search("Sensitive"))[0]
            assert new["atom_id"] != old["atom_id"]
        finally:
            await reopened.close()

    asyncio.run(run())


def test_kernel_retrieval_runs_before_host_validation_and_has_no_index_fallback(
    tmp_path, monkeypatch
):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "retrieval").initialize()
        try:
            await store.admit("doc", "r1", "Native retrieval quote", ["Native retrieval quote"])
            native = store.kernel.retrieve
            calls = []

            async def empty_retrieval(query):
                calls.append(query)
                bundle = await native(query)
                return replace(bundle, relevant_memories=(), citations=())

            monkeypatch.setattr(store.kernel, "retrieve", empty_retrieval)
            assert await store.search("Native") == []
            assert len(calls) == 1 and calls[0].include_current_state is False

            async def revoke_after_retrieval(query):
                bundle = await native(query)
                with store._metadata() as db:
                    db.execute("UPDATE experiment_quote_sources SET active = 0")
                return bundle

            monkeypatch.setattr(store.kernel, "retrieve", revoke_after_retrieval)
            assert await store.search("Native") == []
        finally:
            await store.close()

    asyncio.run(run())


def test_stale_core_bundle_cannot_bypass_revision_validation(tmp_path, monkeypatch):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "stale").initialize()
        try:
            await store.admit("doc", "r1", "Old quote", ["Old quote"])
            old_bundle = await store.kernel.retrieve(MemoryQuery(
                store.scope, "quote", include_current_state=False
            ))
            assert old_bundle.relevant_memories
            await store.admit("doc", "r2", "New quote", ["New quote"])

            async def stale_retrieval(query):
                return old_bundle

            monkeypatch.setattr(store.kernel, "retrieve", stale_retrieval)
            assert await store.search("quote") == []
        finally:
            await store.close()

    asyncio.run(run())


def test_failed_forget_blocks_delivery_even_after_reopen(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "quotes.db"
        store = await QuoteStore(path, "failure").initialize()
        try:
            await store.admit("doc", "r1", "Old quote", ["Old quote"])

            async def failed_forget(request):
                raise OSError("simulated erase failure")

            monkeypatch.setattr(store.kernel, "forget", failed_forget)
            with pytest.raises(OSError, match="erase failure"):
                await store.revoke("doc")
            assert await store.search("quote") == []
        finally:
            await store.close()
        reopened = await QuoteStore(path, "failure").initialize()
        try:
            assert await reopened.search("quote") == []
            await reopened.revoke("doc")
            bundle = await reopened.kernel.retrieve(MemoryQuery(reopened.scope, "quote"))
            assert not bundle.current_state and not bundle.relevant_memories
        finally:
            await reopened.close()

    asyncio.run(run())


def test_interrupted_admission_is_retried_and_erasable(tmp_path, monkeypatch):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "interrupted").initialize()
        try:
            native = store.kernel.admit_event

            async def interrupted(*args, **kwargs):
                await native(*args, **kwargs)
                raise OSError("stopped after core commit")

            monkeypatch.setattr(store.kernel, "admit_event", interrupted)
            with pytest.raises(OSError, match="core commit"):
                await store.admit("doc", "r1", "Exact quote", ["Exact quote"])
            assert await store.search("quote") == []
            monkeypatch.setattr(store.kernel, "admit_event", native)
            assert await store.admit("doc", "r1", "Exact quote", ["Exact quote"]) == 1
            assert len(await store.search("quote")) == 1
            await store.revoke("doc")
            assert await store.search("quote") == []
        finally:
            await store.close()

    asyncio.run(run())


def test_long_document_exact_span_and_restart(tmp_path):
    async def run():
        path = tmp_path / "quotes.db"
        quote = "Only café members qualify."
        source = "x" * 40_000 + quote + "y" * 40_000
        store = await QuoteStore(path, "long").initialize()
        try:
            assert await store.admit("long/doc", "rev 1", source, [quote]) == 1
        finally:
            await store.close()
        reopened = await QuoteStore(path, "long").initialize()
        try:
            item = (await reopened.search("café"))[0]
            assert item["quote"] == quote and item["source_start"] == 40_000
        finally:
            await reopened.close()
        # Core and host records occupy the same file; no separate index file.
        assert {p.name for p in tmp_path.iterdir()} <= {
            "quotes.db", "quotes.db-wal", "quotes.db-shm"
        }

    asyncio.run(run())


def test_namespaces_are_isolated_even_in_same_database(tmp_path):
    async def run():
        path = tmp_path / "quotes.db"
        one = await QuoteStore(path, "one").initialize()
        two = await QuoteStore(path, "two").initialize()
        try:
            await one.admit("doc", "r1", "Arm one quote", ["Arm one quote"])
            await two.admit("doc", "r1", "Arm two quote", ["Arm two quote"])
            assert [r["quote"] for r in await one.search("Arm")] == ["Arm one quote"]
            assert [r["quote"] for r in await two.search("Arm")] == ["Arm two quote"]
            await one.revoke("doc")
            assert await one.search("Arm") == []
            assert len(await two.search("Arm")) == 1
        finally:
            await one.close()
            await two.close()

    asyncio.run(run())


def test_integer_revisions_preserve_type_and_do_not_alias_strings(tmp_path):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "revision-types").initialize()
        try:
            await store.admit("doc", 1, "First quote", ["First quote"])
            assert (await store.search("quote"))[0]["revision"] == 1
            await store.admit("doc", "1", "Second quote", ["Second quote"])
            assert (await store.search("quote"))[0]["revision"] == "1"
            await store.admit("doc", 2, "Third quote", ["Third quote"])
            assert (await store.search("quote"))[0]["revision"] == 2
            for invalid in (-1, True, None, 1.5):
                with pytest.raises(ValueError, match="revision"):
                    await store.admit("doc", invalid, "quote", ["quote"])
        finally:
            await store.close()

    asyncio.run(run())


def test_tampered_source_text_and_missing_core_citation_are_rejected(tmp_path, monkeypatch):
    async def run():
        store = await QuoteStore(tmp_path / "quotes.db", "tamper").initialize()
        try:
            await store.admit("doc", "r1", "Exact quote", ["Exact quote"])
            native = store.kernel.retrieve

            async def no_citation(query):
                return replace(await native(query), citations=())

            monkeypatch.setattr(store.kernel, "retrieve", no_citation)
            assert await store.search("quote") == []
            monkeypatch.setattr(store.kernel, "retrieve", native)
            with store._metadata() as db:
                db.execute("UPDATE experiment_quote_sources SET source_text = 'Different text'")
            assert await store.search("quote") == []
        finally:
            await store.close()

    asyncio.run(run())


def test_api_validation_and_closed_store(tmp_path):
    async def run():
        store = QuoteStore(tmp_path / "quotes.db", "validation")
        with pytest.raises(RuntimeError, match="initialize"):
            await store.search("quote")
        await store.initialize()
        try:
            with pytest.raises(ValueError, match="source_id"):
                await store.admit("", "v1", "quote", ["quote"])
            with pytest.raises(ValueError, match="revision"):
                await store.admit("doc", "", "quote", ["quote"])
            with pytest.raises(ValueError, match="list"):
                await store.admit("doc", "v1", "quote", "quote")
            for invalid in (0, -1, 101, True):
                with pytest.raises(ValueError, match="limit"):
                    await store.search("quote", invalid)
        finally:
            await store.close()
        with pytest.raises(RuntimeError, match="initialize"):
            await store.revoke("doc")

    asyncio.run(run())
    with pytest.raises(ValueError, match="file-backed"):
        QuoteStore(":memory:", "validation")
    with pytest.raises(ValueError, match="namespace"):
        QuoteStore(tmp_path / "quotes.db", "")
