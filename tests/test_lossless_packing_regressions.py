"""Lossless packing regressions against public APIs and real SQLite/PostgreSQL."""

import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from agent_memory import MemoryEvent, MemoryKernel, build_local_kernel
from agent_memory.domain import MemoryItem, MemoryKind, MemoryProposal, MemoryQuery, MemoryScope
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.retrieval.lossless_packing import LosslessBudget, pack_preserving_evidence
from agent_memory.retrieval.packing import token_estimate

SCOPE = MemoryScope("lossless-review", session_id="one")
SOURCE = "Archive invoice details. " * 80 + "Final total is 300, not 300 more."
BUDGET = LosslessBudget(
    lambda state, items: 20 + sum(len(value.text) for value in (*state, *items)),
    "synthetic-characters",
)


@asynccontextmanager
async def kernel_for(tmp_path, backend, enabled=True, counter=BUDGET):
    options = {"lossless_packing_budget": counter} if enabled else {}
    schema = None
    if backend == "sqlite":
        kernel = build_local_kernel(tmp_path / "memory.db", **options)
    else:
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
        if not dsn:
            pytest.skip("requires an isolated PostgreSQL test database")
        if "test" not in urlsplit(dsn).path.casefold():
            pytest.fail("a dedicated test database URI is required")
        pg = pytest.importorskip("agent_memory_postgres")
        psycopg = pytest.importorskip("psycopg")
        from psycopg import sql
        from psycopg.conninfo import make_conninfo

        schema = "lossless_review_" + uuid4().hex
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        repository = pg.PostgresMemoryRepository.from_dsn(
            make_conninfo(dsn, options="-csearch_path=" + schema)
        )
        kernel = MemoryKernel(
            repository,
            MetadataClaimExtractor(),
            TrustedMemoryPolicy(),
            ReciprocalRankFusionReranker(),
            provider_name="postgresql",
            **options,
        )
    try:
        await kernel.initialize()
        yield kernel
    finally:
        await kernel.close()
        if schema is not None:
            with psycopg.connect(dsn, autocommit=True) as connection:
                connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.parametrize("indexed", [False, True], ids=["legacy", "indexed-control"])
def test_complete_source_that_fits_is_not_replaced_by_a_legacy_excerpt(indexed):
    item = MemoryItem(
        "source",
        MemoryKind.EVENT,
        SOURCE,
        1.0,
        datetime.now(UTC),
        {"source_event_ids": ("source",), "event_type": "message"},
    )
    if indexed:
        revision = hashlib.sha256(SOURCE.encode()).hexdigest()
        locator = hashlib.sha256(
            json.dumps(["events", item.id, revision, 0, len(SOURCE)]).encode()
        ).hexdigest()
        item = replace(
            item,
            metadata={
                **item.metadata,
                "source_revision": revision,
                "source_chars": len(SOURCE),
                "source_span": {"start": 0, "end": len(SOURCE), "unit": "characters"},
                "lexical_chunk_id": locator,
            },
        )
    query = MemoryQuery(
        SCOPE,
        "archive invoice",
        token_budget=4096,
        include_current_state=False,
        trace_enabled=False,
    )
    result = pack_preserving_evidence(query, (), (item,), budget=BUDGET)
    actual = {
        "input_chars": len(SOURCE),
        "returned_chars": [len(v.text) for v in result.items],
        "full_payload_cost": BUDGET.measure((), (item,)),
        "budget": query.token_budget,
        "fallback": result.metadata["packing_fallback"],
        "full_preserved": any(v.text == SOURCE for v in result.items),
    }

    assert actual["full_payload_cost"] <= query.token_budget
    assert actual["full_preserved"], actual


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_real_kernel_preserves_complete_source_when_payload_fits(tmp_path, backend):
    async def run():
        async with kernel_for(tmp_path, backend) as kernel:
            await kernel.ingest_event(MemoryEvent(SCOPE, "message", SOURCE, id="source"))
            query = MemoryQuery(
                SCOPE,
                "archive invoice",
                token_budget=4096,
                include_current_state=False,
                trace_enabled=False,
            )
            bundle = await kernel.retrieve(query)
            actual = {
                "input_chars": len(SOURCE),
                "returned_chars": [len(v.text) for v in bundle.relevant_memories],
                "budget": query.token_budget,
                "fallback": bundle.retrieval_metadata.get("packing_fallback"),
                "full_preserved": any(v.text == SOURCE for v in bundle.relevant_memories),
                "tail_preserved": any("Final total" in v.text for v in bundle.relevant_memories),
                "delivery_guard": bundle.retrieval_metadata["delivery_guard"],
            }

            assert actual["full_preserved"], actual

    asyncio.run(run())


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("enabled", [False, True], ids=["default-control", "lossless"])
def test_historical_claim_is_delivered_once_across_state_and_candidates(tmp_path, backend, enabled):
    async def run():
        async with kernel_for(tmp_path, backend, enabled) as kernel:
            event = MemoryEvent(SCOPE, "user.message", "Use email for contact.", id="source")
            await kernel.ingest_event(event)
            proposal = await kernel.propose(
                MemoryProposal(
                    SCOPE,
                    "contact",
                    "email",
                    "Use email for contact.",
                    (event.id,),
                    0,
                )
            )
            assert proposal.status == "accepted"
            now = datetime.now(UTC)
            bundle = await kernel.retrieve(
                MemoryQuery(
                    SCOPE,
                    "contact",
                    token_budget=1024,
                    valid_at=now,
                    known_at=now,
                    trace_enabled=False,
                )
            )
            ids = [v.id for v in (*bundle.current_state, *bundle.relevant_memories)]
            actual = {
                "enabled": enabled,
                "state_count": len(bundle.current_state),
                "relevant_count": len(bundle.relevant_memories),
                "ids": ids,
                "unique_ids": len(set(ids)),
                "citation_count": len(bundle.citations),
                "proof_count": len(bundle.retrieval_metadata["citation_spans"]),
                "token_estimate": bundle.token_estimate,
            }

            assert ids and len(ids) == len(set(ids)), actual
            assert len(bundle.citations) == 1, actual
            assert len(bundle.retrieval_metadata["citation_spans"]) == 1, actual

    asyncio.run(run())


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("enabled", [False, True], ids=["default-control", "lossless"])
def test_top_ranked_historical_fact_survives_unrelated_state(tmp_path, backend, enabled):
    async def run():
        # Match v2's synthetic estimate so this comparison changes selection,
        # not the budget accounting or prompt overhead between the two modes.
        counter = LosslessBudget(
            lambda state, items: sum(token_estimate(v.text) for v in (*state, *items)),
            "synthetic-native-estimate",
        )
        wanted = "The launch key is cobalt. " * 6
        unrelated = "Use email for contact and weekly updates. " * 6
        async with kernel_for(tmp_path, backend, enabled, counter) as kernel:
            ids = {}
            for key, text, importance in (
                ("launch", wanted, 0.5),
                ("contact", unrelated, 1.0),
            ):
                event = MemoryEvent(SCOPE, "message", text, id=key)
                await kernel.ingest_event(event)
                proposal = await kernel.propose(
                    MemoryProposal(
                        SCOPE,
                        key,
                        key,
                        text,
                        (event.id,),
                        0,
                        importance=importance,
                    )
                )
                assert proposal.status == "accepted"
                ids[key] = proposal.claim_id
            now = datetime.now(UTC)
            query = MemoryQuery(
                SCOPE, "launch key", token_budget=64, valid_at=now, known_at=now, trace_enabled=True
            )
            bundle = await kernel.retrieve(query)
            values = (*bundle.current_state, *bundle.relevant_memories)
            ranked = bundle.retrieval_metadata["retrieval_stages"]["reranked"]["items"]
            actual = {
                "enabled": enabled,
                "wanted_id": ids["launch"],
                "returned": [v.text for v in values],
                "ranked": ranked,
                "wanted_payload_cost": token_estimate(wanted),
                "unrelated_payload_cost": token_estimate(unrelated),
                "counter": counter.tokenizer,
                "budget": query.token_budget,
                "token_estimate": bundle.token_estimate,
                "wanted_returned": any(v.id == ids["launch"] for v in values),
                "fallback": bundle.retrieval_metadata.get("packing_fallback"),
            }

            assert ranked[0]["id"] == ids["launch"], actual
            assert actual["wanted_payload_cost"] <= query.token_budget
            assert actual["wanted_returned"], actual

    asyncio.run(run())
