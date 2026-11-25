"""A single source envelope must agree across identity, packing and delivery."""

from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps

import pytest
import test_atom_admission as admission_tests

from agent_memory.domain import MemoryItem, MemoryKind
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.delivery_guard import _text_matches
from agent_memory.retrieval.evidence import SourceEvidence, SourceSpan
from agent_memory.retrieval.packing import _indexed_event_span

SOURCE = "Invoice total is 300. Final due date is Friday."
store = admission_tests.store


def event():
    revision = sha256(SOURCE.encode()).hexdigest()
    locator = sha256(dumps(["events", "one", revision, 0, len(SOURCE)]).encode()).hexdigest()
    return MemoryItem(
        "one",
        MemoryKind.EVENT,
        SOURCE,
        1.0,
        datetime.now(UTC),
        {
            "source_revision": revision,
            "source_chars": len(SOURCE),
            "source_span": SourceSpan(0, len(SOURCE)).to_metadata(),
            "lexical_chunk_id": locator,
        },
    )


def test_complete_and_narrowed_evidence_share_locator_but_verify_exact_bytes():
    full = event()
    metadata = dict(full.metadata)
    metadata["retrieved_lexical_chunk_id"] = metadata.pop("lexical_chunk_id")
    metadata["retrieved_source_span"] = metadata["source_span"]
    metadata["source_span"] = SourceSpan(0, 21).to_metadata()
    narrow = replace(full, text=SOURCE[:21], metadata=metadata)
    assert candidate_identity(full) == candidate_identity(narrow) != full.id
    assert _indexed_event_span(full) is not None
    assert _indexed_event_span(narrow) is None  # A narrowed variant is not an original chunk.
    assert _text_matches(full, SOURCE) and _text_matches(narrow, SOURCE)
    assert not _text_matches(narrow, SOURCE.replace("300", "900"))
    assert SourceEvidence.from_item(narrow).span.to_metadata() == metadata["source_span"]


@pytest.mark.parametrize(
    "override",
    [
        {"source_chars": True},
        {"source_chars": 1},
        {"source_revision": "bad"},
        {"source_span": {"unit": "bytes", "start": 0, "end": len(SOURCE)}},
        {"source_span": {"unit": "characters", "start": False, "end": len(SOURCE)}},
        {"source_span": {"unit": "characters", "start": 1, "end": len(SOURCE)}},
    ],
)
def test_malformed_envelope_never_acquires_a_span_identity_or_delivery(override):
    original = event()
    item = replace(original, metadata={**original.metadata, **override})
    assert candidate_identity(item) == item.id
    assert _indexed_event_span(item) is None
    assert not _text_matches(item, SOURCE)


def test_legacy_full_text_still_requires_exact_source_bytes():
    item = replace(event(), metadata={})
    assert _text_matches(item, SOURCE)
    assert not _text_matches(item, SOURCE + " altered")


def test_empty_stored_claim_retains_its_lexical_header_without_a_quoted_span():
    from agent_memory.retrieval.sqlite_lexical_index import lineage

    metadata = lineage(
        {
            "source_table": "claim_versions",
            "chunk_id": "empty-claim",
            "source_revision": sha256(b"").hexdigest(),
            "source_chars": 0,
            "span_start": 0,
            "span_end": 0,
        }
    )
    assert metadata["source_span"] == {"unit": "characters", "start": 0, "end": 0}
    assert SourceEvidence.from_item(replace(event(), text="", metadata=metadata)) is None


def test_backend_delivery_uses_canonical_source_coordinates(store):
    import asyncio

    from test_delivery_evidence_binding import bundle

    from agent_memory.domain import MemoryEvent, MemoryQuery

    async def run():
        async with store() as (_, kernel, scope, _clock):
            source = MemoryEvent(scope, "message", SOURCE)
            await kernel.ingest_event(source)
            query = MemoryQuery(scope, "invoice", trace_enabled=False)
            await kernel.retrieve(query)
            base = {"event_type": "message", "source_event_ids": (source.id,)}
            full = replace(event(), id=source.id, metadata=base)
            valid_span = {
                **base,
                "source_revision": sha256(SOURCE.encode()).hexdigest(),
                "source_chars": len(SOURCE),
                "source_span": SourceSpan(0, 7).to_metadata(),
            }
            part = replace(full, text=SOURCE[:7], metadata=valid_span)
            inconsistent = (
                replace(full, metadata={**base, "source_revision": "0" * 64}),
                replace(part, metadata={**valid_span, "source_chars": str(len(SOURCE))}),
                replace(part, metadata={**valid_span, "source_chars": 2**70}),
            )
            result = await kernel.guard_pipeline_delivery(
                query, bundle(kernel, (full, part, *inconsistent))
            )
            assert [(item.text, dict(item.metadata)) for item in result.relevant_memories] == [
                (full.text, base),
                (part.text, valid_span),
            ]

    asyncio.run(run())
