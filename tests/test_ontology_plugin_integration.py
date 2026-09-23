import asyncio
from datetime import UTC, datetime

from agent_memory import (
    AgentMemory,
    CallableOntologyEvidenceVerifier,
    Claim,
    ClaimStatus,
    ConsolidationRequest,
    MemoryScope,
    OntologyClass,
    OntologyProperty,
    OntologySchema,
    PluginContext,
    PluginLoader,
    PluginResourceLimits,
    Provenance,
    SQLiteOntologyStore,
    load_ontology_memory,
)


NOW = datetime(2026, 9, 23, tzinfo=UTC)


def _schema() -> OntologySchema:
    return OntologySchema(
        ontology_id="agent.preferences",
        version="1.0.0",
        classes=(
            OntologyClass("person", "Person"),
            OntologyClass("answer_style", "Answer style"),
        ),
        properties=(
            OntologyProperty(
                "prefers",
                "Prefers",
                domain_class="person",
                range_class="answer_style",
                functional=True,
            ),
        ),
        created_at=NOW,
    )


def _claim(scope: MemoryScope) -> Claim:
    return Claim(
        id="claim-preference",
        scope=scope,
        key="user.answer_style",
        value={
            "$ontology": {
                "subject": {"id": "person:user-1", "class": "person", "label": "User"},
                "predicate": "prefers",
                "object": {
                    "id": "answer_style:concise",
                    "class": "answer_style",
                    "label": "Concise answers",
                },
            }
        },
        text="The user prefers concise answers.",
        confidence=0.95,
        importance=0.9,
        status=ClaimStatus.ACTIVE,
        provenance=Provenance(source_event_ids=("event-1",)),
        valid_from=NOW,
        created_at=NOW,
    )


def test_loaded_ontology_extension_drives_default_recall_and_deletion(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant-a", user_id="user-1", session_id="session-a")
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        loader = PluginLoader(core_version="0.1.0")

        async def evidence_exists(candidate_scope, event_ids):
            return candidate_scope == scope and set(event_ids) <= {"event-1"}

        extension = await load_ontology_memory(
            loader,
            PluginContext(
                scope=scope,
                resource_limits=PluginResourceLimits(
                    timeout_ms=1_000,
                    max_candidates=8,
                    max_batch_size=64,
                ),
                request_id="ontology-integration",
            ),
            store,
            _schema(),
            CallableOntologyEvidenceVerifier(evidence_exists),
        )
        try:
            await extension.consolidate(
                ConsolidationRequest(scope=scope, claims=(_claim(scope),))
            )
            async with AgentMemory.local(
                tmp_path / "memory.db",
                scope=scope,
                recall_pipeline=extension.recall_pipeline,
            ) as memory:
                bundle = await memory.recall("concise answers")
                assert [item.text for item in bundle.relevant_memories] == [
                    "The user prefers concise answers."
                ]
                assert bundle.citations[0].source_event_ids == ("event-1",)
                assert bundle.retrieval_metadata["policy_version"] == "ontology-memory-v1"

                await extension.consolidate(
                    ConsolidationRequest(scope=scope, deleted_event_ids=("event-1",))
                )
                deleted_bundle = await memory.recall("concise answers")
                assert deleted_bundle.relevant_memories == ()
        finally:
            await loader.close()

        async with AgentMemory.local(
            tmp_path / "memory.db",
            scope=scope,
            recall_pipeline=extension.recall_pipeline,
        ) as memory:
            degraded = await memory.recall("concise answers")
            assert degraded.relevant_memories == ()
            assert degraded.retrieval_metadata["degraded"] is True
            assert degraded.retrieval_metadata["retrievers"][0]["status"] == "failed"

    asyncio.run(scenario())


def test_ontology_extension_rejects_cross_scope_consolidation(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant-a", user_id="user-1")
        loader = PluginLoader(core_version="0.1.0")

        async def evidence_exists(candidate_scope, event_ids):
            return candidate_scope == scope and bool(event_ids)

        extension = await load_ontology_memory(
            loader,
            PluginContext(scope, PluginResourceLimits(max_batch_size=64)),
            SQLiteOntologyStore(tmp_path / "ontology.db"),
            _schema(),
            CallableOntologyEvidenceVerifier(evidence_exists),
        )
        try:
            foreign = MemoryScope("tenant-b", user_id="user-1")
            try:
                await extension.consolidate(ConsolidationRequest(scope=foreign))
            except Exception as error:
                assert "outside the loaded plugin scope" in str(error)
            else:
                raise AssertionError("cross-scope consolidation must fail")
        finally:
            await loader.close()

    asyncio.run(scenario())
