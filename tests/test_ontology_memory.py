import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from agent_memory import (
    Claim,
    ClaimStatus,
    CallableOntologyEvidenceVerifier,
    ConsolidationRequest,
    MemoryQuery,
    MemoryScope,
    OntologyClass,
    OntologyConflictResolution,
    OntologyProjectionConsolidatorPlugin,
    OntologyProperty,
    OntologyRetrieverPlugin,
    OntologySchema,
    OntologyValidationError,
    PluginContext,
    PluginError,
    PluginResourceLimits,
    Provenance,
    SQLiteOntologyStore,
    project_claim_to_ontology,
)


NOW = datetime(2026, 9, 22, tzinfo=UTC)


def _schema(version: str = "1.0.0") -> OntologySchema:
    return OntologySchema(
        ontology_id="agent.knowledge",
        version=version,
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
            OntologyProperty(
                "display_name",
                "Display name",
                domain_class="person",
            ),
        ),
        created_at=NOW,
    )


def _claim(scope: MemoryScope, event_id: str = "event-1") -> Claim:
    return Claim(
        id="claim-1",
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
        importance=0.8,
        status=ClaimStatus.ACTIVE,
        provenance=Provenance(source_event_ids=(event_id,)),
        valid_from=NOW,
        created_at=NOW,
    )


def test_versioned_ontology_rejects_invalid_schema() -> None:
    with pytest.raises(OntologyValidationError, match="unknown class"):
        OntologySchema(
            ontology_id="broken",
            version="1.0.0",
            classes=(OntologyClass("person", "Person"),),
            properties=(OntologyProperty("owns", "Owns", "person", "asset"),),
            created_at=NOW,
        )


def test_projection_retrieval_scope_and_deletion_contract(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope(tenant_id="ontology", user_id="user-1")
        other = MemoryScope(tenant_id="ontology", user_id="user-2")
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        schema = _schema()
        async def evidence_exists(candidate_scope, event_ids):
            return candidate_scope == scope and set(event_ids) <= {"event-1"}

        consolidator = OntologyProjectionConsolidatorPlugin(
            store,
            schema,
            CallableOntologyEvidenceVerifier(evidence_exists),
        )
        retriever = OntologyRetrieverPlugin(store, schema)
        context = PluginContext(scope, PluginResourceLimits(max_candidates=8, max_batch_size=64))
        await consolidator.initialize(context)
        await retriever.initialize(context)

        await consolidator.consolidate(
            ConsolidationRequest(scope=scope, claims=(_claim(scope),)),
            context,
        )
        candidates = await retriever.retrieve(
            MemoryQuery(scope=scope, text="concise answer"),
            context,
        )
        assert len(candidates) == 1
        assert candidates[0].source_event_ids == ("event-1",)
        assert candidates[0].metadata["ontology_version"] == "1.0.0"
        assert candidates[0].item.metadata["predicate_id"] == "prefers"

        with pytest.raises(PluginError, match="trusted scope"):
            await retriever.retrieve(MemoryQuery(scope=other, text="concise"), context)

        await consolidator.consolidate(
            ConsolidationRequest(
                scope=scope,
                claims=(_claim(scope),),
                deleted_event_ids=("event-1",),
            ),
            context,
        )
        assert await retriever.retrieve(
            MemoryQuery(scope=scope, text="concise answer"), context
        ) == ()

    asyncio.run(scenario())


def test_ontology_versions_are_immutable_and_isolated(tmp_path) -> None:
    async def scenario() -> None:
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        await store.initialize()
        await store.register_schema(_schema("1.0.0"))
        await store.register_schema(_schema("2.0.0"))
        semantically_identical = replace(
            _schema("1.0.0"), created_at=NOW + timedelta(days=1)
        )
        await store.register_schema(semantically_identical)
        changed = OntologySchema(
            ontology_id="agent.knowledge",
            version="1.0.0",
            classes=(OntologyClass("person", "Human"),),
            properties=(OntologyProperty("display_name", "Name", "person"),),
            created_at=NOW,
        )
        with pytest.raises(OntologyValidationError, match="cannot be redefined"):
            await store.register_schema(changed)

    asyncio.run(scenario())


def test_store_revalidates_projection_and_verifier_rejects_missing_evidence(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope(tenant_id="ontology-validation", user_id="user-1")
        other = MemoryScope(tenant_id="ontology-validation", user_id="user-2")
        schema = _schema()
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        await store.initialize()
        await store.register_schema(schema)
        projection = project_claim_to_ontology(_claim(scope), schema)
        assert projection is not None
        invalid_subject = replace(projection.entities[0], scope=other)
        with pytest.raises(OntologyValidationError, match="scope"):
            await store.upsert_projection(
                replace(projection, entities=(invalid_subject, *projection.entities[1:]))
            )

        async def evidence_missing(candidate_scope, event_ids):
            return False

        plugin = OntologyProjectionConsolidatorPlugin(
            store,
            schema,
            CallableOntologyEvidenceVerifier(evidence_missing),
        )
        context = PluginContext(scope, PluginResourceLimits(max_batch_size=64))
        await plugin.initialize(context)
        with pytest.raises(OntologyValidationError, match="does not exist"):
            await plugin.consolidate(
                ConsolidationRequest(scope=scope, claims=(_claim(scope),)), context
            )

    asyncio.run(scenario())


def test_schema_rejects_indirect_class_cycles() -> None:
    with pytest.raises(OntologyValidationError, match="cycle"):
        OntologySchema(
            ontology_id="cycle",
            version="1.0.0",
            classes=(
                OntologyClass("parent", "Parent", parent_ids=("child",)),
                OntologyClass("child", "Child", parent_ids=("parent",)),
            ),
            properties=(OntologyProperty("name", "Name", "parent"),),
            created_at=NOW,
        )


def test_parent_scope_visibility_and_sibling_isolation(tmp_path) -> None:
    async def scenario() -> None:
        user_scope = MemoryScope(tenant_id="scope", user_id="user-1")
        session_scope = MemoryScope(
            tenant_id="scope", user_id="user-1", session_id="session-1"
        )
        sibling_scope = MemoryScope(
            tenant_id="scope", user_id="user-2", session_id="session-2"
        )
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        schema = _schema()

        async def evidence_exists(scope, event_ids):
            return True

        consolidator = OntologyProjectionConsolidatorPlugin(
            store, schema, CallableOntologyEvidenceVerifier(evidence_exists)
        )
        user_context = PluginContext(user_scope, PluginResourceLimits(max_batch_size=64))
        await consolidator.initialize(user_context)
        await consolidator.consolidate(
            ConsolidationRequest(scope=user_scope, claims=(_claim(user_scope),)),
            user_context,
        )
        session_retriever = OntologyRetrieverPlugin(store, schema)
        session_context = PluginContext(
            session_scope, PluginResourceLimits(max_candidates=8, max_batch_size=64)
        )
        await session_retriever.initialize(session_context)
        assert await session_retriever.retrieve(
            MemoryQuery(scope=session_scope, text="concise"), session_context
        )
        sibling_retriever = OntologyRetrieverPlugin(store, schema)
        sibling_context = PluginContext(
            sibling_scope, PluginResourceLimits(max_candidates=8, max_batch_size=64)
        )
        await sibling_retriever.initialize(sibling_context)
        assert await sibling_retriever.retrieve(
            MemoryQuery(scope=sibling_scope, text="concise"), sibling_context
        ) == ()

    asyncio.run(scenario())


def test_functional_property_conflict_requires_host_resolution(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope(tenant_id="conflict", user_id="user-1")
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        schema = _schema()

        async def evidence_exists(candidate_scope, event_ids):
            return True

        plugin = OntologyProjectionConsolidatorPlugin(
            store, schema, CallableOntologyEvidenceVerifier(evidence_exists)
        )
        context = PluginContext(scope, PluginResourceLimits(max_batch_size=64))
        await plugin.initialize(context)
        first = _claim(scope, "event-1")
        second = replace(
            _claim(scope, "event-2"),
            id="claim-2",
            text="The user prefers detailed answers.",
            value={
                "$ontology": {
                    "subject": {
                        "id": "person:user-1",
                        "class": "person",
                        "label": "User",
                    },
                    "predicate": "prefers",
                    "object": {
                        "id": "answer_style:detailed",
                        "class": "answer_style",
                        "label": "Detailed answers",
                    },
                }
            },
        )
        await plugin.consolidate(
            ConsolidationRequest(scope=scope, claims=(first, second)), context
        )
        retriever = OntologyRetrieverPlugin(store, schema)
        await retriever.initialize(context)
        assert await retriever.retrieve(
            MemoryQuery(scope=scope, text="answers"), context
        ) == ()
        conflicts = await store.list_conflicts(
            scope,
            ontology_id=schema.ontology_id,
            ontology_version=schema.version,
        )
        assert len(conflicts) == 2
        winner = await store.resolve_conflict(
            OntologyConflictResolution(
                scope=scope,
                ontology_id=schema.ontology_id,
                ontology_version=schema.version,
                subject_entity_id="person:user-1",
                predicate_id="prefers",
                winner_assertion_id=conflicts[0].assertion_id,
                conflict_assertion_ids=tuple(item.assertion_id for item in conflicts),
                reason="Approved by the host preference review.",
                approved_by="trusted-host",
            )
        )
        assert winner.status.value == "active"
        candidates = await retriever.retrieve(
            MemoryQuery(scope=scope, text="answers"), context
        )
        assert len(candidates) == 1
        assert candidates[0].item.id == winner.assertion_id

    asyncio.run(scenario())
