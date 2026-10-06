"""Host-qualified field evidence and condition-aware reads; no external model needed."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory.conditions import Condition, ContextAttribute, ProjectionPolicy, QueryContext
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.consolidation.qualification import ContextualMemory, target_fingerprint
from agent_memory.domain import (
    AtomDraft,
    MemoryEvent,
    MemoryScope,
    PredicateSpec,
    SourceAuthority,
    utc_now,
)
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
from agent_memory.fact_qualification import SourceSpan
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    with TemporaryDirectory(prefix="agent-memory-context-demo-") as temporary:
        repository = SQLiteMemoryRepository(Path(temporary) / "memory.db")
        await repository.initialize()
        engine = AdmissionEngine(repository)
        scope = MemoryScope("demo", user_id="alice", session_id="session")
        authority = SourceAuthority("user:alice", subjects=("alice",), predicates=("language",))
        admission = AdmissionPolicy([PredicateSpec("language")])
        policy = ProjectionPolicy("language-policy/1", "response")
        service = ContextualMemory(engine, scope, principal="authenticated:alice")
        start = datetime(2026, 10, 1, tzinfo=UTC)
        sentence = "在项目 A 中请使用英语。"
        event = MemoryEvent(scope, "message", sentence, actor="alice", occurred_at=start)
        draft = AtomDraft(
            "alice",
            "language",
            "English",
            sentence,
            sentence,
            valid_from=start,
            conditions=("在项目 A 中",),
        )
        received = await engine.admit(event, (draft,), authority=authority, policy=admission)
        # The authenticated host approves semantics, source authority, time,
        # and the complete field mapping. Quote equality alone is not proof.
        fields = ("subject_id", "predicate", "value", "valid_from", "conditions")
        evidence = EvidenceLink(
            "source-assertion",
            fields,
            target_fingerprint(draft),
            SourceSpan(event.id, 0, len(sentence), sentence),
            authority,
            SupportRange(start),
            source_family="alice-original-message",
            domain_revision="project-A/1",
        )
        await service.qualify(
            received.candidate_ids[0],
            expected_version=1,
            admission_policy=admission,
            projection_policy=policy,
            applicability_id="project-A",
            conditions=(Condition("eq", "project", "A"),),
            links=(evidence,),
            field_support=tuple(FieldSupport(field, ((evidence.id,),)) for field in fields),
        )
        for project in ("A", "B", None):
            attributes = (
                () if project is None else (ContextAttribute("project", project, "host-routing"),)
            )
            query = QueryContext(
                "authenticated:alice",
                scope,
                "alice",
                "response",
                start,
                utc_now(),
                attributes,
                snapshot_token="demo-read",
            )
            result = await service.query(query, predicate="language", policy=policy)
            print({"project": project, "status": result["status"], "value": result["value"]})
            assert result["status"] == ("resolved" if project == "A" else "unknown")
        assert not (await engine.state(scope, valid_at=start, known_at=utc_now()))[0]


if __name__ == "__main__":
    asyncio.run(main())
