"""Run with core + Python SDK installed. Uses an explicit synthetic host review.

This demonstrates the lifecycle, not an automatic extractor or a real-data quality
benchmark. A production host must verify field evidence and source authority.
"""

import asyncio
import tempfile
from datetime import timedelta
from pathlib import Path

from agent_memory_sdk import EmbeddedMemoryClient

from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.consolidation.project_admission import ProjectAdmission, ProjectMembership
from agent_memory.consolidation.qualification import target_fingerprint
from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.project_questions import ProjectDomainContract
from agent_memory.derived.question_model import QuestionContext
from agent_memory.derived.question_service import QuestionService
from agent_memory.domain import AtomDraft, MemoryEvent, MemoryScope, SourceAuthority, utc_now
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
from agent_memory.fact_qualification import FieldEvidence, SourceSpan
from agent_memory.kernel import MemoryKernel
from agent_memory.mcp import MCPRequestContext
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    with tempfile.TemporaryDirectory() as directory:
        repository = SQLiteMemoryRepository(Path(directory) / "memory.db")
        kernel = MemoryKernel(repository, MetadataClaimExtractor(), TrustedMemoryPolicy(),
                              ReciprocalRankFusionReranker())
        await kernel.initialize()
        try:
            scope = MemoryScope("project-demo", user_id="alice", session_id="demo")
            actor = "host:alice"
            contract = ProjectDomainContract(
                "project-demo", "1", "host-field-review/1", "accountable", ("active", "paused"),
            )
            authority = SourceAuthority(
                "project-registry", "tool_observation", ("project-a",),
                tuple(p.predicate for p in contract.predicate_specs),
            )
            admission = ProjectAdmission(
                AdmissionEngine(repository), scope, principal=actor, contract=contract,
                authorities=(authority,), reviewer_version="human-reviewed-demo/1",
                memberships=(ProjectMembership("a", "registry/1", "project-a", "project-a"),),
            )
            questions = QuestionService(
                admission, QuestionContext("host", "project-context/1", {},
                                           utc_now() + timedelta(hours=1)),
            )
            valid_from = utc_now()
            text = f"Project A's accountable owner is Alice, effective {valid_from.isoformat()}."
            source = MemoryEvent(scope, "project_registry", text)
            span = SourceSpan(source.id, 0, len(text), text)
            fields = ("subject_id", "predicate", "value", "valid_from")
            draft = AtomDraft(
                "project-a", "project.owner", "Alice", text, text, valid_from=valid_from,
                field_evidence=tuple(FieldEvidence(field, ((span,),)) for field in fields),
            )
            receipt = await admission.stage_source(
                source, (draft,), source_authority_id=authority.source_id,
                request_id="source-owner-v1", membership_ids=("a",),
            )
            await questions.grant(ProcessingGrant(source.id, (actor,), (admission.purpose,)))
            row = await repository.admission_record(scope, receipt.candidate_ids[0])
            evidence = EvidenceLink(
                "owner-evidence", fields, target_fingerprint(draft), span, authority,
                SupportRange(valid_from),
            )
            await admission.qualify(
                receipt.candidate_ids[0], expected_version=row["version"], review_id="review-owner",
                applicability_id="project-a-explicit", conditions=(), exceptions=(),
                links=(evidence,),
                field_support=tuple(FieldSupport(field, ((evidence.id,),)) for field in fields),
            )
            ids = []
            client = EmbeddedMemoryClient(
                kernel, MCPRequestContext(scope, actor=actor), questions=questions,
            )
            for template in ("owner", "status", "commitments", "risks"):
                question_id = "project-a:" + template
                ids.append(question_id)
                await questions.register(question_id, "project-a", template, readers=(actor,))
                answer = await client.question_answer(question_id, dedupe_key="demo:" + template)
                print(template, answer["answer_status"], "model calls:", answer["model_calls"])
            await questions.pages.register("project-a-overview", ids, readers=(actor,))
            await questions.pages.publish("project-a-overview", actor=actor)
            page = await client.question_page_read("project-a-overview")
            print("Current project page blocks:", len(page["blocks"]))
            # A processing grant revocation always blocks prior generation.
            await questions.grant(
                ProcessingGrant(source.id, (actor,), (admission.purpose,), revoked=True),
                expected_version=1,
            )
            try:
                await client.question_page_read("project-a-overview")
            except Exception as error:
                print("After source permission changes:",
                      getattr(error, "code", type(error).__name__))
            else:
                raise DerivedError("example_expected_page_invalidation")
        finally:
            await kernel.close()


if __name__ == "__main__":
    asyncio.run(main())
