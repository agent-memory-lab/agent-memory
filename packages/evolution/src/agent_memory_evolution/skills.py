"""Reviewable Skill exports and guarded adaptation to an existing host deployment.

No shell, model or workflow is executed here. Install/remove callbacks own the
external artifact; source-current checks and registry gates precede each handoff.
"""

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from agent_memory.domain import canonical_json
from agent_memory.serialization import to_jsonable

from .domain import EvolutionState
from .engine import EvolutionGateError
from .evidence import digest, validate_evidence


@dataclass(frozen=True, slots=True)
class SkillArtifact:
    candidate_id: str
    scope_partition_key: str
    procedure_version: int
    evidence_sha256: str
    markdown: str
    manifest_json: str

    @property
    def sha256(self):
        return digest((self.markdown, self.manifest_json))

    def write(self, directory):
        """Create a new export directory; existing artifacts are never overwritten."""
        directory = Path(directory)
        directory.mkdir(parents=False, exist_ok=False)
        (directory / "SKILL.md").write_text(self.markdown, encoding="utf-8")
        (directory / "manifest.json").write_text(self.manifest_json + "\n", encoding="utf-8")
        return directory


def export_skill(candidate) -> SkillArtifact:
    if (
        candidate.state
        in {
            EvolutionState.INVALIDATED,
            EvolutionState.REJECTED,
            EvolutionState.ROLLING_BACK,
            EvolutionState.ROLLED_BACK,
        }
        or candidate.withdrawal_pending
    ):
        raise EvolutionGateError("withdrawn candidates cannot be exported as Skills")
    validate_evidence(candidate.procedure, candidate.evidence)
    procedure = candidate.procedure
    evidence_sha256 = digest(candidate.evidence)
    parts = [
        "---",
        "name: memory-procedure-" + sha256(candidate.id.encode()).hexdigest()[:16],
        "description: " + json.dumps(procedure.trigger, ensure_ascii=False),
        "---",
        "",
        "# " + procedure.name,
        "",
        "## Applicability",
        "",
        *("- " + value for value in procedure.applicability_conditions),
        "",
        "## Steps",
        "",
        *(f"{index}. {step}" for index, step in enumerate(procedure.steps, 1)),
        "",
        "## Success conditions",
        "",
        *("- " + value for value in procedure.success_conditions),
        "",
        "## Failure patterns and counterexamples",
        "",
        *("- " + value for value in procedure.failure_patterns),
        "",
        "## Evidence",
        "",
        f"Candidate: {candidate.id}",
        f"Procedure version: {procedure.version}",
        f"Scope: {candidate.scope.partition_key()}",
        f"Evidence SHA-256: {evidence_sha256}",
        "",
        "Use only through the host's current evidence and active-candidate checks.",
        "This export does not grant permission to execute a candidate.",
        "",
    ]
    markdown = "\n".join(parts)
    manifest = {
        "schema": "procedure-skill/1",
        "candidate_id": candidate.id,
        "scope": to_jsonable(candidate.scope),
        "procedure": to_jsonable(procedure),
        "evidence": to_jsonable(candidate.evidence),
        "evidence_sha256": evidence_sha256,
        "skill_sha256": sha256(markdown.encode()).hexdigest(),
        "counterexample_episode_ids": list(procedure.counterexample_episode_ids),
    }
    return SkillArtifact(
        candidate.id,
        candidate.scope.partition_key(),
        procedure.version,
        evidence_sha256,
        markdown,
        canonical_json(manifest),
    )


class SkillProcedureDeployment:
    """Adapter for an existing host, with no independent registry or queue.

    install(artifact) and remove(scope, candidate_id) must be idempotent. The host's
    evidence_current(scope, episode_revisions) verifies every episode and source
    revision, including counterexamples. Consumers use load_active for every use;
    an exported file by itself is never an authorization token.
    """

    def __init__(self, registry, *, install, remove, evidence_current):
        if not all(callable(value) for value in (install, remove, evidence_current)):
            raise ValueError("explicit host install, remove and evidence validation required")
        self.registry = registry
        self.install, self.remove, self.evidence_current = install, remove, evidence_current

    async def _checked(self, candidate, states):
        validate_evidence(candidate.procedure, candidate.evidence)
        if await self.evidence_current(candidate.scope, candidate.evidence) is not True:
            raise EvolutionGateError("Skill evidence is no longer current")
        current = await self.registry.get(candidate.id)
        if (
            current.state not in states
            or current.withdrawal_pending
            or current.scope != candidate.scope
            or current.procedure != candidate.procedure
            or current.evidence != candidate.evidence
        ):
            raise EvolutionGateError("Skill candidate is unavailable or changed")
        return current

    async def activate(self, candidate):
        current = await self._checked(candidate, {EvolutionState.ACTIVATING})
        artifact = export_skill(current)
        await self.install(artifact)
        await self._checked(candidate, {EvolutionState.ACTIVATING})

    async def deactivate(self, candidate):
        await self.remove(candidate.scope, candidate.id)

    async def load_active(self, scope):
        candidate_id = await self.registry.active_candidate_id(scope)
        if candidate_id is None:
            raise EvolutionGateError("no active Skill in this scope")
        candidate = await self.registry.get(candidate_id)
        if candidate.scope != scope:
            raise EvolutionGateError("Skill scope mismatch")
        current = await self._checked(candidate, {EvolutionState.ACTIVE})
        # Pointer replacement while evidence validation awaited also revokes use.
        if await self.registry.active_candidate_id(scope) != candidate_id:
            raise EvolutionGateError("active Skill pointer changed")
        current = await self.registry.get(candidate_id)
        if current.state != EvolutionState.ACTIVE or current.withdrawal_pending:
            raise EvolutionGateError("active Skill was withdrawn")
        return export_skill(current)
