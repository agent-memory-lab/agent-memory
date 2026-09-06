from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from agent_memory.domain import ArtifactStatus, Episode, MemoryScope

from .domain import (
    EvaluationReport,
    EvaluationStage,
    EvolutionCandidate,
    EvolutionState,
    PromotionApproval,
    PromotionRecord,
)
from .policy import DeterministicPromotionPolicy
from .ports import EvolutionRegistry, ProcedureCandidateGenerator, ProcedureDeployment


class EvolutionGateError(RuntimeError):
    pass


EXPECTED_EVALUATION = {
    EvolutionState.CANDIDATE: EvaluationStage.OFFLINE,
    EvolutionState.EVALUATED: EvaluationStage.OFFLINE,
    EvolutionState.SHADOW: EvaluationStage.SHADOW,
    EvolutionState.CANARY: EvaluationStage.CANARY,
}


class EvolutionEngine:
    def __init__(
        self,
        registry: EvolutionRegistry,
        policy: DeterministicPromotionPolicy,
        deployment: ProcedureDeployment,
    ) -> None:
        self._registry = registry
        self._policy = policy
        self._deployment = deployment

    async def initialize(self) -> None:
        await self._registry.initialize()

    async def register_procedure(
        self,
        procedure,
        source_episode_ids: Sequence[str],
        *,
        baseline_version: str | None = None,
        generator: str = "external",
        generator_version: str = "1",
    ) -> EvolutionCandidate:
        if procedure.status != ArtifactStatus.CANDIDATE:
            raise EvolutionGateError("only candidate Procedures can enter evolution")
        if not procedure.provenance.source_event_ids:
            raise EvolutionGateError("candidate Procedure requires source-event evidence")
        candidate = EvolutionCandidate(
            scope=procedure.scope,
            procedure=procedure,
            source_episode_ids=tuple(source_episode_ids),
            baseline_version=baseline_version,
            generator=generator,
            generator_version=generator_version,
        )
        await self._registry.register(candidate)
        return candidate

    async def generate_candidates(
        self,
        scope: MemoryScope,
        episodes: Sequence[Episode],
        generator: ProcedureCandidateGenerator,
        *,
        baseline_version: str | None = None,
    ) -> tuple[EvolutionCandidate, ...]:
        generated = await generator.generate(scope, episodes)
        candidates = []
        for item in generated:
            candidates.append(
                await self.register_procedure(
                    item.procedure,
                    item.source_episode_ids,
                    baseline_version=baseline_version,
                    generator=type(generator).__name__,
                )
            )
        return tuple(candidates)

    async def submit_evaluation(self, report: EvaluationReport, *, actor: str) -> EvaluationReport:
        candidate = await self._registry.get(report.candidate_id)
        required_stage = EXPECTED_EVALUATION.get(candidate.state)
        if required_stage != report.stage:
            raise EvolutionGateError(
                f"state {candidate.state} requires {required_stage}, not {report.stage}"
            )
        decision = self._policy.evaluate(report)
        decided = replace(report, passed=decision.passed, gate_reasons=decision.reasons)
        await self._registry.append_evaluation(decided)

        if candidate.state == EvolutionState.CANDIDATE:
            target = EvolutionState.EVALUATED if decision.passed else EvolutionState.REJECTED
            await self._registry.transition(
                candidate.id,
                candidate.state,
                target,
                actor=actor,
                reason="evaluation gates passed"
                if decision.passed
                else "; ".join(decision.reasons),
                evaluation_id=decided.id,
            )
        elif not decision.passed and candidate.state in {
            EvolutionState.SHADOW,
            EvolutionState.CANARY,
        }:
            await self._registry.transition(
                candidate.id,
                candidate.state,
                EvolutionState.ROLLED_BACK,
                actor=actor,
                reason="; ".join(decision.reasons),
                evaluation_id=decided.id,
            )
        return decided

    async def promote(
        self,
        candidate_id: str,
        *,
        actor: str,
        approval: PromotionApproval | None = None,
    ) -> PromotionRecord:
        candidate = await self._registry.get(candidate_id)
        targets = {
            EvolutionState.EVALUATED: (EvaluationStage.OFFLINE, EvolutionState.SHADOW),
            EvolutionState.SHADOW: (EvaluationStage.SHADOW, EvolutionState.CANARY),
            EvolutionState.CANARY: (EvaluationStage.CANARY, EvolutionState.ACTIVATING),
        }
        if candidate.state not in targets:
            raise EvolutionGateError(f"candidate cannot be promoted from {candidate.state}")
        evaluation_stage, target = targets[candidate.state]
        report = await self._registry.latest_evaluation(candidate.id, evaluation_stage)
        if report is None or report.passed is not True:
            raise EvolutionGateError(f"passing {evaluation_stage} evaluation is required")
        if target == EvolutionState.ACTIVATING:
            approval_decision = self._policy.authorize_active(approval)
            if not approval_decision.passed:
                raise EvolutionGateError("; ".join(approval_decision.reasons))

        record = await self._registry.transition(
            candidate.id,
            candidate.state,
            target,
            actor=actor,
            reason=f"passed {evaluation_stage} gates",
            evaluation_id=report.id,
            approval=approval,
        )
        if target != EvolutionState.ACTIVATING:
            return record

        activating = replace(candidate, state=EvolutionState.ACTIVATING)
        try:
            await self._deployment.activate(activating)
        except BaseException:
            await self._registry.transition(
                candidate.id,
                EvolutionState.ACTIVATING,
                EvolutionState.CANARY,
                actor="system",
                reason="activation failed; restored canary state",
                evaluation_id=report.id,
                approval=approval,
            )
            raise
        return await self._registry.transition(
            candidate.id,
            EvolutionState.ACTIVATING,
            EvolutionState.ACTIVE,
            actor=actor,
            reason="deployment activated",
            evaluation_id=report.id,
            approval=approval,
        )

    async def rollback(self, candidate_id: str, *, actor: str, reason: str) -> PromotionRecord:
        if not reason.strip():
            raise ValueError("rollback reason is required")
        candidate = await self._registry.get(candidate_id)
        if candidate.state in {
            EvolutionState.EVALUATED,
            EvolutionState.SHADOW,
            EvolutionState.CANARY,
        }:
            return await self._registry.transition(
                candidate.id,
                candidate.state,
                EvolutionState.ROLLED_BACK,
                actor=actor,
                reason=reason,
            )
        if candidate.state != EvolutionState.ACTIVE:
            raise EvolutionGateError(f"candidate cannot be rolled back from {candidate.state}")
        await self._registry.transition(
            candidate.id,
            EvolutionState.ACTIVE,
            EvolutionState.ROLLING_BACK,
            actor=actor,
            reason=reason,
        )
        try:
            await self._deployment.deactivate(candidate)
        except BaseException:
            await self._registry.transition(
                candidate.id,
                EvolutionState.ROLLING_BACK,
                EvolutionState.ACTIVE,
                actor="system",
                reason="deactivation failed; restored active state",
            )
            raise
        return await self._registry.transition(
            candidate.id,
            EvolutionState.ROLLING_BACK,
            EvolutionState.ROLLED_BACK,
            actor=actor,
            reason=reason,
        )
