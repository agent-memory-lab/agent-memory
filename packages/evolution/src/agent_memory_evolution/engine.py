from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from uuid import NAMESPACE_URL, uuid5

from agent_memory.domain import ArtifactStatus, Episode, MemoryScope, utc_now

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
        source_ids = tuple(sorted(dict.fromkeys(source_episode_ids)))
        candidate_id = str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        procedure.scope.partition_key(),
                        procedure.id,
                        *source_ids,
                        baseline_version or "",
                        generator,
                        generator_version,
                    )
                ),
            )
        )
        candidate = EvolutionCandidate(
            id=candidate_id,
            scope=procedure.scope,
            procedure=procedure,
            source_episode_ids=source_ids,
            baseline_version=baseline_version,
            generator=generator,
            generator_version=generator_version,
        )
        await self._registry.register(candidate)
        return await self._registry.get(candidate.id)

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
        self._validate_evaluation_binding(candidate, report)
        authorization = self._policy.authorize_evaluation(report, actor)
        if not authorization.passed:
            raise EvolutionGateError("; ".join(authorization.reasons))
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
        idempotency_key: str | None = None,
    ) -> PromotionRecord:
        if idempotency_key:
            previous = await self._registry.promotion_by_idempotency_key(
                candidate_id, idempotency_key
            )
            if previous is not None:
                return previous
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
        self._validate_evaluation_binding(candidate, report)
        if target == EvolutionState.ACTIVATING:
            approval_decision = self._policy.authorize_active(approval)
            if not approval_decision.passed:
                raise EvolutionGateError("; ".join(approval_decision.reasons))
            assert approval is not None
            if approval.candidate_id != candidate.id:
                raise EvolutionGateError("approval candidate does not match")
            if approval.candidate_version != candidate.procedure.version:
                raise EvolutionGateError("approval candidate version does not match")
            if approval.scope_partition_key != candidate.scope.partition_key():
                raise EvolutionGateError("approval scope does not match candidate")
            if approval.expires_at is None or approval.expires_at <= utc_now():
                raise EvolutionGateError("approval is expired or has no expiry")

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
        record = await self._registry.transition(
            candidate.id,
            EvolutionState.ACTIVATING,
            EvolutionState.ACTIVE,
            actor=actor,
            reason="deployment activated",
            evaluation_id=report.id,
            approval=approval,
            idempotency_key=idempotency_key,
        )
        await self._registry.set_active_pointer(await self._registry.get(candidate.id))
        return record

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
        record = await self._registry.transition(
            candidate.id,
            EvolutionState.ROLLING_BACK,
            EvolutionState.ROLLED_BACK,
            actor=actor,
            reason=reason,
        )
        await self._registry.clear_active_pointer(candidate.scope, candidate.id)
        return record

    async def active_candidate(self, scope: MemoryScope) -> EvolutionCandidate | None:
        candidate_id = await self._registry.active_candidate_id(scope)
        return await self._registry.get(candidate_id) if candidate_id else None

    async def recover(self) -> tuple[PromotionRecord, ...]:
        recovered: list[PromotionRecord] = []
        candidates = await self._registry.candidates_in_states(
            (EvolutionState.ACTIVATING, EvolutionState.ROLLING_BACK)
        )
        for candidate in candidates:
            await self._deployment.deactivate(candidate)
            if candidate.state == EvolutionState.ACTIVATING:
                target = EvolutionState.CANARY
                reason = "restart recovery compensated incomplete activation"
            else:
                target = EvolutionState.ROLLED_BACK
                reason = "restart recovery completed rollback"
            recovered.append(
                await self._registry.transition(
                    candidate.id,
                    candidate.state,
                    target,
                    actor="system",
                    reason=reason,
                )
            )
            await self._registry.clear_active_pointer(candidate.scope, candidate.id)
        return tuple(recovered)

    async def invalidate_sources(
        self,
        source_event_ids: Sequence[str],
        *,
        actor: str,
        reason: str,
    ) -> tuple[PromotionRecord, ...]:
        if not source_event_ids:
            return ()
        if not reason.strip():
            raise ValueError("invalidation reason is required")
        return tuple(
            await self._registry.invalidate_sources(
                source_event_ids,
                actor=actor,
                reason=reason,
            )
        )

    @staticmethod
    def _validate_evaluation_binding(
        candidate: EvolutionCandidate, report: EvaluationReport
    ) -> None:
        if report.candidate_version != candidate.procedure.version:
            raise EvolutionGateError("evaluation candidate version does not match")
        if report.scope_partition_key != candidate.scope.partition_key():
            raise EvolutionGateError("evaluation scope does not match candidate")
