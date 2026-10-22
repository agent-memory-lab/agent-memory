from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from uuid import NAMESPACE_URL, uuid5

from agent_memory.domain import ArtifactStatus, Episode, MemoryScope, OutcomeStatus, utc_now

from .domain import (
    EvaluationReport,
    EvaluationStage,
    EvolutionCandidate,
    EvolutionState,
    PromotionApproval,
    PromotionRecord,
)
from .evidence import bind_episode, validate_evidence
from .policy import DeterministicPromotionPolicy
from .ports import EvolutionRegistry, ProcedureCandidateGenerator, ProcedureDeployment


class EvolutionGateError(RuntimeError):
    pass


class EvolutionWithdrawalError(RuntimeError):
    """Serving is denied; durable withdrawal remains pending for recovery."""

    def __init__(self, candidate_ids):
        self.candidate_ids = tuple(candidate_ids)
        super().__init__("deployment withdrawal pending; call recover before serving")


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
        self._activating: set[str] = set()

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
        evidence=(),
    ) -> EvolutionCandidate:
        if procedure.status != ArtifactStatus.CANDIDATE:
            raise EvolutionGateError("only candidate Procedures can enter evolution")
        if (not 1 <= len(procedure.provenance.source_event_ids) <= 4096
                or any(not isinstance(item, str) or not 1 <= len(item) <= 256 or not item.strip()
                       for item in procedure.provenance.source_event_ids)):
            raise EvolutionGateError("candidate Procedure requires source-event evidence")
        if (not 1 <= len(source_episode_ids) <= 256
                or any(not isinstance(item, str) or not 1 <= len(item) <= 256 or not item.strip()
                       for item in source_episode_ids)):
            raise EvolutionGateError("bounded source episode identities are required")
        source_ids = tuple(sorted(dict.fromkeys(source_episode_ids)))
        if procedure.source_episode_ids and set(procedure.source_episode_ids) != set(source_ids):
            raise EvolutionGateError("Procedure source episodes do not match registration")
        if not set(procedure.counterexample_episode_ids) <= set(source_ids):
            raise EvolutionGateError("counterexamples must be included in source episodes")
        evidence = tuple(evidence)
        if evidence:
            validate_evidence(procedure, evidence)
        candidate_id = str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        procedure.scope.partition_key(),
                        procedure.id,
                        str(procedure.version),
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
            evidence=evidence,
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
        by_id = {episode.id: episode for episode in episodes}
        if len(by_id) != len(episodes) or any(episode.scope != scope for episode in episodes):
            raise EvolutionGateError("generation requires unique episodes in one exact scope")
        generated = await generator.generate(scope, episodes)
        candidates = []
        for item in generated:
            if (item.procedure.scope != scope or not set(item.source_episode_ids) <= set(by_id)):
                raise EvolutionGateError("generator changed scope or supplied unknown episodes")
            if item.evidence:
                if set(item.source_episode_ids) != set(by_id):
                    raise EvolutionGateError(
                        "pinned candidates must cover the complete selected corpus"
                    )
                failed_ids = {episode.id for episode in episodes
                              if episode.outcome_status != OutcomeStatus.SUCCEEDED}
                if not failed_ids <= set(item.procedure.counterexample_episode_ids):
                    raise EvolutionGateError(
                        "pinned candidates cannot omit observed counterexamples"
                    )
            for binding in item.evidence:
                episode = by_id.get(binding.episode_id)
                if episode is None or bind_episode(
                    episode, {source.source_event_id: source for source in binding.sources}
                ) != binding:
                    raise EvolutionGateError("generator changed episode evidence or version")
            candidates.append(
                await self.register_procedure(
                    item.procedure,
                    item.source_episode_ids,
                    baseline_version=baseline_version,
                    generator=type(generator).__name__,
                    generator_version=getattr(generator, "version", "1"),
                    evidence=item.evidence,
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
        candidate = await self._registry.get(candidate_id)
        if candidate.state in {
            EvolutionState.INVALIDATED, EvolutionState.ROLLING_BACK, EvolutionState.ROLLED_BACK
        }:
            raise EvolutionGateError(f"candidate cannot be promoted from {candidate.state}")
        if idempotency_key:
            previous = await self._registry.promotion_by_idempotency_key(
                candidate_id, idempotency_key
            )
            if previous is not None:
                return previous
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
        self._activating.add(candidate.id)
        try:
            await self._deployment.activate(activating)
            await self._registry.finish_activation(candidate.id)
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
        except BaseException:
            # Activation may have applied before failing or racing an invalidation.
            # Never make such a candidate eligible again without compensation.
            await self._registry.finish_activation(candidate.id)
            current = await self._registry.get(candidate.id)
            if current.state == EvolutionState.INVALIDATED:
                await self._registry.request_withdrawal(candidate.id)
                await self._withdraw_invalidated(candidate_id=candidate.id)
            elif current.state in {EvolutionState.ACTIVATING, EvolutionState.ACTIVE}:
                await self._registry.transition(
                    candidate.id, current.state, EvolutionState.ROLLING_BACK,
                    actor="system", reason="activation did not complete; withdrawal required",
                )
                await self._finish_rollback(await self._registry.get(candidate.id))
            raise
        finally:
            self._activating.discard(candidate.id)

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

        return await self._finish_rollback(candidate, actor=actor, reason=reason)

    async def _finish_rollback(self, candidate, *, actor="system", reason="withdrawal completed"):
        await self._deployment.deactivate(candidate)
        current = await self._registry.get(candidate.id)
        if current.state == EvolutionState.INVALIDATED:
            await self._registry.complete_withdrawal(candidate.id)
            return (await self._registry.history(candidate.id))[-1]
        return await self._registry.transition(
            candidate.id,
            EvolutionState.ROLLING_BACK,
            EvolutionState.ROLLED_BACK,
            actor=actor,
            reason=reason,
        )

    async def active_candidate(self, scope: MemoryScope) -> EvolutionCandidate | None:
        candidate_id = await self._registry.active_candidate_id(scope)
        candidate = await self._registry.get(candidate_id) if candidate_id else None
        return candidate if candidate and candidate.state == EvolutionState.ACTIVE and not (
            candidate.withdrawal_pending or candidate.activation_pending
        ) else None

    async def recover(self) -> tuple[PromotionRecord, ...]:
        """Exclusive startup recovery; deployed adapters must deactivate idempotently.

        One registry has one controlling host. Do not run startup recovery while
        another process is still executing its activation calls.
        """
        recovered: list[PromotionRecord] = []
        failures = []
        candidates = await self._registry.candidates_in_states(
            (EvolutionState.ACTIVATING, EvolutionState.ROLLING_BACK)
        )
        for candidate in candidates:
            if candidate.id in self._activating:
                continue
            try:
                await self._registry.finish_activation(candidate.id)
                if candidate.state == EvolutionState.ACTIVATING:
                    await self._registry.transition(
                        candidate.id, candidate.state, EvolutionState.ROLLING_BACK,
                        actor="system", reason="restart recovery withdraws incomplete activation",
                    )
                recovered.append(await self._finish_rollback(candidate))
            except Exception:
                failures.append(candidate.id)
        for candidate in await self._registry.pending_withdrawals():
            if candidate.id not in self._activating:
                await self._registry.finish_activation(candidate.id)
        try:
            await self._withdraw_invalidated()
        except EvolutionWithdrawalError as error:
            failures.extend(error.candidate_ids)
        if failures:
            raise EvolutionWithdrawalError(tuple(dict.fromkeys(failures)))
        return tuple(recovered)

    async def _withdraw_invalidated(self, *, candidate_id=None, scope=None):
        failures = []
        for candidate in await self._registry.pending_withdrawals():
            if candidate_id is not None and candidate.id != candidate_id:
                continue
            if scope is not None and candidate.scope != scope:
                continue
            try:
                await self._deployment.deactivate(candidate)
                # If activate is still in flight, its intent stays pending. The
                # activator must compensate again after it returns, or on restart.
                await self._registry.complete_withdrawal(candidate.id)
            except Exception:
                failures.append(candidate.id)
        if failures:
            raise EvolutionWithdrawalError(failures)

    async def invalidate_sources(
        self,
        source_event_ids: Sequence[str],
        *,
        scope: MemoryScope,
        actor: str,
        reason: str,
        all_in_scope: bool = False,
        erase: bool = False,
    ) -> tuple[PromotionRecord, ...]:
        if not source_event_ids and not all_in_scope:
            return ()
        if not reason.strip() or not actor.strip() or not isinstance(scope, MemoryScope):
            raise ValueError("invalidation requires an exact scope, actor and reason")
        records = tuple(
            await self._registry.invalidate_sources(
                source_event_ids,
                scope=scope,
                actor=actor,
                reason=reason,
                all_in_scope=all_in_scope,
                erase=erase,
            )
        )
        await self._withdraw_invalidated(scope=scope)
        return records

    async def pending_withdrawals(self, scope: MemoryScope):
        return tuple(candidate for candidate in await self._registry.pending_withdrawals()
                     if candidate.scope == scope)

    @staticmethod
    def _validate_evaluation_binding(
        candidate: EvolutionCandidate, report: EvaluationReport
    ) -> None:
        if report.candidate_version != candidate.procedure.version:
            raise EvolutionGateError("evaluation candidate version does not match")
        if report.scope_partition_key != candidate.scope.partition_key():
            raise EvolutionGateError("evaluation scope does not match candidate")
