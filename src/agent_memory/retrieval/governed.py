"""Opt-in end-to-end candidate governance for AgentMemory.recall()."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol, Sequence

from .bundle import BundleBudget, BundlePackingTrace, pack_memory_bundle
from .diversity import DiversityBudget, DiversityTrace
from .fusion import FusedCandidate
from .guard import (
    CandidateGuardResult,
    GovernedCandidate,
    guard_candidates,
)
from ..domain import Claim, MemoryBundle, MemoryCapabilities, MemoryQuery, MemoryScope
from .parallel import (
    ParallelRetrieverOrchestrator,
    RetrieverExecutionTrace,
)
from ..extensions.loader import LoadedPlugin
from .scoped_lexical import ScopeIsolationError


class CandidateGovernanceResolver(Protocol):
    """Resolve authoritative policy fields from storage for fused candidates."""

    async def resolve(
        self,
        scope: MemoryScope,
        candidates: Sequence[FusedCandidate],
    ) -> Sequence[GovernedCandidate]: ...


class RecallPipeline(Protocol):
    async def retrieve(
        self,
        query: MemoryQuery,
        current_state: Sequence[Claim],
    ) -> MemoryBundle: ...


@dataclass(frozen=True, slots=True)
class GovernedRecallTrace:
    retrievers: tuple[RetrieverExecutionTrace, ...]
    guard: CandidateGuardResult
    diversity: DiversityTrace
    packing: BundlePackingTrace
    degraded: bool


class GovernedRecallPipeline:
    """One bounded retrieval wave followed by mandatory final policy checks."""

    def __init__(
        self,
        plugins: Sequence[LoadedPlugin],
        governance: CandidateGovernanceResolver,
        *,
        orchestrator: ParallelRetrieverOrchestrator | None = None,
        diversity_budget: DiversityBudget | None = None,
        bundle_budget: BundleBudget | None = None,
        capabilities: MemoryCapabilities | None = None,
        policy_version: str = "governed-recall-v1",
    ) -> None:
        if not isinstance(plugins, Sequence) or isinstance(plugins, (str, bytes)) or not plugins:
            raise ValueError("plugins must contain at least one loaded retriever")
        if not isinstance(policy_version, str) or not policy_version.strip():
            raise ValueError("policy_version must be non-empty")
        self._plugins = tuple(plugins)
        self._governance = governance
        # Fusion bounds candidates; final item limits apply after policy guards.
        self._orchestrator = orchestrator or ParallelRetrieverOrchestrator(max_results=100)
        self._diversity_budget = diversity_budget or DiversityBudget()
        self._bundle_budget = bundle_budget or BundleBudget()
        self._capabilities = capabilities or MemoryCapabilities()
        self._policy_version = policy_version
        self._last_trace: GovernedRecallTrace | None = None

    @property
    def last_trace(self) -> GovernedRecallTrace | None:
        return self._last_trace

    async def retrieve(
        self,
        query: MemoryQuery,
        current_state: Sequence[Claim],
    ) -> MemoryBundle:
        if query.valid_at is not None or query.known_at is not None:
            raise NotImplementedError(
                "historical governed recall requires a snapshot-aware governance resolver; "
                "use the provider bitemporal retrieval path"
            )
        # One wave, no unbounded refill. Divide the total cap between loaded
        # retrievers while keeping MemoryQuery's 100-item bound per plugin.
        capacity = self._orchestrator.candidate_capacity
        if capacity < len(self._plugins):
            raise ValueError("candidate capacity must cover each loaded retriever")
        candidate_limit = min(100, capacity // len(self._plugins), max(16, query.limit * 4))
        parallel = await self._orchestrator.retrieve(
            replace(query, limit=candidate_limit), self._plugins,
        )
        fused = parallel.fusion.candidates
        records = await self._governance.resolve(query.scope, fused)
        if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
            raise ScopeIsolationError("governance resolver must return a bounded sequence")
        if len(records) > len(fused):
            raise ScopeIsolationError("governance resolver injected additional candidates")
        expected = {candidate.item.id: candidate for candidate in fused}
        seen: set[str] = set()
        for record in records:
            if not isinstance(record, GovernedCandidate):
                raise ScopeIsolationError("governance resolver returned an unlabelled candidate")
            memory_id = record.candidate.item.id
            if memory_id in seen or expected.get(memory_id) != record.candidate:
                raise ScopeIsolationError("governance resolver altered retrieval candidates")
            seen.add(memory_id)

        guarded = guard_candidates(
            query.scope,
            records,
            now=self._plugins[0].context.clock.now(),
            max_candidates=min(256, max(1, len(fused))),
        )
        diversity_budget = DiversityBudget(
            max_items=min(query.limit, self._diversity_budget.max_items),
            max_per_kind=self._diversity_budget.max_per_kind,
            max_per_source_event=self._diversity_budget.max_per_source_event,
        )
        bundle_budget = BundleBudget(
            max_items=min(query.limit, self._bundle_budget.max_items),
            max_characters=self._bundle_budget.max_characters,
            max_tokens=min(query.token_budget, self._bundle_budget.max_tokens),
        )
        packed = pack_memory_bundle(
            query.scope,
            guarded.accepted,
            current_state=current_state if query.include_current_state else (),
            budget=bundle_budget,
            diversity_budget=diversity_budget,
            capabilities=self._capabilities,
            request_id=query.request_id,
            policy_version=self._policy_version,
        )
        assert packed.diversity_trace is not None
        trace = GovernedRecallTrace(
            retrievers=parallel.traces,
            guard=guarded,
            diversity=packed.diversity_trace,
            packing=packed.trace,
            degraded=parallel.degraded or bool(guarded.rejected),
        )
        self._last_trace = trace
        metadata = dict(packed.bundle.retrieval_metadata)
        metadata.update(
            {
                "degraded": trace.degraded,
                "query_policy_version": query.policy_version,
                "run_id": query.run_id,
                "candidate_limit": candidate_limit,
                "fusion_truncated": parallel.fusion.trace.truncated,
                "coverage": "partial",
                "world_negative": False,
                "retrievers": tuple(
                    {
                        "name": value.name,
                        "status": value.status,
                        "candidate_count": value.candidate_count,
                        "reason": value.reason,
                    }
                    for value in trace.retrievers
                ),
                "guard_rejected": tuple(
                    {"memory_id": value.memory_id, "reason": value.reason.value}
                    for value in trace.guard.rejected
                ),
                "diversity_selected": trace.diversity.selected_count,
            }
        )
        return replace(packed.bundle, retrieval_metadata=metadata)
