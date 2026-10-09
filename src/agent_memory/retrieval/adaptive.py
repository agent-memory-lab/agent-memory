"""Host-only deterministic retrieval planning; no per-query model router.

This facade is opt-in. It does not install itself in AgentMemory, register
QuestionViews, certify retrieved top-k, or turn an empty bundle into absence.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Protocol

from ..domain import Claim, MemoryBundle, MemoryQuery, MemoryScope
from ..extensions.loader import LoadedPlugin
from ..extensions.registry import PluginKind
from .bundle import BundleBudget
from .diversity import DiversityBudget
from .entity import EntityIndex, EntityReference, EntityRetrieverPlugin
from .governed import CandidateGovernanceResolver, GovernedRecallPipeline
from .parallel import ParallelRetrieverOrchestrator
from .question_router import QuestionRouter
from .scoped_lexical import ScopeIsolationError


@dataclass(frozen=True, slots=True)
class RetrievalNeeds:
    """Explicit trusted-host needs; never extracted or guessed from query prose.

    Entities normally add a candidate path. hard_entity_filter instead selects
    only that exactly bound path, excluding unbound lexical/dense candidates.
    """

    entities: tuple[EntityReference, ...] = ()
    temporal: bool = False
    hard_entity_filter: bool = False

    def __post_init__(self) -> None:
        if (
            type(self.entities) is not tuple
            or len(self.entities) > 16
            or any(not isinstance(entity, EntityReference) for entity in self.entities)
        ):
            raise ValueError("entities must contain at most 16 EntityReference values")
        if type(self.temporal) is not bool or type(self.hard_entity_filter) is not bool:
            raise ValueError("temporal and hard_entity_filter must be booleans")
        if self.hard_entity_filter and not self.entities:
            raise ValueError("hard_entity_filter requires explicit entities")
        if self.hard_entity_filter and self.temporal:
            raise ValueError("hard entity and temporal intersection is unsupported")


_DEFAULT_NEEDS = RetrievalNeeds()


class ExplicitEntityRetrieverPlugin(EntityRetrieverPlugin):
    """Native entity plugin bound to an exact, host-validated entity tuple."""

    def __init__(
        self,
        scope: MemoryScope,
        entities: tuple[EntityReference, ...],
        index: EntityIndex,
    ) -> None:
        RetrievalNeeds(entities=entities)
        if not isinstance(scope, MemoryScope) or not entities:
            raise ValueError("an entity binding requires scope and explicit entities")
        self.scope = scope
        self.entities = entities
        super().__init__(self, index)

    async def resolve(self, text: str, scope: MemoryScope) -> tuple[EntityReference, ...]:
        if scope != self.scope:
            raise ScopeIsolationError("entity binding is outside the query scope")
        return self.entities


@dataclass(frozen=True, slots=True)
class RetrievalPlan:
    path: str
    plugin_names: tuple[str, ...]
    candidate_capacity: int
    reasons: tuple[str, ...]
    routing_model_calls: int = 0


@dataclass(frozen=True, slots=True)
class PartialRecallResult:
    bundle: MemoryBundle
    plan: RetrievalPlan
    kind: str = "partial_recall"


@dataclass(frozen=True, slots=True)
class RegisteredQuestionResult:
    """The service response retains its own availability and proof status.

    Routing to a registered question is not itself a successful certification.
    Hosts must inspect the unchanged service response, including incomplete,
    contested, unavailable, or empty-with-certified-domain status.
    """

    response: Mapping[str, Any]
    kind: str = "registered_question"


@dataclass(frozen=True, slots=True)
class QuestionAbstention:
    reason: str
    kind: str = "abstain"
    routing_model_calls: int = 0


class SnapshotProvider(Protocol):
    async def retrieve(self, query: MemoryQuery) -> MemoryBundle: ...


class AdaptiveRetriever:
    """Reuse exact QuestionRouter answers before explicitly partial retrieval.

    With a router, answer() returns the registered response or abstains. It
    never retries an ambiguous, unauthorized, unregistered, parameter-conflicted,
    or unavailable question through general recall. The host can separately
    request retrieve_partial() when exploratory evidence is actually intended.
    """

    def __init__(
        self,
        plugins: Sequence[LoadedPlugin],
        governance: CandidateGovernanceResolver,
        *,
        question_router: QuestionRouter | None = None,
        snapshot_provider: SnapshotProvider | None = None,
        max_candidates: int = 128,
    ) -> None:
        if (
            not isinstance(plugins, Sequence)
            or isinstance(plugins, (str, bytes))
            or len(plugins) > 8
        ):
            raise ValueError("plugins must be a sequence of at most eight loaded retrievers")
        if any(
            not isinstance(plugin, LoadedPlugin) or plugin.manifest.kind is not PluginKind.RETRIEVER
            for plugin in plugins
        ):
            raise ValueError("plugins must be loaded retrievers")
        if len({plugin.manifest.name for plugin in plugins}) != len(plugins):
            raise ValueError("retriever names must be unique")
        if type(max_candidates) is not int or not 16 <= max_candidates <= 256:
            raise ValueError("max_candidates must be between 16 and 256")
        if question_router is not None and not isinstance(question_router, QuestionRouter):
            raise TypeError("question_router must be the registered QuestionRouter")
        self._plugins = tuple(plugins)
        self._governance = governance
        self._router = question_router
        self._snapshot_provider = snapshot_provider
        self._max_candidates = max_candidates

    def plan(self, query: MemoryQuery, needs: RetrievalNeeds = _DEFAULT_NEEDS) -> RetrievalPlan:
        if not isinstance(query, MemoryQuery) or not isinstance(needs, RetrievalNeeds):
            raise TypeError("query and needs must be validated retrieval values")
        capacity = min(self._max_candidates, max(16, min(query.limit * 8, query.token_budget // 8)))
        if query.valid_at is not None or query.known_at is not None:
            if needs.entities:
                raise ValueError(
                    "historical entity retrieval requires a host snapshot-aware binding"
                )
            return RetrievalPlan("snapshot_provider", (), capacity, ("explicit_temporal_axes",))
        selected: list[LoadedPlugin] = []
        reasons: list[str] = []
        # Native lexical + dense are the primary paths. Unrelated advertised
        # capabilities (including graph traversal) do not enable execution.
        primary = () if needs.hard_entity_filter else ("lexical.search", "semantic.search")
        for capability in primary:
            matches = [
                plugin for plugin in self._plugins if capability in plugin.manifest.capabilities
            ]
            if matches:
                plugin = matches[0]
                if plugin not in selected:
                    selected.append(plugin)
            else:
                reasons.append(capability + "_unavailable")
        if needs.entities:
            matches = [
                plugin
                for plugin in self._plugins
                if isinstance(plugin.instance, ExplicitEntityRetrieverPlugin)
                and plugin.instance.scope == query.scope
                and plugin.instance.entities == needs.entities
            ]
            if not matches:
                raise ValueError(
                    "no exact host-validated entity binding matches requested entities"
                )
            if matches[0] not in selected:
                selected.append(matches[0])
            reasons.append(
                "hard_entity_only" if needs.hard_entity_filter else "explicit_entity_need"
            )
        if needs.temporal:
            matches = [
                plugin
                for plugin in self._plugins
                if "temporal.bitemporal" in plugin.manifest.capabilities
            ]
            if not matches:
                raise ValueError("no temporal plugin supports the explicit temporal need")
            if matches[0] not in selected:
                selected.append(matches[0])
            reasons.append("explicit_current_temporal_need")
        if not selected:
            raise ValueError("no supported retrieval paths are configured")
        if any(plugin.context.scope != query.scope for plugin in selected):
            raise ScopeIsolationError("retriever context is outside the query scope")
        return RetrievalPlan(
            "governed_candidates",
            tuple(plugin.manifest.name for plugin in selected),
            capacity,
            tuple(reasons),
        )

    async def retrieve_partial(
        self,
        query: MemoryQuery,
        current_state: Sequence[Claim] = (),
        *,
        needs: RetrievalNeeds = _DEFAULT_NEEDS,
    ) -> PartialRecallResult:
        plan = self.plan(query, needs)
        if plan.path == "snapshot_provider":
            if self._snapshot_provider is None:
                raise NotImplementedError("explicit time axes require a snapshot-aware provider")
            # Do not clear, replace, infer or round either temporal input axis.
            bundle = await self._snapshot_provider.retrieve(query)
        else:
            selected = tuple(
                next(plugin for plugin in self._plugins if plugin.manifest.name == name)
                for name in plan.plugin_names
            )
            pipeline = GovernedRecallPipeline(
                selected,
                self._governance,
                orchestrator=ParallelRetrieverOrchestrator(
                    max_candidates=plan.candidate_capacity,
                    max_results=min(100, plan.candidate_capacity),
                ),
                bundle_budget=BundleBudget(max_items=query.limit, max_tokens=query.token_budget),
                diversity_budget=DiversityBudget(max_items=query.limit),
            )
            # Host state has no authoritative entity-match label. Do not add
            # unbound state alongside a hard-constrained candidate path.
            state = () if needs.hard_entity_filter else current_state
            bundle = await pipeline.retrieve(query, state)
        bundle = replace(
            bundle,
            retrieval_metadata={
                **bundle.retrieval_metadata,
                "coverage": "partial",
                "world_negative": False,
                "adaptive_path": plan.path,
                "adaptive_plugins": plan.plugin_names,
                "adaptive_reasons": plan.reasons,
                "adaptive_candidate_capacity": plan.candidate_capacity,
                "adaptive_hard_entity_filter": needs.hard_entity_filter,
                "routing_model_calls": 0,
            },
        )
        return PartialRecallResult(bundle, plan)

    async def answer(
        self,
        query: MemoryQuery,
        *,
        actor: str,
        dedupe_key: str,
        parameters: dict | None = None,
        current_state: Sequence[Claim] = (),
        needs: RetrievalNeeds = _DEFAULT_NEEDS,
        max_steps: int = 1,
    ) -> RegisteredQuestionResult | QuestionAbstention | PartialRecallResult:
        if self._router is None:
            if parameters is not None:
                return QuestionAbstention("question_router_unavailable")
            return await self.retrieve_partial(query, current_state, needs=needs)
        if self._router.service.scope != query.scope:
            raise ScopeIsolationError("question service is outside the query scope")
        route = await self._router.route(query.text, actor=actor, parameters=parameters)
        if route["route"] != "question":
            return QuestionAbstention(route["reason"])
        if (
            query.valid_at is not None
            or query.known_at is not None
            or needs.entities
            or needs.temporal
        ):
            # Question parameters are validated by the router, not translated
            # from independent retrieval hints or temporal input fields.
            return QuestionAbstention("question_retrieval_constraints_unsupported")
        response = await self._router.service.answer(
            route["question_id"],
            actor=actor,
            dedupe_key=dedupe_key,
            max_steps=max_steps,
        )
        return RegisteredQuestionResult(response)
