from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from json import dumps
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .domain import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    ArtifactStatus,
    Citation,
    Claim,
    ClaimDraft,
    ClaimStatus,
    DecisionRecord,
    Episode,
    ForgetMode,
    ForgetRequest,
    ForgetResult,
    IngestResult,
    MemoryBlock,
    MemoryBundle,
    MemoryCapabilities,
    MemoryChannel,
    MemoryEvent,
    MemoryItem,
    MemoryKind,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    OutcomeEvent,
    Procedure,
    ProposalResult,
    ProposalStatus,
    Provenance,
    ProviderManifest,
    RewardSignal,
    ScopeLevel,
    StateDelta,
    utc_now,
)
from .ports import (
    ClaimExtractor,
    ConsolidationScheduler,
    MemoryPolicy,
    MemoryRepository,
    MemoryUnitOfWork,
    Reranker,
)


def _canonical_value(value: Any) -> str:
    return dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _token_estimate(text: str) -> int:
    latin = sum(1 for character in text if ord(character) < 128)
    non_latin = len(text) - latin
    return max(1, non_latin + (latin + 3) // 4)


class MemoryKernel:
    def __init__(
        self,
        repository: MemoryRepository,
        extractor: ClaimExtractor,
        policy: MemoryPolicy,
        reranker: Reranker,
        *,
        provider_name: str = "sqlite-local",
        provider_version: str = "0.1.0",
        capabilities: MemoryCapabilities | None = None,
        consolidation_scheduler: ConsolidationScheduler | None = None,
    ) -> None:
        self._repository = repository
        self._extractor = extractor
        self._policy = policy
        self._reranker = reranker
        self._consolidation_scheduler = consolidation_scheduler
        self._manifest = ProviderManifest(
            name=provider_name,
            version=provider_version,
            protocol_version=PROTOCOL_VERSION,
            schema_version=SCHEMA_VERSION,
            capabilities=capabilities or MemoryCapabilities(),
        )

    async def initialize(self) -> None:
        await self._repository.initialize()

    async def ingest_event(self, event: MemoryEvent) -> IngestResult:
        drafts: Sequence[ClaimDraft] = ()
        if await self._policy.should_extract(event):
            extracted = await self._extractor.extract(event)
            accepted: list[ClaimDraft] = []
            seen_drafts: set[tuple[ScopeLevel, str]] = set()
            for draft in extracted:
                dedup_key = (draft.scope_level, draft.key)
                if dedup_key in seen_drafts:
                    continue
                if await self._policy.accept_claim(event, draft):
                    seen_drafts.add(dedup_key)
                    accepted.append(draft)
            drafts = tuple(accepted)

        async with self._repository.unit_of_work() as uow:
            if event.idempotency_key:
                stored = await uow.find_event_by_idempotency(event.scope, event.idempotency_key)
                if stored:
                    claim_ids = tuple(await uow.claim_ids_for_event(stored.id))
                    return IngestResult(stored.id, claim_ids, (), True, ())

            await uow.append_event(event)
            accepted_ids: list[str] = []
            delta_ids: list[str] = []
            superseded_ids: list[str] = []
            for draft in drafts:
                claim, delta, superseded = await self._apply_claim(uow, event, draft)
                accepted_ids.append(claim.id)
                if delta:
                    await uow.save_state_delta(delta)
                    delta_ids.append(delta.id)
                if superseded:
                    superseded_ids.append(superseded)

            result = IngestResult(
                event_id=event.id,
                claim_ids=tuple(accepted_ids),
                state_delta_ids=tuple(delta_ids),
                duplicate=False,
                superseded_claim_ids=tuple(superseded_ids),
            )
        if self._consolidation_scheduler:
            await self._consolidation_scheduler.enqueue_event(event, result)
        return result

    async def _apply_claim(
        self,
        uow: MemoryUnitOfWork,
        event: MemoryEvent,
        draft: ClaimDraft,
    ) -> tuple[Claim, StateDelta | None, str | None]:
        claim_scope = event.scope.project(draft.scope_level)
        previous = await uow.find_current_claim(claim_scope, draft.key)
        if previous and _canonical_value(previous.value) == _canonical_value(draft.value):
            await uow.add_claim_source(previous.id, event.id)
            return previous, None, None

        now = utc_now()
        claim_id = str(
            uuid5(NAMESPACE_URL, f"{event.id}:{claim_scope.partition_key()}:{draft.key}")
        )
        provenance = draft.provenance or Provenance(
            source_event_ids=(event.id,),
            extractor=type(self._extractor).__name__,
            provider=self._manifest.name,
            source_uri=event.source_uri,
        )
        claim = Claim(
            id=claim_id,
            scope=claim_scope,
            key=draft.key,
            value=draft.value,
            text=draft.text,
            confidence=draft.confidence,
            importance=draft.importance,
            status=ClaimStatus.ACTIVE,
            provenance=provenance,
            valid_from=draft.valid_from or event.occurred_at,
            created_at=now,
            version=(previous.version + 1) if previous else 1,
            supersedes=previous.id if previous else None,
        )
        operation = "replace" if previous else "add"
        if previous:
            await uow.replace_current_claim(previous, claim)
        else:
            await uow.save_claim(claim)

        delta = StateDelta(
            id=str(uuid5(NAMESPACE_URL, f"delta:{claim.id}")),
            scope=claim_scope,
            key=claim.key,
            operation=operation,
            source_event_id=event.id,
            current_claim_id=claim.id,
            previous_claim_id=previous.id if previous else None,
            created_at=now,
        )
        return claim, delta, previous.id if previous else None

    async def get_state(self, scope: MemoryScope) -> tuple[Claim, ...]:
        return tuple(await self._repository.current_claims(scope))

    async def retrieve(self, query: MemoryQuery) -> MemoryBundle:
        current = (
            tuple(await self._repository.current_claims(query.scope))
            if query.include_current_state
            else ()
        )
        candidates = await self._repository.search(query, query.limit * 6)
        ranked = await self._reranker.rerank(query, candidates)

        selected_state: list[Claim] = []
        selected: list[MemoryItem] = []
        citations: list[Citation] = []
        used_tokens = 0
        state_budget = max(32, query.token_budget // 2)

        for claim in current:
            cost = _token_estimate(claim.text)
            if used_tokens + cost > state_budget:
                break
            selected_state.append(claim)
            citations.append(Citation(claim.id, claim.provenance.source_event_ids))
            used_tokens += cost

        state_ids = {claim.id for claim in selected_state}
        priority = {
            MemoryKind.BLOCK: 0,
            MemoryKind.PROCEDURE: 1,
            MemoryKind.EPISODE: 2,
            MemoryKind.CLAIM: 3,
            MemoryKind.EVENT: 4,
            MemoryKind.LATENT_REFERENCE: 5,
        }
        for item in sorted(ranked, key=lambda value: (priority[value.kind], -value.score)):
            if item.id in state_ids:
                continue
            cost = _token_estimate(item.text)
            if used_tokens + cost > query.token_budget:
                continue
            selected.append(item)
            source_ids = item.metadata.get("source_event_ids", ())
            if isinstance(source_ids, (list, tuple)):
                citations.append(Citation(item.id, tuple(map(str, source_ids))))
            used_tokens += cost
            if len(selected) >= query.limit:
                break

        episodes = tuple(item for item in selected if item.kind == MemoryKind.EPISODE)
        procedures = tuple(item for item in selected if item.kind == MemoryKind.PROCEDURE)
        relevant = tuple(
            item for item in selected if item.kind not in (MemoryKind.EPISODE, MemoryKind.PROCEDURE)
        )
        return MemoryBundle(
            current_state=tuple(selected_state),
            relevant_memories=relevant,
            episodes=episodes,
            procedures=procedures,
            citations=tuple(citations),
            token_estimate=used_tokens,
            retrieval_metadata={
                "candidate_count": len(candidates),
                "selected_count": len(selected),
                "state_count": len(selected_state),
                "strategy": "state_first_rrf",
                "protocol_version": PROTOCOL_VERSION,
            },
            capability_snapshot=self._manifest.capabilities,
        )

    async def propose(self, proposal: MemoryProposal) -> ProposalResult:
        claim_scope = proposal.scope.project(proposal.scope_level)
        async with self._repository.unit_of_work() as uow:
            existing = await uow.find_proposal_result(proposal.id)
            if existing:
                return existing

            if not await uow.events_exist(proposal.scope, proposal.source_event_ids):
                result = ProposalResult(
                    proposal_id=proposal.id,
                    status=ProposalStatus.REJECTED,
                    reason="source evidence is missing or outside the authorized scope",
                )
                await uow.save_proposal(proposal, result)
                return result

            previous = await uow.find_current_claim(claim_scope, proposal.key)
            actual_version = previous.version if previous else 0
            if actual_version != proposal.expected_version:
                result = ProposalResult(
                    proposal_id=proposal.id,
                    status=ProposalStatus.CONFLICT,
                    claim_id=previous.id if previous else None,
                    reason=(
                        f"expected version {proposal.expected_version}, "
                        f"current version {actual_version}"
                    ),
                )
                await uow.save_proposal(proposal, result)
                return result

            audit_event = MemoryEvent(
                id=str(uuid5(NAMESPACE_URL, f"proposal-event:{proposal.id}")),
                scope=proposal.scope,
                event_type="memory.proposal.accepted",
                content=proposal.text,
                metadata={
                    "proposal_id": proposal.id,
                    "source_event_ids": proposal.source_event_ids,
                },
                idempotency_key=f"proposal:{proposal.id}",
                actor=proposal.actor,
            )
            await uow.append_event(audit_event)
            draft = ClaimDraft(
                key=proposal.key,
                value=proposal.value,
                text=proposal.text,
                confidence=proposal.confidence,
                importance=proposal.importance,
                scope_level=proposal.scope_level,
                valid_from=audit_event.occurred_at,
                provenance=Provenance(
                    source_event_ids=(*proposal.source_event_ids, audit_event.id),
                    extractor="MemoryProposal",
                    provider=self._manifest.name,
                ),
            )
            claim, delta, superseded = await self._apply_claim(uow, audit_event, draft)
            if delta:
                await uow.save_state_delta(delta)
            result = ProposalResult(
                proposal_id=proposal.id,
                status=ProposalStatus.ACCEPTED,
                claim_id=claim.id,
                state_delta_id=delta.id if delta else None,
                superseded_claim_id=superseded,
            )
            await uow.save_proposal(proposal, result)
            return result

    async def record_episode(self, episode: Episode) -> str:
        async with self._repository.unit_of_work() as uow:
            await uow.save_episode(episode)
        return episode.id

    async def publish_procedure(self, procedure: Procedure) -> str:
        if procedure.status == ArtifactStatus.ACTIVE and not procedure.provenance.source_event_ids:
            raise ValueError("active procedures require source evidence")
        async with self._repository.unit_of_work() as uow:
            await uow.save_procedure(procedure)
        return procedure.id

    async def write_block(self, block: MemoryBlock, expected_version: int = 0) -> MemoryBlock:
        self._require_block_capability()
        if expected_version < 0:
            raise ValueError("expected_version must be zero or greater")
        if _token_estimate(f"{block.title}\n{block.content}") > block.token_budget:
            raise ValueError("memory block content exceeds its token_budget")
        candidate = replace(
            block,
            provenance=replace(block.provenance, source_event_ids=block.event_ids),
        )
        async with self._repository.unit_of_work() as uow:
            if not await uow.events_exist(block.scope, block.event_ids):
                raise ValueError("source evidence is missing or outside the authorized scope")
            return await uow.save_block(candidate, expected_version)

    async def read_block(self, scope: MemoryScope, block_id: str) -> MemoryBlock | None:
        self._require_block_capability()
        return await self._repository.read_block(scope, block_id)

    async def search_blocks(
        self,
        scope: MemoryScope,
        text: str,
        channels: Sequence[MemoryChannel] = (),
        limit: int = 8,
    ) -> tuple[MemoryBlock, ...]:
        self._require_block_capability()
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        selected_channels = tuple(channels) or tuple(MemoryChannel)
        return tuple(await self._repository.search_blocks(scope, text, selected_channels, limit))

    async def record_decision(self, decision: DecisionRecord) -> str:
        async with self._repository.unit_of_work() as uow:
            await uow.save_decision(decision)
        return decision.id

    async def record_outcome(self, outcome: OutcomeEvent) -> str:
        async with self._repository.unit_of_work() as uow:
            await uow.save_outcome(outcome)
        return outcome.id

    async def record_reward(self, reward: RewardSignal) -> str:
        async with self._repository.unit_of_work() as uow:
            await uow.save_reward(reward)
        return reward.id

    async def forget(self, request: ForgetRequest) -> ForgetResult:
        return await self._repository.forget(request)

    async def forget_block(
        self,
        scope: MemoryScope,
        block_id: str,
        mode: ForgetMode = ForgetMode.ARCHIVE,
    ) -> ForgetResult:
        self._require_block_capability()
        block = await self._repository.read_block(scope, block_id)
        if block is None:
            return ForgetResult(0, 0, 0, mode)
        return await self._repository.forget(
            ForgetRequest(scope=scope, memory_ids=(block.id,), mode=mode)
        )

    def manifest(self) -> ProviderManifest:
        return self._manifest

    def _require_block_capability(self) -> None:
        if not self._manifest.capabilities.memory_blocks:
            raise NotImplementedError("the selected memory provider does not support memory blocks")
