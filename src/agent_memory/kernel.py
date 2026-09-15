from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from inspect import isawaitable
from json import dumps
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

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
    EvaluationRecord,
    FeedbackPage,
    FeedbackReceipt,
    FeedbackStatus,
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
    MemoryUsage,
    OutcomeEvent,
    Procedure,
    ProposalResult,
    ProposalStatus,
    Provenance,
    ProviderManifest,
    RetrievalTrace,
    RewardSignal,
    ScopeLevel,
    StateDelta,
    canonical_json,
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
    MAX_PENDING_FEEDBACK_PER_SCOPE = 1_000

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
        trusted_evaluator_ids: Collection[str] | None = None,
    ) -> None:
        self._repository = repository
        self._extractor = extractor
        self._policy = policy
        self._reranker = reranker
        self._consolidation_scheduler = consolidation_scheduler
        self._trusted_evaluator_ids = (
            frozenset(trusted_evaluator_ids) if trusted_evaluator_ids is not None else None
        )
        self._manifest = ProviderManifest(
            name=provider_name,
            version=provider_version,
            protocol_version=PROTOCOL_VERSION,
            schema_version=SCHEMA_VERSION,
            capabilities=capabilities or MemoryCapabilities(),
        )

    async def initialize(self) -> None:
        await self._repository.initialize()

    async def close(self) -> None:
        close = getattr(self._repository, "close", None)
        if callable(close):
            result = close()
            if isawaitable(result):
                await result

    async def ingest_event(self, event: MemoryEvent) -> IngestResult:
        if event.idempotency_key:
            async with self._repository.unit_of_work() as uow:
                stored = await uow.find_event_by_idempotency(event.scope, event.idempotency_key)
                if stored:
                    self._check_lifecycle_duplicate(stored, event)
                    claim_ids = tuple(await uow.claim_ids_for_event(stored.id))
                    return IngestResult(stored.id, claim_ids, (), True, ())

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
                    self._check_lifecycle_duplicate(stored, event)
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

    @staticmethod
    def _check_lifecycle_duplicate(stored: MemoryEvent, incoming: MemoryEvent) -> None:
        stored_lifecycle = stored.metadata.get("lifecycle")
        incoming_lifecycle = incoming.metadata.get("lifecycle")
        stored_hash = (
            stored_lifecycle.get("content_hash")
            if isinstance(stored_lifecycle, Mapping)
            else None
        )
        incoming_hash = (
            incoming_lifecycle.get("content_hash")
            if isinstance(incoming_lifecycle, Mapping)
            else None
        )
        if (stored_hash is not None or incoming_hash is not None) and stored_hash != incoming_hash:
            raise ValueError("lifecycle event ID reused with different content")

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
        bundle_id = str(uuid4())
        bundle = MemoryBundle(
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
            bundle_id=bundle_id,
            request_id=query.request_id,
        )
        if query.trace_enabled:
            versions = {claim.id: claim.version for claim in selected_state}
            versions.update(
                {
                    item.id: int(item.metadata.get("version", 1))
                    for item in selected
                }
            )
            trace = RetrievalTrace(
                scope=query.scope,
                request_id=query.request_id,
                bundle_id=bundle_id,
                returned_memory_ids=tuple(
                    [claim.id for claim in selected_state] + [item.id for item in selected]
                ),
                returned_versions=versions,
                policy_version=query.policy_version,
                token_budget=query.token_budget,
                token_estimate=used_tokens,
                candidate_count=len(candidates),
                selected_count=len(selected_state) + len(selected),
                truncated=(
                    len(selected_state) < len(current) or len(selected) < len(candidates)
                ),
                run_id=query.run_id,
                idempotency_key=query.request_id,
            )
            async with self._repository.unit_of_work() as uow:
                await uow.save_retrieval_trace(trace)
        return bundle

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
        return await self._record_feedback(decision, "decision")

    async def record_outcome(self, outcome: OutcomeEvent) -> str:
        return await self._record_feedback(
            outcome,
            "outcome",
            parent_id=outcome.decision_id,
            parent_type="decision",
        )

    async def record_evaluation(self, evaluation: EvaluationRecord) -> str:
        return await self._record_feedback(
            evaluation,
            "evaluation",
            parent_id=evaluation.outcome_id,
            parent_type="outcome",
        )

    async def record_reward(self, reward: RewardSignal) -> str:
        parent_id = reward.evaluation_id or reward.outcome_id
        parent_type = "evaluation" if reward.evaluation_id else "outcome"
        return await self._record_feedback(
            reward,
            "reward",
            parent_id=parent_id,
            parent_type=parent_type,
        )

    async def _record_feedback(
        self,
        record: DecisionRecord | OutcomeEvent | EvaluationRecord | RewardSignal,
        record_type: str,
        *,
        parent_id: str | None = None,
        parent_type: str | None = None,
    ) -> str:
        payload = asdict(record)
        comparable = self._feedback_comparable(payload)
        if (
            isinstance(record, EvaluationRecord)
            and self._trusted_evaluator_ids is not None
            and record.evaluator_id not in self._trusted_evaluator_ids
        ):
            raise ValueError("evaluation source is not authorized by the host")
        async with self._repository.unit_of_work() as uow:
            if record_type == "decision" and isinstance(record, DecisionRecord):
                await self._validate_decision_bundle(uow, record)
            existing = await uow.find_feedback_record(record.id, record_type)
            if existing is not None:
                self._validate_feedback_identity(record, comparable, existing)
                return str(existing["id"])

            idempotency_key = getattr(record, "idempotency_key", None)
            if idempotency_key:
                duplicate = await uow.find_feedback_by_idempotency(
                    record.scope, record_type, idempotency_key
                )
                if duplicate is not None:
                    self._validate_feedback_identity(record, comparable, duplicate)
                    return str(duplicate["id"])

            status = FeedbackStatus.ACCEPTED
            if parent_id and parent_type:
                parent = await uow.find_feedback_record(parent_id, parent_type)
                if parent is None:
                    status = FeedbackStatus.PENDING
                elif parent["partition_key"] != record.scope.partition_key():
                    raise ValueError("feedback parent reference is outside the authorized scope")
                elif parent["feedback_status"] != FeedbackStatus.ACCEPTED:
                    status = FeedbackStatus.PENDING

            if (
                status == FeedbackStatus.PENDING
                and await uow.pending_feedback_count(record.scope)
                >= self.MAX_PENDING_FEEDBACK_PER_SCOPE
            ):
                raise ValueError("pending feedback capacity exceeded for this scope")

            corrects_id = getattr(record, "corrects_id", None)
            if corrects_id:
                corrected = await uow.find_feedback_record(corrects_id, record_type)
                if (
                    corrected is None
                    or corrected["partition_key"] != record.scope.partition_key()
                ):
                    raise ValueError("corrected feedback is missing or outside authorized scope")

            replacements: dict[str, object] = {"feedback_status": status}
            if status == FeedbackStatus.PENDING and hasattr(record, "expires_at"):
                expires_at = getattr(record, "expires_at", None)
                replacements["expires_at"] = expires_at or (utc_now() + timedelta(hours=24))
            stored = replace(record, **replacements)
            if record_type == "decision":
                await uow.save_decision(stored)
            elif record_type == "outcome":
                await uow.save_outcome(stored)
            elif record_type == "evaluation":
                await uow.save_evaluation(stored)
            else:
                await uow.save_reward(stored)

            if corrects_id:
                await uow.supersede_feedback(record.scope, corrects_id)
            if status == FeedbackStatus.ACCEPTED:
                frontier = [record.id]
                while frontier:
                    frontier.extend(
                        await uow.activate_pending_children(record.scope, frontier.pop())
                    )
        return record.id

    async def feedback_status(
        self, scope: MemoryScope, record_id: str, record_type: str
    ) -> FeedbackReceipt | None:
        if record_type not in {"retrieval", "decision", "outcome", "evaluation", "reward"}:
            raise ValueError("unsupported feedback record_type")
        async with self._repository.unit_of_work() as uow:
            record = await uow.find_feedback_record(record_id, record_type)
            if record is None or record["partition_key"] != scope.partition_key():
                return None
        return self._feedback_receipt(record)

    async def feedback_history(
        self,
        scope: MemoryScope,
        record_type: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> FeedbackPage:
        if record_type not in {"retrieval", "decision", "outcome", "evaluation", "reward"}:
            raise ValueError("unsupported feedback record_type")
        if not 1 <= limit <= 100:
            raise ValueError("feedback page limit must be between 1 and 100")
        rows = await self._repository.list_feedback(scope, record_type, limit + 1, cursor)
        page_rows = rows[:limit]
        items = tuple(self._feedback_receipt(row) for row in page_rows)
        next_cursor = items[-1].record_id if len(rows) > limit and items else None
        return FeedbackPage(items=items, next_cursor=next_cursor)

    @staticmethod
    def _feedback_receipt(record: Mapping[str, object]) -> FeedbackReceipt:
        return FeedbackReceipt(
            record_id=str(record["id"]),
            record_type=str(record["record_type"]),
            status=FeedbackStatus(str(record["feedback_status"])),
            parent_id=str(record["parent_id"]) if record["parent_id"] else None,
            idempotency_key=(
                str(record["idempotency_key"]) if record["idempotency_key"] else None
            ),
            corrects_id=str(record["corrects_id"]) if record["corrects_id"] else None,
            expires_at=record["expires_at"] if isinstance(record["expires_at"], datetime) else None,
        )

    @staticmethod
    async def _validate_decision_bundle(
        uow: MemoryUnitOfWork, decision: DecisionRecord
    ) -> None:
        if decision.bundle_id is None:
            return
        trace = await uow.find_feedback_record(decision.bundle_id, "retrieval")
        if trace is None or trace["partition_key"] != decision.scope.partition_key():
            raise ValueError("retrieval bundle is missing or outside the authorized scope")
        payload = trace.get("payload")
        if not isinstance(payload, dict):
            raise RuntimeError("stored retrieval trace payload is invalid")
        returned = set(map(str, payload.get("returned_memory_ids", ())))
        used = set(decision.memory_ids) | set(decision.procedure_ids)
        if decision.memory_usage == MemoryUsage.CONFIRMED and not used <= returned:
            raise ValueError("confirmed memory references are not present in the bundle")

    @staticmethod
    def _validate_feedback_identity(
        record: DecisionRecord | OutcomeEvent | EvaluationRecord | RewardSignal,
        comparable: dict[str, Any],
        existing: Mapping[str, object],
    ) -> None:
        if existing["partition_key"] != record.scope.partition_key():
            raise ValueError("feedback id is already used outside the authorized scope")
        stored_payload = existing.get("payload")
        if not isinstance(stored_payload, dict):
            raise RuntimeError("stored feedback payload is invalid")
        stored_comparable = MemoryKernel._feedback_comparable(stored_payload)
        if canonical_json(stored_comparable) != canonical_json(comparable):
            raise ValueError("feedback idempotency conflict: payload differs")

    @staticmethod
    def _feedback_comparable(payload: Mapping[str, object]) -> dict[str, object]:
        comparable = dict(payload)
        for generated in (
            "id",
            "created_at",
            "occurred_at",
            "feedback_status",
            "expires_at",
        ):
            comparable.pop(generated, None)
        return comparable

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

    @property
    def consolidation_scheduler(self) -> ConsolidationScheduler | None:
        """Return the optional scheduler so a host can run its worker separately."""
        return self._consolidation_scheduler

    def _require_block_capability(self) -> None:
        if not self._manifest.capabilities.memory_blocks:
            raise NotImplementedError("the selected memory provider does not support memory blocks")
