"""Opt-in bounded evidence acquisition over governed memory retrieval.

All callbacks are trusted host adapters, not arbitrary functions supplied by a
model. Reader/planner adapters enforce CallAllowance before calling a provider;
they return typed, still-untrusted proposals. No evaluator/example dependency.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256
from time import monotonic
from uuid import uuid4

from ..domain import MemoryBundle, canonical_json
from .delivery_guard import delivery_evidence_key
from .evidence_research_contracts import (
    CallAllowance,
    CoverageReceipt,
    EvidenceGap,
    EvidenceRecord,
    EvidenceRequirement,
    EvidenceUse,
    FinalizedEvidence,
    QueryContract,
    ReaderAssessment,
    ResearchEvent,
    ResearchLimits,
    ResearchPlan,
    ResearchResult,
    RetrievalBatch,
    TemporalResolution,
    digest,
)
from .packing import token_estimate


def evidence_records(bundles):
    """Bind request-local references to text AND provenance/version, not IDs alone."""
    records = {}
    for bundle in bundles:
        for value in (
            *bundle.current_state,
            *bundle.relevant_memories,
            *bundle.episodes,
            *bundle.procedures,
        ):
            key = (
                delivery_evidence_key(value)
                if hasattr(value, "metadata")
                else digest(asdict(value))
            )
            if key is None:
                raise ValueError("unbound_research_evidence")
            occurred_at = getattr(value, "occurred_at", None)
            occurred_at = occurred_at.isoformat() if occurred_at is not None else None
            key = digest((key, occurred_at))
            revision = getattr(value, "metadata", {}).get("source_revision")
            records[key] = EvidenceRecord(
                key,
                value.id,
                value.text,
                sha256(value.text.encode()).hexdigest(),
                revision,
                occurred_at,
            )
    return tuple(records.values())


class _Stop(Exception):
    pass


class _Session:
    def __init__(self, contract, sink):
        self.contract, self.sink = contract, sink
        self.started = monotonic()
        self.calls = self.tokens = self.models = self.actual_models = 0
        self.unknown_usage = self.known_input = self.known_output = 0
        self.events = []
        self.attempt_id = uuid4().hex

    async def call(self, stage, invoke, *, inputs=(), output=0, models=0, final=False, overhead=0):
        limits = self.contract.limits
        remaining = limits.max_elapsed_ms - int((monotonic() - self.started) * 1000)
        if remaining <= 0:
            raise _Stop("deadline_exceeded")
        # A final revalidation slot is never spent on a reader or a planner.
        if self.calls >= limits.max_calls - (not final):
            raise _Stop("call_budget_exhausted")
        if type(overhead) is not int or not 0 <= overhead <= 32768:
            raise _Stop("invalid_adapter_reservation")
        input_tokens = (len(canonical_json(inputs).encode()) if inputs else 0) + overhead
        reserved = input_tokens + output
        if self.tokens + reserved > limits.max_reserved_tokens:
            raise _Stop("token_budget_exhausted")
        if self.models + models > limits.max_model_calls:
            raise _Stop("model_call_budget_exhausted")
        self.calls += 1
        self.tokens += reserved
        self.models += models
        allowance = CallAllowance(
            digest((self.contract.fingerprint, self.attempt_id, self.calls, stage)),
            input_tokens,
            output,
            models,
            remaining,
        )
        self.events.append(ResearchEvent(stage, "reserved", allowance.call_id, reserved))
        try:
            async with asyncio.timeout(remaining / 1000):
                result = await invoke(allowance)
        except TimeoutError as error:
            self.unknown_usage += models
            self.events.append(ResearchEvent(stage, "timed_out", allowance.call_id, reserved))
            raise _Stop("deadline_exceeded") from error
        except asyncio.CancelledError:
            self.events.append(ResearchEvent(stage, "cancelled", allowance.call_id, reserved))
            raise
        except Exception as error:
            self.unknown_usage += models
            self.events.append(ResearchEvent(stage, "failed", allowance.call_id, reserved))
            raise _Stop("callback_failed") from error
        if isinstance(result, (ReaderAssessment, ResearchPlan, RetrievalBatch)):
            if result.model_calls > models:
                raise _Stop("unbudgeted_model_call")
            self.actual_models += result.model_calls
            if result.model_calls:
                self.unknown_usage += result.input_tokens is None or result.output_tokens is None
                self.known_input += result.input_tokens or 0
                self.known_output += result.output_tokens or 0
                if result.output_tokens is not None and result.output_tokens > output:
                    raise _Stop("model_output_budget_exceeded")
                if result.input_tokens is not None and result.input_tokens > input_tokens:
                    raise _Stop("model_input_budget_exceeded")
        elif models:
            self.unknown_usage += models
        if isinstance(result, (ReaderAssessment, ResearchPlan)):
            if len(canonical_json(asdict(result)).encode()) > max(512, output * 4):
                raise _Stop("response_body_budget_exceeded")
        self.events.append(ResearchEvent(stage, "completed", allowance.call_id, reserved))
        return result

    async def record(self, action, outcome, records=()):
        keys = tuple(record.key for record in records)
        self.events.append(ResearchEvent(action, outcome, evidence_keys=keys))
        if self.sink is None:
            return
        from ..operations.decision_records import PolicyDecisionRecord

        record = PolicyDecisionRecord(
            id=digest(
                (self.contract.fingerprint, self.attempt_id, len(self.events), action, outcome)
            ),
            scope=self.contract.query.scope,
            stage="retrieval",
            action="research_" + action,
            reasons=(outcome,),
            policy_version="evidence-research/1",
            input_versions=(self.contract.fingerprint,),
            evidence_references=keys,
            memory_ids=tuple(dict.fromkeys(r.source_id for r in records)),
            cost_references=tuple(e.call_id for e in self.events if e.outcome == "reserved"),
            outcome="proposed" if action == "delivery_candidate" else "applied",
        )
        await self.call("decision_journal", lambda _: self.sink(record))

    def result(
        self, status, reason, *, answer=None, records=(), receipts=(), missing=(), view=None
    ):
        return ResearchResult(
            status=status,
            answer=answer,
            reason=reason,
            evidence=tuple(records),
            coverage_receipts=tuple(receipts),
            missing_requirements=tuple(missing),
            events=tuple(self.events),
            calls=self.calls,
            reserved_tokens=self.tokens,
            model_calls=self.actual_models,
            model_call_reservations=self.models,
            unknown_model_usage_calls=self.unknown_usage,
            known_input_tokens=self.known_input,
            known_output_tokens=self.known_output,
            view_response=view,
        )


class EvidenceResearchLoop:
    """A host-owned query, one initial recall, then at most N bounded waves.

    ``backend.search`` returns RetrievalBatch, ``backend.finalize`` returns
    FinalizedEvidence. ``reader`` and optional ``planner`` are async callables
    accepting the contract, immutable record tuple and CallAllowance. A temporal
    resolver is trusted typed extraction; it does not call a language model.
    """

    def __init__(
        self,
        backend,
        reader,
        *,
        planner=None,
        temporal_resolver=None,
        view=None,
        decision_sink=None,
    ):
        self.backend, self.reader, self.planner = backend, reader, planner
        self.temporal_resolver, self.view, self.decision_sink = (
            temporal_resolver,
            view,
            decision_sink,
        )

    async def answer(self, contract: QueryContract) -> ResearchResult:
        if type(contract) is not QueryContract:
            raise TypeError("host_query_contract_required")
        contract = deepcopy(contract)
        if (
            hasattr(self.backend, "bound_scope")
            and contract.query.scope != self.backend.bound_scope
        ):
            raise ValueError("research_scope_outside_memory")
        session = _Session(contract, self.decision_sink)
        bundles, receipts, records = [], [], ()
        requirements = {r.id: r for r in contract.requirements}
        missing = tuple(requirements)
        try:
            if contract.registered_question is not None:
                if self.view is None:
                    return session.result("partial", "registered_view_unavailable", missing=missing)
                # Registered questions never fall through to weaker general recall.
                response = await session.call(
                    "registered_view", lambda a: self.view.read(contract, a)
                )
                return session.result(
                    "view" if response.get("availability_status") == "valid" else "partial",
                    "registered_view",
                    view=response,
                    missing=missing,
                )
            if (
                contract.query.valid_at is not None or contract.query.known_at is not None
            ) and not getattr(self.backend, "supports_history", False):
                return session.result("partial", "historical_research_unsupported", missing=missing)

            pending = (None,)
            attempted = set()
            for round_index in range(contract.limits.max_rounds):
                before = {r.key for r in records}
                before_receipts = {r.signature for r in receipts}
                for gap in pending:
                    requirement = requirements[gap.requirement_id] if gap else None
                    route = gap.route if gap else "native"
                    batch = await session.call(
                        "retrieve_" + route,
                        lambda a, r=requirement, p=route: self.backend.search(
                            contract,
                            r,
                            p,
                            a,
                        ),
                        inputs=(
                            contract.fingerprint,
                            requirement.search_text if requirement else contract.query.text,
                        ),
                        output=contract.query.token_budget,
                        models=1
                        if route == "semantic" and getattr(self.backend, "semantic_model_calls", 0)
                        else 0,
                    )
                    if type(batch) is not RetrievalBatch or type(batch.bundle) is not MemoryBundle:
                        raise _Stop("invalid_retrieval_batch")
                    if type(batch.receipts) is not tuple or len(batch.receipts) > 1:
                        raise _Stop("invalid_coverage_receipts")
                    if batch.partial_reason:
                        session.events.append(
                            ResearchEvent("retrieve_" + route, batch.partial_reason)
                        )
                    fresh = evidence_records((*bundles, batch.bundle))
                    if (
                        len(fresh) > contract.limits.max_evidence_items
                        or sum(token_estimate(r.text) for r in fresh) > contract.query.token_budget
                    ):
                        # Never trim a complete census and keep its complete receipt.
                        raise _Stop("evidence_budget_exhausted")
                    bundles.append(deepcopy(batch.bundle))
                    for receipt in batch.receipts:
                        if (
                            type(receipt) is not CoverageReceipt
                            or requirement is None
                            or receipt.requirement_id != requirement.id
                            or receipt.contract_fingerprint != contract.fingerprint
                            or receipt.domain != requirement.coverage_domain
                        ):
                            raise _Stop("invalid_coverage_binding")
                    receipts.extend(deepcopy(batch.receipts))
                    records = fresh
                    if gap:
                        attempted.add((gap.requirement_id, gap.route))
                if (
                    round_index
                    and before == {r.key for r in records}
                    and before_receipts == {r.signature for r in receipts}
                ):
                    raise _Stop("no_new_evidence")
                await session.record("evidence", "acquired", records)
                assessment = await session.call(
                    "reader",
                    lambda a, records=records: self.reader(
                        deepcopy(contract), deepcopy(records), a
                    ),
                    inputs=(asdict(contract), tuple(asdict(r) for r in records)),
                    output=contract.limits.reader_output_tokens,
                    models=1,
                    overhead=getattr(self.reader, "input_overhead_bytes", 0),
                )
                if type(assessment) is not ReaderAssessment:
                    raise _Stop("invalid_reader_assessment")
                support = self._support(assessment, requirements, records)
                temporal = await self._resolve_temporal(session, requirements, records, support)
                receipt_ids = {r.requirement_id for r in receipts}
                missing = tuple(
                    r.id
                    for r in contract.requirements
                    if r.id not in support
                    or r.kind in ("enumeration", "absence")
                    and r.id not in receipt_ids
                    or r.kind == "temporal"
                    and r.id not in temporal
                )
                if not missing:
                    await session.record(
                        "delivery_candidate", "ready_for_final_validation", records
                    )
                    finalized = await session.call(
                        "final_guard",
                        lambda a: self.backend.finalize(
                            contract,
                            tuple(bundles),
                            tuple(receipts),
                            a,
                        ),
                        final=True,
                    )
                    # No model, journal or other external await after this boundary.
                    if type(finalized) is not FinalizedEvidence:
                        raise _Stop("invalid_final_guard")
                    if (
                        type(finalized.bundles) is not tuple
                        or len(finalized.bundles) > len(bundles)
                        or type(finalized.valid_receipts) is not tuple
                        or len(finalized.valid_receipts) > len(receipts)
                    ):
                        raise _Stop("unbounded_final_guard")
                    final_records = evidence_records(finalized.bundles)
                    final_keys = {r.key for r in final_records}
                    if not final_keys <= {r.key for r in records}:
                        raise _Stop("final_guard_injected_evidence")
                    if not all(set(keys) <= final_keys for keys in support.values()):
                        return session.result(
                            "partial",
                            "evidence_changed_before_delivery",
                            missing=tuple(requirements),
                        )
                    valid = {r.requirement_id: r for r in finalized.valid_receipts if r in receipts}
                    if any(
                        r.kind in ("enumeration", "absence") and r.id not in valid
                        for r in contract.requirements
                    ):
                        return session.result(
                            "partial",
                            "coverage_changed_before_delivery",
                            missing=tuple(requirements),
                        )
                    for req in contract.requirements:
                        if req.id in valid and (
                            not set(valid[req.id].evidence_keys) <= final_keys
                            or req.kind == "absence"
                            and valid[req.id].source_versions
                        ):
                            return session.result(
                                "partial", "closed_world_condition_unsatisfied", missing=(req.id,)
                            )
                    answer = self._render(contract, assessment, temporal, valid)
                    if not answer.strip():
                        raise _Stop("empty_answer")
                    session.events.append(
                        ResearchEvent(
                            "delivery", "validated", evidence_keys=tuple(sorted(final_keys))
                        )
                    )
                    return session.result(
                        "answered",
                        "supported_by_delivered_evidence",
                        answer=answer,
                        records=final_records,
                        receipts=tuple(valid.values()),
                    )
                if round_index + 1 == contract.limits.max_rounds:
                    raise _Stop("round_budget_exhausted")
                gaps = assessment.gaps
                if self.planner is not None:
                    plan = await session.call(
                        "planner",
                        lambda a, assessment=assessment, records=records: self.planner(
                            deepcopy(contract), deepcopy(assessment), deepcopy(records), a
                        ),
                        inputs=(
                            asdict(contract),
                            asdict(assessment),
                            tuple(asdict(r) for r in records),
                        ),
                        output=contract.limits.reader_output_tokens,
                        models=1,
                        overhead=getattr(self.planner, "input_overhead_bytes", 0),
                    )
                    if type(plan) is not ResearchPlan:
                        raise _Stop("invalid_research_plan")
                    gaps = plan.gaps
                if not gaps:
                    gaps = tuple(
                        EvidenceGap(
                            rid,
                            "census"
                            if requirements[rid].kind in ("enumeration", "absence")
                            else requirements[rid].allowed_routes[0],
                        )
                        for rid in missing
                    )
                self._gaps(gaps, requirements, missing)
                pending = tuple(g for g in gaps if (g.requirement_id, g.route) not in attempted)
                if not pending:
                    raise _Stop("no_untried_search")
            raise _Stop("round_budget_exhausted")
        except _Stop as stop:
            session.events.append(ResearchEvent("stop", str(stop)))
            # Retained evidence is diagnostic only and is not redelivered after
            # an interrupted run, because it has not passed the final boundary.
            return session.result("partial", str(stop), missing=missing)

    async def _resolve_temporal(self, session, requirements, records, support):
        contract = session.contract
        needed = tuple(r for r in contract.requirements if r.kind == "temporal" and r.id in support)
        if not needed or self.temporal_resolver is None:
            return {}
        values = await session.call(
            "temporal_policy",
            lambda allowance: self.temporal_resolver(
                deepcopy(contract), deepcopy(records), allowance
            ),
        )
        if type(values) is not tuple or len(values) > len(needed):
            raise _Stop("invalid_temporal_policy")
        temporal = {}
        for value in values:
            if type(value) is not TemporalResolution or value.requirement_id in temporal:
                raise _Stop("invalid_temporal_policy")
            req = requirements.get(value.requirement_id)
            if (
                req is None
                or req.kind != "temporal"
                or value.precision != req.precision
                or value.approximate
                and not contract.allow_approximate
                or not set(value.evidence_keys) <= set(support.get(req.id, ()))
            ):
                raise _Stop("precision_policy_mismatch")
            temporal[req.id] = value
        return temporal

    @staticmethod
    def _support(assessment, requirements, records):
        keys, support = {r.key for r in records}, {}
        for use in assessment.supports:
            if (
                use.requirement_id not in requirements
                or use.requirement_id in support
                or not set(use.evidence_keys) <= keys
            ):
                raise _Stop("invalid_reader_support")
            req = requirements[use.requirement_id]
            if not use.evidence_keys and req.kind not in ("enumeration", "absence"):
                raise _Stop("uncited_reader_support")
            support[use.requirement_id] = use.evidence_keys
        EvidenceResearchLoop._gaps(assessment.gaps, requirements, tuple(requirements))
        return support

    @staticmethod
    def _gaps(gaps, requirements, missing):
        if type(gaps) is not tuple or len(gaps) > 16:
            raise _Stop("invalid_gap_plan")
        seen = set()
        for gap in gaps:
            if (
                type(gap) is not EvidenceGap
                or gap.requirement_id not in missing
                or gap.requirement_id not in requirements
                or gap.route not in requirements[gap.requirement_id].allowed_routes
                or (gap.requirement_id, gap.route) in seen
            ):
                raise _Stop("invalid_gap_plan")
            seen.add((gap.requirement_id, gap.route))

    @staticmethod
    def _render(contract, assessment, temporal, receipts):
        # Closed counts and time values are never copied from the reader's prose.
        if all(r.kind == "fact" for r in contract.requirements):
            return assessment.answer
        values = []
        for req in contract.requirements:
            if req.kind == "enumeration":
                value = (
                    f"{len(receipts[req.id].source_versions)} recorded rows "
                    f"in {req.coverage_domain}."
                )
            elif req.kind == "absence":
                value = f"No recorded rows in {req.coverage_domain}."
            elif req.kind == "temporal":
                result = temporal[req.id]
                value = ("Approximately: " if result.approximate else "") + result.value
            else:
                # Freeform fact prose cannot silently smuggle a different
                # complete count or a finer time claim into mixed output.
                value = "Supported by the cited evidence."
            values.append(req.id + ": " + value)
        return "\n".join(values)


__all__ = [
    "EvidenceResearchLoop",
    "QueryContract",
    "EvidenceRequirement",
    "EvidenceGap",
    "EvidenceUse",
    "ReaderAssessment",
    "ResearchPlan",
    "ResearchLimits",
    "ResearchResult",
    "CoverageReceipt",
    "TemporalResolution",
    "CallAllowance",
    "evidence_records",
]
