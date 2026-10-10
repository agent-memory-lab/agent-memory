"""A9 execution adapter for governed raw-source project-memory workflows.

The trusted factory binds tools, membership, storage, endpoint and frozen arm
controls. The adapter consumes raw events rather than gold or preaccepted atoms,
uses the real durable host, and returns every recorded call and unfinished job.
No judge, license, tariff or zero provider bill is invented here.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from inspect import isawaitable

from ..derived.model import DerivedError
from ..domain import ForgetMode, ForgetRequest, MemoryEvent
from ..operations.memory_host import MemoryHost
from ..retrieval.model_contracts import canonical, digest
from .question_cost import CostPhase, ModelEvidence, PendingResponsibility


def semantic_configuration_sha256(host):
    """Pin both governed source-call roles and the complete extraction contract."""
    from ..consolidation.model_extraction import GovernedSourceCalls

    calls = tuple(
        getattr(adapter, "calls", None)
        for adapter in (host.pipeline.generator, host.pipeline.reviewer)
    )
    if any(not isinstance(call, GovernedSourceCalls) for call in calls):
        raise ValueError("governed_semantic_model_binding_required")
    return ModelEvidence.group_fingerprint(
        tuple(call.port.configuration.fingerprint for call in calls)
    )


@dataclass(frozen=True, slots=True)
class RawWorkflowBinding:
    host: MemoryHost
    ledger: object
    actor: str
    register: object
    close: object
    applied_controls_json: str
    question_ids: tuple[str, ...] = ()
    model_answers: object = None
    erase: object = None
    advance: object = None
    settle: object = None

    def __post_init__(self):
        if not isinstance(self.host, MemoryHost) or self.host.questions is None:
            raise ValueError("raw workflow requires a configured project memory host")
        if getattr(self.host, "project_bridge", None) is None or self.host.verification is None:
            raise ValueError("raw workflow requires automatic project handoff and verification")
        if (
            not callable(self.register)
            or not callable(self.close)
            or not callable(self.ledger.snapshot)
        ):
            raise ValueError("measured registration, cleanup and model ledger required")
        if type(self.actor) is not str or not 1 <= len(self.actor) <= 256:
            raise ValueError("trusted workflow actor required")
        controls = json.loads(self.applied_controls_json)
        if type(controls) is not dict:
            raise ValueError("host applied controls must be an object")
        if not 1 <= len(self.question_ids) <= 128 or len(set(self.question_ids)) != len(
            self.question_ids
        ):
            raise ValueError("bounded registered question inventory required")
        object.__setattr__(self, "applied_controls_json", canonical(controls))


class RawSourceWorkflowAdapter:
    def __init__(self, inputs, arm, *, binding_factory):
        if not callable(binding_factory) or any(
            item.gold_json is not None for item in inputs.items
        ):
            raise ValueError("gold-free inputs and trusted raw workflow binding factory required")
        self.adapter_revision = inputs.adapter_revision
        self.configuration_json = inputs.configuration_json
        self.initial_snapshot_json = inputs.initial_snapshot_json
        self.inputs, self.arm, self.binding_factory = inputs, arm, binding_factory
        self.controls = json.loads(arm.controls_json)
        self.acceptance_controls_sha256 = digest(self.controls)
        self.runtime = None
        self._initialized = False
        self.call_bindings = {}
        self._closed = False
        self.config = json.loads(inputs.configuration_json)
        limits = self.config.get("raw_workflow", {})
        self.work_steps = limits.get("work_steps", 16)
        self.drain_steps = limits.get("drain_steps", 128)
        for value in (self.work_steps, self.drain_steps):
            if type(value) is not int or not 1 <= value <= 4096:
                raise ValueError("raw workflow requires a finite work/drain limit")

    async def _initialize(self):
        if self.runtime is not None:
            if not self._initialized:
                raise ValueError("raw workflow initialization failed")
            return
        binding = self.binding_factory(self.inputs, self.arm)
        binding = await binding if isawaitable(binding) else binding
        if type(binding) is not RawWorkflowBinding:
            raise ValueError("typed raw workflow runtime binding required")
        self.runtime = binding  # Preserve cleanup ownership even if checks fail.
        if digest(json.loads(binding.applied_controls_json)) != self.acceptance_controls_sha256:
            raise ValueError("raw workflow host did not apply the frozen arm controls")
        audit = binding.host.pipeline.source_audit is not None
        if self.controls.get("source_omission_audit", False) != audit:
            raise ValueError("raw workflow source audit control differs from actual pipeline")
        relations = bool(binding.host.questions.admission.contract.relation_plans)
        if self.controls.get("relation_views", False) != relations:
            raise ValueError("raw workflow relation control differs from actual contract")
        if self.inputs.semantic_model.execution == "real":
            semantic_configuration_sha256(binding.host)
            if any(
                not self.inputs.semantic_model.accepts_configuration(
                    adapter.calls.port.configuration.fingerprint
                )
                for adapter in (binding.host.pipeline.generator, binding.host.pipeline.reviewer)
            ):
                raise ValueError("governed_semantic_model_configuration_changed")
        if self.inputs.generation_model.execution == "real":
            if (
                binding.model_answers is None
                or binding.model_answers.questions is not binding.host.questions
                or not self.inputs.generation_model.accepts_configuration(
                    binding.model_answers.answers.port.configuration.fingerprint
                )
            ):
                raise ValueError("governed_generation_model_configuration_changed")
        await binding.host.initialize()
        self._initialized = True

    async def _work(self, maximum):
        cycles = []
        for _ in range(maximum):
            result = await self.runtime.host.run_once()
            cycles.append(result)
            if result.get("stage_errors"):
                raise ValueError("raw_workflow_stage_failed")
            extraction = result.get("extraction") or {}
            refresh = result.get("refresh") or {}
            if (
                not extraction.get("claimed", 0)
                and not result.get("verification_scheduled", 0)
                and result.get("verification") in {None, "idle", "disabled"}
                and not refresh.get("claimed", 0)
            ):
                break
        return {"cycles": len(cycles), "metrics": await self.runtime.host.metrics()}

    async def _write(self, item):
        value = json.loads(item.payload_json)
        action = value.get("action", "source")
        host = self.runtime.host
        if action == "source":
            if set(value) - {"action", "event_id", "content", "occurred_at", "actor", "source_uri"}:
                raise ValueError("raw source cannot carry annotations, authority or membership")
            event = MemoryEvent(
                host.scope,
                "message",
                value["content"],
                id=value["event_id"],
                actor=value.get("actor", self.runtime.actor),
                occurred_at=datetime.fromisoformat(value["occurred_at"]),
                source_uri=value.get("source_uri"),
            )
            receipt = await host.submit(
                event, request_id=item.identity, producer_id="a9-raw-source"
            )
            return {"event_id": event.id, "durable": True, "duplicate": receipt.duplicate}
        if action == "revoke":
            from ..derived.model import ProcessingGrant

            if set(value) - {"action", "source_id", "expected_version"}:
                raise ValueError("unexpected raw workflow revocation fields")
            await host.questions.grant(
                ProcessingGrant(
                    value["source_id"],
                    (self.runtime.actor,),
                    (host.questions.admission.purpose,),
                    revoked=True,
                ),
                expected_version=value["expected_version"],
            )
            return {"revoked": True}
        if action == "erase" and callable(self.runtime.erase):
            await self.runtime.erase(
                ForgetRequest(host.scope, (value["source_id"],), mode=ForgetMode.ERASE)
            )
            return {"erased": True}
        if action == "advance" and callable(self.runtime.advance):
            result = self.runtime.advance(value["seconds"])
            if isawaitable(result):
                await result
            return {"clock_advanced": True}
        raise ValueError("unsupported_raw_workflow_operation")

    async def execute(self, phase, item):
        phase = CostPhase(phase)
        if self._closed:
            raise ValueError("raw workflow adapter closed")
        if item is not None and item.gold_json is not None:
            raise ValueError("gold must never reach the raw workflow adapter")
        if phase == CostPhase.COLD_START:
            await self._initialize()
            return {"initialized": True}
        if not self._initialized:
            raise ValueError("raw workflow initialization must succeed before work")
        if phase == CostPhase.REGISTRATION:
            result = self.runtime.register()
            if isawaitable(result):
                await result
            return {"registered_questions": len(self.runtime.question_ids)}
        if phase == CostPhase.WRITE:
            return await self._write(item)
        if phase in {CostPhase.BACKGROUND, CostPhase.DRAIN}:
            return await self._work(
                self.drain_steps if phase == CostPhase.DRAIN else self.work_steps
            )
        if phase == CostPhase.PREWARM:
            if self.controls.get("prewarm", False):
                for question_id in self.runtime.question_ids:
                    await self.runtime.host.questions.request(
                        question_id,
                        actor=self.runtime.actor,
                        dedupe_key="prewarm:" + question_id,
                    )
                return await self._work(self.work_steps)
            return {"prewarm_performed": False}
        if item is not None and item.is_request:
            value = json.loads(item.payload_json)
            questions = tuple(value.get("question_ids", (value.get("question_id"),)))
            if not 1 <= len(questions) <= 4 or not set(questions) <= set(self.runtime.question_ids):
                raise ValueError("request outside registered raw workflow questions")
            for question_id in questions:
                await self.runtime.host.questions.request(
                    question_id,
                    actor=self.runtime.actor,
                    dedupe_key="request:" + item.identity + ":" + question_id,
                )
            await self._work(self.work_steps)
            try:
                service = self.runtime.host.questions
                if self.controls.get("shared_proof", False):
                    answers = await service.read_many(questions, actor=self.runtime.actor)
                else:
                    answers = tuple(
                        [await service.read(q, actor=self.runtime.actor) for q in questions]
                    )
                explanations = ()
                if self.runtime.model_answers is not None:
                    before = {call["call_id"] for call in await self.runtime.ledger.snapshot()}
                    explanations = tuple(
                        [
                            await self.runtime.model_answers.answer(q, actor=self.runtime.actor)
                            for q in questions
                        ]
                    )
                    for call in await self.runtime.ledger.snapshot():
                        if call["call_id"] not in before:
                            self.call_bindings[call["call_id"]] = (item.identity,)
                return {
                    "answers": answers,
                    "explanations": explanations,
                    "source_kind": "raw_source_workflow",
                }
            except DerivedError as error:
                # Keep a denial distinct from a system failure; the independent
                # judge, not this adapter, decides whether the refusal is correct.
                return {"unavailable": error.code, "answers": ()}
        return await self.inspect(phase)

    async def inspect(self, phase):
        if self.runtime is None:
            return {"phase": CostPhase(phase).value, "initialized": False}
        return {"phase": CostPhase(phase).value, "metrics": await self.runtime.host.metrics()}

    async def finish(self):
        if self.runtime is None:
            return (), {}, (PendingResponsibility("runtime_uninitialized", (), None),)
        if self.runtime.settle is not None:
            result = self.runtime.settle(self.runtime.ledger)
            if isawaitable(result):
                await result
        host = self.runtime.host
        async with host.repository.unit_of_work() as uow:
            requests = await uow.retention_active(host.scope)
            verification = await uow.verification_active(host.scope)
            refresh = await uow.derived_records(host.scope, "refresh_demand")
        pending = []
        pending.extend(
            PendingResponsibility("extract:" + r["request_id"], (), None)
            for r in requests
            if r["status"] not in {"completed", "cancelled", "dead"}
        )
        pending.extend(PendingResponsibility(r["identity"], (), None) for r in verification)
        pending.extend(
            PendingResponsibility(r["identity"], (), None)
            for r in refresh
            if r["payload"].get("requested")
        )
        return await self.runtime.ledger.snapshot(), dict(self.call_bindings), tuple(pending)

    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self.runtime is not None:
            self.runtime.host.stop()
            result = self.runtime.close()
            if isawaitable(result):
                await result
