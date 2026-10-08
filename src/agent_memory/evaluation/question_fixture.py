"""Offline SQLite A9 integration fixture. Never a real-model benchmark.

This runs ProjectAdmission, QuestionService, its durable request/claim/publication
queue, and QuestionModelRuntime's actual input/dispatch/cache/delivery guards.
The only transport is a deterministic local stub. Evaluation-only controls force
full computation/single-use demand units or disable cache lookup; no authorization
guard is bypassed, and production code does not import this module.
"""

import asyncio
import json
import platform
import sqlite3
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from ..conditions import Condition
from ..consolidation.admission_runtime import AdmissionEngine
from ..consolidation.project_admission import ProjectAdmission, ProjectMembership
from ..consolidation.qualification import target_fingerprint
from ..derived.contracts import HostGrantAuthority
from ..derived.model import DerivedError, ProcessingGrant, digest
from ..derived.project_questions import ProjectDomainContract
from ..derived.question_materialize import materialize
from ..derived.question_model import QuestionContext
from ..derived.question_service import QuestionService
from ..domain import AtomDraft, ForgetMode, ForgetRequest, MemoryEvent, MemoryScope, SourceAuthority
from ..evidence_support import EvidenceLink, FieldSupport, SupportRange
from ..fact_qualification import FieldEvidence, SourceSpan
from ..operations.model_budget import BudgetAccount, ModelBudget
from ..operations.refresh_policy import RefreshLimits, RefreshPolicy
from ..retrieval.model_contracts import ModelConfiguration, ModelError, ModelResponse
from ..retrieval.ollama import OllamaPort
from ..retrieval.question_models import QuestionModelRuntime
from ..retrieval.question_router import QuestionRouter
from ..serialization import to_jsonable
from ..sqlite import SQLiteMemoryRepository
from .evidence import _digest
from .question_cost import (
    AnswerOutcome,
    CostPhase,
    ModelEvidence,
    PendingResponsibility,
    QuestionAcceptanceProfile,
    RequestResult,
)
from .question_experiment import (
    DEFAULT_CONTRASTS,
    ExperimentArm,
    ExperimentContrast,
    ExperimentPlan,
    WorkItem,
    canonical,
    run_experiment,
)
from .question_statistics import BootstrapProtocol

REVISION = "sqlite-question-runtime-fixture/1"
ACTOR = "fixture:reader"
TEMPLATE = (
    "Return only matched question rows; preserve empty, unknown and other nonresolved statuses."
)
JUDGE = _digest("fixture-filtered-fields-explicit-qualifier-and-denial/4")
_ACTIVE = False


def _code_fingerprint():
    """Freeze installed Python/SQL source bytes, independent of checkout location."""
    root = Path(__file__).parents[1]
    return _digest(
        [
            (path.relative_to(root).as_posix(), _digest(path.read_bytes().hex()))
            for path in sorted(root.rglob("*"))
            if path.suffix in {".py", ".sql"}
        ]
    )


def model_configuration():
    return ModelConfiguration(
        provider="synthetic-local",
        endpoint="http://localhost:1",
        account="fixture",
        region="no-network",
        processing_policy="synthetic-only/1",
        model="fixture-owner",
        model_revision=_digest("fixture-owner/1"),
        runtime_manifest_sha256=_digest(REVISION),
        overflow_guard_sha256=_digest("byte-limit/1"),
        tokenizer_revision="not-measured",
        prompt_revision="1",
        template_sha256=digest(TEMPLATE),
        options_json=canonical({"num_ctx": 32768, "num_predict": 128}),
        output_schema_json="{}",
        output_revision="text/1",
        language="en",
        max_input_bytes=131072,
        max_output_bytes=32768,
        timeout_seconds=5,
    )


class _LocalPort:
    def __init__(self, configuration):
        self.configuration, self.fail = configuration, False
        self.payload_hashes = []

    async def generate(self, sealed):
        self.payload_hashes.append(sealed.payload_sha256)
        if self.fail:
            raise ModelError("synthetic_injected_transport_failure")
        result = json.loads(json.loads(sealed.payload_json)["messages"][1]["content"])
        values = [
            value
            for row in result["result"]["rows"]
            if row["matches"] is True
            for field in row["fields"]
            for value in field["known_values"]
        ]
        status = result["answer_status"]
        rendered = canonical(values) if status == "resolved" else status
        if status == "empty":
            rendered = "empty_known_scope"
        qualifiers = {
            canonical(
                {
                    "conditions": candidate["fact"]["conditions"],
                    "exceptions": candidate["fact"]["exceptions"],
                }
            )
            for row in result["result"]["rows"]
            for field in row["fields"]
            for candidate in field["candidates"]
            if candidate["fact"]["conditions"] or candidate["fact"]["exceptions"]
        }
        if qualifiers:
            rendered = canonical(
                dict(
                    status=status,
                    values=values if status == "resolved" else [],
                    qualifiers=[json.loads(value) for value in sorted(qualifiers)],
                )
            )
        # No fabricated model tokens, duration or dollar bill.
        return ModelResponse(rendered, None, None, None)


class SQLiteQuestionFixture:
    adapter_revision = REVISION

    def __init__(self, plan, arm, *, _real_binding=False, _settle_calls=None):
        self.real_model, self.settle_calls = _real_binding, _settle_calls
        if not _real_binding and (
            plan.dataset_kind != "synthetic" or plan.generation_model.execution != "stub"
        ):
            raise ValueError("local fixture cannot claim real data or model execution")
        self.configuration_json, self.initial_snapshot_json = (
            plan.configuration_json,
            plan.initial_snapshot_json,
        )
        self.config = json.loads(self.configuration_json)
        self.model_configuration = ModelConfiguration(**self.config["model"])
        self.public_template = self.config["public_template"] if _real_binding else TEMPLATE
        self.review_reference = (
            self.config["host_review_reference"] if _real_binding else "fixture-review/1"
        )
        if not _real_binding and self.config["model"] != asdict(model_configuration()):
            raise ValueError("fixture model differs from frozen configuration")
        if plan.generation_model.configuration_sha256 != self.model_configuration.fingerprint:
            raise ValueError("fixture model declaration differs from executed model")
        self.plan, self.arm = plan, arm
        self.controls = json.loads(arm.controls_json)
        self.initial = json.loads(self.initial_snapshot_json)
        self.questions = self.initial.get(
            "questions",
            [
                {
                    "id": project,
                    "project": project,
                    "question": "owner",
                    "alias": "Owner " + project,
                }
                for project in ("project-a", "project-b")
            ],
        )
        self.now = datetime.fromisoformat(self.config["start_at"])
        self.temp = None
        self.clock_patches = None
        self.scope = MemoryScope("a9-synthetic", user_id="reader", session_id="fixture")
        self.sources, self.bindings, self.seen_calls = {}, {}, set()
        self.unit_sequence = 0
        self.operations, self.delivered_certificates = [], set()
        self.pending = []

    async def _register(self):
        self.contract = ProjectDomainContract(
            "fixture-projects", "1", "review/1", "owner", ("active", "paused", "blocked", "done")
        )
        memberships = tuple(
            ProjectMembership(**value)
            for value in self.initial.get(
                "memberships",
                [
                    {
                        "binding_id": project,
                        "registry_revision": "1",
                        "project_id": project,
                        "entity_id": project,
                    }
                    for project in ("project-a", "project-b")
                ],
            )
        )
        self.authority = SourceAuthority(
            "fixture-system",
            "tool_observation",
            tuple(sorted({member.entity_id for member in memberships})),
            tuple(spec.predicate for spec in self.contract.predicate_specs),
        )
        if self.real_model:
            self.authority = SourceAuthority(**self.initial["source_authority"])
        self.admission = ProjectAdmission(
            AdmissionEngine(self.repository),
            self.scope,
            principal=ACTOR,
            contract=self.contract,
            authorities=(self.authority,),
            memberships=memberships,
            reviewer_version=self.review_reference,
            clock=lambda: self.now,
            authority_id="fixture-host",
            authority_min_version=1,
        )
        authority = HostGrantAuthority(
            "fixture-host", (ACTOR,), self.now + timedelta(days=2), purposes=("project_questions",)
        )
        async with self.repository.unit_of_work() as uow:
            epoch = await uow.retention_epoch(self.scope)
            await uow.derived_put(
                self.scope,
                "authority",
                authority.id,
                dict(
                    spec=authority.payload(),
                    version=1,
                    epoch=epoch,
                    fingerprint=digest(authority.payload()),
                ),
            )
        self.service = QuestionService(
            self.admission,
            QuestionContext(
                "fixture-host",
                "1",
                self.initial.get("attributes", {}),
                self.now + timedelta(days=1),
            ),
            limits=RefreshLimits(**self.config["budget"]["refresh_limits"]),
        )
        if self.controls["full_compute"]:

            async def no_baseline(uow, definition, proof, *, expected_head=None):
                # Disable reuse BEFORE reading any old processing body. Retain
                # only the CAS head metadata required for guarded publication.
                header = (
                    expected_head
                    if expected_head is not None
                    else await uow.derived_get(
                        self.scope,
                        "question_head",
                        definition["facet_id"],
                    )
                )
                return dict(
                    expected_head=header,
                    generation_safe=False,
                    generation_until=None,
                    delta_state=None,
                    previous_content=None,
                    change_logs={},
                )

            self.service._baseline = no_baseline

            def force_full(snapshot):
                owned = deepcopy(snapshot)
                owned.update(delta_state=None, previous_content=None, generation_safe=False)
                return materialize(self.contract, owned)

            self.service.prepare = force_full
        if self.controls["single_use_demand"]:
            # Evaluation-only single-use unit identity forces actual queue work on
            # each demand. The original complete input proof and all guards remain.
            original = self.service._unit
            self.service._unit = lambda definition, proof: {
                **original(definition, proof),
                "evaluation_demand": self.unit_sequence,
            }
        self.ledger = ModelBudget(self.repository)
        accounts = await self.ledger.configure((BudgetAccount("fixture", "run", "USD", "1", None),))
        self.port = (
            OllamaPort(self.model_configuration)
            if self.real_model
            else _LocalPort(self.model_configuration)
        )
        self.models = QuestionModelRuntime(
            self.service,
            self.port,
            public_template=self.public_template,
            account_keys=accounts,
            validate_output=lambda value, _: type(value) is str,
        )
        if not self.controls["exact_cache"]:

            async def no_cache(_):
                return None

            self.models.answers._cached = no_cache
        for source in self.initial["sources"]:
            await self._write(source)
        for question in self.questions:
            await self.service.register(
                question["id"],
                question["project"],
                question["question"],
                readers=(ACTOR,),
                aliases=(question.get("alias", "Question " + question["id"]),),
                overdue_only=question.get("overdue_only", False),
                refresh_policy=RefreshPolicy(mode=self.controls["refresh_mode"]),
            )
        await self.service.queue.initialize()
        return {
            "registered": len(self.questions),
            "model_configuration_sha256": self.port.configuration.fingerprint,
            "controls": self.controls,
        }

    async def _write(self, payload):
        action = payload.get("action", "add")
        if action == "advance":
            self.now += timedelta(seconds=payload["seconds"])
            return {"clock_advanced": payload["seconds"]}
        if action == "grant":
            source = self.sources[payload["source"]]
            source["grant_version"] += 1
            await self.service.grant(
                ProcessingGrant(
                    payload["source"],
                    (ACTOR,),
                    ("project_questions",),
                    revoked=payload.get("revoked", False),
                ),
                expected_version=source["grant_version"] - 1,
            )
            self.now += timedelta(milliseconds=self.config["event_tick_milliseconds"])
            return {"grant_version": source["grant_version"]}
        if action == "withdraw":
            source = self.sources[payload["source"]]
            await self.admission.withdraw(
                source["candidate"],
                expected_version=source["version"],
                review_id="withdraw:" + payload["source"],
                reasons=("fixture",),
            )
            self.now += timedelta(milliseconds=self.config["event_tick_milliseconds"])
            source["version"] += 1
            return {"withdrawn": payload["source"]}
        if action == "move":
            source = self.sources[payload["source"]]
            source["version"] = await self.admission.replace_membership(
                source["candidate"],
                expected_version=source["version"],
                membership_id=payload["membership"],
            )
            # Moving membership revokes semantic qualification. Only an explicit
            # frozen host re-review may make it qualified in the new project.
            if payload.get("rereview", False):
                await self._qualify(
                    payload["source"], source["version"], suffix=payload["membership"]
                )
            self.now += timedelta(milliseconds=self.config["event_tick_milliseconds"])
            return {
                "membership_moved": payload["source"],
                "rereviewed": payload.get("rereview", False),
            }
        if action == "erase":
            result = await self.repository.forget(
                ForgetRequest(
                    self.scope,
                    memory_ids=(payload["source"],),
                    mode=ForgetMode.ERASE,
                )
            )
            self.now += timedelta(milliseconds=self.config["event_tick_milliseconds"])
            return {"erased_source": payload["source"], "affected_events": result.affected_events}
        if action != "add":
            raise ValueError("unsupported fixture event")
        identity, project, value = payload["source"], payload["project"], payload["value"]
        subject, predicate = (
            payload.get("subject", project),
            payload.get("predicate", "project.owner"),
        )
        conditions = tuple(Condition(**condition) for condition in payload.get("conditions", ()))
        exceptions = tuple(Condition(**condition) for condition in payload.get("exceptions", ()))
        raw_conditions = tuple(canonical(to_jsonable(condition)) for condition in conditions)
        raw_exceptions = tuple(canonical(to_jsonable(condition)) for condition in exceptions)
        text = (
            payload["text"]
            if self.real_model
            else (
                f"{subject} {predicate} {value} "
                f"conditions={raw_conditions} exceptions={raw_exceptions}"
            )
        )
        start = datetime.fromisoformat(payload.get("valid_from", "2026-01-01T00:00:00+00:00"))
        until = datetime.fromisoformat(payload["valid_to"]) if payload.get("valid_to") else None
        occurred = (
            datetime.fromisoformat(payload["occurred_at"]) if payload.get("occurred_at") else start
        )
        event = MemoryEvent(
            self.scope, "message", text, id=identity, occurred_at=occurred, ingested_at=self.now
        )
        span = (
            SourceSpan(identity, **payload["reviewed_span"])
            if self.real_model
            else SourceSpan(identity, 0, len(text), text)
        )
        fields = (
            "subject_id",
            "predicate",
            "value",
            "valid_from",
            *(("valid_to",) if until else ()),
            *(("conditions",) if conditions else ()),
            *(("exceptions",) if exceptions else ()),
        )
        draft = AtomDraft(
            subject,
            predicate,
            value,
            text,
            text,
            valid_from=start,
            valid_to=until,
            conditions=raw_conditions,
            exceptions=raw_exceptions,
            field_evidence=tuple(FieldEvidence(field, ((span,),)) for field in fields),
        )
        receipt = await self.admission.stage_source(
            event,
            (draft,),
            source_authority_id=self.authority.source_id,
            request_id="stage:" + identity,
            membership_ids=(payload.get("membership", project),),
        )
        async with self.repository.unit_of_work() as uow:
            await uow.derived_put(
                self.scope,
                "grant",
                identity,
                {
                    **ProcessingGrant(identity, (ACTOR,), ("project_questions",)).payload(),
                    "version": 1,
                    "authority_id": "fixture-host",
                    "authority_version": 1,
                },
            )
        link = EvidenceLink(
            "support:" + identity,
            fields,
            target_fingerprint(draft),
            span,
            self.authority,
            SupportRange(start, until),
        )
        self.sources[identity] = {
            "candidate": receipt.candidate_ids[0],
            "grant_version": 1,
            "version": 2,
            "link": link,
            "fields": fields,
            "conditions": conditions,
            "exceptions": exceptions,
            "review_reference": payload.get("review_reference", self.review_reference),
        }
        await self._qualify(identity, 2)
        await self.models.authority.allow_processing(
            identity,
            readers=(ACTOR,),
            purposes=("project_questions",),
            expires_at=self.now + timedelta(hours=12),
        )
        self.now += timedelta(milliseconds=self.config["event_tick_milliseconds"])
        return {"source_added": identity}

    async def _qualify(self, identity, expected, suffix="initial"):
        source = self.sources[identity]
        link = source["link"]
        source["version"] = await self.admission.qualify(
            source["candidate"],
            expected_version=expected,
            review_id="review:" + digest([identity, suffix, source["review_reference"]]),
            applicability_id="explicit",
            conditions=source["conditions"],
            exceptions=source["exceptions"],
            links=(link,),
            field_support=tuple(FieldSupport(field, ((link.id,),)) for field in source["fields"]),
        )

    async def _work(self, limit):
        completed = []
        for _ in range(limit):
            lease = await self.service.queue.claim("fixture-worker", lease_seconds=30)
            if lease is None:
                self.operations.append({"event": "claim_empty", "phase": self.phase.value})
                # claim may first re-key an incompatible dirty demand. A bounded
                # next claim lets the real queue process that successor.
                async with self.repository.unit_of_work() as uow:
                    demands = await uow.derived_records(self.scope, "refresh_demand")
                if any(
                    row["payload"].get("requested")
                    and row["payload"].get("status") in {"pending", "retry", "deferred"}
                    for row in demands
                ):
                    continue
                break
            try:
                async with self.repository.unit_of_work() as uow:
                    execution = await uow.derived_get(
                        self.scope, "refresh_execution", lease.task.payload["refresh_execution"]
                    )
                    demand = await uow.derived_get(
                        self.scope, "refresh_demand", execution["demand_id"]
                    )
                claimed_times = [
                    datetime.fromisoformat(demand["obligation_at"][key])
                    for key in execution["claimed"]
                ]
                snapshot = await self.service.snapshot(lease.task)
                prepared = self.service.prepare(snapshot)
                await self.service.publish(lease.task, snapshot, prepared)
                await self.service.queue.complete(lease)
                row = dict(
                    event="refresh_completed",
                    phase=self.phase.value,
                    unit_id=snapshot["unit_id"],
                    content_id=prepared["content_id"],
                    certificate_id=prepared["certificate_id"],
                    claimed_obligations=len(execution["claimed"]),
                    oldest_obligation_lag_ms=max(
                        ((self.now - at).total_seconds() * 1000 for at in claimed_times), default=0
                    ),
                    compute_mode=prepared["trace"]["compute_mode"],
                    compute_trace=prepared["trace"],
                    source_count=len(snapshot["proof"]["sources"]),
                    published_at=self.now.isoformat(),
                )
                completed.append(row)
                self.operations.append(row)
            except Exception as error:
                await self.service.queue.fail(lease, error)
                self.operations.append({"event": "refresh_failed", "phase": self.phase.value})
                raise
        return completed

    async def _answer(self, project, identity, *, use_model=True, fail=False):
        if self.controls["route_alias"]:
            question = next(question for question in self.questions if question["id"] == project)
            routed = await QuestionRouter(self.service).route(
                question.get("alias", "Question " + project),
                actor=ACTOR,
            )
            self.operations.append(
                {"event": "route", "route": routed["route"], "phase": self.phase.value}
            )
            if routed["route"] != "question" or routed["question_id"] != project:
                raise ValueError("fixture route differs from frozen request")
        if self.controls["single_use_demand"]:
            self.unit_sequence += 1
        try:
            result = await self.service.read(project, actor=ACTOR)
            reused = True
        except ValueError:
            receipt = await self.service.request(project, actor=ACTOR, dedupe_key=identity)
            self.operations.append(
                {
                    "event": "refresh_requested",
                    "target_id": receipt["target_id"],
                    "phase": self.phase.value,
                }
            )
            await self._work(self.config["budget"]["work_steps"])
            result = await self.service.read(project, actor=ACTOR)
            reused = False
        if not use_model:
            return result
        self.models.answers.cost_phase = self.phase.value
        if self.real_model and fail:
            raise ValueError("synthetic fault injection is not a real-model workload operation")
        if not self.real_model:
            self.port.fail = fail
        before = {row["call_id"] for row in await self.ledger.snapshot()}
        try:
            answer = await self.models.answer(project, actor=ACTOR)
        finally:
            if not self.real_model:
                self.port.fail = False
            for row in await self.ledger.snapshot():
                if row["call_id"] not in before:
                    self.bindings[row["call_id"]] = (
                        (identity,) if identity in self.plan.workload.request_ids else ()
                    )
        if identity in self.plan.workload.request_ids:
            self.delivered_certificates.add(result["certificate_revision_id"])
        try:
            rendered = json.loads(answer["text"])
        except ValueError:
            rendered = None
        rendered_qualifiers = rendered.get("qualifiers") if isinstance(rendered, dict) else []
        output = dict(
            text=answer["text"],
            safe=True,
            fresh=result["availability_status"] == "valid",
            evidence_complete=bool(result["citations"])
            or (result["answer_status"] == "empty" and result["coverage"]["candidates_complete"]),
            evidence_source_ids=sorted({span["source_event_id"] for span in result["citations"]}),
            question_reused=reused,
            cache_hit=answer["cache_hit"],
            content_id=result["content_revision_id"],
            certificate_id=result["certificate_revision_id"],
            call_id=answer["call_id"],
            compute_mode=result["compute_mode"],
            answer_status=result["answer_status"],
            matched_ids=result["result"].get("matched_ids", []),
            scope_basis=result["result"]["scope_basis"],
            world_negative=result["result"]["world_negative"],
            rendered_qualifiers=rendered_qualifiers,
        )
        self.operations.append({"event": "guarded_delivery", "phase": self.phase.value, **output})
        return output

    async def execute(self, phase, item):
        self.phase = phase
        if phase == CostPhase.COLD_START:
            global _ACTIVE
            if _ACTIVE:
                raise RuntimeError("fixture requires a dedicated sequential process")
            if self.config["code"] != _code_fingerprint():
                raise ValueError("runtime source changed after plan freeze")
            _ACTIVE = True
            # Every runtime writer and scheduler must share the same clock.
            # Restored on close; this fixture may not run beside production work.
            from .. import domain, sqlite
            from ..consolidation import admission_runtime
            from ..derived import subscriptions
            from ..retrieval import temporal_history

            self.clock_patches = ExitStack()
            for module in (domain, sqlite, admission_runtime, subscriptions, temporal_history):
                self.clock_patches.enter_context(patch.object(module, "utc_now", lambda: self.now))
            self.temp = TemporaryDirectory(prefix="a9-fixture-")
            self.repository = SQLiteMemoryRepository(Path(self.temp.name) / "fixture.db")
            await self.repository.initialize()
            return {"backend": "sqlite", "isolated": True}
        if phase == CostPhase.REGISTRATION:
            return await self._register()
        if phase == CostPhase.PREWARM:
            if not self.controls["prewarm"]:
                return {"policy": "on_demand", "prewarm_performed": False}
            # project-b is intentionally warmed before any request to show that
            # maintenance cost exists independently from hits.
            for question in self.questions:
                await self._answer(question["id"], "prewarm:" + question["id"])
            return {"prewarm_performed": True}
        if phase == CostPhase.WRITE:
            return await self._write(json.loads(item.payload_json))
        if phase == CostPhase.DEPENDENCY:
            if self.controls["eager_after_write"]:
                await self._work(self.config["budget"]["work_steps"])
            async with self.repository.unit_of_work() as uow:
                rows = await uow.derived_records(self.scope, "refresh_demand")
            return {
                "finite_dirty_obligations": sum(
                    len(row["payload"].get("requested", ())) for row in rows
                )
            }
        if phase == CostPhase.BACKGROUND:
            if self.controls["refresh_mode"] == "on_demand":
                return await self.inspect(phase)
            return {"completed": await self._work(self.config["budget"]["work_steps"])}
        if item and item.is_request:
            payload = json.loads(item.payload_json)
            try:
                return await self._answer(
                    payload.get("question_id", payload.get("project")),
                    item.identity,
                    fail=payload.get("fail_model", False),
                )
            except (DerivedError, ModelError) as error:
                if error.code not in {
                    "project_processing_denied",
                    "project_processing_grant_expired",
                    "model_processing_unauthorized",
                    "processing_unauthorized",
                    "question_erased",
                    "derived_read_denied",
                    "project_source_unavailable",
                    "question_original_generation_unavailable",
                }:
                    raise
                output = {
                    "denied": error.code,
                    "safe": True,
                    "fresh": True,
                    "evidence_complete": False,
                    "cache_hit": False,
                    "rendered_qualifiers": [],
                }
                self.operations.append({"event": "guarded_denial", "phase": phase.value, **output})
                return output
        if phase == CostPhase.DRAIN:
            # Stop injection and drain only existing finite responsibility. A
            # dirty definition alone is not debt; erase may have canceled its
            # policy and obligations. Do not invent new work or resurrect it.
            async with self.repository.unit_of_work() as uow:
                definitions = await uow.derived_records(self.scope, "definition")
                demands = await uow.derived_records(self.scope, "refresh_demand")
            pending_facets = {
                row["payload"].get("facet_id") for row in demands if row["payload"].get("requested")
            }
            for row in definitions:
                if row["identity"] in pending_facets and not row["payload"].get("disabled"):
                    project = row["payload"]["spec"]["question_id"]
                    await self.service.request(project, actor=ACTOR, dedupe_key="drain:" + project)
            return {"completed": await self._work(self.config["drain_policy"]["max_steps"])}
        return await self.inspect(phase)

    async def inspect(self, phase):
        calls = await self.ledger.snapshot()
        return {
            "inspection": phase.value,
            "recorded_model_calls": len(calls),
            "failed_calls": sum(row["outcome"] == "failed" for row in calls),
        }

    async def finish(self):
        if self.settle_calls is not None:
            from inspect import isawaitable

            settlement = self.settle_calls(self.ledger)
            if isawaitable(settlement):
                await settlement
        async with self.repository.unit_of_work() as uow:
            demands = await uow.derived_records(self.scope, "refresh_demand")
        pending = tuple(
            PendingResponsibility(row["identity"], (), None)
            for row in demands
            if row["payload"].get("requested")
        )
        self.pending = pending
        for operation in self.operations:
            if operation["event"] == "refresh_completed":
                operation["unused_by_foreground"] = (
                    operation["certificate_id"] not in self.delivered_certificates
                )
        return await self.ledger.snapshot(), self.bindings, pending

    async def close(self):
        global _ACTIVE
        try:
            if self.temp:
                self.temp.cleanup()
        finally:
            if self.clock_patches:
                self.clock_patches.close()
                _ACTIVE = False


def fixture_judge(item, output, latency):
    gold = json.loads(item.gold_json)
    if "denied" in output:
        correct = not gold["answerable"] and output["denied"] in gold.get("denial_codes", ())
    else:
        correct = output["text"] == gold.get("text")
        for name in ("answer_status", "matched_ids", "scope_basis", "world_negative"):
            if name in gold:
                correct = correct and output[name] == gold[name]
    outcome = (
        AnswerOutcome.CORRECT_ANSWER if gold["answerable"] else AnswerOutcome.REASONABLE_UNKNOWN
    )
    if not correct:
        outcome = AnswerOutcome.INCORRECT_ANSWER
    evidence = output["evidence_complete"] and set(gold.get("required_sources", ())) <= set(
        output.get("evidence_source_ids", ())
    )
    safe = output["safe"] and not set(gold.get("forbidden_sources", ())) & set(
        output.get("evidence_source_ids", ())
    )
    qualifiers_preserved = "qualifiers" in gold and gold["qualifiers"] == output.get(
        "rendered_qualifiers"
    )
    return RequestResult(
        item.identity,
        item.group_id,
        gold["answerable"],
        outcome,
        evidence,
        qualifiers_preserved,
        output["fresh"],
        safe,
        latency,
        gold.get("diagnostic_only", False),
    )


fixture_judge.configuration_sha256 = JUDGE


def fixture_plan(*, seed=917, resamples=200, include_ablations=True):
    config = dict(
        semantics="reviewed-project-owner/1",
        security="current-runtime-guards/1",
        budget={"work_steps": 8, "refresh_limits": RefreshLimits().payload()},
        hardware={
            "platform": platform.system(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        drain_policy={"max_steps": 32, "mode": "finite-stop-injection-then-drain/1"},
        model=asdict(model_configuration()),
        start_at="2100-01-01T00:00:00+00:00",
        event_tick_milliseconds=1,
        code=_code_fingerprint(),
        backend={"name": "sqlite", "version": sqlite3.sqlite_version},
    )
    items = []

    def request(
        identity, project="project-a", value="Alice", phase=CostPhase.FOREGROUND, fail=False
    ):
        items.append(
            WorkItem(
                identity,
                phase,
                canonical({"project": project, "fail_model": fail}),
                project,
                canonical({"answerable": True, "text": canonical([value]), "qualifiers": []}),
            )
        )

    request("a1")
    request("a2")
    request("b1", "project-b", "Bob")
    items.append(
        WorkItem("withdraw-a", CostPhase.WRITE, canonical({"action": "withdraw", "source": "a"}))
    )
    items.append(
        WorkItem(
            "add-a2",
            CostPhase.WRITE,
            canonical({"source": "a2", "project": "project-a", "value": "Carol"}),
        )
    )
    items.append(
        WorkItem("lag-window", CostPhase.WRITE, canonical({"action": "advance", "seconds": 2}))
    )
    items.append(WorkItem("batch-refresh", CostPhase.BACKGROUND, "{}"))
    request("failed", value="Carol", phase=CostPhase.FAILURE, fail=True)
    request("retry", value="Carol", phase=CostPhase.RETRY)
    request("a3", value="Carol")
    items.append(
        WorkItem("grant-same", CostPhase.WRITE, canonical({"action": "grant", "source": "a2"}))
    )
    items.append(WorkItem("proof-refresh", CostPhase.BACKGROUND, "{}"))
    request("a4", value="Carol")
    request("b2", "project-b", "Bob")
    # This successful refresh is never used by another foreground request.
    items.append(
        WorkItem("grant-unused", CostPhase.WRITE, canonical({"action": "grant", "source": "b"}))
    )
    model = model_configuration()
    controls = dict(
        full_compute=True,
        single_use_demand=False,
        exact_cache=False,
        prewarm=True,
        refresh_mode="on_change",
        route_alias=False,
        eager_after_write=False,
    )
    arms = (
        ExperimentArm(
            "on_demand_full",
            "on_demand_full",
            canonical(
                {
                    **controls,
                    "prewarm": False,
                    "single_use_demand": True,
                    "refresh_mode": "on_demand",
                }
            ),
        ),
        ExperimentArm("coalesced_full", "coalesced_full", canonical(controls)),
        ExperimentArm("delta_proof", "delta_proof", canonical({**controls, "full_compute": False})),
        ExperimentArm(
            "exact_cache",
            "exact_cache",
            canonical({**controls, "full_compute": False, "exact_cache": True}),
        ),
    )
    contrasts = (
        DEFAULT_CONTRASTS[0],
        replace(DEFAULT_CONTRASTS[1], varied_control="full_compute"),
        replace(DEFAULT_CONTRASTS[2], varied_control="exact_cache"),
        *DEFAULT_CONTRASTS[3:],
    )
    if include_ablations:
        arms += tuple(
            ExperimentArm(name, "coalesced_full", canonical({**controls, key: value}))
            for name, key, value in (
                ("eager_full", "eager_after_write", True),
                ("routed_full", "route_alias", True),
                ("cold_full", "refresh_mode", "on_demand"),
            )
        )
        contrasts += (
            ExperimentContrast("coalesced_full", "eager_full", "coalescing", "eager_after_write"),
            ExperimentContrast("routed_full", "coalesced_full", "routing", "route_alias"),
            ExperimentContrast(
                "coalesced_full", "cold_full", "static_hot_cold_policy", "refresh_mode"
            ),
        )
    return ExperimentPlan(
        "a9-local-fixture",
        REVISION,
        canonical(config),
        canonical(
            {
                "sources": [
                    {"source": "a", "project": "project-a", "value": "Alice"},
                    {"source": "b", "project": "project-b", "value": "Bob"},
                ]
            }
        ),
        tuple(items),
        "synthetic",
        None,
        JUDGE,
        ModelEvidence(_digest("none"), "not_used"),
        ModelEvidence(model.fingerprint, "stub"),
        BootstrapProtocol(seed, resamples, 0.95, 2, 2),
        QuestionAcceptanceProfile("a9-unapproved-fixture", "1", None, None, None),
        arms=arms,
        contrasts=contrasts,
    )


def lifecycle_plan(*, seed=918, resamples=200):
    """Compact finite-template/lifecycle corpus; annotations are synthetic only."""
    plan = fixture_plan(seed=seed, resamples=resamples, include_ablations=False)
    sources = [
        dict(
            source="owner-a",
            project="project-a",
            value="Alice",
            valid_to="2100-01-01T00:00:05+00:00",
        ),
        dict(source="owner-b", project="project-b", value="Bob"),
        dict(
            source="status",
            project="project-a",
            predicate="project.status",
            value="active",
            conditions=[dict(op="eq", attribute="region", value="EU")],
            exceptions=[dict(op="eq", attribute="holiday", value=True)],
        ),
    ]
    promise_values = ("Alice", "ship", "open", "2100-01-01T00:00:04+00:00")
    promise_fields = ("promisor", "action", "state", "deadline")
    for field, value in zip(promise_fields, promise_values, strict=True):
        sources.append(
            dict(
                source="promise-" + field,
                project="project-a",
                subject="promise-1",
                membership="promise-a",
                predicate="commitment." + field,
                value=value,
            )
        )
    for field, value in (("label", "Delay"), ("state", "open")):
        sources.append(
            dict(
                source="risk-" + field,
                project="project-a",
                subject="risk-1",
                membership="risk-a",
                predicate="risk." + field,
                value=value,
            )
        )
    members = [
        dict(binding_id=p, registry_revision="1", project_id=p, entity_id=p)
        for p in ("project-a", "project-b")
    ]
    members += [
        dict(binding_id=binding, registry_revision="1", project_id=project, entity_id=entity)
        for binding, project, entity in (
            ("promise-a", "project-a", "promise-1"),
            ("promise-b", "project-b", "promise-1"),
            ("risk-a", "project-a", "risk-1"),
        )
    ]
    questions = [
        dict(
            id=identity, project=project, question=question, overdue_only=question == "commitments"
        )
        for identity, project, question in (
            ("owner-a", "project-a", "owner"),
            ("owner-b", "project-b", "owner"),
            ("status-a", "project-a", "status"),
            ("promises-a", "project-a", "commitments"),
            ("promises-b", "project-b", "commitments"),
            ("risks-a", "project-a", "risks"),
        )
    ]
    items = []

    def request(identity, question, values=None, *, answerable=True, **gold):
        label = dict(
            answerable=answerable, text=canonical(values) if values else "unknown", qualifiers=[]
        )
        label.update(gold)
        group = next(q["project"] for q in questions if q["id"] == question)
        items.append(
            WorkItem(
                identity,
                CostPhase.FOREGROUND,
                canonical({"question_id": question}),
                group,
                canonical(label),
            )
        )

    def event(identity, **payload):
        items.append(WorkItem(identity, CostPhase.WRITE, canonical(payload)))

    request("owner-before", "owner-a", ["Alice"], required_sources=["owner-a"])
    status_qualifiers = [
        {
            "conditions": [{"op": "eq", "attribute": "region", "value": "EU", "children": []}],
            "exceptions": [{"op": "eq", "attribute": "holiday", "value": True, "children": []}],
        }
    ]
    request(
        "status-before",
        "status-a",
        required_sources=["status"],
        qualifiers=status_qualifiers,
        text=canonical(
            {"status": "resolved", "values": ["active"], "qualifiers": status_qualifiers}
        ),
    )
    request(
        "promise-before",
        "promises-a",
        text="empty_known_scope",
        matched_ids=[],
        answer_status="empty",
    )
    request(
        "risk-before",
        "risks-a",
        ["Delay", "open"],
        matched_ids=["risk-1"],
        required_sources=["risk-label", "risk-state"],
    )
    request("owner-b-before", "owner-b", ["Bob"], required_sources=["owner-b"])
    event("pure-time", action="advance", seconds=6)
    request("owner-expired", "owner-a", answerable=False, answer_status="unknown")
    request("promise-overdue", "promises-a", promise_values, matched_ids=["promise-1"])
    event(
        "late-owner",
        source="late-owner",
        project="project-a",
        value="Carol",
        valid_from="2025-01-01T00:00:00+00:00",
        occurred_at="2025-01-01T00:00:00+00:00",
    )
    request("late-observed", "owner-a", ["Carol"], required_sources=["late-owner"])
    for field in promise_fields:
        event(
            "move-" + field,
            action="move",
            source="promise-" + field,
            membership="promise-b",
            rereview=True,
        )
    request(
        "old-membership-empty",
        "promises-a",
        text="empty_known_scope",
        matched_ids=[],
        answer_status="empty",
    )
    request(
        "new-membership",
        "promises-b",
        promise_values,
        matched_ids=["promise-1"],
        required_sources=["promise-" + field for field in promise_fields],
    )
    event("revoke-risk", action="grant", source="risk-state", revoked=True)
    request(
        "revoked-denial",
        "risks-a",
        answerable=False,
        denial_codes=["project_processing_denied", "processing_unauthorized"],
    )
    event("restore-risk", action="grant", source="risk-state", revoked=False)
    request("restored-risk", "risks-a", ["Delay", "open"], matched_ids=["risk-1"])
    event("erase-owner", action="erase", source="late-owner")
    request("erased-denial", "owner-a", answerable=False, denial_codes=["question_erased"])
    return replace(
        plan,
        experiment_id="a9-local-lifecycle",
        initial_snapshot_json=canonical(
            {
                "sources": sources,
                "memberships": members,
                "questions": questions,
                "attributes": {"region": "EU", "holiday": False},
            }
        ),
        items=tuple(items),
    )


async def run_ollama_experiment(
    plan,
    judge,
    observer_factory,
    *,
    expected_plan_sha256,
    allow_real_model=False,
    settle_calls=None,
):
    """Explicit production-port binding over host-reviewed L1, not extraction.

    The caller needs prior permission to send the dataset to this exact endpoint.
    No endpoint/configuration, source text/review, gold, rate or bill is invented.
    Platform telemetry and authoritative invoice adapters remain host integration.
    """
    if allow_real_model is not True:
        raise ValueError("real model execution requires explicit opt-in")
    if plan.fingerprint != expected_plan_sha256:
        raise ValueError("experiment plan changed after freeze")
    if plan.dataset_kind != "licensed_real" or not plan.license_reference:
        raise ValueError("licensed dataset provenance required")
    if plan.generation_model.execution != "real" or plan.semantic_model.execution != "not_used":
        raise ValueError("this binding requires real generation and host-reviewed semantic inputs")
    config, initial = json.loads(plan.configuration_json), json.loads(plan.initial_snapshot_json)
    cfg = ModelConfiguration(**config["model"])
    if cfg.provider != "ollama" or cfg.fingerprint != plan.generation_model.configuration_sha256:
        raise ValueError("frozen Ollama configuration required")
    if digest(config.get("public_template")) != cfg.template_sha256 or not config.get(
        "host_review_reference"
    ):
        raise ValueError("frozen template and host review provenance required")
    if "source_authority" not in initial:
        raise ValueError("authenticated host source-authority registry required")
    SourceAuthority(**initial["source_authority"])
    if judge is None or getattr(judge, "configuration_sha256", None) != plan.judge_sha256:
        raise ValueError("frozen independent judge required")
    if any(item.is_request and item.gold_json is None for item in plan.items):
        raise ValueError("frozen gold required for every real-model request")
    for source in [
        *initial["sources"],
        *(json.loads(item.payload_json) for item in plan.items if item.phase == CostPhase.WRITE),
    ]:
        if source.get("action", "add") == "add" and not all(
            key in source and source[key]
            for key in ("text", "reviewed_span", "review_reference", "occurred_at")
        ):
            raise ValueError(
                "real sources require text, reviewed span, review and occurrence provenance"
            )
    if any(json.loads(item.payload_json).get("fail_model") for item in plan.items):
        raise ValueError("remove synthetic transport faults from real-model plans")
    if (
        plan.resource_pricing is None
        or plan.resource_pricing.tariff is None
        or plan.resource_pricing.observer_configuration_sha256 is None
        or observer_factory is None
    ):
        raise ValueError("frozen host resource observer and tariff required")
    if settle_calls is not None and (
        not config.get("billing_reconciler_sha256")
        or getattr(settle_calls, "configuration_sha256", None)
        != config.get("billing_reconciler_sha256")
    ):
        raise ValueError("settlement adapter differs from frozen billing configuration")
    adapters = {}

    def factory(inputs, arm):
        adapters[arm.name] = SQLiteQuestionFixture(
            inputs, arm, _real_binding=True, _settle_calls=settle_calls
        )
        return adapters[arm.name]

    report = await run_experiment(
        plan,
        factory,
        judge,
        expected_plan_sha256=expected_plan_sha256,
        observer_factory=observer_factory,
    )
    report["runtime_operations"] = {arm: adapter.operations for arm, adapter in adapters.items()}
    report["evidence_scope"] = (
        "host-configured Ollama generation over host-reviewed source annotations"
    )
    return report


async def run_fixture(plan=None):
    plan = plan or fixture_plan()
    adapters = {}

    def factory(frozen, arm):
        adapters[arm.name] = SQLiteQuestionFixture(frozen, arm)
        return adapters[arm.name]

    result = await run_experiment(
        plan, factory, fixture_judge, expected_plan_sha256=plan.fingerprint
    )
    result["runtime_operations"] = {arm: adapter.operations for arm, adapter in adapters.items()}
    result["evidence_scope"] = (
        "synthetic local SQLite runtime integration; "
        "no real models, tokens, bills or production benefit"
    )
    result["unmeasured_capabilities"] = [
        "adaptive_temperature_calibration",
        "real_semantic_model_quality",
        "real_generation_model_quality",
        "licensed_held_out_gold",
        "gpu_io_storage_network",
        "whole_cost_tariffs",
    ]
    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=917, help="synthetic demo protocol only")
    parser.add_argument("--resamples", type=int, default=200, help="synthetic demo protocol only")
    parser.add_argument("--scenario", choices=("ablations", "lifecycle"), default="ablations")
    args = parser.parse_args()
    make_plan = fixture_plan if args.scenario == "ablations" else lifecycle_plan
    plan = make_plan(seed=args.seed, resamples=args.resamples)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Persist the complete plan BEFORE any candidate observation.
    frozen_path = args.output.with_suffix(".plan.json")
    frozen_path.write_text(
        json.dumps({"plan_sha256": plan.fingerprint, "plan": to_jsonable(plan)}, indent=2) + "\n"
    )
    result = asyncio.run(run_fixture(plan))
    args.output.write_text(json.dumps(to_jsonable(result), indent=2, allow_nan=False) + "\n")
    print(f"Synthetic A9 report: {args.output}; production benefit: false")


if __name__ == "__main__":
    main()
