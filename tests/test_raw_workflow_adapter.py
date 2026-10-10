import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest
import test_atom_admission as base
from test_project_extraction_lifecycle import assembly
from test_question_runtime_v7 import ACTOR

from agent_memory.evaluation.question_cost import CostPhase, ModelEvidence
from agent_memory.evaluation.question_experiment import ExperimentArm, WorkItem
from agent_memory.evaluation.raw_workflow import RawSourceWorkflowAdapter, RawWorkflowBinding
from agent_memory.retrieval.model_contracts import canonical

store = base.store


def inputs(items=()):
    return SimpleNamespace(
        adapter_revision="raw-workflow/1",
        configuration_json="{}",
        initial_snapshot_json="{}",
        items=items,
        semantic_model=ModelEvidence("1" * 64, "stub"),
        generation_model=ModelEvidence("2" * 64, "not_used"),
    )


def arm(**changes):
    return ExperimentArm(
        "exact_cache",
        "exact_cache",
        canonical(
            {
                "shared_proof": False,
                "source_omission_audit": False,
                "relation_views": False,
                **changes,
            }
        ),
    )


def test_raw_events_run_the_real_host_without_gold_or_preaccepted_annotations(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, tool_calls = await assembly(engine, scope, clock)
            cleanup = []

            class Ledger:
                async def snapshot(self):
                    return ()

            async def register():
                pass  # assembly already performs actual registered host bindings.

            async def close():
                cleanup.append(True)

            def factory(clean, strategy):
                assert all(item.gold_json is None for item in clean.items)
                return RawWorkflowBinding(
                    host,
                    Ledger(),
                    ACTOR,
                    register,
                    close,
                    strategy.controls_json,
                    question_ids=("project-a:owner",),
                    advance=lambda seconds: clock.__setitem__(
                        0, clock[0] + timedelta(seconds=seconds)
                    ),
                )

            adapter = RawSourceWorkflowAdapter(
                inputs(), arm(shared_proof=True), binding_factory=factory
            )
            await adapter.execute(CostPhase.COLD_START, None)
            await adapter.execute(CostPhase.REGISTRATION, None)
            raw = WorkItem(
                "raw",
                CostPhase.WRITE,
                canonical(
                    {
                        "event_id": event.id,
                        "content": event.content,
                        "occurred_at": event.occurred_at.isoformat(),
                    }
                ),
            )
            await adapter.execute(CostPhase.WRITE, raw)
            await adapter.execute(CostPhase.BACKGROUND, None)
            await adapter.execute(
                CostPhase.WRITE,
                WorkItem("clock", CostPhase.WRITE, canonical({"action": "advance", "seconds": 2})),
            )
            request = WorkItem(
                "read",
                CostPhase.FOREGROUND,
                canonical({"question_id": "project-a:owner"}),
                "project-a",
            )
            result = await adapter.execute(CostPhase.FOREGROUND, request)
            assert result["answers"][0]["answer_status"] == "resolved"
            assert len(tool_calls) == 1
            calls, bindings, pending = await adapter.finish()
            assert calls == () and bindings == {}
            # The authored verification fixture retains a separate source under
            # an unmatched extraction configuration; retain that real debt.
            assert {p.responsibility_id for p in pending} == {"extract:evidence:independent-record"}
            await adapter.close()
            await adapter.close()
            assert cleanup == [True]

    asyncio.run(run())


def test_unperformed_jobs_remain_cost_debt_and_annotation_inputs_fail_before_capture(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, _ = await assembly(engine, scope, clock)

            class Ledger:
                async def snapshot(self):
                    return ()

            async def nothing():
                pass

            adapter = RawSourceWorkflowAdapter(
                inputs(),
                arm(),
                binding_factory=lambda clean, strategy: RawWorkflowBinding(
                    host,
                    Ledger(),
                    ACTOR,
                    nothing,
                    nothing,
                    strategy.controls_json,
                    question_ids=("project-a:owner",),
                ),
            )
            await adapter.execute(CostPhase.COLD_START, None)
            raw = dict(
                event_id=event.id, content=event.content, occurred_at=event.occurred_at.isoformat()
            )
            with pytest.raises(ValueError, match="annotations"):
                await adapter.execute(
                    CostPhase.WRITE,
                    WorkItem(
                        "bad",
                        CostPhase.WRITE,
                        canonical({**raw, "value": "Alice", "authority": "fake"}),
                    ),
                )
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, event.id) is None
            await adapter.execute(
                CostPhase.WRITE, WorkItem("pending", CostPhase.WRITE, canonical(raw))
            )
            _, _, pending = await adapter.finish()
            assert any(item.responsibility_id == "extract:pending" for item in pending)
            assert all(item.maximum_microunits is None for item in pending)
            await adapter.close()

    asyncio.run(run())


def test_gold_never_reaches_the_runtime_factory_and_invalid_arms_cannot_claim_features():
    def factory(*_):
        pytest.fail("factory must never receive gold")

    labeled = WorkItem("q", CostPhase.FOREGROUND, "{}", "group", '{"answerable":true}')
    with pytest.raises(ValueError, match="gold-free"):
        RawSourceWorkflowAdapter(inputs((labeled,)), arm(), binding_factory=factory)


def test_declaration_of_real_semantic_execution_requires_governed_model_bindings(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, _, _ = await assembly(engine, scope, clock)

            class Ledger:
                async def snapshot(self):
                    return ()

            async def nothing():
                pass

            frozen = inputs()
            frozen.semantic_model = ModelEvidence("1" * 64, "real")
            adapter = RawSourceWorkflowAdapter(
                frozen,
                arm(),
                binding_factory=lambda clean, strategy: RawWorkflowBinding(
                    host,
                    Ledger(),
                    ACTOR,
                    nothing,
                    nothing,
                    strategy.controls_json,
                    question_ids=("project-a:owner",),
                ),
            )
            with pytest.raises(ValueError, match="governed_semantic"):
                await adapter.execute(CostPhase.COLD_START, None)
            with pytest.raises(ValueError, match="initialization"):
                await adapter.execute(CostPhase.REGISTRATION, None)
            await adapter.close()

    asyncio.run(run())


@pytest.mark.parametrize("change", ["revoke", "erase"])
def test_raw_workflow_records_current_security_changes_and_finite_drain(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            host, event, _ = await assembly(engine, scope, clock)
            settled = []

            class Ledger:
                async def snapshot(self):
                    return ()

            async def nothing():
                pass

            async def advance(seconds):
                clock[0] += timedelta(seconds=seconds)

            async def settle(ledger):
                settled.append(await ledger.snapshot())

            async def factory(clean, strategy):
                return RawWorkflowBinding(
                    host,
                    Ledger(),
                    ACTOR,
                    nothing,
                    nothing,
                    strategy.controls_json,
                    question_ids=("project-a:owner",),
                    erase=kernel.forget,
                    advance=advance,
                    settle=settle,
                )

            adapter = RawSourceWorkflowAdapter(inputs(), arm(prewarm=True), binding_factory=factory)
            assert (await adapter.inspect(CostPhase.COLD_START))["initialized"] is False
            assert (await adapter.finish())[2][0].maximum_microunits is None
            await adapter.execute(CostPhase.COLD_START, None)
            await adapter.execute(CostPhase.COLD_START, None)
            await adapter.execute(
                CostPhase.WRITE,
                WorkItem(
                    "raw",
                    CostPhase.WRITE,
                    canonical(
                        {
                            "event_id": event.id,
                            "content": event.content,
                            "occurred_at": event.occurred_at.isoformat(),
                        }
                    ),
                ),
            )
            await adapter.execute(CostPhase.BACKGROUND, None)
            await adapter.execute(
                CostPhase.WRITE,
                WorkItem("clock", CostPhase.WRITE, canonical({"action": "advance", "seconds": 2})),
            )
            await adapter.execute(CostPhase.PREWARM, None)
            changed = {"action": change, "source_id": event.id}
            if change == "revoke":
                changed["expected_version"] = 1
            await adapter.execute(
                CostPhase.WRITE, WorkItem("change", CostPhase.WRITE, canonical(changed))
            )
            await adapter.execute(CostPhase.DRAIN, None)
            from agent_memory.derived.model import DerivedError

            with pytest.raises(DerivedError):
                await host.questions.read("project-a:owner", actor=ACTOR)
            assert (await adapter.execute(CostPhase.DEPENDENCY, None))["phase"] == "dependency"
            await adapter.finish()
            assert settled == [()]
            await adapter.close()
            with pytest.raises(ValueError, match="closed"):
                await adapter.execute(CostPhase.REGISTRATION, None)

    asyncio.run(run())


@pytest.mark.parametrize("generation_declaration", ["stub", "real_group"])
def test_model_explanations_keep_actual_call_ids_and_unknown_bills(store, generation_declaration):
    import test_project_admission_v7 as project
    from test_question_models_v7 import configured_question

    from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
    from agent_memory.consolidation.project_extraction import ProjectExtractionBridge
    from agent_memory.operations.domain_verification import (
        DomainVerificationQueue,
        VerificationFinding,
        VerificationToolSpec,
        project_publisher,
    )
    from agent_memory.operations.memory_host import MemoryHost

    async def run():
        async with store() as (engine, kernel, scope, clock):
            questions, models, ledger, port = await configured_question(engine, scope, clock)

            class Generator:
                version = "no-new-source/1"

                async def generate_atoms(self, source):
                    raise AssertionError("model explanation must not re-extract existing sources")

            class Reviewer:
                version = "no-new-source-review/1"

                async def review_atoms(self, source, candidates):
                    raise AssertionError("model explanation must not re-review existing sources")

            class Tool:
                spec = VerificationToolSpec(
                    "registry",
                    "1",
                    project.AUTHORITY,
                    project.AUTHORITY.subjects,
                    project.AUTHORITY.predicates,
                )

                async def verify(self, candidate):
                    return VerificationFinding("unknown")

            async def accept(*_):
                return True

            verification = DomainVerificationQueue(
                engine.repository,
                scope,
                (Tool(),),
                authorize=accept,
                publisher=project_publisher(questions.admission, accept_evidence=accept),
                clock=lambda: clock[0],
            )
            host = MemoryHost(
                engine.repository,
                scope,
                AtomExtractionPipeline(Generator(), Reviewer()),
                questions.admission.admission_policy,
                project.AUTHORITY,
                on_accept=accept,
                verification=verification,
                questions=questions,
                project_bridge=ProjectExtractionBridge(
                    questions.admission,
                    membership_ids={"project-a": "a"},
                    source_authority_id=project.AUTHORITY.source_id,
                    revision="model-accounting-test/1",
                ),
                clock=lambda: clock[0],
            )

            async def nothing():
                pass

            frozen = inputs()
            frozen.generation_model = ModelEvidence(port.configuration.fingerprint, "stub")
            if generation_declaration == "real_group":
                # An authored port exercises group binding, not real inference.
                frozen.generation_model = ModelEvidence.group(
                    (port.configuration.fingerprint, "b" * 64), "real"
                )
            adapter = RawSourceWorkflowAdapter(
                frozen,
                arm(),
                binding_factory=lambda clean, strategy: RawWorkflowBinding(
                    host,
                    ledger,
                    ACTOR,
                    nothing,
                    nothing,
                    strategy.controls_json,
                    question_ids=("project-a:owner",),
                    model_answers=models,
                ),
            )
            await adapter.execute(CostPhase.COLD_START, None)
            result = await adapter.execute(
                CostPhase.FOREGROUND,
                WorkItem(
                    "explain",
                    CostPhase.FOREGROUND,
                    canonical({"question_id": "project-a:owner"}),
                    "group",
                ),
            )
            assert result["explanations"][0]["text"] == "zh-CN"
            calls, bindings, _ = await adapter.finish()
            assert len(calls) == 1 and bindings == {calls[0]["call_id"]: ("explain",)}
            assert calls[0]["actual_microunits"] is None
            await adapter.close()

    asyncio.run(run())


@pytest.mark.parametrize("feature", ["source_omission_audit", "relation_views"])
def test_advertised_feature_without_actual_runtime_implementation_fails_before_work(store, feature):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, _, _ = await assembly(engine, scope, clock)

            class Ledger:
                async def snapshot(self):
                    return ()

            async def nothing():
                pass

            adapter = RawSourceWorkflowAdapter(
                inputs(),
                arm(**{feature: True}),
                binding_factory=lambda clean, strategy: RawWorkflowBinding(
                    host,
                    Ledger(),
                    ACTOR,
                    nothing,
                    nothing,
                    strategy.controls_json,
                    question_ids=("project-a:owner",),
                ),
            )
            with pytest.raises(ValueError, match="actual"):
                await adapter.execute(CostPhase.COLD_START, None)
            await adapter.close()

    asyncio.run(run())
