import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvaluationReport,
    EvaluationStage,
    EvolutionEngine,
    EvolutionGateError,
    EvolutionState,
    MultiStepProcedureGenerator,
    ProcedureTask,
    PromotionApproval,
    SkillProcedureDeployment,
    SourceRevision,
    SQLiteEvolutionRegistry,
    export_skill,
)
from agent_memory_evolution.evidence import digest

from agent_memory import ArtifactStatus, Episode, MemoryScope, Provenance
from agent_memory.domain import OutcomeStatus, utc_now

SCOPE = MemoryScope("offline", session_id="build-recovery")
STEPS = ("Inspect the failed build", "Correct the configuration", "Run the build again")


def fixture():
    episodes, sources = [], {}
    at = datetime(2026, 1, 1, tzinfo=UTC)
    for run in range(4):
        for index, step in enumerate(STEPS):
            identity = f"run-{run}-step-{index}"
            source_id = "event-" + identity
            failed = run == 3 and index == 2
            episodes.append(
                Episode(
                    id=identity,
                    scope=SCOPE,
                    observation="local build failed",
                    action=step,
                    outcome="permission denied" if failed else "build step passed",
                    lesson="write access is required" if failed else "validated local build step",
                    status=ArtifactStatus.ACTIVE,
                    quality=0.9,
                    run_id=f"run-{run}",
                    outcome_status=OutcomeStatus.FAILED if failed else OutcomeStatus.SUCCEEDED,
                    provenance=Provenance(source_event_ids=(source_id,), created_at=at),
                    occurred_at=at + timedelta(seconds=run * 100 + index),
                    version=2,
                )
            )
            sources[source_id] = SourceRevision(
                SCOPE, source_id, 3, sha256(identity.encode()).hexdigest()
            )
    return tuple(episodes), sources


def generator(sources):
    return MultiStepProcedureGenerator(
        ProcedureTask(
            "Recover a local build",
            "A local build fails due to configuration",
            ("Local repository", "Configuration is writable"),
            reward_definition_id="build-success",
            reward_formula_version="1",
        ),
        sources,
    )


async def promote(engine, candidate):
    for stage, count in (
        (EvaluationStage.OFFLINE, 30),
        (EvaluationStage.SHADOW, 50),
        (EvaluationStage.CANARY, 100),
    ):
        await engine.submit_evaluation(
            EvaluationReport(
                candidate_id=candidate.id,
                candidate_version=candidate.procedure.version,
                scope_partition_key=SCOPE.partition_key(),
                stage=stage,
                dataset_id="fixture",
                dataset_version="1",
                evaluator_id="evaluator",
                evaluator_version="1",
                rubric_id="build",
                rubric_version="1",
                sample_size=count,
                metrics={
                    "task_success_rate": 0.9,
                    "task_success_delta": 0.1,
                    "cross_scope_leakage": 0,
                    "safety_violation_rate": 0,
                },
                evidence_digest="fixture-v1",
            ),
            actor="evaluator",
        )
        approval = (
            PromotionApproval(
                "reviewer",
                "review",
                "reviewed fixture",
                candidate_id=candidate.id,
                candidate_version=1,
                scope_partition_key=SCOPE.partition_key(),
                expires_at=utc_now() + timedelta(hours=1),
            )
            if stage == EvaluationStage.CANARY
            else None
        )
        await engine.promote(candidate.id, actor="host", approval=approval)


def test_multiple_steps_and_counterexample_versions_roundtrip_and_export(tmp_path):
    async def scenario():
        episodes, sources = fixture()
        registry = SQLiteEvolutionRegistry(tmp_path / "registry.db")
        installed = {}

        async def install(artifact):
            installed[artifact.candidate_id] = artifact

        async def remove(scope, candidate_id):
            assert scope == SCOPE
            installed.pop(candidate_id, None)

        async def current(scope, evidence):
            live = {episode.id: episode for episode in episodes}
            return scope == SCOPE and all(
                item.episode_id in live
                and digest(live[item.episode_id]) == item.content_sha256
                and all(sources.get(source.source_event_id) == source for source in item.sources)
                for item in evidence
            )

        host = SkillProcedureDeployment(
            registry, install=install, remove=remove, evidence_current=current
        )
        engine = EvolutionEngine(registry, DeterministicPromotionPolicy(), host)
        await engine.initialize()
        (candidate,) = await engine.generate_candidates(SCOPE, episodes, generator(sources))
        assert candidate.procedure.steps == STEPS
        assert set(candidate.procedure.counterexample_episode_ids) == {
            episode.id for episode in episodes if episode.run_id == "run-3"
        }
        assert all(item.version == 2 for item in candidate.evidence)
        assert all(source.version == 3 for item in candidate.evidence for source in item.sources)
        assert (await registry.get(candidate.id)) == candidate
        artifact = export_skill(candidate)
        exported = artifact.write(tmp_path / "reviewable-skill")
        manifest = json.loads((exported / "manifest.json").read_text())
        assert len(manifest["evidence"]) == 12
        assert manifest["counterexample_episode_ids"] == list(
            candidate.procedure.counterexample_episode_ids
        )
        assert "write access is required" in artifact.markdown
        with pytest.raises(FileExistsError):
            artifact.write(exported)
        with pytest.raises(EvolutionGateError):
            await host.load_active(SCOPE)
        await promote(engine, candidate)
        assert (await host.load_active(SCOPE)).sha256 == installed[candidate.id].sha256
        # Changing a counterexample version revokes use just like changed support.
        counter_source = episodes[-1].provenance.source_event_ids[0]
        previous = sources[counter_source]
        sources[counter_source] = replace(previous, version=4)
        with pytest.raises(EvolutionGateError, match="evidence"):
            await host.load_active(SCOPE)
        await engine.invalidate_sources(
            (counter_source,), scope=SCOPE, actor="host", reason="counterexample revised"
        )
        assert candidate.id not in installed
        assert (await registry.get(candidate.id)).state == EvolutionState.INVALIDATED
        with pytest.raises(EvolutionGateError):
            await host.load_active(SCOPE)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "damage",
    [
        "scope",
        "unknown",
        "duplicate",
        "missing_source",
        "source_scope",
        "ambiguous_order",
        "inactive",
        "reused_source",
    ],
)
def test_unqualified_trajectories_are_rejected(damage):
    async def scenario():
        episodes, sources = fixture()
        episodes = list(episodes)
        if damage == "scope":
            episodes[0] = replace(episodes[0], scope=MemoryScope("other"))
        elif damage == "unknown":
            episodes[0] = replace(episodes[0], outcome_status=OutcomeStatus.UNKNOWN)
        elif damage == "duplicate":
            episodes.append(episodes[0])
        elif damage == "missing_source":
            sources.pop(episodes[-1].provenance.source_event_ids[0])
        elif damage == "source_scope":
            key = episodes[0].provenance.source_event_ids[0]
            sources[key] = replace(sources[key], scope=MemoryScope("other"))
        elif damage == "ambiguous_order":
            episodes[1] = replace(episodes[1], occurred_at=episodes[0].occurred_at)
        elif damage == "inactive":
            episodes[0] = replace(episodes[0], status=ArtifactStatus.ARCHIVED)
        else:
            episodes[3] = replace(episodes[3], provenance=episodes[0].provenance)
        with pytest.raises(ValueError):
            await generator(sources).generate(SCOPE, episodes)

    asyncio.run(scenario())


def test_conflicting_successful_sequences_produce_no_candidate():
    async def scenario():
        episodes, sources = fixture()
        changed = (replace(episodes[0], action="an incompatible fix"), *episodes[1:])
        assert await generator(sources).generate(SCOPE, changed) == ()
        assert await generator(sources).generate(SCOPE, episodes[:6]) == ()

    asyncio.run(scenario())


def test_engine_rejects_generator_rewriting_episode_version(tmp_path):
    async def scenario():
        episodes, sources = fixture()
        altered = (replace(episodes[0], version=99), *episodes[1:])
        generated = await generator(sources).generate(SCOPE, altered)

        class UntrustedGenerator:
            async def generate(self, scope, inputs):
                return generated

        registry = SQLiteEvolutionRegistry(tmp_path / "e.db")
        from agent_memory_evolution import NullProcedureDeployment

        engine = EvolutionEngine(
            registry, DeterministicPromotionPolicy(), NullProcedureDeployment()
        )
        await engine.initialize()
        with pytest.raises(EvolutionGateError, match="evidence or version"):
            await engine.generate_candidates(SCOPE, episodes, UntrustedGenerator())

    asyncio.run(scenario())


def test_pinned_generator_cannot_drop_observed_counterexamples(tmp_path):
    async def scenario():
        from agent_memory_evolution import GeneratedProcedure, NullProcedureDeployment

        episodes, sources = fixture()
        (original,) = await generator(sources).generate(SCOPE, episodes)

        class CherryPickedGenerator:
            async def generate(self, scope, inputs):
                return (
                    GeneratedProcedure(
                        replace(original.procedure, counterexample_episode_ids=()),
                        original.source_episode_ids,
                        original.evidence,
                    ),
                )

        engine = EvolutionEngine(
            SQLiteEvolutionRegistry(tmp_path / "e.db"),
            DeterministicPromotionPolicy(),
            NullProcedureDeployment(),
        )
        await engine.initialize()
        with pytest.raises(EvolutionGateError, match="counterexamples"):
            await engine.generate_candidates(SCOPE, episodes, CherryPickedGenerator())

    asyncio.run(scenario())
