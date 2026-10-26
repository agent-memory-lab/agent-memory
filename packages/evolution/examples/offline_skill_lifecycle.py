"""Run a real deterministic configuration-repair fixture without a model or network.

From the repository root:
PYTHONPATH=src:packages/evolution/src python packages/evolution/examples/offline_skill_lifecycle.py

The small fixture validates lifecycle plumbing only; its scores are not a public
benchmark or evidence of general task improvement. No learned text is executed.
"""

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvaluationReport,
    EvaluationStage,
    EvolutionDeletionTarget,
    EvolutionEngine,
    EvolutionGateError,
    MultiStepProcedureGenerator,
    ProcedureTask,
    PromotionApproval,
    SkillProcedureDeployment,
    SourceRevision,
    SQLiteEvolutionRegistry,
    export_skill,
)
from agent_memory_evolution.evidence import digest

from agent_memory import ArtifactStatus, Episode, ForgetMode, ForgetRequest, MemoryScope, Provenance
from agent_memory.domain import OutcomeStatus, utc_now

STEPS = ("Inspect the timeout", "Set a valid timeout", "Verify the configuration")


def step(config, action):
    # A fixed host allowlist, not eval, shell execution or a generic Skill runtime.
    if action == STEPS[0]:
        return config["timeout"] <= 0
    if action == STEPS[1]:
        if not config["writable"]:
            return False
        config["timeout"] = 30
        return True
    if action == STEPS[2]:
        return config["timeout"] > 0
    raise ValueError("unregistered fixture action")


async def run_demo(directory):
    directory = Path(directory)
    directory.mkdir(exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("demo output directory must be empty")
    scope = MemoryScope("offline-demo", session_id="configuration-repair")
    episodes, sources = [], {}
    at = datetime(2026, 1, 1, tzinfo=UTC)
    for run in range(4):
        config = {"timeout": -run - 1, "writable": run != 3}
        for index, action in enumerate(STEPS):
            before = dict(config)
            success = step(config, action)
            identity = f"run-{run}-step-{index}"
            source_id = "source-" + identity
            source_digest = digest((before, action, config, success))
            sources[source_id] = SourceRevision(scope, source_id, 1, source_digest)
            episodes.append(
                Episode(
                    id=identity,
                    scope=scope,
                    observation="invalid local timeout",
                    action=action,
                    outcome="configuration step passed" if success else "configuration step failed",
                    lesson="validated local step"
                    if success
                    else "writable configuration is required",
                    quality=1,
                    status=ArtifactStatus.ACTIVE,
                    run_id=f"run-{run}",
                    outcome_status=OutcomeStatus.SUCCEEDED if success else OutcomeStatus.FAILED,
                    provenance=Provenance(source_event_ids=(source_id,), created_at=at),
                    occurred_at=at + timedelta(seconds=run * 10 + index),
                )
            )
    current_episodes = {episode.id: episode for episode in episodes}
    registry = SQLiteEvolutionRegistry(directory / "evolution.db")
    installed = {}

    async def install(artifact):
        installed[artifact.candidate_id] = artifact

    async def remove(request_scope, candidate_id):
        assert request_scope == scope
        installed.pop(candidate_id, None)

    async def evidence_current(request_scope, evidence):
        return request_scope == scope and all(
            item.episode_id in current_episodes
            and digest(current_episodes[item.episode_id]) == item.content_sha256
            and all(sources.get(source.source_event_id) == source for source in item.sources)
            for item in evidence
        )

    host = SkillProcedureDeployment(
        registry, install=install, remove=remove, evidence_current=evidence_current
    )
    engine = EvolutionEngine(registry, DeterministicPromotionPolicy(), host)
    await engine.initialize()
    generator = MultiStepProcedureGenerator(
        ProcedureTask(
            "Repair an invalid local timeout",
            "A local timeout setting is non-positive",
            ("Configuration is writable", "Timeout is a local integer setting"),
        ),
        sources,
    )
    (candidate,) = await engine.generate_candidates(scope, episodes, generator)
    export = export_skill(candidate).write(directory / "review-skill")
    results = []
    for stage, count in (
        (EvaluationStage.OFFLINE, 30),
        (EvaluationStage.SHADOW, 50),
        (EvaluationStage.CANARY, 100),
    ):
        outcomes, baseline_outcomes = [], []
        for index in range(count):
            config = {"timeout": -index - 1, "writable": True}
            # The no-Skill baseline actually checks the same original settings.
            baseline_outcomes.append(step(dict(config), STEPS[2]))
            outcomes.append(
                all(step(config, action) for action in candidate.procedure.steps)
                and config["timeout"] > 0
            )
        rate = sum(outcomes) / count
        baseline_rate = sum(baseline_outcomes) / count
        results.append(
            {
                "stage": stage.value,
                "samples": count,
                "successes": sum(outcomes),
                "baseline_successes": sum(baseline_outcomes),
                "task_success_delta": rate - baseline_rate,
            }
        )
        await engine.submit_evaluation(
            EvaluationReport(
                candidate_id=candidate.id,
                candidate_version=1,
                scope_partition_key=scope.partition_key(),
                stage=stage,
                dataset_id="local-timeout-fixture",
                dataset_version="1",
                evaluator_id="evaluator",
                evaluator_version="1",
                rubric_id="positive-timeout",
                rubric_version="1",
                sample_size=count,
                metrics={
                    "task_success_rate": rate,
                    "task_success_delta": rate - baseline_rate,
                    "cross_scope_leakage": 0,
                    "safety_violation_rate": 0,
                },
                evidence_digest=digest((baseline_outcomes, outcomes)),
            ),
            actor="evaluator",
        )
        approval = (
            PromotionApproval(
                "reviewer",
                "offline-fixture-approval",
                "fixture only",
                candidate_id=candidate.id,
                candidate_version=1,
                scope_partition_key=scope.partition_key(),
                expires_at=utc_now() + timedelta(hours=1),
            )
            if stage == EvaluationStage.CANARY
            else None
        )
        await engine.promote(candidate.id, actor="fixture-host", approval=approval)
    artifact = await host.load_active(scope)
    config = {"timeout": -10, "writable": True}
    # The host binds the approved manifest to its own finite action allowlist.
    manifest = json.loads(artifact.manifest_json)
    assert all(step(config, action) for action in manifest["procedure"]["steps"])
    assert config["timeout"] == 30
    await EvolutionDeletionTarget(engine, scope).forget_sources(
        ForgetRequest(scope, (episodes[-1].provenance.source_event_ids[0],), mode=ForgetMode.ERASE)
    )
    denied = False
    try:
        await host.load_active(scope)
    except EvolutionGateError:
        denied = True
    assert denied and not installed and (await registry.get(candidate.id)).erased
    return {
        "fixture_only": True,
        "steps": len(candidate.procedure.steps),
        "counterexample_episodes": len(candidate.procedure.counterexample_episode_ids),
        "replays": results,
        "repaired_timeout": config["timeout"],
        "withdrawal_denied_use": denied,
        "registry_payload_erased": True,
        "review_export": str(export),
        "export_note": "Review export is a detached copy; its retention belongs to the host.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="an empty directory for review artifacts")
    args = parser.parse_args()
    if args.output:
        result = asyncio.run(run_demo(args.output))
    else:
        with TemporaryDirectory(prefix="evolution-offline-") as directory:
            result = asyncio.run(run_demo(directory))
            result["review_export"] = "temporary export verified and removed"
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
