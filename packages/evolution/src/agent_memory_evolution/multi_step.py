"""Conservative multi-step induction from complete host-selected run trajectories."""

from collections import defaultdict
from dataclasses import dataclass

from agent_memory.domain import ArtifactStatus, OutcomeStatus, Procedure, Provenance

from .domain import GeneratedProcedure
from .evidence import SourceRevision, bind_episode, digest


@dataclass(frozen=True, slots=True)
class ProcedureTask:
    name: str
    trigger: str
    applicability_conditions: tuple[str, ...]
    revision: str = "1"
    reward_definition_id: str | None = None
    reward_formula_version: str | None = None

    def __post_init__(self):
        values = (self.name, self.trigger, self.revision, *self.applicability_conditions)
        if (
            type(self.applicability_conditions) is not tuple
            or not 1 <= len(self.applicability_conditions) <= 32
            or any(
                not isinstance(value, str) or not 1 <= len(value) <= 4096 or not value.strip()
                for value in values
            )
        ):
            raise ValueError("bounded host task and applicability conditions required")
        if bool(self.reward_definition_id) != bool(self.reward_formula_version):
            raise ValueError("reward definition and formula version must be pinned together")
        if self.reward_definition_id is not None and any(
            not isinstance(value, str) or not 1 <= len(value) <= 256 or not value.strip()
            for value in (self.reward_definition_id, self.reward_formula_version)
        ):
            raise ValueError("bounded reward definition and formula versions required")


class MultiStepProcedureGenerator:
    """Reuses an exact repeated action sequence; never invents or executes steps.

    The host supplies all steps of each selected run, including failed runs, and
    authoritative source revisions. Successful runs must have disjoint source IDs.
    Conflicting successful sequences or insufficient support produce no candidate.
    """

    version = "multi-step-procedure/1"

    def __init__(
        self,
        task: ProcedureTask,
        source_revisions,
        *,
        minimum_runs=3,
        minimum_quality=0.75,
        max_episodes=256,
        max_steps=32,
    ):
        if (
            type(task) is not ProcedureTask
            or type(minimum_runs) is not int
            or not 2 <= minimum_runs <= 100
            or not 0 <= minimum_quality <= 1
            or type(max_episodes) is not int
            or not 2 <= max_episodes <= 256
            or type(max_steps) is not int
            or not 2 <= max_steps <= 32
        ):
            raise ValueError("bounded multi-step induction limits required")
        self.task = task
        self.sources = dict(source_revisions)
        if len(self.sources) > 4096 or any(
            type(source) is not SourceRevision or source.source_event_id != identity
            for identity, source in self.sources.items()
        ):
            raise ValueError("source revision keys must match their identities")
        self.minimum_runs, self.minimum_quality = minimum_runs, minimum_quality
        self.max_episodes, self.max_steps = max_episodes, max_steps

    async def generate(self, scope, episodes):
        episodes = tuple(episodes)
        if not episodes:
            return ()
        if len(episodes) > self.max_episodes or len({item.id for item in episodes}) != len(
            episodes
        ):
            raise ValueError("bounded unique episodes required")
        runs = defaultdict(list)
        evidence = []
        for episode in episodes:
            if (
                episode.scope != scope
                or episode.status != ArtifactStatus.ACTIVE
                or not episode.run_id
                or len(episode.run_id) > 256
                or episode.outcome_status == OutcomeStatus.UNKNOWN
                or episode.occurred_at.utcoffset() is None
            ):
                raise ValueError("complete active episodes in one exact scope and run are required")
            for value in (episode.action, episode.outcome, episode.lesson):
                if not isinstance(value, str) or not value.strip() or len(value) > 4096:
                    raise ValueError("bounded observed actions, outcomes and lessons required")
            evidence.append(bind_episode(episode, self.sources))
            runs[episode.run_id].append(episode)
        successful, counterexamples, used_sources = [], [], set()
        for run_id in sorted(runs):
            run = sorted(runs[run_id], key=lambda item: item.occurred_at)
            if not 2 <= len(run) <= self.max_steps or len(
                {item.occurred_at for item in run}
            ) != len(run):
                raise ValueError("each run requires an unambiguous bounded multi-step order")
            if all(
                item.outcome_status == OutcomeStatus.SUCCEEDED
                and item.quality >= self.minimum_quality
                for item in run
            ):
                sources = {source for item in run for source in item.provenance.source_event_ids}
                if used_sources.intersection(sources):
                    raise ValueError("supporting runs must not reuse source identities")
                used_sources.update(sources)
                successful.append(run)
            else:
                counterexamples.extend(run)
        sequences = {tuple(item.action for item in run) for run in successful}
        if len(successful) < self.minimum_runs or len(sequences) != 1:
            return ()
        steps = sequences.pop()
        evidence = tuple(sorted(evidence, key=lambda item: item.episode_id))
        source_episode_ids = tuple(item.episode_id for item in evidence)
        source_ids = tuple(
            sorted({source.source_event_id for item in evidence for source in item.sources})
        )
        observed_start = min(item.occurred_at for item in episodes)
        observed_end = max(item.occurred_at for item in episodes)
        failures = tuple(dict.fromkeys(item.lesson for item in counterexamples))
        if len(failures) > 64:
            raise ValueError("counterexample context exceeds the export envelope")
        procedure = Procedure(
            id="procedure-" + digest((self.version, self.task, evidence, steps)),
            scope=scope,
            name=self.task.name,
            trigger=self.task.trigger,
            steps=steps,
            success_conditions=tuple(sorted({run[-1].outcome for run in successful})),
            applicability_conditions=self.task.applicability_conditions,
            failure_patterns=failures,
            source_episode_ids=source_episode_ids,
            counterexample_episode_ids=tuple(sorted(item.id for item in counterexamples)),
            extractor_version=self.version,
            evidence_start_at=observed_start,
            evidence_end_at=observed_end,
            reward_definition_id=self.task.reward_definition_id,
            reward_formula_version=self.task.reward_formula_version,
            provenance=Provenance(
                source_event_ids=source_ids,
                extractor=type(self).__name__,
                provider="agent-memory-evolution",
                prompt_version=self.version,
                created_at=observed_end,
            ),
            created_at=observed_end,
        )
        return (GeneratedProcedure(procedure, source_episode_ids, evidence),)
