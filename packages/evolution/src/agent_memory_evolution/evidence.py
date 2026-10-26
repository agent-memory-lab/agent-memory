"""Immutable exact-scope evidence pins for exported procedural experience."""

from dataclasses import dataclass
from hashlib import sha256

from agent_memory.domain import Episode, MemoryScope, Procedure, canonical_json
from agent_memory.serialization import to_jsonable


def digest(value) -> str:
    return sha256(canonical_json(to_jsonable(value)).encode()).hexdigest()


def _identity(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 256 or not value.strip():
        raise ValueError("bounded evidence identity required")


def _revision(version, content_sha256):
    if type(version) is not int or version < 1:
        raise ValueError("positive evidence version required")
    if (
        not isinstance(content_sha256, str)
        or len(content_sha256) != 64
        or any(char not in "0123456789abcdef" for char in content_sha256)
    ):
        raise ValueError("evidence requires a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class SourceRevision:
    scope: MemoryScope
    source_event_id: str
    version: int
    content_sha256: str

    def __post_init__(self):
        if not isinstance(self.scope, MemoryScope):
            raise ValueError("source evidence requires exact scope")
        _identity(self.source_event_id)
        _revision(self.version, self.content_sha256)


@dataclass(frozen=True, slots=True)
class EpisodeRevision:
    scope: MemoryScope
    episode_id: str
    version: int
    content_sha256: str
    sources: tuple[SourceRevision, ...]

    def __post_init__(self):
        _identity(self.episode_id)
        _revision(self.version, self.content_sha256)
        if (
            not isinstance(self.scope, MemoryScope)
            or type(self.sources) is not tuple
            or not 1 <= len(self.sources) <= 256
            or any(
                type(source) is not SourceRevision or source.scope != self.scope
                for source in self.sources
            )
            or len({source.source_event_id for source in self.sources}) != len(self.sources)
        ):
            raise ValueError("unique source revisions in the episode scope are required")


def bind_episode(episode: Episode, sources) -> EpisodeRevision:
    identities = tuple(sorted(set(episode.provenance.source_event_ids)))
    if not identities or any(identity not in sources for identity in identities):
        raise ValueError("every episode source requires an authoritative revision")
    return EpisodeRevision(
        episode.scope,
        episode.id,
        episode.version,
        digest(episode),
        tuple(sources[identity] for identity in identities),
    )


def validate_evidence(procedure: Procedure, evidence: tuple[EpisodeRevision, ...]) -> None:
    if (
        type(evidence) is not tuple
        or not 1 <= len(evidence) <= 256
        or any(
            type(item) is not EpisodeRevision or item.scope != procedure.scope for item in evidence
        )
    ):
        raise ValueError("bounded exact-scope episode evidence is required")
    identities = {item.episode_id for item in evidence}
    if len(identities) != len(evidence) or identities != set(procedure.source_episode_ids):
        raise ValueError("Procedure evidence must cover every source episode exactly once")
    if not set(procedure.counterexample_episode_ids) <= identities:
        raise ValueError("counterexamples require pinned episode evidence")
    sources = {}
    for episode in evidence:
        for source in episode.sources:
            previous = sources.setdefault(source.source_event_id, source)
            if previous != source:
                raise ValueError("source evidence versions disagree")
    if set(sources) != set(procedure.provenance.source_event_ids):
        raise ValueError("Procedure provenance must equal the complete evidence source set")


def load_evidence(values) -> tuple[EpisodeRevision, ...]:
    return tuple(
        EpisodeRevision(
            MemoryScope(**item["scope"]),
            item["episode_id"],
            item["version"],
            item["content_sha256"],
            tuple(
                SourceRevision(
                    MemoryScope(**source["scope"]),
                    source["source_event_id"],
                    source["version"],
                    source["content_sha256"],
                )
                for source in item["sources"]
            ),
        )
        for item in values
    )
