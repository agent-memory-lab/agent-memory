"""Deterministic, source-disjoint synthetic evidence-obligation episodes.

There are two deliberately separate views of this benchmark. ``Episode`` and its
source/query/event records are the entire runtime input. They contain plain prose,
opaque IDs, actor identity, and source revisions, but no ideal slots, facts, gap
labels, routing keys, ranks, or expected answers. ``GoldAnswer`` is evaluator-only.
Never pass ``Dataset`` or ``Dataset.gold`` to a treatment or answer client: pass
individual episodes from ``Dataset.dev`` or ``Dataset.heldout`` instead.

Gold is declared independently of retrieval, extraction, and treatment output.
Both views describe the same synthetic scenario, but gold is built through a
separate manifest, not inferred from answers or reconstructed from citations.
This is a controlled synthetic benchmark, not evidence of real-world efficacy.

Each episode starts with six documents. Schedules vary across groups, including
no-change-only, unrelated-update-only, deletion before reuse, updates or access
revocation before the first query, and cross-agent-first reuse. A keyword-rich
static digest supplies the
main value but omits a necessary restriction. A separate authoritative bulletin
supplies the complete answer. Four administrative documents are distractors.
Updates, removal and access revocation operate on that bulletin's opaque ID.
The digest is explicitly a static snapshot; it is never sufficient to answer the
whole question, including after the bulletin disappears. An unrelated document
update tests whether invalidation is appropriately scoped.

``dataclasses.asdict`` produces JSON-serializable records (tuples encode as JSON
arrays). ``runtime_payload`` is an allowlisted serialization boundary that cannot
accidentally include gold. Mutation events must be applied before the next query.
Deletion and revocation both remove a document from the accessible corpus;
restoration uses an explicit higher-revision upsert. No network or model is used.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import asdict, dataclass
from typing import Literal

Split = Literal["dev", "heldout"]
EventKind = Literal["upsert", "delete", "revoke"]


@dataclass(frozen=True)
class SourceRecord:
    source_id: str
    source_group: str
    revision: int
    text: str


@dataclass(frozen=True)
class Query:
    query_id: str
    text: str
    agent_id: str = "reader"


@dataclass(frozen=True)
class SourceEvent:
    kind: EventKind
    source_id: str
    source: SourceRecord | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("upsert", "delete", "revoke"):
            raise ValueError(f"Unknown source event: {self.kind!r}")
        if self.kind == "upsert":
            if self.source is None or self.source.source_id != self.source_id:
                raise ValueError("An upsert needs a matching source record")
        elif self.source is not None:
            raise ValueError("Delete/revoke events must not carry source contents")


@dataclass(frozen=True)
class Episode:
    episode_id: str
    split: Split
    source_group: str
    initial_sources: tuple[SourceRecord, ...]
    steps: tuple[Query | SourceEvent, ...]

    @property
    def queries(self) -> tuple[Query, ...]:
        return tuple(step for step in self.steps if isinstance(step, Query))


@dataclass(frozen=True)
class GoldAnswer:
    """Offline expectations; forbidden input to retrieval/answer treatments.

    ``required_values`` and ``required_qualifiers`` are literal, case-insensitive
    answer substrings for a deliberately simple evaluator. Unanswerable means the
    complete requested answer is unsupported, even if a static digest contains a
    partial or stale value. Such entries contain no answer values or citations.
    ``category`` is for evaluator slicing only.
    """

    query_id: str
    required_values: tuple[str, ...]
    required_qualifiers: tuple[str, ...]
    answerable: bool
    supporting_source_ids: tuple[str, ...]
    category: str
    supporting_source_revisions: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class Dataset:
    seed: int
    dev: tuple[Episode, ...]
    heldout: tuple[Episode, ...]
    gold: dict[str, GoldAnswer]

    @property
    def episodes(self) -> tuple[Episode, ...]:
        return self.dev + self.heldout


@dataclass(frozen=True)
class _Template:
    topic: str
    question: str
    paraphrase: str
    fact: str
    restriction: str
    unsupported: str
    unsupported_paraphrase: str
    states: tuple[tuple[str, str], ...]


# Development and held-out templates are disjoint as well as their source groups.
# These private authoring specifications are never part of the runtime payload.
_TEMPLATES = (
    _Template(
        "rollout ownership",
        "Who owns the {entity} rollout, and what must happen before they can proceed?",
        "For {entity}, name the rollout lead and the prerequisite for moving ahead.",
        "The rollout lead is {value}.",
        "The rollout can proceed {qualifier}.",
        "What is the signed-off total headcount for the {entity} rollout?",
        "How many people are formally allocated to the {entity} launch?",
        (
            ("Mira Sen", "only after the risk review is signed"),
            ("Tomas Reed", "only after the security checklist is approved"),
            ("Nadia Park", "only after the launch review is completed"),
        ),
    ),
    _Template(
        "seat refunds",
        "What is the {entity} refund window, and which seats qualify?",
        "For {entity}, how long can a seat refund be requested and what is the restriction?",
        "The request window is {value}.",
        "Refund eligibility is limited to {qualifier}.",
        "What is the refund processing fee for {entity}?",
        "How much does {entity} charge to process a refund?",
        (
            ("14 calendar days", "unused seats only"),
            ("21 calendar days", "unactivated annual seats only"),
            ("30 calendar days", "unused monthly seats only"),
        ),
    ),
    _Template(
        "submission deadlines",
        "When is the {entity} submission due, and what approval is required?",
        "Give the {entity} filing cutoff and the prerequisite sign-off.",
        "The submission cutoff is {value}.",
        "Submission is permitted {qualifier}.",
        "What is the monetary penalty for a late {entity} submission?",
        "How much is charged when the {entity} filing is overdue?",
        (
            ("2027-03-18 17:00 UTC", "after finance approval"),
            ("2027-04-09 12:00 UTC", "after legal approval"),
            ("2027-05-06 16:00 UTC", "after compliance approval"),
        ),
    ),
    _Template(
        "record retention",
        "How long does {entity} retain raw records, and what is the audit-record exception?",
        "State the {entity} raw-data retention period and the separate rule for audits.",
        "Raw records are kept for {value}.",
        "The exception is that {qualifier}.",
        "Which encryption cipher does {entity} use for stored records?",
        "What algorithm encrypts the {entity} archive?",
        (
            ("45 days", "hashed audit records are retained for 400 days"),
            ("60 days", "hashed audit records are retained for 500 days"),
            ("75 days", "hashed audit records are retained for 600 days"),
        ),
    ),
    _Template(
        "operating budgets",
        "What is the monthly {entity} budget cap, and which charges are excluded?",
        "Give the {entity} monthly spending limit together with its exclusions.",
        "The monthly spending ceiling is {value}.",
        "For the ceiling calculation, {qualifier}.",
        "What is {entity}'s approved budget for the following fiscal year?",
        "How much annual funding is approved for {entity} next year?",
        (
            ("USD 8,400", "tax and pass-through charges are excluded"),
            ("USD 9,700", "tax and migration charges are excluded"),
            ("USD 7,600", "tax and training charges are excluded"),
        ),
    ),
    _Template(
        "incident escalation",
        "Where should {entity} incidents be escalated, and when is escalation allowed?",
        "Name the {entity} incident destination and the trigger needed to use it.",
        "The escalation destination is {value}.",
        "Send the escalation {qualifier}.",
        "What is the emergency telephone number for {entity}?",
        "Which phone number should be dialed during a {entity} emergency?",
        (
            ("the amber response queue", "after two failed automatic retries"),
            ("the cobalt response queue", "after three failed automatic retries"),
            ("the jade response queue", "after four failed automatic retries"),
        ),
    ),
    _Template(
        "processing capacity",
        "What is the {entity} hourly processing cap, and where does it apply?",
        "State {entity}'s throughput ceiling and its geographic scope.",
        "The throughput ceiling is {value}.",
        "The ceiling applies {qualifier}.",
        "How much does an extra job above the {entity} cap cost?",
        "What is the overage price per additional {entity} job?",
        (
            ("600 jobs per hour", "only in the northern region"),
            ("750 jobs per hour", "only in the western region"),
            ("900 jobs per hour", "only in the eastern region"),
        ),
    ),
    _Template(
        "release acceptance",
        "What pass rate does {entity} require for release, and over which sample?",
        "For {entity}, give the release success threshold and its measurement population.",
        "The release threshold is {value}.",
        "That threshold is {qualifier}.",
        "Which individual has final veto authority over a {entity} release?",
        "Who can personally veto the {entity} deployment?",
        (
            ("99.6%", "measured over the last 800 requests"),
            ("99.7%", "measured over the last 900 requests"),
            ("99.8%", "measured over the last 1000 requests"),
        ),
    ),
    _Template(
        "expense reimbursement",
        "What is the {entity} reimbursement ceiling, and which receipt rule applies?",
        "Give the {entity} expense limit and the required supporting paperwork.",
        "The reimbursement ceiling is {value}.",
        "For reimbursement, {qualifier}.",
        "What exchange-rate provider is used for {entity} reimbursements?",
        "Which currency conversion service sets the {entity} expense rate?",
        (
            ("USD 320", "itemized receipts are mandatory"),
            ("USD 450", "original paper receipts are mandatory"),
            ("USD 510", "digitally signed receipts are mandatory"),
        ),
    ),
    _Template(
        "maintenance windows",
        "When is the {entity} maintenance window, and how much notice is required?",
        "Give the {entity} service-work slot and the advance-notice requirement.",
        "The maintenance slot is {value}.",
        "For planned work, {qualifier}.",
        "How much downtime compensation is payable for {entity} maintenance?",
        "What credit is owed for planned {entity} downtime?",
        (
            ("Saturday 02:00-03:00 UTC", "notification must arrive 48 hours in advance"),
            ("Sunday 03:00-04:00 UTC", "notification must arrive 72 hours in advance"),
            ("Tuesday 01:00-02:00 UTC", "notification must arrive 96 hours in advance"),
        ),
    ),
    _Template(
        "vendor responses",
        "What is the {entity} response-time commitment, and which tickets does it cover?",
        "State the {entity} reply deadline together with its supported ticket class.",
        "The initial-response commitment is {value}.",
        "Under this commitment, {qualifier}.",
        "What is the contractual service-credit amount for a missed {entity} response?",
        "How large is the credit when {entity} misses its response promise?",
        (
            ("12 business hours", "only priority-two tickets are covered"),
            ("8 business hours", "only priority-three tickets are covered"),
            ("6 business hours", "only priority-four tickets are covered"),
        ),
    ),
    _Template(
        "program eligibility",
        "What is the {entity} tenure requirement, and which workers are excluded?",
        "Give the {entity} qualifying service duration and the worker exclusion.",
        "The tenure requirement is {value}.",
        "In every case, {qualifier}.",
        "What is the maximum age for participating in {entity}?",
        "Is there an upper age cutoff for joining {entity}, and what is it?",
        (
            ("at least 90 active days", "contractors are excluded"),
            ("at least 120 active days", "temporary workers are excluded"),
            ("at least 180 active days", "external consultants are excluded"),
        ),
    ),
    _Template(
        "inventory replenishment",
        "What is the {entity} reorder threshold, and which supplier condition must hold?",
        "State when {entity} stock needs replenishment and the supplier prerequisite.",
        "The reorder threshold is {value}.",
        "For an order to be triggered, {qualifier}.",
        "What is the fixed shipping charge for a {entity} replenishment order?",
        "How much flat-rate freight is charged on {entity} stock orders?",
        (
            ("120 units", "the supplier lead time must exceed 21 days"),
            ("160 units", "the supplier lead time must exceed 28 days"),
            ("200 units", "the supplier lead time must exceed 35 days"),
        ),
    ),
    _Template(
        "data exports",
        "What export format does {entity} require, and which records must be removed?",
        "Give the {entity} output encoding and file type, plus the exclusion rule.",
        "The export format is {value}.",
        "Before export, {qualifier}.",
        "What is the maximum permitted export-file size for {entity}?",
        "How large may an individual {entity} export file be?",
        (
            ("UTF-8 CSV", "records containing personal identifiers must be removed"),
            ("UTF-16 TSV", "records containing payment details must be removed"),
            ("UTF-8 JSONL", "records containing authentication secrets must be removed"),
        ),
    ),
)

_ENTITIES = (
    "Aster", "Birch", "Cedar", "Dahlia", "Elm", "Fennel", "Garnet", "Hazel",
    "Indigo", "Juniper", "Kestrel", "Linden", "Maple", "Nectar",
)


# Event placement is intentionally heterogeneous. The first development group
# retains the full lifecycle for a smoke run; schedule IDs never reach runtime.
_SCHEDULES = (
    ("ask", "exact", "para", "cross", "unsupported", "unsupported_repeat",
     "irrelevant", "exact", "v2", "ask", "cross", "delete", "ask", "cross",
     "v3", "ask", "revoke", "ask", "cross"),
    ("ask", "exact", "para", "cross", "exact", "para", "cross",
     "unsupported", "unsupported_repeat"),
    ("ask", "exact", "irrelevant", "para", "cross", "exact",
     "unsupported", "unsupported_repeat"),
    ("ask", "delete", "exact", "para", "cross", "unsupported", "unsupported_repeat",
     "v3", "ask", "exact", "cross", "revoke", "para"),
    ("v2", "ask", "exact", "para", "cross", "irrelevant", "exact", "unsupported",
     "unsupported_repeat", "delete", "ask", "v3", "para", "cross"),
    ("revoke", "ask", "para", "cross", "unsupported", "unsupported_repeat", "v3",
     "ask", "exact", "para", "cross", "irrelevant", "exact"),
    ("cross", "para", "exact", "v2", "cross", "para", "exact", "unsupported",
     "unsupported_repeat", "irrelevant", "cross"),
    ("delete", "ask", "exact", "para", "cross", "v2", "cross", "para", "exact",
     "unsupported", "unsupported_repeat", "v3", "ask", "cross"),
)

# Hand-declared, evaluator-only truth schedule aligned to query positions above.
# Keeping this separate enables tests to catch mistakes in runtime event order.
_GOLD_SCHEDULES = (
    ((1, "initial_gap"), (1, "exact_repeat"), (1, "paraphrase"), (1, "cross_agent"),
     (None, "unsupported"), (None, "unsupported_repeat"), (1, "irrelevant_update"),
     (2, "source_update"), (2, "source_update_cross_agent"), (None, "source_deleted"),
     (None, "source_deleted_repeat"), (3, "source_restored"), (None, "source_revoked"),
     (None, "source_revoked_repeat")),
    ((1, "initial_gap"), (1, "exact_repeat"), (1, "paraphrase"), (1, "cross_agent"),
     (1, "exact_repeat"), (1, "paraphrase"), (1, "cross_agent"),
     (None, "unsupported"), (None, "unsupported_repeat")),
    ((1, "initial_gap"), (1, "exact_repeat"), (1, "irrelevant_update"), (1, "cross_agent"),
     (1, "exact_repeat"), (None, "unsupported"), (None, "unsupported_repeat")),
    ((1, "initial_gap"), (None, "source_deleted"), (None, "source_deleted_repeat"),
     (None, "source_deleted_repeat"), (None, "unsupported"), (None, "unsupported_repeat"),
     (3, "source_restored"), (3, "exact_repeat"), (3, "cross_agent"), (None, "source_revoked")),
    ((2, "source_update"), (2, "exact_repeat"), (2, "paraphrase"), (2, "cross_agent"),
     (2, "irrelevant_update"), (None, "unsupported"), (None, "unsupported_repeat"),
     (None, "source_deleted"), (3, "source_restored"), (3, "cross_agent")),
    ((None, "source_revoked"), (None, "source_revoked_repeat"),
     (None, "source_revoked_repeat"), (None, "unsupported"), (None, "unsupported_repeat"),
     (3, "source_restored"), (3, "exact_repeat"), (3, "paraphrase"), (3, "cross_agent"),
     (3, "irrelevant_update")),
    ((1, "cross_agent"), (1, "paraphrase"), (1, "exact_repeat"),
     (2, "source_update_cross_agent"), (2, "paraphrase"), (2, "exact_repeat"),
     (None, "unsupported"), (None, "unsupported_repeat"), (2, "irrelevant_update")),
    ((None, "source_deleted"), (None, "source_deleted_repeat"),
     (None, "source_deleted_repeat"), (None, "source_deleted_repeat"),
     (2, "source_restored"), (2, "paraphrase"), (2, "exact_repeat"),
     (None, "unsupported"), (None, "unsupported_repeat"),
     (3, "source_update"), (3, "source_update_cross_agent")),
)


@dataclass(frozen=True)
class _Scenario:
    seed: int
    split: Split
    index: int
    entity: str
    template: _Template
    schedule: int
    states: tuple[tuple[str, str], ...]

    def identity(self, role: str) -> str:
        raw = f"evidence-obligations-v1:{self.seed}:{self.split}:{self.index}:{role}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]

    @property
    def group(self) -> str:
        return f"group-{self.identity('group')}"

    def source_id(self, index: int) -> str:
        return f"source-{self.identity(f'source-{index}')}"

    def query_id(self, index: int) -> str:
        return f"query-{self.identity(f'query-{index}')}"


def _bulletin(scenario: _Scenario, revision: int) -> SourceRecord:
    value, qualifier = scenario.states[revision - 1]
    template = scenario.template
    status = "Current authoritative bulletin" if revision == 1 else "Replacement bulletin"
    text = (
        f"{scenario.entity}: {status}, revision {revision}. "
        f"This is the sole authoritative current instruction for {template.topic}; "
        "it supersedes earlier bulletins and static directory snapshots. "
        + template.fact.format(value=value)
        + " "
        + template.restriction.format(qualifier=qualifier)
    )
    return SourceRecord(scenario.source_id(1), scenario.group, revision, text)


def _runtime_episode(scenario: _Scenario) -> Episode:
    """Render runtime prose and events without reading evaluator expectations."""
    template = scenario.template
    entity = scenario.entity
    question = template.question.format(entity=entity)
    paraphrase = template.paraphrase.format(entity=entity)
    initial_value = scenario.states[0][0]
    digest = SourceRecord(
        scenario.source_id(0), scenario.group, 1,
        f"{entity} directory FAQ: {question} "
        + template.fact.format(value=initial_value)
        + " This is a static, nonbinding snapshot. It omits the detailed restrictions; "
        "consult the current authoritative bulletin for those and for any later changes.",
    )
    administrative_topics = (
        "Document formatting",
        "Internal distribution",
        "Training-session notes",
        "Glossary maintenance",
    )
    distractors = tuple(
        SourceRecord(
            scenario.source_id(index + 2), scenario.group, 1,
            f"{entity} {template.topic}: {topic}. "
            "This administrative note describes how the team labels reference material. "
            "It establishes no operational values, eligibility rules, or exceptions. "
            f"Use the {entity} current authoritative bulletin for operating instructions.",
        )
        for index, topic in enumerate(administrative_topics)
    )
    # Corpus order is seeded independently of prose, labels, and treatment behavior.
    initial_sources = [digest, _bulletin(scenario, 1), *distractors]
    random.Random(scenario.identity("source-order")).shuffle(initial_sources)

    def query(index: int, text: str, agent_id: str = "reader") -> Query:
        return Query(scenario.query_id(index), text, agent_id)

    irrelevant_revision = SourceRecord(
        scenario.source_id(2), scenario.group, 2,
        f"{entity} {template.topic}: document formatting, revision 2. "
        "Reference documents now use sentence-case headings. "
        "This change affects formatting only and establishes no operational instructions.",
    )
    steps: list[Query | SourceEvent] = []
    query_index = 0
    for action in _SCHEDULES[scenario.schedule]:
        if action == "irrelevant":
            steps.append(SourceEvent("upsert", irrelevant_revision.source_id, irrelevant_revision))
        elif action in ("v2", "v3"):
            revision = 2 if action == "v2" else 3
            steps.append(SourceEvent(
                "upsert", scenario.source_id(1), _bulletin(scenario, revision),
            ))
        elif action in ("delete", "revoke"):
            steps.append(SourceEvent(action, scenario.source_id(1)))
        else:
            if action in ("ask", "exact"):
                text, agent = question, "reader"
            elif action == "para":
                text, agent = paraphrase, "reader"
            elif action == "cross":
                text, agent = paraphrase, "colleague"
            elif action == "unsupported":
                text, agent = template.unsupported.format(entity=entity), "reader"
            elif action == "unsupported_repeat":
                text = template.unsupported_paraphrase.format(entity=entity)
                agent = "colleague"
            else:
                raise AssertionError(f"Unknown private schedule action: {action}")
            steps.append(query(query_index, text, agent))
            query_index += 1

    return Episode(
        f"episode-{scenario.identity('episode')}", scenario.split, scenario.group,
        tuple(initial_sources), tuple(steps),
    )


def _gold_answers(scenario: _Scenario) -> tuple[GoldAnswer, ...]:
    """Independent declared truth schedule; no retrieval/answer/corpus inspection."""
    schedule = _GOLD_SCHEDULES[scenario.schedule]
    answers = []
    for index, (revision, category) in enumerate(schedule):
        if revision is None:
            answers.append(GoldAnswer(scenario.query_id(index), (), (), False, (), category))
        else:
            value, qualifier = scenario.states[revision - 1]
            source_id = scenario.source_id(1)
            answers.append(GoldAnswer(
                scenario.query_id(index), (value,), (qualifier,), True, (source_id,), category,
                ((source_id, revision),),
            ))
    return tuple(answers)


def build_dataset(seed: int = 17) -> Dataset:
    """Build six development and eight held-out groups using only a local PRNG.

    A seed changes synthetic entity suffixes, value/restriction assignments,
    opaque identities and document order. It never changes split membership or
    aggregate lifecycle coverage. There is no wall-clock, filesystem, network, or global
    PRNG dependency. Seed must be an integer (not a boolean).
    """
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("seed must be an integer")
    rng = random.Random(seed)
    dev = []
    heldout = []
    gold: dict[str, GoldAnswer] = {}
    for index, template in enumerate(_TEMPLATES):
        split: Split = "dev" if index < 6 else "heldout"
        states = list(template.states)
        rng.shuffle(states)
        scenario = _Scenario(
            seed, split, index, f"{_ENTITIES[index]}-{rng.randrange(100, 1000)}",
            template, index if index < 6 else index - 6, tuple(states),
        )
        episode = _runtime_episode(scenario)
        (dev if split == "dev" else heldout).append(episode)
        gold.update((answer.query_id, answer) for answer in _gold_answers(scenario))
    return Dataset(seed, tuple(dev), tuple(heldout), gold)


def runtime_payload(episode: Episode) -> dict[str, object]:
    """Serialize only the runtime view; add a step discriminator for JSON readers."""
    if not isinstance(episode, Episode):
        raise TypeError("runtime_payload expects an Episode, never a Dataset or gold")
    return {
        "episode_id": episode.episode_id,
        "split": episode.split,
        "source_group": episode.source_group,
        "initial_sources": [asdict(source) for source in episode.initial_sources],
        "steps": [
            {"type": "query" if isinstance(step, Query) else "source_event", **asdict(step)}
            for step in episode.steps
        ],
    }


def apply_source_event(corpus: dict[str, SourceRecord], event: SourceEvent) -> None:
    """Apply an explicit source mutation to a runtime corpus, in place.

    Reject non-increasing revisions of existing documents so stale events cannot
    silently roll back a source. Deletion/revocation are idempotent. A treatment
    still owns invalidation of any separate caches, obligations, and repaired
    atoms; mutating this corpus does not do that work for it.
    """
    if event.kind == "upsert":
        assert event.source is not None  # Validated by SourceEvent.__post_init__.
        previous = corpus.get(event.source_id)
        if previous is not None:
            if event.source.source_group != previous.source_group:
                raise ValueError("A source cannot move between source groups")
            if event.source.revision <= previous.revision:
                raise ValueError("An upsert must increase the existing source revision")
        corpus[event.source_id] = event.source
    else:
        corpus.pop(event.source_id, None)
