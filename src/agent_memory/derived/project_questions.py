"""Protocol-only, deterministic reference semantics for registered project questions.

These immutable inputs are *host attestations*, not an admission mechanism. A host
must authenticate source revisions, review field entailment, bind project membership
and permission/purpose, and obtain a complete consistent census before constructing
one. Exact source spans and a matching fingerprint alone do not establish truth.
There is intentionally no capture adapter, persistence, SDK/MCP exposure, or enabled
capability here. These full functions are B0 oracles for later integrated B3 plans.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..conditions import Condition, QueryContext, applicability, instant
from ..domain import MemoryScope, PredicateSpec, SourceAuthority
from ..fact_qualification import FieldEvidence, SourceSpan
from ..serialization import to_jsonable
from .model import DerivedError, digest, identity
from .question_model import AnswerStatus

PROJECT_CONTRACT_SCHEMA = "project-question-domain/1"
PROJECT_INPUT_SCHEMA = "qualified-project-snapshot/1"
PROJECT_FULL_ALGORITHM = "project-full-oracle/1"
PROJECT_QUESTIONS = frozenset({"owner", "status", "commitments", "risks"})
_PREDICATES = (
    "project.owner",
    "project.status",
    "project.phase",
    "commitment.promisor",
    "commitment.action",
    "commitment.state",
    "commitment.deadline",
    "risk.label",
    "risk.state",
)
_BASE_SUPPORT = frozenset({"subject_id", "predicate", "value", "valid_from"})


def _error(code):
    raise DerivedError(code)


def _sequence(values, cls, *, maximum, minimum=0):
    if not isinstance(values, (tuple, list)) or not minimum <= len(values) <= maximum:
        _error("project_sequence_capacity")
    if any(type(value) is not cls for value in values):
        _error("project_typed_input_required")
    return tuple(values)


def _names(values, *, maximum=32, minimum=0):
    values = _sequence(values, str, maximum=maximum, minimum=minimum)
    for value in values:
        identity(value)
    if len(set(values)) != len(values):
        _error("project_duplicate_identity")
    return tuple(sorted(values))


def _hash(value):
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        _error("project_fingerprint_required")


@dataclass(frozen=True, slots=True)
class ProjectRiskRule:
    """One bounded equality rule; inferred risks remain distinct from observations."""

    id: str
    version: str
    predicate: str
    equals: str
    label: str
    impact_conditions: tuple[Condition, ...] = ()

    def __post_init__(self):
        for value in (self.id, self.version, self.equals, self.label):
            identity(value)
        if self.predicate not in {"project.status", "project.phase"}:
            _error("unsupported_project_risk_rule")
        object.__setattr__(
            self, "impact_conditions", _sequence(self.impact_conditions, Condition, maximum=16)
        )


@dataclass(frozen=True, slots=True)
class ProjectDomainContract:
    """Host registration of business meanings, independent of refresh policy.

    Owner role and cardinality belong to this registration. End/delegation and
    replacement must already be represented by qualified bitemporal revisions;
    this oracle never guesses a successor from recency or free-form text.
    """

    id: str
    version: str
    qualification_revision: str
    owner_role: str
    project_statuses: tuple[str, ...]
    owner_cardinality: str = "single_exclusive"
    project_phases: tuple[str, ...] = ()
    require_phase: bool = False
    commitment_open_states: tuple[str, ...] = ("open",)
    commitment_terminal_states: tuple[str, ...] = ("completed", "cancelled")
    risk_rules: tuple[ProjectRiskRule, ...] = ()
    timezone: str = "UTC"
    calendar_version: str = "absolute-deadline/1"
    schema: str = PROJECT_CONTRACT_SCHEMA

    def __post_init__(self):
        for value in (self.id, self.version, self.qualification_revision, self.owner_role):
            identity(value)
        if self.schema != PROJECT_CONTRACT_SCHEMA or self.calendar_version != "absolute-deadline/1":
            _error("unsupported_project_domain_contract")
        if self.owner_cardinality not in {"single_exclusive", "multiple"}:
            _error("unsupported_project_owner_cardinality")
        for name in (
            "project_statuses",
            "project_phases",
            "commitment_open_states",
            "commitment_terminal_states",
        ):
            object.__setattr__(
                self,
                name,
                _names(getattr(self, name), minimum=0 if name == "project_phases" else 1),
            )
        if type(self.require_phase) is not bool or (self.require_phase and not self.project_phases):
            _error("invalid_project_phase_contract")
        if (set(self.commitment_open_states) & set(self.commitment_terminal_states)) or (
            "unknown" in (*self.commitment_open_states, *self.commitment_terminal_states)
        ):
            _error("ambiguous_project_commitment_states")
        rules = _sequence(self.risk_rules, ProjectRiskRule, maximum=16)
        if len({rule.id for rule in rules}) != len(rules):
            _error("project_duplicate_risk_rule")
        for rule in rules:
            values = (
                self.project_statuses if rule.predicate == "project.status" else self.project_phases
            )
            if rule.equals not in values:
                _error("unregistered_project_risk_value")
        object.__setattr__(self, "risk_rules", tuple(sorted(rules, key=lambda rule: rule.id)))
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError, TypeError) as error:
            raise DerivedError("invalid_project_timezone") from error

    @property
    def fingerprint(self):
        return digest(to_jsonable(self))

    @property
    def predicate_specs(self):
        """Registration inputs for existing admission, not an admission bypass."""
        return tuple(
            PredicateSpec(
                predicate,
                value_type="string",
                allow_self_report=False,
                required_evidence_fields=tuple(sorted(_BASE_SUPPORT)),
            )
            for predicate in _PREDICATES
        )


def project_query_fingerprint(contract, scope, project_id):
    """Bind coverage to the entire registered project census, before filtering."""
    if type(contract) is not ProjectDomainContract or type(scope) is not MemoryScope:
        _error("project_registered_contract_and_scope_required")
    identity(project_id)
    return digest(
        {
            "schema": "project-candidate-census/1",
            "scope": to_jsonable(scope),
            "project_id": project_id,
            "contract_fingerprint": contract.fingerprint,
            "predicates": list(_PREDICATES),
            "membership": "all_project_candidates",
        }
    )


@dataclass(frozen=True, slots=True)
class ProjectFact:
    """Untrusted structural assertion. This object alone is never an oracle input."""

    id: str
    project_id: str
    entity_id: str
    predicate: str
    value: str
    valid_from: datetime
    known_from: datetime
    valid_to: datetime | None = None
    known_to: datetime | None = None
    conditions: tuple[Condition, ...] = ()
    exceptions: tuple[Condition, ...] = ()
    origin: str = "observed"

    def __post_init__(self):
        for value in (self.id, self.project_id, self.entity_id, self.predicate):
            identity(value)
        if not isinstance(self.value, str) or not self.value.strip() or len(self.value) > 4096:
            _error("project_fact_requires_bounded_string")
        if self.origin not in {"observed", "inferred"}:
            _error("invalid_project_fact_origin")
        instant(self.valid_from)
        instant(self.known_from)
        for name in ("valid_from", "known_from", "valid_to", "known_to"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, instant(value))
        if (self.valid_to is not None and self.valid_to <= self.valid_from) or (
            self.known_to is not None and self.known_to <= self.known_from
        ):
            _error("invalid_project_fact_interval")
        for name in ("conditions", "exceptions"):
            object.__setattr__(self, name, _sequence(getattr(self, name), Condition, maximum=16))

    @property
    def fingerprint(self):
        return digest(to_jsonable(self))

    def active(self, valid_at, known_at):
        return (
            self.known_from <= known_at
            and (self.known_to is None or known_at < self.known_to)
            and self.valid_from <= valid_at
            and (self.valid_to is None or valid_at < self.valid_to)
        )


@dataclass(frozen=True, slots=True)
class ProjectQualification:
    """Explicit prior host review reference; not proof that review really occurred.

    FieldEvidence retains AND/OR source locators. Missing field support is legal
    protocol input but cannot become a resolved field. The integrated host must
    validate exact source bytes, entailment, revision/current security, and census.
    """

    review_id: str
    policy_revision: str
    target_sha256: str
    authority: SourceAuthority
    field_evidence: tuple[FieldEvidence, ...]

    def __post_init__(self):
        identity(self.review_id)
        identity(self.policy_revision)
        _hash(self.target_sha256)
        if not isinstance(self.authority, SourceAuthority):
            _error("project_host_authority_required")
        evidence = _sequence(self.field_evidence, FieldEvidence, maximum=8, minimum=1)
        if len({item.field for item in evidence}) != len(evidence):
            _error("project_duplicate_field_evidence")
        object.__setattr__(
            self, "field_evidence", tuple(sorted(evidence, key=lambda item: item.field))
        )


@dataclass(frozen=True, slots=True)
class QualifiedProjectFact:
    fact: ProjectFact
    qualification: ProjectQualification

    def __post_init__(self):
        if not isinstance(self.fact, ProjectFact) or not isinstance(
            self.qualification, ProjectQualification
        ):
            _error("project_explicit_qualification_required")
        if self.fact.fingerprint != self.qualification.target_sha256:
            _error("project_qualification_target_mismatch")
        authority = self.qualification.authority
        if (
            self.fact.entity_id not in authority.subjects
            or self.fact.predicate not in authority.predicates
        ):
            _error("project_qualification_authority_mismatch")
        if authority.kind not in {"tool_observation", "document"}:
            _error("project_authoritative_source_required")

    @property
    def missing_support(self):
        required = set(_BASE_SUPPORT)
        for name in ("valid_to", "conditions", "exceptions"):
            if getattr(self.fact, name):
                required.add(name)
        return tuple(sorted(required - {item.field for item in self.qualification.field_evidence}))

    def possibly_active(self, valid_at, known_at):
        """Only proven valid-time fields may exclude an otherwise relevant input.

        Known-time coordinates are host ledger metadata, not extracted source
        fields. A missing valid boundary proof leaves an unknown candidate.
        """
        fact = self.fact
        if fact.known_from > known_at or (fact.known_to is not None and known_at >= fact.known_to):
            return False
        supported = {item.field for item in self.qualification.field_evidence}
        if "valid_from" in supported and valid_at < fact.valid_from:
            return False
        if "valid_to" in supported and fact.valid_to is not None and valid_at >= fact.valid_to:
            return False
        return True

    @property
    def source_references(self) -> tuple[SourceSpan, ...]:
        spans = {
            span
            for evidence in self.qualification.field_evidence
            for branch in evidence.alternatives
            for span in branch
        }
        return tuple(
            sorted(spans, key=lambda span: (span.source_event_id, span.start, span.end, span.quote))
        )


@dataclass(frozen=True, slots=True)
class ProjectSnapshotCoverage:
    """Declared census at the snapshot coordinates; never upstream/world coverage."""

    query_fingerprint: str
    source_basis: str
    candidate_census_complete: bool
    expected_fact_count: int
    truncation_reasons: tuple[str, ...] = ()
    unqualified_candidate_ids: tuple[str, ...] = ()
    publication_closed: bool | None = None

    def __post_init__(self):
        _hash(self.query_fingerprint)
        if self.source_basis not in {"admitted_l1", "publication_manifest"}:
            _error("unsupported_project_source_basis")
        if type(self.candidate_census_complete) is not bool or (
            type(self.expected_fact_count) is not int or not 0 <= self.expected_fact_count <= 1024
        ):
            _error("invalid_project_census")
        for name in ("truncation_reasons", "unqualified_candidate_ids"):
            object.__setattr__(self, name, _names(getattr(self, name), maximum=128))
        if self.source_basis == "publication_manifest":
            if type(self.publication_closed) is not bool:
                _error("project_publication_closure_required")
        elif self.publication_closed is not None:
            _error("unexpected_project_publication_closure")

    @property
    def incomplete_reasons(self):
        reasons = list(self.truncation_reasons)
        if not self.candidate_census_complete:
            reasons.append("candidate_census_incomplete")
        if self.unqualified_candidate_ids:
            reasons.append("unqualified_candidates")
        if self.publication_closed is False:
            reasons.append("publication_manifest_open")
        return tuple(sorted(set(reasons)))


@dataclass(frozen=True, slots=True)
class ProjectInputSnapshot:
    """Immutable, host-qualified protocol snapshot. No raw records are upgraded."""

    id: str
    scope: MemoryScope
    project_id: str
    contract_fingerprint: str
    context: QueryContext
    coverage: ProjectSnapshotCoverage
    facts: tuple[QualifiedProjectFact, ...]
    schema: str = PROJECT_INPUT_SCHEMA

    def __post_init__(self):
        identity(self.id)
        identity(self.project_id)
        _hash(self.contract_fingerprint)
        if self.schema != PROJECT_INPUT_SCHEMA:
            _error("unsupported_project_snapshot_schema")
        if type(self.scope) is not MemoryScope or type(self.context) is not QueryContext:
            _error("project_trusted_context_required")
        # The legacy MemoryScope constructor validates only tenant/namespace.
        # Freeze the stronger V7 boundary: no mutable routing coordinate may be
        # retained inside an ostensibly immutable snapshot or its context.
        for scope in (self.scope, self.context.scope):
            if type(scope) is not MemoryScope:
                _error("project_exact_scope_required")
            for name in MemoryScope.__dataclass_fields__:
                value = getattr(scope, name)
                if value is not None and type(value) is not str:
                    _error("project_exact_scope_required")
                if value is not None:
                    identity(value)
        if self.context.scope != self.scope or self.context.subject_id != self.project_id:
            _error("project_snapshot_context_mismatch")
        if not isinstance(self.coverage, ProjectSnapshotCoverage):
            _error("project_explicit_coverage_required")
        facts = _sequence(self.facts, QualifiedProjectFact, maximum=1024)
        if len({item.fact.id for item in facts}) != len(facts):
            _error("project_duplicate_fact_revision")
        if any(item.fact.project_id != self.project_id for item in facts):
            _error("project_snapshot_membership_mismatch")
        if self.coverage.expected_fact_count != len(facts):
            _error("project_snapshot_census_count_mismatch")
        object.__setattr__(self, "facts", tuple(sorted(facts, key=lambda item: item.fact.id)))
        if len(str(to_jsonable(self)).encode()) > 2_000_000:
            _error("project_snapshot_capacity")


@dataclass(frozen=True, slots=True)
class ProjectFieldResult:
    name: str
    status: AnswerStatus
    known_values: tuple[str, ...]
    candidates: tuple[QualifiedProjectFact, ...]
    reasons: tuple[str, ...] = ()

    @property
    def value(self):
        return (
            self.known_values[0]
            if self.status == AnswerStatus.RESOLVED and len(self.known_values) == 1
            else None
        )


@dataclass(frozen=True, slots=True)
class ProjectRowResult:
    id: str
    status: AnswerStatus
    matches: bool | None
    fields: tuple[ProjectFieldResult, ...]
    origin: str = "observed"
    rule: ProjectRiskRule | None = None

    def field(self, name):
        return next(item for item in self.fields if item.name == name)


@dataclass(frozen=True, slots=True)
class ProjectQuestionResult:
    question: str
    contract_fingerprint: str
    snapshot_id: str
    input_fingerprint: str
    processing_references: tuple[SourceSpan, ...]
    valid_at: datetime
    known_at: datetime
    status: AnswerStatus
    rows: tuple[ProjectRowResult, ...]
    coverage: ProjectSnapshotCoverage
    reasons: tuple[str, ...]
    next_transition_at: datetime | None
    overdue_only: bool = False

    @property
    def matched_ids(self):
        return tuple(row.id for row in self.rows if row.matches is True)

    @property
    def protocol_only(self):
        return True

    @property
    def world_negative(self):
        return False

    def payload(self):
        return {
            **to_jsonable(self),
            "schema": "project-question-result/1",
            "algorithm": PROJECT_FULL_ALGORITHM,
            "protocol_only": True,
            "world_negative": False,
            "scope_basis": "declared_known_snapshot_scope",
            "matched_ids": list(self.matched_ids),
        }


def _field(name, candidates, context, *, multiple=False):
    selected, values, reasons = [], set(), set()
    for item in candidates:
        # An unproved qualifier cannot hide a competing candidate, even when its
        # arbitrary expression happens to evaluate false in this context.
        if item.missing_support:
            selected.append(item)
            reasons.update("unsupported_field:" + field for field in item.missing_support)
            continue
        applicable = applicability(item.fact.conditions, item.fact.exceptions, context)
        if applicable is False:
            continue
        selected.append(item)
        if applicable is None:
            reasons.add("unknown_applicability")
        elif item.fact.origin != "observed":
            reasons.add("inference_not_authoritative_state")
        elif item.fact.value == "unknown" and name in {
            "project.status",
            "project.phase",
            "commitment.state",
            "risk.state",
        }:
            reasons.add("explicit_unknown_state")
        else:
            values.add(
                _deadline(item.fact.value).isoformat()
                if name == "commitment.deadline"
                else item.fact.value
            )
    if len(values) > 1 and not multiple:
        status = AnswerStatus.CONTESTED
    elif reasons or not values:
        status = AnswerStatus.UNKNOWN
    else:
        status = AnswerStatus.RESOLVED
    if not selected:
        reasons.add("no_qualified_record")
    return ProjectFieldResult(
        name, status, tuple(sorted(values)), tuple(selected), tuple(sorted(reasons))
    )


def _row_status(fields):
    states = {item.status for item in fields}
    if AnswerStatus.CONTESTED in states:
        return AnswerStatus.CONTESTED
    return AnswerStatus.UNKNOWN if AnswerStatus.UNKNOWN in states else AnswerStatus.RESOLVED


def _deadline(value):
    try:
        return instant(datetime.fromisoformat(value))
    except (ValueError, TypeError) as error:
        raise DerivedError("project_deadline_requires_absolute_instant") from error


def _validate_values(contract, snapshot):
    enums = {
        "project.status": contract.project_statuses,
        "project.phase": contract.project_phases,
        "commitment.state": (
            *contract.commitment_open_states,
            *contract.commitment_terminal_states,
            "unknown",
        ),
        "risk.state": ("open", "closed", "unknown"),
    }
    for item in snapshot.facts:
        fact = item.fact
        if fact.entity_id.startswith("rule:"):
            _error("project_reserved_entity_identity")
        if fact.predicate not in _PREDICATES:
            _error("unregistered_project_predicate")
        if fact.predicate.startswith("project.") and fact.entity_id != snapshot.project_id:
            _error("project_subject_binding_mismatch")
        if item.qualification.policy_revision != contract.qualification_revision:
            _error("project_qualification_revision_mismatch")
        if fact.predicate in enums and fact.value not in enums[fact.predicate]:
            _error("unregistered_project_state")
        if fact.predicate == "commitment.deadline":
            _deadline(fact.value)


def full_project_question(contract, snapshot, question, *, overdue_only=False):
    """Full, order-independent protocol oracle; never a current-delivery certificate.

    Snapshot context fixes both temporal coordinates. A caller cannot override
    them with a wall clock, and late evidence cannot rewrite earlier knowledge.
    Known-scope emptiness requires the entire declared census, never top-k.
    """
    if not isinstance(contract, ProjectDomainContract) or not isinstance(
        snapshot, ProjectInputSnapshot
    ):
        _error("project_registered_contract_and_snapshot_required")
    if (
        question not in PROJECT_QUESTIONS
        or type(overdue_only) is not bool
        or (overdue_only and question != "commitments")
    ):
        _error("unsupported_project_question")
    if contract.fingerprint != snapshot.contract_fingerprint:
        _error("project_contract_fingerprint_mismatch")
    if snapshot.coverage.query_fingerprint != project_query_fingerprint(
        contract, snapshot.scope, snapshot.project_id
    ):
        _error("project_query_fingerprint_mismatch")
    if snapshot.context.timezone != contract.timezone:
        _error("project_calendar_binding_mismatch")
    _validate_values(contract, snapshot)
    context = snapshot.context
    active = [
        item for item in snapshot.facts if item.possibly_active(context.valid_at, context.known_at)
    ]
    rows, transitions = [], set()
    for item in snapshot.facts:
        fact = item.fact
        if fact.known_from <= context.known_at and (
            fact.known_to is None or context.known_at < fact.known_to
        ):
            transitions.update(
                value
                for value in (fact.valid_from, fact.valid_to)
                if value and value > context.valid_at
            )
    # A conservative calendar boundary is safe even if an individual weekday
    # expression does not actually change there. No TTL/rounded time identity.
    if any(item.fact.conditions or item.fact.exceptions for item in active) or any(
        rule.impact_conditions for rule in contract.risk_rules
    ):
        local = context.valid_at.astimezone(ZoneInfo(contract.timezone))
        transitions.add(
            instant(datetime.combine(local.date() + timedelta(days=1), time(), local.tzinfo))
        )

    def field(name, entity, *, multiple=False):
        return _field(
            name,
            [
                item
                for item in active
                if item.fact.entity_id == entity and item.fact.predicate == name
            ],
            context,
            multiple=multiple,
        )

    if question in {"owner", "status"}:
        names = ["project.owner"] if question == "owner" else ["project.status"]
        if question == "status" and contract.require_phase:
            names.append("project.phase")
        fields = tuple(
            field(
                name,
                snapshot.project_id,
                multiple=(question == "owner" and contract.owner_cardinality == "multiple"),
            )
            for name in names
        )
        state = _row_status(fields)
        rows.append(
            ProjectRowResult(
                snapshot.project_id, state, True if state == AnswerStatus.RESOLVED else None, fields
            )
        )
    elif question == "commitments":
        entities = sorted(
            {
                item.fact.entity_id
                for item in active
                if item.fact.predicate.startswith("commitment.")
            }
        )
        for entity in entities:
            fields = tuple(
                field(name, entity)
                for name in (
                    "commitment.promisor",
                    "commitment.action",
                    "commitment.state",
                    "commitment.deadline",
                )
            )
            required = fields if overdue_only else fields[:3]
            state, matches = _row_status(required), None
            observed_state = fields[2].value
            if observed_state in contract.commitment_terminal_states:
                matches = False
            elif (
                observed_state in contract.commitment_open_states and state == AnswerStatus.RESOLVED
            ):
                matches = True
                if overdue_only:
                    deadline = _deadline(fields[3].value)
                    matches = context.valid_at >= deadline
                    if deadline > context.valid_at:
                        transitions.add(deadline)
            elif observed_state == "unknown":
                state = AnswerStatus.UNKNOWN
            rows.append(ProjectRowResult(entity, state, matches, fields))
    else:
        entities = sorted(
            {item.fact.entity_id for item in active if item.fact.predicate.startswith("risk.")}
        )
        for entity in entities:
            fields = (field("risk.label", entity), field("risk.state", entity))
            state, matches = _row_status(fields), None
            if fields[1].value == "closed":
                matches = False
            elif fields[1].value == "open" and state == AnswerStatus.RESOLVED:
                matches = True
            elif fields[1].value == "unknown":
                state = AnswerStatus.UNKNOWN
            rows.append(ProjectRowResult(entity, state, matches, fields))
        for rule in contract.risk_rules:
            source = field(rule.predicate, snapshot.project_id)
            condition = applicability(rule.impact_conditions, (), context)
            state, matches = source.status, None
            if condition is False:
                matches, state = False, AnswerStatus.RESOLVED
            elif condition is None:
                state = AnswerStatus.UNKNOWN
            elif source.status == AnswerStatus.RESOLVED:
                matches = source.value == rule.equals
            rows.append(
                ProjectRowResult("rule:" + rule.id, state, matches, (source,), "inferred", rule)
            )
    rows = tuple(sorted(rows, key=lambda row: (row.id, row.origin)))
    relevant = [row for row in rows if row.matches is not False]
    reasons = set(snapshot.coverage.incomplete_reasons)
    # An unproved subject/predicate cannot safely route a census member out of
    # this question. Preserve known rows, but block a resolved/empty answer.
    if any({"subject_id", "predicate"} & set(item.missing_support) for item in active):
        reasons.add("candidate_membership_unproved")
    if reasons:
        status = AnswerStatus.INCOMPLETE
    elif any(row.status == AnswerStatus.CONTESTED for row in relevant):
        status = AnswerStatus.CONTESTED
    elif any(row.matches is None for row in relevant):
        status = AnswerStatus.UNKNOWN
    elif any(row.matches is True for row in relevant):
        status = AnswerStatus.RESOLVED
    else:
        status = (
            AnswerStatus.EMPTY if question in {"commitments", "risks"} else AnswerStatus.UNKNOWN
        )
    if status == AnswerStatus.EMPTY:
        reasons.add("no_matches_in_complete_known_scope")
    references = tuple(
        sorted(
            {span for item in snapshot.facts for span in item.source_references},
            key=lambda span: (span.source_event_id, span.start, span.end, span.quote),
        )
    )
    return ProjectQuestionResult(
        question,
        contract.fingerprint,
        snapshot.id,
        digest(to_jsonable(snapshot)),
        references,
        context.valid_at,
        context.known_at,
        status,
        rows,
        snapshot.coverage,
        tuple(sorted(reasons)),
        min(transitions, default=None),
        overdue_only,
    )
