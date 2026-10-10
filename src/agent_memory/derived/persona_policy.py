"""Host-approved persona definitions and finite categorical semantic policy.

Frequency is evidence for a scoped hypothesis, never truth promotion. Policy owns
its target/text and separate adapters independently check the complete census.
"""

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime

from .model import DerivedError, digest, identity


@dataclass(frozen=True, slots=True)
class PersonaDefinition:
    """Trusted host selector; models cannot choose scope, identity, rights or policy."""

    label: str
    subject_id: str
    predicates: tuple[str, ...]
    readers: tuple[str, ...] = ("alice",)
    purpose: str = "agent_context"
    context: dict = field(default_factory=dict)
    validity_seconds: int = 3600
    minimum_span_seconds: int = 86400
    revision: str = "1"

    def __post_init__(self):
        for value in (self.label, self.subject_id, self.purpose, self.revision):
            identity(value)
        for values, maximum in ((self.predicates, 16), (self.readers, 32)):
            if (
                type(values) is not tuple
                or not 1 <= len(values) <= maximum
                or len(set(values)) != len(values)
            ):
                raise DerivedError("invalid_persona_definition")
            for value in values:
                identity(value)
        if (
            type(self.context) is not dict
            or len(__import__("json").dumps(self.context)) > 4096
            or type(self.validity_seconds) is not int
            or not 1 <= self.validity_seconds <= 86400
            or type(self.minimum_span_seconds) is not int
            or not 0 <= self.minimum_span_seconds <= 31536000
        ):
            raise DerivedError("invalid_persona_definition")
        object.__setattr__(self, "context", deepcopy(self.context))


@dataclass(frozen=True, slots=True)
class PersonaProposal:
    """A bounded hypothesis and exhaustive support/counterexample classification."""

    text: str
    relations: tuple[tuple[str, str], ...]

    def __post_init__(self):
        if (
            type(self.text) is not str
            or not 1 <= len(self.text) <= 4096
            or type(self.relations) is not tuple
            or not 1 <= len(self.relations) <= 32
        ):
            raise DerivedError("invalid_persona_proposal")
        seen = set()
        for item in self.relations:
            if (
                type(item) is not tuple
                or len(item) != 2
                or item[1] not in {"supports", "counterexample", "not_relevant"}
                or item[0] in seen
            ):
                raise DerivedError("invalid_persona_proposal")
            identity(item[0])
            seen.add(item[0])


@dataclass(frozen=True, slots=True)
class PersonaReview:
    decision: str
    reason: str

    def __post_init__(self):
        if self.decision not in {"publish", "withdraw"} or self.reason not in {
            "reviewed",
            "contradicted",
            "unstable",
            "unsupported",
        }:
            raise DerivedError("invalid_persona_review")


@dataclass(frozen=True, slots=True)
class CategoricalPersonaPolicy:
    """Approved literal statement for one registered categorical predicate."""

    predicate: str
    target_value: str
    text: str
    context: dict = field(default_factory=dict)
    minimum_families: int = 3
    revision: str = "1"

    def __post_init__(self):
        identity(self.predicate)
        identity(self.revision)
        if (
            type(self.target_value) is not str
            or not 1 <= len(self.target_value) <= 1024
            or type(self.text) is not str
            or not 1 <= len(self.text) <= 4096
            or type(self.minimum_families) is not int
            or not 2 <= self.minimum_families <= 16
            or type(self.context) is not dict
            or len(__import__("json").dumps(self.context)) > 4096
        ):
            raise DerivedError("invalid_categorical_persona_policy")
        object.__setattr__(self, "context", deepcopy(self.context))

    @property
    def fingerprint(self):
        from dataclasses import asdict

        return digest(asdict(self))


def _bound(policy, definition):
    if (
        tuple(definition["predicates"]) != (policy.predicate,)
        or definition["context"] != policy.context
    ):
        raise DerivedError("categorical_persona_policy_mismatch")


def _relation(policy, fact):
    if (
        fact["predicate"] != policy.predicate
        or fact["modality"] != "asserted"
        or fact["kind"] not in {"fact", "preference", "constraint"}
        or fact["conditions"]
        or fact["exceptions"]
    ):
        return "not_relevant"
    accepted = fact["admission_action"] == "ACCEPT"
    if accepted and fact["value"] == policy.target_value and not fact["negated"]:
        return "supports"
    faithful = accepted or fact["faithfulness"] == "supported"
    if faithful and (
        fact["negated"]
        and fact["value"] == policy.target_value
        or not fact["negated"]
        and fact["value"] != policy.target_value
    ):
        return "counterexample"
    return "not_relevant"


class CategoricalPersonaProposer:
    """Deterministic host template; no free-text model truth authority."""

    def __init__(self, policy):
        if type(policy) is not CategoricalPersonaPolicy:
            raise TypeError("trusted CategoricalPersonaPolicy required")
        self.policy = deepcopy(policy)

    @property
    def revision(self):
        return "categorical-persona-proposer/1:" + self.policy.fingerprint

    async def propose(self, definition, facts):
        _bound(self.policy, definition)
        return PersonaProposal(
            self.policy.text,
            tuple((fact["atom_id"], _relation(self.policy, fact)) for fact in facts),
        )


class CategoricalPersonaReviewer:
    """Independent deterministic semantic check of the full current census."""

    def __init__(self, policy):
        if type(policy) is not CategoricalPersonaPolicy:
            raise TypeError("trusted CategoricalPersonaPolicy required")
        self.policy = deepcopy(policy)

    @property
    def revision(self):
        return "categorical-persona-review/1:" + self.policy.fingerprint

    async def review(self, definition, proposal, facts):
        _bound(self.policy, definition)
        expected = tuple((fact["atom_id"], _relation(self.policy, fact)) for fact in facts)
        if proposal.text != self.policy.text or proposal.relations != expected:
            return PersonaReview("withdraw", "unsupported")
        supporting = [fact for fact in facts if dict(expected)[fact["atom_id"]] == "supports"]
        families = {fact["evidence"]["family"] for fact in supporting}
        observations = [datetime.fromisoformat(fact["observed_at"]) for fact in supporting]
        span = (max(observations) - min(observations)).total_seconds() if observations else 0
        if (
            len(families) < self.policy.minimum_families
            or span < definition["minimum_span_seconds"]
        ):
            return PersonaReview("withdraw", "unstable")
        return PersonaReview("publish", "reviewed")
