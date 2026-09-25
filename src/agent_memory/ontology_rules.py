"""Bounded relation-chain inference with revalidated, non-active candidates."""
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
import json

from .domain import canonical_json
from .ontology_schema import ontology_schema_digest
from .serialization import to_jsonable


@dataclass(frozen=True, slots=True)
class RelationRule:
    rule_id: str
    version: str
    left_predicate: str
    right_predicate: str
    conclusion_predicate: str

    def __post_init__(self):
        for value in (self.rule_id, self.version, self.left_predicate,
                      self.right_predicate, self.conclusion_predicate):
            if not isinstance(value, str) or not value.strip() or len(value) > 128:
                raise ValueError("rule identifiers require 1 to 128 characters")


@dataclass(frozen=True, slots=True)
class DerivedMemory:
    candidate_id: str
    scope: object
    schema_digest: str
    rules_digest: str
    subject: str
    predicate: str
    object: str
    premise_ids: tuple[str, ...]
    premise_digest: str
    source_event_ids: tuple[str, ...]
    rule_versions: tuple[str, ...]
    valid_from: datetime
    valid_to: datetime | None
    confidence: float
    status: str = "candidate"


@dataclass(frozen=True, slots=True)
class InferenceResult:
    candidates: tuple[DerivedMemory, ...]
    truncated: bool


class OntologyRuleEngine:
    """Host-selected rules only. No OWL claims, activation, or implicit writes.

    All premises must belong to one exact scope. Revalidation refetches root
    assertions, verifies live evidence and reruns the pinned rule set. Consumers
    must call valid() before using a cached candidate; it is not an active fact.
    """

    def __init__(self, store, schema, verifier, rules, *, max_rounds=4, max_candidates=128):
        self.store, self.schema, self.verifier = store, schema, verifier
        self.rules = tuple(rules)
        if not 1 <= len(self.rules) <= 32 or len({r.rule_id for r in self.rules}) != len(self.rules):
            raise ValueError("configure one to 32 uniquely named rules")
        if type(max_rounds) is not int or not 1 <= max_rounds <= 8:
            raise ValueError("max_rounds must be between 1 and 8")
        if type(max_candidates) is not int or not 1 <= max_candidates <= 128:
            raise ValueError("max_candidates must be between 1 and 128")
        self.max_rounds, self.max_candidates = max_rounds, max_candidates
        for rule in self.rules:
            left, right, head = (schema.property_by_id(p) for p in
                (rule.left_predicate, rule.right_predicate, rule.conclusion_predicate))
            if any(p.range_class is None for p in (left, right, head)):
                raise ValueError("relation-chain rules require entity-valued properties")
            if not (schema.is_a(left.range_class, right.domain_class)
                    and schema.is_a(left.domain_class, head.domain_class)
                    and schema.is_a(right.range_class, head.range_class)):
                raise ValueError("rule domains and ranges are not type compatible")
        self.rules_digest = sha256(canonical_json(to_jsonable(self.rules)).encode()).hexdigest()

    async def derive(self, scope, premise_ids, *, at_time=None):
        now = at_time or datetime.now(UTC)
        if now.utcoffset() is None:
            raise ValueError("at_time must be timezone aware")
        if not isinstance(premise_ids, (tuple, list)) or not 1 <= len(premise_ids) <= 128:
            raise ValueError("provide one to 128 root assertion IDs")
        ids = tuple(dict.fromkeys(premise_ids))
        roots = await self.store.get_assertions(scope, ids, ontology_id=self.schema.ontology_id,
            ontology_version=self.schema.version, at_time=now)
        if {a.assertion_id for a in roots} != set(ids) or any(a.scope != scope for a in roots):
            raise ValueError("premises are missing, inactive, or outside the exact scope")
        for assertion in roots:
            if not await self.verifier.verify(scope, assertion.source_event_ids):
                raise ValueError("premise evidence is no longer available")
        by_id = {a.assertion_id: a for a in roots}
        facts = {(a.subject_entity_id, a.predicate_id, a.object_entity_id): (a.assertion_id,)
                 for a in roots if a.object_entity_id is not None}
        original = set(facts)
        steps = {key: () for key in facts}
        results = {}
        truncated = False
        for round_number in range(self.max_rounds):
            added = {}
            for rule in self.rules:
                lefts = [(key, proof) for key, proof in facts.items() if key[1] == rule.left_predicate]
                rights = [(key, proof) for key, proof in facts.items() if key[1] == rule.right_predicate]
                for left, left_proof in lefts:
                    for right, right_proof in rights:
                        if left[2] != right[0]:
                            continue
                        key = (left[0], rule.conclusion_predicate, right[2])
                        if key in facts or key in added or key in original:
                            continue
                        proof = tuple(sorted(set(left_proof + right_proof)))
                        evidence = tuple(sorted({e for pid in proof for e in by_id[pid].source_event_ids}))
                        if len(evidence) > 256:
                            truncated = True
                            continue
                        start = max(by_id[pid].valid_from for pid in proof)
                        ends = [by_id[pid].valid_to for pid in proof if by_id[pid].valid_to is not None]
                        end = min(ends) if ends else None
                        if end is not None and start >= end:
                            continue
                        versions = tuple(sorted(set(steps[left] + steps[right] + (rule.rule_id + "@" + rule.version,))))
                        digest = sha256(canonical_json(to_jsonable(tuple(by_id[p] for p in proof))).encode()).hexdigest()
                        identity = canonical_json((scope.partition_key(), ontology_schema_digest(self.schema),
                            self.rules_digest, key, proof, digest, versions))
                        candidate = DerivedMemory("derived-" + sha256(identity.encode()).hexdigest(),
                            scope, ontology_schema_digest(self.schema), self.rules_digest,
                            *key, proof, digest, evidence, versions, start, end,
                            min(by_id[pid].confidence for pid in proof))
                        if len(results) >= self.max_candidates:
                            truncated = True
                            continue
                        results[key] = candidate
                        added[key] = proof
                        steps[key] = versions
            if not added:
                break
            facts.update(added)
            if round_number + 1 == self.max_rounds:
                truncated = True
        # A source verifier can change while the bounded derivation runs.
        for assertion in roots:
            if not await self.verifier.verify(scope, assertion.source_event_ids):
                raise ValueError("premise evidence changed during inference")
        return InferenceResult(tuple(results.values()), truncated)

    async def valid(self, candidate, *, at_time=None):
        if (candidate.status != "candidate" or candidate.rules_digest != self.rules_digest
                or candidate.schema_digest != ontology_schema_digest(self.schema)):
            return False
        try:
            result = await self.derive(candidate.scope, candidate.premise_ids, at_time=at_time)
        except (ValueError, LookupError):
            return False
        return any(value == candidate for value in result.candidates)
