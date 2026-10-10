"""Host-authored storage and domain-verification policies, independent of model scores.

Source review establishes what was said. These rules separately decide whether
the assertion is useful in the selected scope and which authenticated witness
may turn a pending assertion into usable knowledge. Capture retention remains
the host's responsibility; L0_ONLY never promises to erase the original source.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256

from ..domain import ScopeLevel, canonical_json
from .admission import AdmissionPolicy


@dataclass(frozen=True, slots=True)
class MemoryRule:
    predicate: str
    storage: str = "durable"
    verification: str = "source"
    allowed_scopes: tuple[ScopeLevel, ...] = (ScopeLevel.SESSION,)
    allowed_kinds: tuple[str, ...] = ("fact", "preference", "constraint")
    verification_sources: tuple[str, ...] = ()

    def __post_init__(self):
        if type(self.predicate) is not str or not 1 <= len(self.predicate) <= 256:
            raise ValueError("bounded business predicate required")
        if self.storage not in {"durable", "session", "l0_only"}:
            raise ValueError("invalid business storage policy")
        if self.verification not in {"source", "domain"}:
            raise ValueError("invalid business verification policy")
        scopes = tuple(ScopeLevel(value) for value in self.allowed_scopes)
        if not scopes or len(scopes) > len(ScopeLevel) or len(set(scopes)) != len(scopes):
            raise ValueError("bounded unique business scopes required")
        object.__setattr__(self, "allowed_scopes", scopes)
        for name, allowed, limit in (
            ("allowed_kinds", {"fact", "preference", "constraint", "event"}, 4),
            ("verification_sources", None, 32),
        ):
            values = tuple(getattr(self, name))
            if len(values) > limit or len(set(values)) != len(values):
                raise ValueError("bounded unique business rule identifiers required")
            if any(type(v) is not str or not 1 <= len(v) <= 256 for v in values):
                raise ValueError("invalid business rule identifier")
            if allowed is not None and (not values or not set(values) <= allowed):
                raise ValueError("invalid business atom kinds")
            object.__setattr__(self, name, values)
        if self.verification == "domain" and not self.verification_sources:
            raise ValueError("domain verification requires registered witness identities")
        if self.storage == "session" and scopes != (ScopeLevel.SESSION,):
            raise ValueError("session storage cannot promote outside the session")


@dataclass(frozen=True, slots=True)
class BusinessDecision:
    action: str
    reasons: tuple[str, ...]
    policy_sha256: str
    storage: str
    verification: str


class BusinessAdmissionPolicy(AdmissionPolicy):
    version = "business-memory-policy/1"

    def __init__(self, predicates, rules, *, revision, source_families=()):
        predicates, rules, source_families = tuple(predicates), tuple(rules), tuple(source_families)
        super().__init__(predicates)
        if type(revision) is not str or not 1 <= len(revision) <= 128:
            raise ValueError("host policy revision required")
        if not 1 <= len(rules) <= 128 or any(type(r) is not MemoryRule for r in rules):
            raise ValueError("bounded business rules required")
        if len({r.predicate for r in rules}) != len(rules):
            raise ValueError("duplicate business predicate rule")
        if {r.predicate for r in rules} != set(self._predicates):
            raise ValueError("each registered predicate requires an explicit business rule")
        if len(source_families) > 256:
            raise ValueError("business source family capacity exceeded")
        families = {}
        for pair in source_families:
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise ValueError("source family binding must have identity and family")
            identity, family = pair
            if (
                any(type(v) is not str or not 1 <= len(v) <= 256 for v in pair)
                or identity in families
            ):
                raise ValueError("invalid source family binding")
            families[identity] = family
        self.rules = {r.predicate: r for r in rules}
        self.revision, self.source_families = revision, families

    def config_payload(self):
        return {
            **super().config_payload(),
            "revision": self.revision,
            "rules": [asdict(self.rules[key]) for key in sorted(self.rules)],
            "source_families": sorted(self.source_families.items()),
        }

    @property
    def fingerprint(self):
        return sha256(canonical_json(self.config_payload()).encode()).hexdigest()

    def _storage(self, draft):
        rule = self.rules.get(draft.predicate)
        if rule is None:
            return None, ("business_predicate_not_registered",)
        if rule.storage == "l0_only":
            return rule, ("business_storage_l0_only",)
        if draft.scope_level not in rule.allowed_scopes:
            return rule, ("business_scope_not_allowed",)
        if draft.kind not in rule.allowed_kinds:
            return rule, ("business_kind_not_reusable",)
        return rule, ()

    def evaluate(self, event, draft, authority):
        action, reasons = super().evaluate(event, draft, authority)
        rule, storage_reasons = self._storage(draft)
        if storage_reasons:
            return ("L0_ONLY" if rule else "PENDING_VERIFICATION"), (*storage_reasons, *reasons)
        if action == "ACCEPT" and rule.verification == "domain":
            return "PENDING_VERIFICATION", ("business_requires_domain_verification", *reasons)
        return action, reasons

    def assess(self, event, draft, authority):
        action, reasons = self.evaluate(event, draft, authority)
        rule = self.rules.get(draft.predicate)
        return BusinessDecision(
            action,
            reasons,
            self.fingerprint,
            rule.storage if rule else "unregistered",
            rule.verification if rule else "unregistered",
        )

    def evaluate_verification(self, event, draft, authority, *, original_authority):
        action, reasons = super().evaluate(event, draft, authority)
        rule, storage_reasons = self._storage(draft)
        if storage_reasons:
            return "PENDING_VERIFICATION", (*storage_reasons, *reasons)
        if rule.verification == "domain":
            failures = []
            if authority.source_id not in rule.verification_sources:
                failures.append("business_verifier_not_registered")
            if authority.kind not in {"tool_observation", "document"}:
                failures.append("business_verifier_kind_not_authoritative")
            left = self.source_families.get(authority.source_id, authority.source_id)
            right = self.source_families.get(
                original_authority.source_id, original_authority.source_id
            )
            if authority.source_id == original_authority.source_id or left == right:
                failures.append("business_verifier_not_independent")
            if failures:
                return "PENDING_VERIFICATION", (*failures, *reasons)
        return action, ("business_verification_policy_satisfied", *reasons)

    def guard_domain_publisher(self, publisher):
        """Apply the same witness policy before a native field/project publisher.

        Native publishers own semantic field/time qualification. This wrapper
        only adds the host's storage and independent-witness contract and keeps
        its configuration frozen across the awaited native publication.
        """
        from ..retrieval.model_contracts import ModelError
        from .admission import authority_from_payload, draft_from_payload

        if not callable(publisher):
            raise ValueError("native domain publisher required")
        expected = self.fingerprint

        async def guarded(uow, candidate, finding, spec):
            if self.fingerprint != expected:
                raise ModelError("business_verification_policy_changed")
            draft = draft_from_payload(candidate["payload"]["draft"])
            rule, storage_reasons = self._storage(draft)
            if storage_reasons:
                raise ModelError("business_verification_storage_denied")
            original = authority_from_payload(candidate["payload"]["authority"])
            if rule.verification == "domain":
                left = self.source_families.get(spec.authority.source_id, spec.authority.source_id)
                right = self.source_families.get(original.source_id, original.source_id)
                if spec.authority.source_id not in rule.verification_sources:
                    raise ModelError("business_verifier_not_registered")
                if spec.authority.source_id == original.source_id or left == right:
                    raise ModelError("business_verifier_not_independent")
            await publisher(uow, candidate, finding, spec)
            if self.fingerprint != expected:
                raise ModelError("business_verification_policy_changed")

        guarded.business_policy_sha256 = expected
        return guarded
