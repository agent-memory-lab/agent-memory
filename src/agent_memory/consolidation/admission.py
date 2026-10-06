"""Pure, explicit admission for registered single-state predicates.

The host supplies an authenticated SourceAuthority. A matching quote locates
evidence in saved content; it does not establish semantic entailment or truth.
This policy contains no model-confidence or event-metadata trust fallback.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from hashlib import sha256
from json import dumps
from typing import Any

from ..domain import AtomDraft, MemoryEvent, MemoryScope, PredicateSpec, SourceAuthority


def _canonical(payload: Any) -> str:
    return dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def draft_to_payload(draft: AtomDraft) -> dict[str, Any]:
    payload = asdict(draft)
    payload["scope_level"] = draft.scope_level.value
    for name in ("valid_from", "valid_to"):
        value = getattr(draft, name)
        payload[name] = value.isoformat() if value is not None else None
    return payload


def draft_from_payload(payload: Mapping[str, Any]) -> AtomDraft:
    values = dict(payload)
    for name in ("valid_from", "valid_to"):
        if values.get(name) is not None:
            if not isinstance(values[name], str):
                raise ValueError(f"{name} must be an ISO timestamp string")
            values[name] = datetime.fromisoformat(values[name])
    return AtomDraft(**values)


def authority_to_payload(authority: SourceAuthority) -> dict[str, Any]:
    return {
        "source_id": authority.source_id,
        "kind": authority.kind,
        "subjects": list(authority.subjects),
        "predicates": list(authority.predicates),
    }


def authority_from_payload(payload: Mapping[str, Any]) -> SourceAuthority:
    return SourceAuthority(**dict(payload))


def slot_key(scope: MemoryScope, draft: AtomDraft) -> str:
    """Identify an attribute, independent of value and evidence event."""
    payload = {
        "scope": asdict(scope.project(draft.scope_level)),
        "subject_id": draft.subject_id,
        "predicate": draft.predicate,
    }
    return "atom:" + sha256(_canonical(payload).encode("utf-8")).hexdigest()


def candidate_id(event: MemoryEvent, draft: AtomDraft) -> str:
    """Stable identity for a saved input candidate, retaining every draft field."""
    payload = {"event_id": event.id, "scope": asdict(event.scope), "draft": draft_to_payload(draft)}
    return "candidate:" + sha256(_canonical(payload).encode("utf-8")).hexdigest()


class AdmissionPolicy:
    version = "atom-admission-v1"

    def __init__(self, predicates: Sequence[PredicateSpec]) -> None:
        self._predicates: dict[str, PredicateSpec] = {}
        for spec in predicates:
            if spec.predicate in self._predicates:
                raise ValueError(f"duplicate predicate: {spec.predicate}")
            self._predicates[spec.predicate] = spec

    def config_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "predicates": [asdict(self._predicates[key]) for key in sorted(self._predicates)],
        }

    def evaluate(
        self,
        event: MemoryEvent,
        draft: AtomDraft,
        authority: SourceAuthority,
    ) -> tuple[str, tuple[str, ...]]:
        reasons: list[str] = []
        if not draft.source_quote.strip():
            reasons.append("source_quote_missing")
        elif draft.source_quote not in event.content:
            reasons.append("source_quote_not_found")
        if draft.subject_id not in authority.subjects:
            reasons.append("subject_not_authorized")
        if draft.predicate not in authority.predicates:
            reasons.append("predicate_not_authorized")
        if authority.kind not in {"self_report", "tool_observation", "document"}:
            reasons.append("unsupported_source_kind")
        spec = self._predicates.get(draft.predicate)
        if spec is None:
            reasons.append("predicate_not_registered")
        else:
            types = {
                "string": (str,),
                "integer": (int,),
                "number": (int, float),
                "boolean": (bool,),
            }
            allowed = types.get(spec.value_type)
            if allowed is None:
                reasons.append("unsupported_value_type")
            elif type(draft.value) not in allowed:
                reasons.append("value_type_mismatch")
            if authority.kind == "self_report" and not spec.allow_self_report:
                reasons.append("self_report_not_allowed")
        try:
            event.scope.project(draft.scope_level)
        except ValueError:
            reasons.append("scope_not_available")
        effective_from = draft.valid_from or event.occurred_at
        if not isinstance(effective_from, datetime) or effective_from.utcoffset() is None:
            reasons.append("invalid_observation_time")
        elif draft.valid_to is not None and draft.valid_to <= effective_from:
            reasons.append("invalid_effective_interval")
        if reasons:
            return "PENDING_VERIFICATION", tuple(reasons)
        if draft.modality != "asserted":
            return "L0_ONLY", ("non_asserted_modality",)
        if draft.kind not in {"fact", "preference", "constraint"}:
            return "L0_ONLY", ("unsupported_atom_kind",)
        return "ACCEPT", ("registered_predicate_with_authorized_source",)
