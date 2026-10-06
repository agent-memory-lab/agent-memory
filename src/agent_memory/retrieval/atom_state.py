"""Pure bitemporal projection of qualified Atom snapshots and evidence."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from ..contribution_state import waived_barriers, withdrawal_barrier
from ..domain import Claim, ClaimStatus, MemoryScope, Provenance


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _inside(payload: dict[str, Any], at: datetime) -> bool:
    start, end = _time(payload["valid_from"]), _time(payload.get("valid_to"))
    return start <= at and (end is None or at < end)


def _claim(payload: dict[str, Any]) -> Claim:
    values = dict(payload["claim"])
    values["scope"] = MemoryScope(**values["scope"])
    values["status"] = ClaimStatus(values["status"])
    provenance = dict(values["provenance"])
    provenance["source_event_ids"] = tuple(provenance["source_event_ids"])
    provenance["created_at"] = _time(provenance["created_at"])
    values["provenance"] = Provenance(**provenance)
    for name in ("valid_from", "valid_to", "created_at", "system_from", "system_to"):
        values[name] = _time(values.get(name))
    return Claim(**values)


def project_records(
    records: Sequence[dict[str, Any]],
    at: datetime,
) -> tuple[tuple[Claim, ...], dict[str, Any]]:
    """Project already selected system snapshots at one reality-time point."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault(record["slot_key"], []).append(record)
    claims, conflicts, support, context_required = [], [], {}, []
    for key, rows in sorted(groups.items()):
        if any(
            r["payload"].get("qualification")
            and r["payload"]["action"] not in {"WITHDRAWN", "REJECT", "L0_ONLY"}
            for r in rows
        ):
            context_required.append(key)
            continue
        barriers = [(r, withdrawal_barrier(r["payload"])) for r in rows]
        waivers = {r["id"]: waived_barriers(r["payload"]) for r in rows}
        suppressed = {
            identity
            for r, ids in barriers
            if ids and _time(r["payload"]["valid_from"]) <= at
            for identity in ids
            if r["id"] not in waivers.get(identity, ())
        }
        accepted = [
            r for r in rows if r["payload"]["action"] == "ACCEPT" and r["id"] not in suppressed
        ]
        corrected = {r["payload"].get("corrects") for r in accepted}
        accepted = [r for r in accepted if r["id"] not in corrected]
        replacement_starts = [
            _time(r["payload"]["valid_from"])
            for r in accepted
            if r["payload"]["draft"]["change_kind"] != "temporary_override"
        ]
        disputes = [r for r in rows if r["payload"]["action"] == "CONTESTED"]
        blocked = []
        for dispute in disputes:
            start = _time(dispute["payload"]["valid_from"])
            later = [boundary for boundary in replacement_starts if boundary > start]
            # An unresolved possible replacement cannot revive the older value
            # on expiry. A later independently accepted replacement bounds it.
            end = min(later) if later else None
            if start <= at and (end is None or at < end):
                blocked.append(dispute)
        if blocked:
            conflicts.append({"slot_key": key, "candidate_ids": [r["id"] for r in blocked]})
            continue
        replacements = [
            r
            for r in accepted
            if r["payload"]["draft"]["change_kind"] != "temporary_override"
            and _time(r["payload"]["valid_from"]) <= at
        ]
        if not replacements:
            continue
        # Select a boundary before checking its end. Expired replacement means
        # unknown, not resurrection of the previous value.
        base = max(
            replacements,
            key=lambda r: (
                _time(r["payload"]["valid_from"]),
                r["payload"].get("resolved_at", ""),
                r["recorded_at"],
                r["id"],
            ),
        )
        if not _inside(base["payload"], at):
            continue
        related_overrides = [
            r for r in accepted if r["payload"].get("base_candidate_id") == base["id"]
        ]
        overrides = [r for r in related_overrides if _inside(r["payload"], at)]
        chosen = overrides[0] if len(overrides) == 1 else base
        if len(overrides) > 1:
            conflicts.append({"slot_key": key, "candidate_ids": [r["id"] for r in overrides]})
            continue
        payload = chosen["payload"]
        claim = _claim(payload)
        segment_start = claim.valid_from
        if chosen is base:
            prior_ends = [
                _time(r["payload"]["valid_to"])
                for r in related_overrides
                if _time(r["payload"]["valid_to"]) <= at
            ]
            segment_start = max([segment_start, *prior_ends])
        # The next true replacement ends this state, even when learned later.
        boundaries = [
            _time(r["payload"]["valid_from"])
            for r in replacements
            if _time(r["payload"]["valid_from"]) > claim.valid_from
        ]
        boundaries += [
            _time(r["payload"]["valid_from"])
            for r in accepted
            if r["payload"]["draft"]["change_kind"] != "temporary_override"
            and _time(r["payload"]["valid_from"]) > at
        ]
        if chosen is base:
            boundaries += [
                _time(r["payload"]["valid_from"])
                for r in related_overrides
                if _time(r["payload"]["valid_from"]) > at
            ]
        boundaries += [
            _time(r["payload"]["valid_from"])
            for r in disputes
            if _time(r["payload"]["valid_from"]) > at
        ]
        boundaries += [
            _time(r["payload"]["valid_from"])
            for r, ids in barriers
            if base["id"] in ids
            and _time(r["payload"]["valid_from"]) > at
            and r["id"] not in waivers.get(base["id"], ())
        ]
        ends = [value for value in [claim.valid_to, *boundaries] if value is not None]
        knowledge_starts = [_time(r["recorded_at"]) for r in rows if r["recorded_at"]]
        knowledge_ends = [_time(r["_next_recorded_at"]) for r in rows if r.get("_next_recorded_at")]
        claim = replace(
            claim,
            valid_from=segment_start,
            valid_to=min(ends) if ends else None,
            system_from=max(knowledge_starts),
            system_to=min(knowledge_ends) if knowledge_ends else None,
        )
        claims.append(claim)
        applicable = []
        for evidence in payload["evidence"]:
            if evidence["support_kind"] == "point":
                applies = _time(evidence["support_at"]) == at
            else:
                start, end = _time(evidence["support_from"]), _time(evidence["support_to"])
                applies = start <= at and (end is None or at < end)
            if applies:
                applicable.append(evidence)
        support[claim.id] = {
            "candidate_id": chosen["id"],
            "evidence": applicable,
            "basis": "source_assertion" if applicable else "assumed_continuity",
            **({"termination": payload["termination"]} if payload.get("termination") else {}),
            **({"transitions": payload["transitions"]} if payload.get("transitions") else {}),
        }
    return tuple(claims), {
        "conflicts": conflicts,
        "atom_support": support,
        **({"context_required": context_required} if context_required else {}),
    }
