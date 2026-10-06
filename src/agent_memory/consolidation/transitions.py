"""Explicit state transitions and independently authorized continuity corrections."""

from dataclasses import replace
from datetime import datetime, timedelta

from ..contribution_state import withdrawal_barrier
from ..domain import canonical_json
from ..retrieval.atom_state import project_records
from .admission import authority_to_payload, draft_from_payload


async def transition(
    service,
    successor_id,
    *,
    predecessor_ids,
    valid_from,
    event,
    expected_versions,
    authority,
    policy,
    source_quote,
):
    """Close all independent supports of one predecessor using separate end evidence."""
    if not isinstance(valid_from, datetime) or valid_from.utcoffset() is None:
        raise ValueError("transition boundary requires a timezone")
    predecessor_ids = tuple(sorted(set(predecessor_ids)))
    if not predecessor_ids:
        raise ValueError("transition requires predecessor contributions")
    operation = {
        "kind": "transition",
        "successor": successor_id,
        "predecessors": predecessor_ids,
        "valid_from": valid_from.isoformat(),
        "expected_versions": expected_versions,
        "authority": authority_to_payload(authority),
        "policy": policy.config_payload(),
        "quote": source_quote,
    }
    async with service.engine.repository.unit_of_work() as uow:
        await uow.lock_admission_scope(service.scope)
        event, duplicate = await service._command(uow, event, operation)
        if duplicate:
            return duplicate
        rows, successor = await service._rows(uow, successor_id, expected_versions)
        p = successor["payload"]
        if p["action"] != "ACCEPT" or datetime.fromisoformat(p["valid_from"]) != valid_from:
            raise ValueError("successor must be accepted at the transition boundary")
        predecessors = [r for r in rows if r["id"] in predecessor_ids]
        if len(predecessors) != len(predecessor_ids):
            raise ValueError("predecessor unavailable in the same slot")
        old_value = canonical_json(predecessors[0]["payload"]["draft"]["value"])
        prior, _ = project_records(rows, valid_from - timedelta(microseconds=1))
        if len(prior) != 1 or canonical_json(prior[0].value) != old_value:
            raise ValueError("transition predecessor is not the resolved preceding state")
        required = {
            r["id"]
            for r in rows
            if r["payload"]["action"] == "ACCEPT"
            and canonical_json(r["payload"]["draft"]["value"]) == old_value
            and datetime.fromisoformat(r["payload"]["valid_from"]) < valid_from
            and (
                r["payload"]["valid_to"] is None
                or datetime.fromisoformat(r["payload"]["valid_to"]) >= valid_from
            )
        }
        if set(predecessor_ids) != required or old_value == canonical_json(p["draft"]["value"]):
            raise ValueError("transition must cover all independent predecessor supports")
        for row in predecessors:
            service._authorize(event, row, authority, policy, source_quote)
            old = row["payload"]
            if old.get("transitions"):
                raise ValueError("transition correction requires a separate continuity review")
            old["transitions"] = [
                {
                    "id": event.id,
                    "successor_id": successor_id,
                    "predecessor_ids": list(predecessor_ids),
                    "valid_from": valid_from.isoformat(),
                    "principal": service.principal,
                    "policy": policy.config_payload(),
                    "end_support": {
                        "source_event_id": event.id,
                        "source_quote": source_quote,
                        "authority": authority_to_payload(authority),
                    },
                    "start_support": {"source_event_id": successor["event_id"]},
                }
            ]
            old["valid_to"] = valid_from.isoformat()
            old["claim"] = {**old["claim"], "valid_to": valid_from.isoformat()}
            old["reasons"] = ["host_approved_transition"]
            service.engine._decision(old, policy)
        # Source references must exist before admission snapshots are saved.
        result = {"operation_id": event.id, "candidate_ids": [*predecessor_ids, successor_id]}
        await uow.append_event(
            replace(
                event,
                metadata={
                    **event.metadata,
                    "contribution_result": result,
                },
                content_hash="",
            )
        )
        for row in rows:
            await uow.save_admission_record(
                service.scope,
                row["id"],
                row["event_id"],
                row["slot_key"],
                row["payload"],
                row["version"],
            )
        return {**result, "duplicate": False}


async def correct_transition(
    service,
    predecessor_id,
    *,
    transition_id,
    valid_to,
    event,
    expected_versions,
    authority,
    policy,
    source_quote,
):
    """Requalify continuity explicitly after the competing new state lost all support.

    This bounded correction reopens/extends the predecessor from the recorded
    transition boundary. Moving its start or changing its identity is unsupported.
    """
    if valid_to is not None and (
        not isinstance(valid_to, datetime) or valid_to.utcoffset() is None
    ):
        raise ValueError("corrected end requires a timezone")
    operation = {
        "kind": "correct_transition",
        "predecessor": predecessor_id,
        "transition_id": transition_id,
        "valid_to": valid_to.isoformat() if valid_to else None,
        "expected_versions": expected_versions,
        "authority": authority_to_payload(authority),
        "policy": policy.config_payload(),
        "quote": source_quote,
    }
    async with service.engine.repository.unit_of_work() as uow:
        await uow.lock_admission_scope(service.scope)
        event, duplicate = await service._command(uow, event, operation)
        if duplicate:
            return duplicate
        rows, anchor = await service._rows(uow, predecessor_id, expected_versions)
        target = next(
            (t for t in anchor["payload"].get("transitions", []) if t["id"] == transition_id),
            None,
        )
        if target is None or target.get("correction"):
            raise ValueError("transition unavailable or already corrected")
        boundary = datetime.fromisoformat(target["valid_from"])
        if valid_to is not None and valid_to <= boundary:
            raise ValueError("continuity correction must extend past the prior boundary")
        predecessors = [r for r in rows if r["id"] in target["predecessor_ids"]]
        if any(r["payload"]["action"] != "ACCEPT" for r in predecessors):
            raise ValueError("withdrawn predecessor cannot be restored implicitly")
        value = canonical_json(anchor["payload"]["draft"]["value"])
        for row in rows:
            p = row["payload"]
            if (
                p["action"] in {"ACCEPT", "CONTESTED"}
                and canonical_json(p["draft"]["value"]) != value
                and (valid_to is None or datetime.fromisoformat(p["valid_from"]) < valid_to)
                and (p["valid_to"] is None or datetime.fromisoformat(p["valid_to"]) > boundary)
            ):
                raise ValueError("continuity remains opposed by independent evidence")
        barriers = await uow.list_admission_barriers(service.scope, anchor["slot_key"])
        for predecessor in predecessors:
            payload = predecessor["payload"]
            transition = next(
                (t for t in payload.get("transitions", []) if t["id"] == transition_id), None
            )
            if transition is None or transition.get("correction"):
                raise ValueError("predecessor transition changed")
            draft = replace(
                draft_from_payload(payload["draft"]),
                source_quote=source_quote,
                valid_from=boundary,
                valid_to=valid_to,
            )
            action, reasons = policy.evaluate(event, draft, authority)
            if action != "ACCEPT":
                raise ValueError("continuity evidence failed admission: " + ", ".join(reasons))
            barrier_ids = [
                r["id"]
                for r in (*rows, *barriers)
                if predecessor["id"] in withdrawal_barrier(r["payload"])
                and datetime.fromisoformat(r["payload"]["valid_from"]) >= boundary
                and (
                    valid_to is None
                    or datetime.fromisoformat(r["payload"]["valid_from"]) < valid_to
                )
            ]
            transition["correction"] = {
                "id": event.id,
                "principal": service.principal,
                "policy": policy.config_payload(),
                "valid_to": valid_to.isoformat() if valid_to else None,
                "barrier_ids": sorted(barrier_ids),
                "evidence": {
                    "source_event_id": event.id,
                    "source_quote": source_quote,
                    "authority": authority_to_payload(authority),
                },
            }
            transition["correction_status"] = "qualified"
            payload["valid_to"] = valid_to.isoformat() if valid_to else None
            payload["claim"] = {**payload["claim"], "valid_to": payload["valid_to"]}
            payload["reasons"] = ["host_requalified_predecessor_continuity"]
            service.engine._decision(payload, policy)
        result = {"operation_id": event.id, "candidate_ids": [r["id"] for r in predecessors]}
        await uow.append_event(
            replace(
                event,
                metadata={
                    **event.metadata,
                    "contribution_result": result,
                },
                content_hash="",
            )
        )
        for row in rows:
            await uow.save_admission_record(
                service.scope,
                row["id"],
                row["event_id"],
                row["slot_key"],
                row["payload"],
                row["version"],
            )
        return {**result, "duplicate": False}
