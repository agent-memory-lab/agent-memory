"""Host-authorized valid-time termination with a separate recorded-time snapshot."""

from dataclasses import replace
from datetime import datetime
from hashlib import sha256

from .. import domain
from ..domain import EvidenceSupport, canonical_json
from ..lifecycle import is_memory_context
from ..serialization import to_jsonable
from .admission import authority_to_payload, draft_from_payload


async def retract(
    engine, scope, identity, *, event, authority, policy, expected_version, valid_to, source_quote
):
    """End an accepted interval; do not assert a replacement or revive older values.

    Exact-scope host API. The new event independently supplies the termination
    basis, which survives as evidence and a versioned decision. Removing either
    source uses the existing conservative slot withdrawal policy.
    """
    engine._require_support()
    if event.scope != scope or is_memory_context(event):
        raise ValueError("retraction requires independent evidence in the exact scope")
    if type(expected_version) is not int or expected_version < 1:
        raise ValueError("expected_version must be positive")
    evidence = EvidenceSupport(
        event.id,
        authority.kind,
        relation="refutes",
        support_kind="interval",
        support_from=valid_to,
        recorded_at=domain.utc_now(),
    )
    valid_to = evidence.support_from
    if len(event.content) > 32_000:
        raise ValueError("retraction event exceeds content limit")
    fingerprint = sha256(
        canonical_json(
            {
                "candidate": identity,
                "valid_to": valid_to.isoformat(),
                "quote": source_quote,
                "content": event.content,
                "actor": event.actor,
                "source_uri": event.source_uri,
                "occurred_at": event.occurred_at.isoformat(),
                "authority": authority_to_payload(authority),
                "policy": policy.config_payload(),
            }
        ).encode()
    ).hexdigest()
    event = replace(
        event,
        event_type="memory.atom.verification",
        idempotency_key=event.idempotency_key or event.id,
        metadata={"atom_retraction_fingerprint": fingerprint},
        content_hash="",
    )
    async with engine.repository.unit_of_work() as uow:
        await uow.lock_admission_scope(scope)
        row = await uow.get_admission_record(scope, identity)
        saved = await uow.find_event_by_idempotency(scope, event.idempotency_key)
        if saved is not None:
            if (
                saved.metadata.get("atom_retraction_fingerprint") != fingerprint
                or row is None
                or not await uow.events_exist(scope, (saved.id,))
            ):
                raise ValueError("retraction identity conflict or deleted evidence")
            return engine.receipt(saved.id, [row], duplicate=True, initial=False)
        if row is None or row["version"] != expected_version:
            raise ValueError("candidate version changed or was deleted")
        payload = row["payload"]
        if payload["action"] != "ACCEPT" or payload["draft"]["change_kind"] == "temporary_override":
            raise ValueError("retraction requires an accepted ordinary state")
        if valid_to <= datetime.fromisoformat(payload["valid_from"]):
            raise ValueError("termination must be after the interval start")
        if payload["valid_to"] and valid_to >= datetime.fromisoformat(payload["valid_to"]):
            raise ValueError("retraction cannot extend an interval")
        original = draft_from_payload(payload["draft"])
        checked = replace(original, source_quote=source_quote, valid_from=valid_to, valid_to=None)
        action, reasons = policy.evaluate(event, checked, authority)
        if action != "ACCEPT":
            raise ValueError("termination source failed admission: " + ", ".join(reasons))
        await uow.append_event(event)
        payload["source_event_ids"].append(event.id)
        payload["termination"] = {
            **to_jsonable(evidence),
            "source_quote": source_quote,
            "authority": authority_to_payload(authority),
        }
        payload["valid_to"] = valid_to.isoformat()
        payload["claim"] = {**payload["claim"], "valid_to": valid_to.isoformat()}
        payload["reasons"] = ["host_terminated_with_independent_evidence"]
        engine._decision(payload, policy)
        await uow.save_admission_record(
            scope, identity, row["event_id"], row["slot_key"], payload, expected_version
        )
        return engine.receipt(event.id, [row], initial=False)
