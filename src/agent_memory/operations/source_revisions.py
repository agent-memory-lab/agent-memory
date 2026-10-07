"""Immutable source revisions and document-head CAS within the receive transaction."""

from contextlib import nullcontext

from .. import domain
from .retention import RetentionError, _identity


async def document_head(uow, source):
    metadata = source.metadata.get("_retention")
    if not isinstance(metadata, dict):
        raise RetentionError("source_is_not_retained")
    document_id = metadata.get("document_id", source.id)
    head = await uow.retention_head_get(source.scope, "document", document_id)
    if head is None:
        # Adopt previously committed v1 sources without rewriting their L0 rows.
        ticket = await uow.retention_get(source.scope, "ticket", metadata["request_id"])
        if ticket is None or ticket["revoked"]:
            raise RetentionError("source_unavailable")
        payload = {
            "event_id": source.id,
            "revision": 1,
            "producer_id": ticket["producer_id"],
            "epoch": ticket["epoch"],
        }
        await uow.retention_head_put(source.scope, "document", document_id, payload, 0)
        head = {"generation": 1, "payload": payload}
    return document_id, head


async def source_is_current(uow, source):
    _, head = await document_head(uow, source)
    return head["payload"]["event_id"] == source.id and head["payload"][
        "epoch"
    ] == await uow.retention_epoch(source.scope)


async def interpretation_head(uow, source):
    """The registered primary stream is one head per immutable source revision."""
    head = await uow.retention_head_get(source.scope, "interpretation", source.id)
    if head is not None:
        if head["payload"].get("publication_closed") is False:
            raise RetentionError("initial_interpretation_not_ready")
        return head
    initial = await uow.retention_get(
        source.scope, "request", source.metadata["_retention"]["request_id"]
    )
    if initial is None or initial["status"] != "completed":
        raise RetentionError("initial_interpretation_not_ready")
    rows = [r for r in await uow.list_admission_records(source.scope) if r["event_id"] == source.id]
    active = [
        r["id"] for r in rows if r["payload"]["action"] not in {"REJECT", "L0_ONLY", "WITHDRAWN"}
    ]
    payload = {"active_ids": active, "request_id": initial["request_id"], "stream": "primary"}
    await uow.retention_head_put(source.scope, "interpretation", source.id, payload, 0)
    return {"generation": 1, "payload": payload}


async def withdraw_revision(uow, source, request_id):
    """Stop this source revision's current contributions; preserve known-at history."""
    records = [
        r
        for r in await uow.list_admission_records(source.scope)
        if source.id in r["payload"]["source_event_ids"]
    ]
    for row in records:
        payload = row["payload"]
        if payload["action"] in {"WITHDRAWN", "REJECT", "L0_ONLY"}:
            continue
        if (
            payload["source_event_ids"] != [source.id]
            or payload["draft"]["change_kind"] != "replace"
            or payload.get("termination")
        ):
            raise RetentionError("interpretation_capability_unsupported")
        payload.update(action="WITHDRAWN", reasons=["source_revision_superseded"])
        payload["decisions"].append(
            {
                "action": "WITHDRAWN",
                "reasons": payload["reasons"],
                "recorded_at": domain.utc_now().isoformat(),
                "request_id": request_id,
            }
        )
        await uow.save_admission_record(
            source.scope, row["id"], row["event_id"], row["slot_key"], payload, row["version"]
        )
    head = await uow.retention_head_get(source.scope, "interpretation", source.id)
    if head is not None:
        await uow.retention_head_put(
            source.scope,
            "interpretation",
            source.id,
            {"active_ids": [], "request_id": request_id, "stream": "primary", "superseded": True},
            head["generation"],
        )
    # Release obsolete work capacity in the same transaction as the revision.
    for row in await uow.retention_active(source.scope):
        if row["event_id"] != source.id:
            continue
        row = {
            k: v
            for k, v in row.items()
            if k not in {"prepared", "result", "input_manifest", "lease_token", "publication_manifest"}
        }
        row.update(status="superseded", superseded_by=request_id)
        await uow.retention_update(source.scope, row["request_id"], row)


async def revise(
    receiver,
    event,
    *,
    base_event_id,
    expected_revision,
    request_id,
    producer_id,
    configuration_sha256,
    _unit_of_work=None,
):
    """Trusted host document update; new event identity is mandatory, not an overwrite."""
    _identity(base_event_id)
    _identity(request_id)
    if type(expected_revision) is not int or expected_revision < 1 or event.id == base_event_id:
        raise RetentionError("invalid_source_revision")
    source, fingerprint = receiver._input(event, producer_id, configuration_sha256)
    revision_input = {
        "base_event_id": base_event_id,
        "expected_revision": expected_revision,
        "source_fingerprint": fingerprint,
    }
    context = (
        nullcontext(_unit_of_work)
        if _unit_of_work is not None
        else receiver.repository.unit_of_work()
    )
    async with context as uow:
        await receiver._check_support(uow, source.scope)
        old = await uow.retention_get(source.scope, "request", request_id)
        if old is not None:
            if old.get("revision_input") != revision_input:
                raise RetentionError("request_input_conflict")
            if old["epoch"] != await uow.retention_epoch(
                source.scope
            ) or not await uow.events_exist(source.scope, (old["event_id"],)):
                raise RetentionError("source_unavailable")
            return receiver._receipt(old, duplicate=True)
        base = await uow.get_source_event(source.scope, base_event_id)
        if base is None:
            raise RetentionError("source_unavailable")
        document_id, head = await document_head(uow, base)
        payload = head["payload"]
        if payload["epoch"] != await uow.retention_epoch(source.scope):
            raise RetentionError("source_unavailable")
        if payload["producer_id"] != producer_id or base.actor != event.actor:
            raise RetentionError("source_owner_mismatch")
        if payload["event_id"] != base.id or payload["revision"] != expected_revision:
            raise RetentionError("document_head_changed")
        await withdraw_revision(uow, base, request_id)
        ticket = await receiver.issue_ticket(
            source,
            request_id=request_id,
            producer_id=producer_id,
            configuration_sha256=configuration_sha256,
            _unit_of_work=uow,
        )
        receipt = await receiver.submit(
            source,
            ticket=ticket,
            producer_id=producer_id,
            configuration_sha256=configuration_sha256,
            _unit_of_work=uow,
            _revision={
                "document_id": document_id,
                "revision": expected_revision + 1,
                "parent_event_id": base.id,
            },
        )
        await uow.retention_head_put(
            source.scope,
            "document",
            document_id,
            {**payload, "event_id": source.id, "revision": expected_revision + 1},
            head["generation"],
        )
        row = await uow.retention_get(source.scope, "request", request_id)
        row["revision_input"] = revision_input
        await uow.retention_update(source.scope, request_id, row)
        return receipt


async def source_available_at(uow, source, known_at):
    """Revision visibility at known_at, while source existence obeys current erasure."""
    if "_retention" not in source.metadata:
        return True
    _, head = await document_head(uow, source)
    identity = head["payload"]["event_id"]
    for _ in range(256):
        revision = await uow.get_source_event(source.scope, identity)
        if revision is None:
            return False
        metadata = revision.metadata["_retention"]
        request = await uow.retention_get(source.scope, "request", metadata["request_id"])
        if request is None:
            return False
        from datetime import datetime

        if datetime.fromisoformat(request["received_at"]) <= known_at:
            return revision.id == source.id
        identity = metadata.get("parent_event_id")
        if identity is None:
            return False
    raise ValueError("source revision history budget exceeded")
