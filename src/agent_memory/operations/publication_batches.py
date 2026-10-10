"""Bounded, slot-preserving publication; replacement activation remains atomic."""

from copy import deepcopy

from ..consolidation.admission import draft_from_payload, slot_key
from ..serialization import to_jsonable
from .publication_manifest import PublicationPolicy as PublicationPolicy
from .publication_manifest import (
    append_batch,
    close,
    close_batches,
    digest,
    open_batches,
    valid_manifest,
)
from .retention import RetentionError, _time


def pack(items, size):
    groups = {}
    for identity, slot in items:
        groups.setdefault(slot, []).append(identity)
    batches, current = [], []
    for group in groups.values():
        if current and len(current) + len(group) > size:
            batches.append(current)
            current = []
        current.extend(group)
    if current:
        batches.append(current)
    return batches or [[]]


def initial_plan(scope, prepared, policy):
    return pack(
        ((i, slot_key(scope, draft_from_payload(d))) for i, d in enumerate(prepared["drafts"])),
        policy.batch_size,
    )


def subset(prepared, indices, first):
    batch = deepcopy(prepared)
    positions = {original: local for local, original in enumerate(indices)}
    batch["drafts"] = [batch["drafts"][i] for i in indices]
    reports = []
    for report in batch["audit"]["reports"]:
        index = report.get("draft_index")
        if index in positions:
            report["draft_index"] = positions[index]
            reports.append(report)
        elif index is None and first:
            reports.append(report)
    batch["audit"]["reports"] = reports
    return batch


def aggregate(row, audit):
    receipts = [p["receipt"] for p in row["publication_manifest"]["publications"]]
    return {
        "event_id": row["event_id"],
        "duplicate": False,
        "extraction": audit,
        **{
            name: [item for receipt in receipts for item in receipt[name]]
            for name in ("candidate_ids", "claim_ids", "decisions", "pending_ids")
        },
    }


async def save(handler, uow, scope, row):
    if not valid_manifest(scope, row):
        raise RetentionError("publication_manifest_invalid")
    if handler.index_channel is not None:
        from .indexing import enqueue

        await enqueue(uow, scope, row, handler.index_channel)
    await uow.retention_update(scope, row["request_id"], row)


async def checked_head(uow, scope, source, row):
    current = await uow.get_source_event(scope, source.id)
    if current is None or current.content_hash != source.content_hash:
        raise RetentionError("source_revision_changed")
    publications = row["publication_manifest"]["publications"]
    head = await uow.retention_head_get(scope, "interpretation", source.id)
    expected_ids = [
        d["candidate_id"]
        for p in publications
        for d in p["receipt"]["decisions"]
        if d["action"] not in {"REJECT", "L0_ONLY"}
    ]
    if (not publications and head is not None) or (
        publications
        and (
            head is None
            or head["generation"] != len(publications)
            or head["payload"]
            != {
                "active_ids": expected_ids,
                "request_id": row["request_id"],
                "stream": "primary",
                "publication_closed": False,
            }
        )
    ):
        raise RetentionError("interpretation_head_changed")
    return head


async def initial(handler, task, source, prepared):
    """Each batch and its outbox commit once. Closure is a separate fenced commit."""
    queue, scope = handler.queue, task.scope
    plan = initial_plan(scope, prepared, handler.publication_policy)
    prepared_hash = digest(prepared)
    for index, indices in enumerate(plan):
        handler._check_config()
        async with queue.repository.unit_of_work() as uow:
            row = await queue.checked(uow, task.id, task.payload["fence"])
            manifest = row.get("publication_manifest", {})
            if manifest.get("schema") != "publication-manifest/2":
                if manifest.get("closed") or manifest.get("publication_commit_tokens"):
                    raise RetentionError("publication_manifest_invalid")
                open_batches(scope, row, handler.publication_policy, plan, prepared_hash)
                manifest = row["publication_manifest"]
            if (
                not valid_manifest(scope, row)
                or manifest["plan"] != plan
                or manifest["prepared_sha256"] != prepared_hash
                or manifest["policy"] != handler.publication_policy.payload()
            ):
                raise RetentionError("publication_manifest_invalid")
            if index < len(manifest["publications"]):
                continue
            head = await checked_head(uow, scope, source, row)
            receipt = await handler.pipeline.publish_prepared(
                queue.repository,
                source,
                subset(prepared, indices, index == 0),
                authority=handler.authority,
                policy=handler.policy,
                unit_of_work=uow,
                retained=True,
            )
            payload = to_jsonable(receipt)
            append_batch(
                scope,
                row,
                payload,
                [{"candidate_id": d.candidate_id, "action": d.action} for d in receipt.decisions],
                _time(queue.clock()).isoformat(),
            )
            row["result"] = aggregate(row, prepared["audit"])
            active = list(head["payload"]["active_ids"]) if head else []
            active.extend(
                d.candidate_id for d in receipt.decisions if d.action not in {"REJECT", "L0_ONLY"}
            )
            await uow.retention_head_put(
                scope,
                "interpretation",
                source.id,
                {
                    "active_ids": active,
                    "request_id": task.id,
                    "stream": "primary",
                    "publication_closed": False,
                },
                head["generation"] if head else 0,
            )
            await save(handler, uow, scope, row)
            await handler.pipeline.validate_prepared_source(
                source, prepared, authority=handler.authority, policy=handler.policy,
                unit_of_work=uow,
            )
    handler._check_config()
    async with queue.repository.unit_of_work() as uow:
        row = await queue.checked(uow, task.id, task.payload["fence"])
        if (
            not valid_manifest(scope, row)
            or row["publication_manifest"]["prepared_sha256"] != prepared_hash
        ):
            raise RetentionError("publication_manifest_invalid")
        head = await checked_head(uow, scope, source, row)
        await uow.retention_head_put(
            scope,
            "interpretation",
            source.id,
            {**head["payload"], "publication_closed": True},
            head["generation"],
        )
        row.update(status="completed", completed_at=_time(queue.clock()).isoformat())
        close_batches(row)
        row.pop("prepared", None)
        await save(handler, uow, scope, row)
        await handler.pipeline.validate_prepared_source(
            source, prepared, authority=handler.authority, policy=handler.policy,
            unit_of_work=uow,
        )


async def atomic_manifest(uow, scope, row, receipt, interpretation, prepared, policy):
    """All replacement writes and proofs share activate's transaction, never partial heads."""
    close(scope, row, receipt, interpretation)
    dispositions = row["publication_manifest"]["dispositions"]
    slots = []
    for item in dispositions:
        record = await uow.get_admission_record(scope, item["candidate_id"])
        if record is None:
            raise RetentionError("interpretation_contribution_changed")
        slots.append((item["candidate_id"], record["slot_key"]))
    plan = pack(slots, policy.batch_size)
    open_batches(scope, row, policy, plan, digest(prepared), mode="atomic_activation")
    by_id = {d["candidate_id"]: d for d in dispositions}
    payload = to_jsonable(receipt)
    claims = {}
    for identity in payload["candidate_ids"]:
        record = await uow.get_admission_record(scope, identity)
        if record["payload"]["action"] == "ACCEPT" and record["payload"].get("claim_id"):
            claims[identity] = record["payload"]["claim_id"]
    # The activation result is the aggregate receipt; each proof carries its own subset.
    for ids in plan:
        batch_receipt = {
            **payload,
            "candidate_ids": [i for i in payload["candidate_ids"] if i in ids],
            "decisions": [d for d in payload["decisions"] if d["candidate_id"] in ids],
            "pending_ids": [i for i in payload["pending_ids"] if i in ids],
            "claim_ids": [claims[i] for i in ids if i in claims],
        }
        append_batch(scope, row, batch_receipt, [by_id[i] for i in ids], row["completed_at"])
    close_batches(row)
    if not valid_manifest(scope, row):
        raise RetentionError("publication_manifest_invalid")
