"""Finite request targets and publication manifests; no index completion is inferred."""

import json
from collections import Counter
from hashlib import sha256

from .retention import RetentionError, _identity


def commit_token(scope, row, kind):
    coordinates = [
        scope.partition_key(),
        row["epoch"],
        row["request_id"],
        row["configuration_sha256"],
    ]
    return {
        "kind": kind,
        "id": kind
        + ":"
        + sha256(json.dumps(coordinates, separators=(",", ":")).encode()).hexdigest(),
        "scope_key": coordinates[0],
        "epoch": row["epoch"],
        "generation": row["request_id"],
        "configuration_sha256": row["configuration_sha256"],
    }


def begin(scope, row):
    row["capture_commit_token"] = commit_token(scope, row, "capture")
    row["publication_manifest"] = {
        "schema": "publication-manifest/1",
        "generation": row["request_id"],
        "version": 0,
        "closed": False,
        "publication_commit_tokens": [],
        "dispositions": [],
        "no_outputs": None,
        "no_indexable_outputs": None,
    }


def close(scope, row, receipt, interpretation=None):
    if "publication_manifest" not in row:
        # The live worker can adopt a legacy request because it actually commits this publication.
        begin(scope, row)
    dispositions = [{"candidate_id": d.candidate_id, "action": d.action} for d in receipt.decisions]
    if interpretation:
        # Reconciliation can withdraw old candidates even when it produces no new candidates.
        seen = {d["candidate_id"] for d in dispositions}
        dispositions.extend(
            {
                "candidate_id": item["candidate_id"],
                "action": {
                    "withdraw_source_support": "WITHDRAWN",
                    "qualification_pending": "PENDING_VERIFICATION",
                    "retain": "ACCEPT",
                    "carry_forward": "CARRY_FORWARD",
                }[item["action"]],
            }
            for item in interpretation["dispositions"]
            if item["candidate_id"] not in seen
        )
    row["publication_manifest"] = {
        "schema": "publication-manifest/1",
        "generation": row["request_id"],
        "version": 1,
        "closed": True,
        "publication_commit_tokens": [commit_token(scope, row, "publication")]
        if dispositions
        else [],
        "dispositions": dispositions,
        "no_outputs": not dispositions,
        "no_indexable_outputs": not receipt.claim_ids,
    }


def valid_manifest(scope, row):
    manifest = row.get("publication_manifest")
    if not isinstance(manifest, dict):
        return False
    closed = manifest.get("closed")
    dispositions = manifest.get("dispositions")
    if (
        manifest.get("schema") != "publication-manifest/1"
        or manifest.get("generation") != row["request_id"]
        or type(closed) is not bool
        or type(manifest.get("version")) is not int
        or manifest["version"] != int(closed)
        or not isinstance(dispositions, list)
        or any(
            not isinstance(d, dict)
            or not isinstance(d.get("candidate_id"), str)
            or not isinstance(d.get("action"), str)
            for d in dispositions
        )
        or row.get("capture_commit_token") != commit_token(scope, row, "capture")
    ):
        return False
    if not closed:
        return (
            not dispositions
            and manifest.get("publication_commit_tokens") == []
            and manifest.get("no_outputs") is None
            and manifest.get("no_indexable_outputs") is None
        )
    return (
        row["status"] == "completed"
        and type(manifest.get("no_outputs")) is bool
        and manifest["no_outputs"] == (not dispositions)
        and type(manifest.get("no_indexable_outputs")) is bool
        and manifest["no_indexable_outputs"] == (not row.get("result", {}).get("claim_ids"))
        and manifest.get("publication_commit_tokens")
        == ([commit_token(scope, row, "publication")] if dispositions else [])
    )


def project(rows, stage):
    """Evaluate only already-authorized finite members; indexes remain unsupported."""
    manifests = [row["publication_manifest"] for row in rows]
    closed = all(m["closed"] for m in manifests)
    counts = Counter(d["action"] for m in manifests for d in m["dispositions"])
    if stage not in {"source_persisted", "l1_decided"}:
        state = "unsupported"
    elif stage == "source_persisted":
        state = "reached"
    elif any(row["status"] in {"dead", "conflict"} for row in rows):
        state = "failed"
    elif any(row["status"] in {"needs_resolution", "superseded", "cancelled"} for row in rows):
        state = "blocked"
    elif closed and all(row["status"] == "completed" for row in rows):
        state = "reached"
    else:
        state = "processing"
    return {
        "state": state,
        "publication_manifest_closed": closed,
        "capture_commit_tokens": [row["capture_commit_token"] for row in rows],
        "publication_commit_tokens": [t for m in manifests for t in m["publication_commit_tokens"]],
        "publication_manifests": manifests,
        "disposition_counts": dict(counts),
        "no_outputs": all(m["no_outputs"] for m in manifests) if closed else None,
        "no_indexable_outputs": all(m["no_indexable_outputs"] for m in manifests)
        if closed
        else None,
        "uncovered_request_ids": [
            r["request_id"] for r in rows if not r["publication_manifest"]["closed"]
        ],
        "status_version": [
            {
                "request_id": r["request_id"],
                "status": r["status"],
                "attempts": r.get("attempts", 0),
                "manifest_version": r["publication_manifest"]["version"],
                "interpretation_current": r.get("interpretation_current", False),
            }
            for r in rows
        ],
        "index_visible": "unsupported",
    }


class DurableReadiness:
    """Bind finite targets to authenticated producer requests, not a moving latest head."""

    def __init__(self, producer):
        self.producer = producer

    async def freeze(self, scope, session, *, sequences, actor):
        if (
            not isinstance(sequences, (list, tuple))
            or not 1 <= len(sequences) <= 128
            or any(type(n) is not int or not 1 <= n <= 2**53 for n in sequences)
            or len(set(sequences)) != len(sequences)
        ):
            raise RetentionError("invalid_readiness_target")
        sequences = sorted(sequences)
        async with self.producer.receiver.repository.unit_of_work() as uow:
            await self.producer._check(uow, scope, session, actor)
            if not callable(getattr(uow, "delivery_get", None)):
                raise RetentionError("staged_readiness_unsupported")
            members = []
            for sequence in sequences:
                identity = self.producer._request_key(scope, session, sequence)
                row = await uow.retention_get(scope, "request", identity)
                if row is None:
                    raise RetentionError("readiness_source_not_received")
                if "publication_manifest" not in row:
                    raise RetentionError("readiness_history_unavailable")
                if row["status"] in {"cancelled", "superseded"} or not await uow.events_exist(
                    scope, (row["event_id"],)
                ):
                    raise RetentionError("source_unavailable")
                members.append(
                    {
                        "sequence": sequence,
                        "request_id": identity,
                        "configuration_sha256": row["configuration_sha256"],
                    }
                )
            payload = {
                "schema": "durable-target/1",
                "producer_id": session.producer_id,
                "epoch": session.epoch,
                "scope_key": scope.partition_key(),
                "members": members,
            }
            if self.producer.index_channel is not None:
                payload["index_channel"] = self.producer.index_channel.payload()
            target_id = "target:" + sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            if await uow.delivery_get(scope, "target", target_id) is None:
                if await uow.delivery_count(scope, "target") >= 1000:
                    raise RetentionError("readiness_target_capacity")
                await uow.delivery_insert(scope, "target", target_id, payload)
            return {**payload, "target_id": target_id}

    async def status(self, scope, session, *, target_id, stage, actor):
        _identity(target_id)
        if not isinstance(stage, str) or not 1 <= len(stage) <= 64:
            raise RetentionError("invalid_readiness_stage")
        async with self.producer.receiver.repository.unit_of_work() as uow:
            await self.producer._check(uow, scope, session, actor)
            if not callable(getattr(uow, "delivery_get", None)):
                raise RetentionError("staged_readiness_unsupported")
            target = await uow.delivery_get(scope, "target", target_id)
            if (
                target is None
                or target["producer_id"] != session.producer_id
                or target["epoch"] != session.epoch
            ):
                raise RetentionError("invalid_readiness_target")
            binding = {
                "schema": "durable-readiness/1",
                "target_id": target_id,
                "stage": stage,
                "scope_key": scope.partition_key(),
                "producer_id": session.producer_id,
                "epoch": session.epoch,
            }
            rows = []
            for member in target["members"]:
                row = await uow.retention_get(scope, "request", member["request_id"])
                if row is None or row["configuration_sha256"] != member["configuration_sha256"]:
                    return {
                        **binding,
                        "state": "blocked",
                        "reason": "readiness_history_unavailable",
                    }
                if row["epoch"] != session.epoch or row["status"] in {"cancelled", "superseded"}:
                    return {**binding, "state": "blocked", "reason": "source_unavailable"}
                source = await uow.get_source_event(scope, row["event_id"])
                if source is None:
                    return {**binding, "state": "blocked", "reason": "source_unavailable"}
                from .source_revisions import source_is_current

                if not await source_is_current(uow, source):
                    return {**binding, "state": "blocked", "reason": "source_revision_changed"}
                if not valid_manifest(scope, row):
                    return {
                        **binding,
                        "state": "blocked",
                        "reason": "readiness_history_unavailable",
                    }
                head = await uow.retention_head_get(scope, "interpretation", row["event_id"])
                row["interpretation_current"] = (
                    head is not None and head["payload"]["request_id"] == row["request_id"]
                )
                rows.append(row)
            if stage == "index_visible":
                from .indexing import coverage

                return {**binding, **await coverage(uow, scope, rows, target.get("index_channel"))}
            return {**binding, **project(rows, stage)}
