"""Host-acknowledged archival of full model audit evidence, never automatic expiry.

The host owns the durable archive and its erase/restore lifecycle. A digest is a
commitment, not a substitute for those archived records. No provider or archive
I/O occurs here. Money reservations, invoices and receipts are never collected.
"""

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta

from ..derived.question_gc import KNOWN_KINDS, V7_SCHEMAS, _strings, census_limits
from ..derived.service import open_derived
from ..retrieval.model_contracts import ModelError, canonical, count, digest, hash_value, text

CONTRACT = "model-authorization-archive/1"
STATE_KIND = "model_authorization_archive"
PENDING_KIND = "model_authorization_archive_pending"


@dataclass(frozen=True, slots=True)
class ModelAuthorizationArchivePolicy:
    """Explicit host retention decision for one archive destination.

    Acknowledgment asserts durable storage of the WHOLE exported envelope and
    enforcement of the current deletion journal before any archive read/restore.
    Resolve legal holds before choosing age/recent limits. Zero age is permitted
    only as an explicit host choice. Unacknowledged evidence remains local.
    """

    policy_id: str
    archive_id: str
    minimum_age_seconds: int = 86400
    retain_recent: int = 256
    max_batch: int = 256
    max_records: int = 32768
    max_edges: int = 131072
    max_bytes: int = 67108864

    def __post_init__(self):
        text(self.policy_id)
        text(self.archive_id)
        for key, low, high in (
            ("minimum_age_seconds", 0, 315360000),
            ("retain_recent", 0, 4095),
            ("max_batch", 1, 4096),
        ):
            value = getattr(self, key)
            if type(value) is not int or not low <= value <= high:
                raise ModelError("invalid_model_authorization_retention")
        census_limits(self.max_records, self.max_edges, self.max_bytes)


def _policy(value):
    if type(value) is not ModelAuthorizationArchivePolicy:
        raise ModelError("invalid_model_authorization_retention")
    return replace(value)


def _audit(row, key):
    if row == {"state": "erased"}:
        return "erased", None
    if isinstance(row, dict) and row.get("state") == "reserved":
        if (
            set(row)
            != {
                "schema",
                "state",
                "id",
                "stage",
                "consumed",
                "token",
                "expires_at",
                "call_id",
                "key",
                "sources",
                "parents",
            }
            or row["schema"] != "model-delivery-reservation/1"
            or row["id"] != key
            or row["stage"] != "delivery"
            or row["consumed"] is not False
        ):
            raise ModelError("model_authorization_archive_record_unsupported")
        text(key)
        text(row["token"])
        text(row["call_id"])
        hash_value(row["key"])
        for field in ("sources", "parents"):
            if type(row[field]) is not list or len(row[field]) > (
                256 if field == "sources" else 8192
            ):
                raise ModelError("model_authorization_archive_record_unsupported")
            for value in row[field]:
                text(value)
        try:
            if datetime.fromisoformat(row["expires_at"]).utcoffset() is None:
                raise ValueError
        except (TypeError, ValueError):
            raise ModelError("model_authorization_archive_record_unsupported") from None
        return "reserved", None
    if not isinstance(row, dict) or set(row) != {
        "id",
        "stage",
        "payload_sha256",
        "call_id",
        "sources",
        "parents",
        "key",
        "authorized_at",
        "consumed",
    }:
        raise ModelError("model_authorization_archive_record_unsupported")
    if (
        row["id"] != key
        or row["stage"] not in {"dispatch", "delivery"}
        or row["consumed"] is not True
    ):
        raise ModelError("model_authorization_archive_record_unsupported")
    text(key)
    text(row["call_id"])
    hash_value(row["payload_sha256"])
    hash_value(row["key"])
    for field in ("sources", "parents"):
        if type(row[field]) is not list or len(row[field]) > (256 if field == "sources" else 8192):
            raise ModelError("model_authorization_archive_record_unsupported")
        for value in row[field]:
            text(value)
    try:
        at = datetime.fromisoformat(row["authorized_at"])
        if at.utcoffset() is None:
            raise ValueError
    except (TypeError, ValueError):
        raise ModelError("model_authorization_archive_record_unsupported") from None
    return row["stage"], at


def _proof(snapshot, selected, policy):
    """The same identity/string/edge reachability semantics as Question GC.

    Copy historical proof into the archive before releasing its local audit root.
    Also preserve external model-cache owner edges. Unknown kinds/schemas fail
    closed rather than exporting an incomplete proof graph.
    """
    rows = {(r["kind"], r["identity"]): r["payload"] for r in snapshot["rows"]}
    aliases, owners = defaultdict(set), defaultdict(set)
    for node, row in rows.items():
        if node[0] not in KNOWN_KINDS or not isinstance(row, dict):
            raise ModelError("model_authorization_archive_record_unsupported")
        if (
            node[0] in V7_SCHEMAS
            and row.get("state") != "erased"
            and row.get("status") != "erased"
            and row.get("schema") != V7_SCHEMAS[node[0]]
        ):
            raise ModelError("model_authorization_archive_record_unsupported")
        owners[node[1]].add(node)
        aliases[node[1]].add(node)
        aliases["derived:" + node[1]].add(node)
    graph = {}
    links = 0
    for node, row in rows.items():
        graph[node] = set(owners[node[1]])
        for value in _strings(row):
            graph[node].update(aliases.get(value, ()))
        links += len(graph[node])
        if links > policy.max_edges:
            raise ModelError("model_authorization_archive_census_limit")
    for edge in snapshot["edges"]:
        origins = owners.get(edge["revision_id"], ())
        if edge["revision_id"].startswith("model-cache:"):
            origins = owners.get(edge["revision_id"].removeprefix("model-cache:"), ())
        for node in origins:
            graph[node].update(aliases.get(edge["parent_id"], ()))
    if sum(len(targets) for targets in graph.values()) > policy.max_edges:
        raise ModelError("model_authorization_archive_census_limit")
    reachable = set(("model_authorization", key) for key in selected)
    todo = list(reachable)
    while todo:
        for node in graph[todo.pop()] - reachable:
            reachable.add(node)
            todo.append(node)
    identities = {key for _, key in reachable}
    edges = [
        edge
        for edge in snapshot["edges"]
        if edge["revision_id"] in identities
        or edge["revision_id"].removeprefix("model-cache:") in identities
    ]
    return [
        dict(kind=kind, identity=key, payload=rows[kind, key]) for kind, key in sorted(reachable)
    ], edges


def verify_model_authorization_archive(batch, *, expected_checkpoint):
    """Verify the full exported artifact against an independently pinned digest.

    The compact descriptor commits to both the exact deletion selection and the
    complete evidence bytes. A digest without this envelope is not audit detail.
    """
    hash_value(expected_checkpoint)
    if not isinstance(batch, dict) or batch.get("checkpoint") != expected_checkpoint:
        raise ModelError("model_authorization_archive_invalid")
    descriptor = {
        key: value for key, value in batch.items() if key not in {"records", "edges", "checkpoint"}
    }
    try:
        if (
            batch["schema"] != CONTRACT
            or digest(descriptor) != expected_checkpoint
            or digest({"records": batch["records"], "edges": batch["edges"]})
            != batch["evidence_sha256"]
            or batch["authorization_ids"] != sorted(batch["authorizations"])
        ):
            raise ValueError
        selected = {
            item["identity"]: item["payload"]
            for item in batch["records"]
            if item["kind"] == "model_authorization" and item["identity"] in batch["authorizations"]
        }
        if {key: digest(row) for key, row in selected.items()} != batch["authorizations"]:
            raise ValueError
        stages = Counter(_audit(row, key)[0] for key, row in selected.items())
        if dict(stages) != batch["stages"] or "reserved" in stages:
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ModelError("model_authorization_archive_invalid") from None
    return True


class ModelAuthorizationArchive:
    """Finite local audit window with one outstanding, host-pinned export.

    Use export -> host durable archive + independent checkpoint pin -> acknowledge.
    No acknowledgment means no deletion. Cancel only discards a pending export
    descriptor, never its source records. SDK/model transports do not expose this.
    """

    def __init__(self, authority):
        self.authority = authority
        self.repository, self.scope, self.clock = (
            authority.repository,
            authority.scope,
            authority.clock,
        )

    async def _open(self, uow):
        if (
            getattr(uow, "model_authorization_archive_contract", None) != CONTRACT
            or not callable(getattr(uow, "model_authorization_archive_delete", None))
            or not callable(getattr(uow, "derived_gc_snapshot", None))
            or not callable(getattr(uow, "purge_head", None))
        ):
            raise ModelError("model_authorization_archive_backend_unsupported")
        epoch = await open_derived(uow, self.scope)
        authority = await self.authority.service.registry.authority(
            uow, self.authority.service.authority_id
        )
        return epoch, digest(authority), await uow.purge_head(self.scope)

    async def _state(self, uow):
        value = await uow.derived_get(self.scope, STATE_KIND, "scope")
        if value is None:
            return dict(
                schema=CONTRACT,
                sequence=0,
                archived_records=0,
                stages={"dispatch": 0, "delivery": 0, "erased": 0},
                checkpoint=digest([CONTRACT, self.scope.partition_key()]),
            )
        if value.get("schema") != CONTRACT or value.get("checkpoint") != digest(
            {k: v for k, v in value.items() if k != "checkpoint"}
        ):
            raise ModelError("model_authorization_archive_state_invalid")
        try:
            count(value["sequence"])
            count(value["archived_records"])
            if (
                value["sequence"] == 0
                or set(value["stages"]) != {"dispatch", "delivery", "erased"}
                or sum(value["stages"].values()) != value["archived_records"]
            ):
                raise ValueError
            for total in value["stages"].values():
                count(total)
        except (KeyError, TypeError, ValueError):
            raise ModelError("model_authorization_archive_state_invalid") from None
        return value

    async def status(self):
        """Recover the outstanding checkpoint after a host crash, without bodies."""
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            state = await self._state(uow)
            pending = await uow.derived_get(self.scope, PENDING_KIND, "scope")
            rows = await uow.derived_records(self.scope, "model_authorization")
            stages = Counter(_audit(item["payload"], item["identity"])[0] for item in rows)
            return dict(
                capacity=dict(
                    limit=4096,
                    used=len(rows),
                    available=4096 - len(rows),
                    consumed=stages["dispatch"] + stages["delivery"],
                    dispatch=stages["dispatch"],
                    delivery=stages["delivery"],
                    reserved=stages["reserved"],
                    erased=stages["erased"],
                    first_dispatch_available=len(rows) <= 4094,
                ),
                state=state,
                pending=None
                if pending is None
                else dict(
                    checkpoint=pending["checkpoint"],
                    records=len(pending["authorizations"]),
                    policy_sha256=pending["policy_sha256"],
                ),
            )

    async def export(self, policy, *, expected_previous_checkpoint):
        """Return a bounded full-evidence envelope; does not free capacity."""
        policy = _policy(policy)
        hash_value(expected_previous_checkpoint)
        async with self.repository.unit_of_work() as uow:
            epoch, authority, purge_head = await self._open(uow)
            if await uow.derived_get(self.scope, PENDING_KIND, "scope") is not None:
                raise ModelError("model_authorization_archive_pending")
            state = await self._state(uow)
            if state["checkpoint"] != expected_previous_checkpoint:
                raise ModelError("model_authorization_archive_stale")
            snapshot = await uow.derived_gc_snapshot(
                self.scope,
                max_records=policy.max_records,
                max_edges=policy.max_edges,
                max_bytes=policy.max_bytes,
            )
            if snapshot is None:
                raise ModelError("model_authorization_archive_census_limit")
            from .refresh_demand import observed_clock

            now = await observed_clock(uow, self.scope, self.clock)
            rows, stages = [], {}
            for item in snapshot["rows"]:
                if item["kind"] == "model_authorization":
                    stage, at = _audit(item["payload"], item["identity"])
                    if stage == "reserved":
                        continue
                    rows.append((at or datetime.min.replace(tzinfo=now.tzinfo), item))
                    stages[item["identity"]] = stage
            rows.sort(key=lambda pair: (pair[0], pair[1]["identity"]))
            eligible = rows[: max(0, len(rows) - policy.retain_recent)]
            selected = {
                item["identity"]: item["payload"]
                for at, item in eligible
                if at <= now - timedelta(seconds=policy.minimum_age_seconds)
            }
            selected = dict(list(selected.items())[: policy.max_batch])
            if not selected:
                raise ModelError("model_authorization_archive_empty")
            proof, edges = _proof(snapshot, selected, policy)
            descriptor = dict(
                schema=CONTRACT,
                scope_key=self.scope.partition_key(),
                epoch=epoch,
                purge_head=purge_head,
                authority_sha256=authority,
                policy_sha256=digest(asdict(policy)),
                exported_at=now.isoformat(),
                previous_checkpoint=state["checkpoint"],
                authorization_ids=sorted(selected),
                authorizations={key: digest(row) for key, row in selected.items()},
                stages=dict(Counter(stages[key] for key in selected)),
                evidence_sha256=digest({"records": proof, "edges": edges}),
            )
            checkpoint = digest(descriptor)
            pending = {**descriptor, "checkpoint": checkpoint}
            batch = {**pending, "records": proof, "edges": edges}
            if len(canonical(batch).encode()) > policy.max_bytes:
                raise ModelError("model_authorization_archive_census_limit")
            verify_model_authorization_archive(batch, expected_checkpoint=checkpoint)
            if await self._open(uow) != (epoch, authority, purge_head):
                raise ModelError("model_authorization_archive_stale")
            await uow.derived_put(self.scope, PENDING_KIND, "scope", pending)
            return batch

    async def acknowledge(self, policy, *, expected_checkpoint, archive_receipt_sha256):
        """Host confirms the complete archive and pins its durable-storage receipt.

        The receipt digest identifies the host's storage acknowledgment. This
        library cannot verify a remote service's durability. A fabricated receipt
        violates the host contract and must never be synthesized as maintenance.
        """
        policy = _policy(policy)
        hash_value(expected_checkpoint)
        hash_value(archive_receipt_sha256)
        async with self.repository.unit_of_work() as uow:
            epoch, authority, purge_head = await self._open(uow)
            state = await self._state(uow)
            pending = await uow.derived_get(self.scope, PENDING_KIND, "scope")
            if pending is None:
                if (
                    state.get("batch_checkpoint") == expected_checkpoint
                    and state.get("archive_receipt_sha256") == archive_receipt_sha256
                    and state.get("policy_sha256") == digest(asdict(policy))
                ):
                    return state
                raise ModelError("model_authorization_archive_stale")
            if (
                digest({k: v for k, v in pending.items() if k != "checkpoint"})
                != expected_checkpoint
                or pending.get("schema") != CONTRACT
                or pending.get("scope_key") != self.scope.partition_key()
                or pending.get("checkpoint") != expected_checkpoint
                or pending.get("previous_checkpoint") != state["checkpoint"]
                or pending.get("policy_sha256") != digest(asdict(policy))
                or pending.get("authority_sha256") != authority
                or pending.get("epoch") != epoch
                or pending.get("purge_head") != purge_head
            ):
                raise ModelError("model_authorization_archive_stale")
            # Recheck the exact selected rows after durable storage, under the
            # same scope lock as dispatch, delivery, selective erase and GC.
            for key, expected in pending["authorizations"].items():
                row = await uow.derived_get(self.scope, "model_authorization", key)
                if row is None or digest(row) != expected:
                    raise ModelError("model_authorization_archive_stale")
            stages = Counter(state["stages"])
            stages.update(pending["stages"])
            updated = dict(
                schema=CONTRACT,
                sequence=state["sequence"] + 1,
                archived_records=state["archived_records"] + len(pending["authorizations"]),
                stages=dict(stages),
                previous_checkpoint=state["checkpoint"],
                batch_checkpoint=expected_checkpoint,
                archive_receipt_sha256=archive_receipt_sha256,
                policy_sha256=pending["policy_sha256"],
            )
            for value in (updated["sequence"], updated["archived_records"], *stages.values()):
                count(value)
            updated["checkpoint"] = digest(updated)
            await uow.derived_put(self.scope, STATE_KIND, "scope", updated)
            for key in pending["authorizations"]:
                await uow.model_authorization_archive_delete(self.scope, "model_authorization", key)
            await uow.model_authorization_archive_delete(self.scope, PENDING_KIND, "scope")
            if await self._open(uow) != (epoch, authority, purge_head):
                raise ModelError("model_authorization_archive_stale")
            return updated

    async def cancel(self, *, expected_checkpoint):
        """Discard a failed/stale pending descriptor; no audit evidence is deleted."""
        hash_value(expected_checkpoint)
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            pending = await uow.derived_get(self.scope, PENDING_KIND, "scope")
            if pending is None or pending.get("checkpoint") != expected_checkpoint:
                raise ModelError("model_authorization_archive_stale")
            await uow.model_authorization_archive_delete(self.scope, PENDING_KIND, "scope")
