"""Host-only replay of an independently pinned deletion journal into an offline backup."""

import hmac
from hashlib import sha256

from ..domain import ForgetMode, ForgetRequest, MemoryScope, canonical_json, utc_now
from .retention import DurableReceiver, RetentionError, _identity, _time

MAX_ENTRIES = 4096
MAX_BYTES = 2_000_000


def digest(value):
    return sha256(canonical_json(value).encode()).hexdigest()


def checked_entries(entries, head, epoch):
    if (
        type(head) is not int or not 0 <= head <= MAX_ENTRIES
        or type(epoch) is not int or epoch < 0
        or not isinstance(entries, list) or len(entries) != head
    ):
        raise RetentionError("purge_restore_history_unavailable")
    generation = 0
    for cursor, entry in enumerate(entries, 1):
        if not isinstance(entry, dict) or set(entry) != {
            "cursor", "source_event_id", "epoch", "all_in_scope", "mode"
        }:
            raise RetentionError("purge_restore_history_unavailable")
        if entry["all_in_scope"] is True:
            generation += 1
        if (
            type(entry["cursor"]) is not int or entry["cursor"] != cursor
            or type(entry["epoch"]) is not int or entry["epoch"] != generation
            or type(entry["all_in_scope"]) is not bool
            or not isinstance(entry["mode"], str)
            or entry["mode"] not in {ForgetMode.ERASE.value, ForgetMode.ARCHIVE.value}
            or not isinstance(entry["source_event_id"], str)
            or (entry["all_in_scope"] and entry["source_event_id"] != "")
        ):
            raise RetentionError("purge_restore_history_unavailable")
        if not entry["all_in_scope"]:
            _identity(entry["source_event_id"])
    if epoch != generation:
        raise RetentionError("purge_restore_history_unavailable")
    return entries


async def read_journal(uow, scope):
    head, epoch = await uow.purge_head(scope), await uow.retention_epoch(scope)
    if type(head) is not int or not 0 <= head <= MAX_ENTRIES:
        raise RetentionError("purge_restore_capacity")
    entries = []
    while len(entries) < head:
        page = await uow.purge_page(scope, len(entries), min(128, head - len(entries)))
        if len(page) != min(128, head - len(entries)):
            raise RetentionError("purge_restore_history_unavailable")
        entries.extend(page)
    return checked_entries(entries, head, epoch), epoch


class PurgeRestore:
    """Caller keeps the restored database offline until replay commits and is verified.

    The latest checkpoint must come from trusted storage outside the content backup.
    HMAC proves provenance/integrity, while that independent pin proves freshness.
    No MCP tool, background task or automatic serving gate is installed here.
    """

    def __init__(self, repository, scope, *, authority_id, secret, actor, clock=utc_now):
        if not isinstance(scope, MemoryScope):
            raise TypeError("expected exact MemoryScope")
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError("journal integrity key must contain at least 32 bytes")
        self.repository, self.scope = repository, scope
        self.authority_id, self.actor = _identity(authority_id), _identity(actor)
        self.secret, self.clock = secret, clock

    async def _open(self, uow):
        await DurableReceiver._check_support(uow, self.scope)
        if any(not callable(getattr(uow, name, None)) for name in (
            "purge_head", "purge_page", "purge_import", "purge_restore_get",
            "purge_restore_put", "purge_restore_count", "forget_for_restore",
        )):
            raise RetentionError("purge_restore_unsupported")

    def _sign(self, body):
        return hmac.new(self.secret, canonical_json(body).encode(), sha256).hexdigest()

    async def export(self):
        """Export a complete finite journal under the same scope lock as deletion."""
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            entries, epoch = await read_journal(uow, self.scope)
            checkpoint = {
                "schema": "purge-restore-checkpoint/1",
                "authority_id": self.authority_id,
                "scope_key": self.scope.partition_key(),
                "head": len(entries), "scope_epoch": epoch,
                "entries_sha256": digest(entries),
            }
            body = {"schema": "purge-restore-journal/1", "checkpoint": checkpoint,
                    "entries": entries}
            snapshot = {**body, "signature": self._sign(body)}
            if len(canonical_json(snapshot).encode()) > MAX_BYTES:
                raise RetentionError("purge_restore_capacity")
            return snapshot

    def _verify(self, snapshot, expected_checkpoint):
        # Copy JSON before awaiting: caller-owned mutable input cannot change the checked contract.
        import json

        try:
            raw = canonical_json(snapshot).encode()
            if len(raw) > MAX_BYTES:
                raise RetentionError("purge_restore_capacity")
            snapshot = json.loads(raw)
        except (TypeError, ValueError) as error:
            raise RetentionError("purge_restore_snapshot_invalid") from error
        if not isinstance(snapshot, dict) or set(snapshot) != {
            "schema", "checkpoint", "entries", "signature"
        } or snapshot["schema"] != "purge-restore-journal/1":
            raise RetentionError("purge_restore_snapshot_invalid")
        checkpoint = snapshot["checkpoint"]
        if not isinstance(checkpoint, dict) or set(checkpoint) != {
            "schema", "authority_id", "scope_key", "head", "scope_epoch", "entries_sha256"
        } or checkpoint["schema"] != "purge-restore-checkpoint/1":
            raise RetentionError("purge_restore_snapshot_invalid")
        if (checkpoint["authority_id"] != self.authority_id
                or checkpoint["scope_key"] != self.scope.partition_key()):
            raise RetentionError("purge_restore_authority_mismatch")
        if expected_checkpoint != checkpoint:
            raise RetentionError("purge_restore_checkpoint_mismatch")
        body = {k: snapshot[k] for k in ("schema", "checkpoint", "entries")}
        if (
            not isinstance(snapshot["signature"], str)
            or len(snapshot["signature"]) != 64
            or any(c not in "0123456789abcdef" for c in snapshot["signature"])
        ) or not hmac.compare_digest(
            snapshot["signature"], self._sign(body)
        ):
            raise RetentionError("purge_restore_integrity_unavailable")
        checked_entries(snapshot["entries"], checkpoint["head"], checkpoint["scope_epoch"])
        if digest(snapshot["entries"]) != checkpoint["entries_sha256"]:
            raise RetentionError("purge_restore_integrity_unavailable")
        return snapshot

    async def replay(self, snapshot, *, expected_checkpoint, restore_id, reason):
        _identity(restore_id)
        _identity(reason)
        snapshot = self._verify(snapshot, expected_checkpoint)
        checkpoint, entries = snapshot["checkpoint"], snapshot["entries"]
        operation = "purge-restore:" + digest([self.scope.partition_key(), restore_id])
        contract = digest([checkpoint, self.actor, reason])
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            local, epoch = await read_journal(uow, self.scope)
            if (len(local) > len(entries) or epoch > checkpoint["scope_epoch"]
                    or local != entries[:len(local)]):
                raise RetentionError("purge_restore_history_conflict")
            existing = await uow.purge_restore_get(self.scope, operation)
            if existing is not None:
                if existing["contract_sha256"] != contract:
                    raise RetentionError("purge_restore_idempotency_conflict")
                if local != entries or epoch != checkpoint["scope_epoch"]:
                    raise RetentionError("purge_restore_history_conflict")
                return existing
            if await uow.purge_restore_count(self.scope) >= 1000:
                raise RetentionError("purge_restore_capacity")
            counts = {"events": 0, "claims": 0, "artifacts": 0}
            for entry in entries[len(local):]:
                request = ForgetRequest(
                    self.scope,
                    memory_ids=() if entry["all_in_scope"] else (entry["source_event_id"],),
                    all_in_scope=entry["all_in_scope"], mode=ForgetMode(entry["mode"]),
                )
                result = await uow.forget_for_restore(request)
                if await uow.retention_epoch(self.scope) != entry["epoch"]:
                    raise RetentionError("purge_restore_history_conflict")
                await uow.purge_import(self.scope, entry)
                for name in counts:
                    counts[name] += getattr(result, "affected_" + name)
            final, epoch = await read_journal(uow, self.scope)
            if final != entries or epoch != checkpoint["scope_epoch"]:
                raise RetentionError("purge_restore_history_conflict")
            receipt = {
                "schema": "purge-restore-receipt/1", "id": operation,
                "checkpoint": checkpoint, "contract_sha256": contract,
                "actor": self.actor, "reason": reason,
                "from_cursor": len(local), "replayed_entries": len(entries) - len(local),
                "affected": counts, "committed_at": _time(self.clock()).isoformat(),
                "state": "replayed",
            }
            await uow.purge_restore_put(self.scope, operation, receipt)
            return receipt
