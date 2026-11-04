"""Bounded framing for host-owned, complete deletion-journal streams.

The signed manifest commits to the canonical JSON array of *all* entries. Pages
are transport framing, not independently acceptable recovery checkpoints.
"""

import json
from hashlib import sha256

from ..domain import ForgetMode, canonical_json
from .retention import RetentionError, _identity

PAGE_ENTRIES = 128
MAX_FRAME_BYTES = 512_000
MAX_CURSOR = 2**63 - 1  # The SQLite/PostgreSQL signed integer storage boundary.
STREAM_SCHEMA = "purge-restore-stream/1"
CHECKPOINT_SCHEMA = "purge-restore-checkpoint/3"


def check_position(head, epoch):
    if (type(head) is not int or not 0 <= head <= MAX_CURSOR
            or type(epoch) is not int or not 0 <= epoch <= head):
        raise RetentionError("purge_restore_history_unavailable")


class JournalCheck:
    """Validate a contiguous full prefix and hash it without retaining entries."""

    def __init__(self):
        self.cursor = self.epoch = 0
        self._hash = sha256(b"[")

    def page(self, entries, count):
        if (not isinstance(entries, (list, tuple)) or len(entries) != count
                or not 1 <= count <= PAGE_ENTRIES):
            raise RetentionError("purge_restore_history_unavailable")
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {
                "cursor", "source_event_id", "epoch", "all_in_scope", "mode"
            }:
                raise RetentionError("purge_restore_history_unavailable")
            epoch = self.epoch + int(entry["all_in_scope"] is True)
            if (type(entry["cursor"]) is not int or entry["cursor"] != self.cursor + 1
                    or type(entry["epoch"]) is not int or entry["epoch"] != epoch
                    or type(entry["all_in_scope"]) is not bool
                    or not isinstance(entry["mode"], str)
                    or entry["mode"] not in {ForgetMode.ERASE.value, ForgetMode.ARCHIVE.value}
                    or not isinstance(entry["source_event_id"], str)
                    or (entry["all_in_scope"] and entry["source_event_id"] != "")):
                raise RetentionError("purge_restore_history_unavailable")
            if not entry["all_in_scope"]:
                _identity(entry["source_event_id"])
            if self.cursor:
                self._hash.update(b",")
            self._hash.update(canonical_json(entry).encode())
            self.cursor, self.epoch = entry["cursor"], epoch

    def finish(self, head, epoch):
        if self.cursor != head or self.epoch != epoch:
            raise RetentionError("purge_restore_history_unavailable")
        complete = self._hash.copy()
        complete.update(b"]")
        return complete.hexdigest()


async def journal_pages(uow, scope, head, epoch):
    check_position(head, epoch)
    check = JournalCheck()
    while check.cursor < head:
        count = min(PAGE_ENTRIES, head - check.cursor)
        page = await uow.purge_page(scope, check.cursor, count)
        check.page(page, count)
        yield page
    check.finish(head, epoch)
    # A lying/stale head must not hide retained deletion records.
    if await uow.purge_page(scope, head, 1):
        raise RetentionError("purge_restore_history_unavailable")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def read_frame(source):
    raw = source.readline(MAX_FRAME_BYTES + 1)
    if not isinstance(raw, bytes):
        raise TypeError("purge restore streams require binary file objects")
    if len(raw) > MAX_FRAME_BYTES:
        raise RetentionError("purge_restore_capacity")
    if not raw.endswith(b"\n"):
        raise RetentionError("purge_restore_snapshot_invalid")
    try:
        return json.loads(raw, object_pairs_hook=_unique_object)
    except (UnicodeError, TypeError, ValueError, RecursionError) as error:
        raise RetentionError("purge_restore_snapshot_invalid") from error


def write_frame(destination, value):
    raw = canonical_json(value).encode() + b"\n"
    if len(raw) > MAX_FRAME_BYTES:
        raise RetentionError("purge_restore_capacity")
    remaining = memoryview(raw)
    while remaining:
        count = destination.write(remaining)
        if type(count) is not int or not 0 < count <= len(remaining):
            raise OSError("purge restore stream write made no progress")
        remaining = remaining[count:]


def copy_checkpoint(checkpoint):
    """Snapshot the independently supplied pin before any asynchronous boundary."""
    try:
        raw = json.dumps(checkpoint, allow_nan=False).encode()
    except (TypeError, ValueError, RecursionError) as error:
        raise RetentionError("purge_restore_snapshot_invalid") from error
    if len(raw) > MAX_FRAME_BYTES:
        raise RetentionError("purge_restore_capacity")
    return json.loads(raw)
