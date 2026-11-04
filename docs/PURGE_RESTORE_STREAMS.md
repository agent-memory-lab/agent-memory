# Deletion journal capacity and streamed restore

Long-lived deployments should use `PurgeRestore.export_stream(binary_file)` and
`PurgeRestore.replay_stream(binary_file, expected_checkpoint=..., restore_id=...,
reason=...)`. These host-only APIs retain and replay the **complete deletion
history**, including identities deleted before the content backup was created.
They do not truncate, compact, expire, or evict deletion records.

## Compatibility

- Existing `export()` and `replay()` retain the signed JSON `/1` and `/2` formats
  and their 4,096-entry / 2,000,000-byte limits. Their return types and checkpoint
  hashes are unchanged. Existing hosts must explicitly adopt the stream APIs;
  the legacy API does not silently switch formats when it fills up.
- The new transport is `purge-restore-stream/1`, with a signed
  `purge-restore-checkpoint/3` manifest and binary UTF-8 JSONL pages. Older
  software rejects this format instead of treating a partial history as valid.
- No database migration or additional backend port is needed. Existing SQLite
  and PostgreSQL journals already preserve all deletion records and support
  cursor-keyset pagination. Old content backups can be replayed after normal
  repository initialization.
- Restore receipts keep `purge-restore-receipt/1`. A restore ID is still bound to
  the complete checkpoint, actor and reason. Changing format/checkpoint under
  the same restore ID is an idempotency conflict; use a new ID for a new operation.

## Custody and recovery procedure

1. Export from the current authoritative database to a new, trusted binary file:

   ```python
   with journal_path.open("xb") as output:
       checkpoint = await authority.export_stream(output)
       # The method flushes the stream; the host owns filesystem durability.
       os.fsync(output.fileno())
   ```

2. Finish durable publication of that file, including any directory fsync or
   object-store completion required by the host. Only then atomically publish
   the returned checkpoint in trusted control-plane storage **outside the
   content backup**. Preserve the corresponding complete journal. If export or
   storage fails, discard the incomplete output and do not advance the pin.
3. Restore the content backup into an isolated database. Do not connect it to
   serving traffic, workers, model dispatch, or index publication.
4. Load the latest checkpoint from its independent trusted custody. Do not use
   the journal's own manifest as the freshness authority:

   ```python
   with journal_path.open("rb") as source:
       receipt = await operator.replay_stream(
           source,
           expected_checkpoint=independently_pinned_checkpoint,
           restore_id="restore-2026-11-04",
           reason="offline-backup-recovery",
       )
   ```

5. Verify the committed receipt and recheck the latest authority checkpoint and
   deployment gates before promotion. If more deletions happened during
   recovery, keep the backup offline and replay the newer complete stream with
   its independently pinned checkpoint. This API does not install a serving gate.

HMAC authenticates origin and integrity, not freshness. A correctly signed old
stream is rejected when it does not match the independently supplied current pin.
Never restore an old signing key, journal, or trusted pin from a content backup in
place of the current control-plane state.

## Bounded memory and integrity

The manifest binds authority, exact scope, final cursor, scope epoch, scheduler
clock floor (or its absence), and SHA-256 of the entire canonical entry array.
The digest is identical to the legacy entry-array digest for the same history.
Pages contain exactly 128 entries except the final page and are at most 512,000
bytes, including the newline. Empty history consists of the manifest only.

Export scans cursor-keyset pages twice under the same deletion/scope lock: first
to compute the commitment, then to emit the manifest and pages. Both passes
validate every cursor, identity, mode and epoch. An extra row beyond the stored
head, a missing row, or a changing history fails instead of exporting a prefix.

Replay copies the independent checkpoint before its first asynchronous boundary,
authenticates the manifest, and validates **every** page, the final hash and exact
EOF into a private temporary file before opening a database write transaction.
This happens even when a matching receipt already exists. Duplicate, reordered,
missing, oversized, truncated, modified or extra pages fail. The private copy
prevents a caller from changing verified input during replay.

Inside one transaction, replay verifies that local history is an exact prefix,
applies only the suffix, imports original cursors and epochs, raises the scheduler
clock floor without lowering a newer one, rechecks the complete resulting journal,
and saves the receipt. Historical scope erasures in the existing prefix are not
reapplied to legitimate later-epoch input. Any exception, cancellation or process
failure before commit leaves no partial deletion, imported history, clock update,
or success receipt. A committed retry returns the original receipt only after
validating the full input and the local complete journal again.

## Remaining operational bounds

- Stream history has no 4,096-record or aggregate 2 MB limit. Cursor storage still
  uses signed 64-bit database integers. Individual identities retain their
  existing 256-character limit.
- Journal framing and verification memory is bounded by a few pages, rather
  than total history. Existing backend deletion work has its own resource costs. The private
  verification file, durable journal, database transaction and WAL need disk
  space proportional to history and affected content. Disk/I/O errors propagate;
  the host must keep the target offline and retry from a complete trusted stream.
- Export holds the scope lock for both passes. Replay holds one atomic write
  transaction for the complete suffix. These are offline/maintenance operations,
  not a claim of bounded latency or incremental online restoration.
- Binary file I/O is synchronous and intended for local trusted files. The API
  does not manage network streams, encryption at rest, file permissions outside
  its private temporary file, publication of trusted pins, or key rotation.
- The existing 1,000 restore-audit-receipt limit per scope remains explicit. No
  audit or deletion-history eviction was added.

`tests/test_purge_restore_stream.py` covers actual SQLite histories above both
legacy limits, old-backup recovery, bounded pages, restart and idempotence,
scope/authority/key/pin checks, late tampering and truncation, exact-prefix epochs,
scheduler floors, transactional failure injection and real process SIGKILL around
commit. Backend-neutral cases also run against the opt-in isolated PostgreSQL test
fixture when configured; a skipped PostgreSQL case is not live-backend validation.
