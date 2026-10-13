"""Fail-closed, durable response checkpoints for one sequence/arm execution.

Wrap the normal bounded client; this module never opens, changes, resets, or
unlocks its accounting ledger. A fresh wrapper replays calls starting at zero.
Only exact request hashes may replay, and only a fully persisted response avoids
dispatch. An intent without a response always requires manual reconciliation,
even when the underlying ledger has already marked the provider call completed.

Keep the entire directory with its run artifacts. Never delete checkpoints to
retry a call. Response contents must be the experiment's public synthetic data.
Request bodies, credentials, headers, and environment variables are not stored.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .model_client import MAX_OUTPUT_TOKENS, ClientError

_PHASE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_RECEIPT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}\Z")
_ARTIFACT = re.compile(r"([0-9]{8})\.(intent|response)\.json\Z")
_RESPONSE_FIELDS = {
    "content", "input_tokens", "output_tokens", "latency", "receipt_id",
    "shared_extraction_reused", "phase",
}


class CheckpointError(ClientError):
    """A checkpoint is invalid or cannot be safely persisted."""


class CheckpointMismatch(CheckpointError):
    """The replay request or call sequence differs from the persisted execution."""


class CheckpointIncomplete(CheckpointError):
    """A dispatched or possibly dispatched call needs manual reconciliation."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _make_directory(path: Path) -> None:
    # Persist each new directory entry as well as the files that it will contain.
    missing = []
    ancestor = path
    while not ancestor.exists():
        missing.append(ancestor)
        ancestor = ancestor.parent
    for directory in reversed(missing):
        directory.mkdir(exist_ok=True)
        _fsync_directory(directory.parent)
    if not path.is_dir():
        raise OSError("Checkpoint path is not a directory.")


def _atomic_json(path: Path, value: Any) -> None:
    temporary = None
    try:
        if path.exists() or path.is_symlink():
            raise CheckpointError("Existing checkpoint artifacts must never be overwritten.")
        descriptor, temporary = tempfile.mkstemp(prefix=".checkpoint-", dir=path.parent)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_canonical(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        _fsync_directory(path.parent)
    except CheckpointError:
        raise
    except Exception:
        raise CheckpointError(
            "Checkpoint persistence failed; manual reconciliation is required before retry."
        ) from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file():
            raise ValueError
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
        if type(value) is not dict:
            raise ValueError
        return value
    except Exception:
        raise CheckpointError(
            "Checkpoint artifact is corrupt or unreadable; manual reconciliation is required."
        ) from None


def _normalize_request(messages, max_output_tokens, phase):
    if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= MAX_OUTPUT_TOKENS:
        raise ValueError("Maximum output tokens must be an integer between 1 and 32768.")
    if not isinstance(phase, str) or not _PHASE.fullmatch(phase):
        raise ValueError("Phase must be a short alphanumeric accounting label.")
    if not isinstance(messages, (list, tuple)) or not 1 <= len(messages) <= 128:
        raise ValueError("Provide between 1 and 128 text chat messages.")
    normalized = []
    for message in messages:
        if not isinstance(message, Mapping) or set(message) != {"role", "content"}:
            raise ValueError("Each chat message must contain only role and text content.")
        if message["role"] not in {"system", "user", "assistant"}:
            raise ValueError("Unsupported message role.")
        if not isinstance(message["content"], str):
            raise ValueError("Chat message content must be text.")
        normalized.append({"role": message["role"], "content": message["content"]})
    request = dict(messages=normalized, max_output_tokens=max_output_tokens, phase=phase)
    try:
        request_hash = _digest(request)
    except (ValueError, UnicodeError):
        raise ValueError("Messages could not be encoded as UTF-8 JSON.") from None
    return normalized, request_hash


def _normalize_response(raw, phase):
    try:
        if not isinstance(raw, Mapping):
            raise ValueError
        response = {key: raw[key] for key in (
            "content", "input_tokens", "output_tokens", "latency", "receipt_id",
        )}
        response["phase"] = raw.get("phase", phase)
        response["shared_extraction_reused"] = raw.get("shared_extraction_reused", False)
        if not isinstance(response["content"], str):
            raise ValueError
        for field, minimum in (("input_tokens", 1), ("output_tokens", 0)):
            if type(response[field]) is not int or response[field] < minimum:
                raise ValueError
        if type(response["latency"]) not in (int, float):
            raise ValueError
        if not math.isfinite(response["latency"]) or response["latency"] < 0:
            raise ValueError
        if not isinstance(response["receipt_id"], str) or not _RECEIPT.fullmatch(
            response["receipt_id"],
        ):
            raise ValueError
        if response["phase"] != phase or type(response["shared_extraction_reused"]) is not bool:
            raise ValueError
        _canonical(response)
        return response
    except Exception:
        raise CheckpointError(
            "Response cannot be checkpointed safely; manual reconciliation is required."
        ) from None


class DurableCallCache:
    """Ordered, single-writer cache around ``client.complete``.

    Use a distinct directory for every sequence/arm and retain it across reruns.
    Construct a fresh wrapper for a deterministic replay from call index zero.
    The process lock spans intent persistence, dispatch, and response persistence.
    There is deliberately no reset, unlock, retry-uncertain, or skip-safety option.
    """

    def __init__(self, client, path: str | Path) -> None:
        self.client = client
        self.path = Path(path)
        self._call_index = 0
        try:
            _make_directory(self.path)
        except Exception:
            raise CheckpointError("Checkpoint directory could not be initialized.") from None

    @contextmanager
    def _locked(self):
        try:
            with (self.path / ".lock").open("a+b") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        except OSError:
            raise CheckpointError(
                "Checkpoint directory could not be safely locked or read."
            ) from None

    def _on_checkpoint(self, stage: str) -> None:
        """Fault-injection seam for offline tests; production has no bypass flag."""

    def _load_entries(self):
        intents, responses = {}, {}
        for path in self.path.iterdir():
            if path.name == ".lock" or path.name.startswith(".checkpoint-"):
                continue
            match = _ARTIFACT.fullmatch(path.name)
            if not match:
                raise CheckpointError("Unexpected checkpoint artifact; manual review is required.")
            index, kind = int(match[1]), match[2]
            (intents if kind == "intent" else responses)[index] = _read_json(path)
        if sorted(intents) != list(range(len(intents))) or not responses.keys() <= intents.keys():
            raise CheckpointMismatch("Checkpoint call index mismatch; manual review is required.")
        for index, intent in intents.items():
            if (
                set(intent) != {"version", "call_index", "request_sha256", "phase"}
                or type(intent["version"]) is not int or intent["version"] != 1
                or type(intent["call_index"]) is not int or intent["call_index"] != index
                or not isinstance(intent["request_sha256"], str)
                or not _HASH.fullmatch(intent["request_sha256"])
                or not isinstance(intent["phase"], str) or not _PHASE.fullmatch(intent["phase"])
            ):
                raise CheckpointError("Invalid checkpoint intent; manual review is required.")
            if index not in responses:
                if index != len(intents) - 1:
                    raise CheckpointIncomplete(
                        "An earlier call has no saved response; manual reconciliation is required."
                    )
                continue
            envelope = responses[index]
            if set(envelope) != {*intent, "response", "response_sha256"} or any(
                type(envelope.get(key)) is not type(value) or envelope.get(key) != value
                for key, value in intent.items()
            ):
                raise CheckpointError(
                    "Invalid checkpoint response envelope; manual review is required."
                )
            response = envelope["response"]
            if type(response) is not dict or set(response) != _RESPONSE_FIELDS:
                raise CheckpointError(
                    "Invalid checkpoint response fields; manual review is required."
                )
            normalized = _normalize_response(response, intent["phase"])
            if envelope["response_sha256"] != _digest(normalized):
                raise CheckpointError(
                    "Checkpoint response checksum mismatch; manual review is required."
                )
        return intents, responses

    def complete(self, messages, max_output_tokens: int, phase: str) -> dict[str, Any]:
        normalized, request_hash = _normalize_request(messages, max_output_tokens, phase)
        with self._locked():
            intents, responses = self._load_entries()
            index = self._call_index
            intent = dict(version=1, call_index=index, request_sha256=request_hash, phase=phase)
            if index in intents:
                if intents[index] != intent:
                    raise CheckpointMismatch(
                        "Checkpoint request mismatch at this call index; manual review is required."
                    )
                if index not in responses:
                    raise CheckpointIncomplete(
                        "Call intent has no saved response; manual reconciliation is required. "
                        "A completed ledger receipt alone cannot authorize retry."
                    )
                self._call_index += 1
                return {**responses[index]["response"], "checkpoint_reused": True}
            if index != len(intents):
                raise CheckpointMismatch(
                    "Checkpoint call index mismatch; manual review is required."
                )
            if len(responses) != len(intents):
                raise CheckpointIncomplete(
                    "An outstanding call blocks dispatch; manual reconciliation is required."
                )
            if index > 99_999_999:
                raise CheckpointError("Checkpoint call index exceeds the supported range.")
            _atomic_json(self.path / f"{index:08d}.intent.json", intent)
            self._on_checkpoint("after_intent")
            try:
                raw = self.client.complete(
                    normalized, max_output_tokens=max_output_tokens, phase=phase,
                )
            except ClientError:
                raise
            except Exception:
                raise CheckpointIncomplete(
                    "Dispatch failed; the saved intent requires manual reconciliation before retry."
                ) from None
            self._on_checkpoint("after_dispatch")
            response = _normalize_response(raw, phase)
            if response["output_tokens"] > max_output_tokens:
                raise CheckpointError("Response usage exceeds the checkpointed request limit.")
            envelope = {**intent, "response": response, "response_sha256": _digest(response)}
            _atomic_json(self.path / f"{index:08d}.response.json", envelope)
            self._on_checkpoint("after_response")
            self._call_index += 1
            return {**response, "checkpoint_reused": False}
