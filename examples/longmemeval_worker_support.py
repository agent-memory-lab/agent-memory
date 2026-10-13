"""Local process ownership and immutable package provenance for a durable worker."""

from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path


@contextmanager
def worker_lock(output):
    """Refuse concurrent recovery; the OS releases ownership after a crash."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with (output / "worker.lock").open("a+") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another worker owns this output directory") from error
        stream.seek(0)
        stream.truncate()
        json.dump({"pid": os.getpid()}, stream)
        stream.flush()
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def package_digest(root):
    """Hash paths and source bytes, ignoring runtime bytecode and timestamps."""
    root = Path(root)
    digest = sha256()
    files = sorted(root.rglob("*.py"))
    if not files:
        raise ValueError("package must contain Python source")
    for path in files:
        relative = path.relative_to(root).as_posix().encode()
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def load_evaluation_selection(
    path,
    expected_sha256,
    available_arms,
    *,
    arms=None,
    question_ids=None,
    expected_question_count=500,
):
    """Validate the complete frozen source before selecting a regression subset.

    Selected questions retain dataset order, regardless of CLI label order. The
    source hash covers all source questions, including every unselected question.
    """
    content = Path(path).read_bytes()
    if sha256(content).hexdigest() != expected_sha256:
        raise ValueError("expected the frozen official cleaned LongMemEval-S dataset")
    dataset = json.loads(content)
    if not isinstance(dataset, list) or len(dataset) != expected_question_count:
        raise ValueError(f"source dataset requires exactly {expected_question_count} questions")
    ids = [item.get("question_id") if isinstance(item, dict) else None for item in dataset]
    if any(not isinstance(value, str) or not 1 <= len(value) <= 128 for value in ids):
        raise ValueError("source question IDs must be bounded nonempty strings")
    if len(set(ids)) != len(ids):
        raise ValueError("source question IDs must be unique")

    def labels(values, name, allowed):
        if isinstance(values, (str, bytes)):
            raise ValueError(f"{name} must be a nonempty sequence of labels")
        selected = tuple(values)
        if not selected or any(not isinstance(value, str) or not value for value in selected):
            raise ValueError(f"{name} must be a nonempty sequence of labels")
        if len(set(selected)) != len(selected):
            raise ValueError(f"{name} must not contain duplicate labels")
        if set(selected) - set(allowed):
            raise ValueError(f"{name} contains unknown labels")
        return selected

    available = tuple(available_arms)
    selected_arms = labels(available if arms is None else arms, "arms", available)
    requested = labels(ids if question_ids is None else question_ids, "question_ids", ids)
    requested_set = set(requested)
    selected = [item for item in dataset if item["question_id"] in requested_set]
    full = len(selected) == len(dataset) and set(selected_arms) == set(available)
    return {
        "items": selected,
        "arms": list(selected_arms),
        "selected_question_ids": [item["question_id"] for item in selected],
        "source_question_count": len(dataset),
        "question_count": len(selected),
        "planned_tasks": len(selected) * len(selected_arms),
        "evaluation_scope": "full" if full else "selected_regression",
    }


def _durable_replace(path, content):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def recover_call_log_tails(output):
    """Repair only incomplete last records while the caller owns worker_lock.

    Each discarded tail is preserved outside the JSONL glob and recorded as an
    explicit unknown accounting gap. The durable gap is written before truncation
    so an interrupted recovery can never discard a record without disclosure.
    Valid JSON without its final newline is preserved and normalized instead.
    """
    output = Path(output)
    gap_path = output / "accounting_gaps.json"
    gaps = json.loads(gap_path.read_text(encoding="utf-8")) if gap_path.exists() else []
    if not isinstance(gaps, list) or any(
        not isinstance(gap, dict)
        or not {"filename", "offset", "bytes", "sha256", "reason", "backup_file"} <= gap.keys()
        for gap in gaps
    ):
        raise ValueError("existing accounting gap ledger is invalid")
    repairs = []
    # Validate the entire directory before changing any file. A completed bad
    # record must not be mistaken for an interrupted append, even in the last line.
    for path in sorted((output / "calls").glob("*.jsonl")):
        with path.open("rb") as stream:
            line_number = 0
            while True:
                offset = stream.tell()
                raw = stream.readline()
                if not raw:
                    break
                line_number += 1
                reason = None
                try:
                    json.loads(raw.decode("utf-8"))
                except UnicodeDecodeError:
                    reason = "incomplete_final_utf8"
                except json.JSONDecodeError:
                    reason = "incomplete_final_json"
                if reason is not None:
                    if raw.endswith(b"\n"):
                        raise ValueError(
                            f"completed call log record is invalid: {path.name}:{line_number}"
                        )
                    repairs.append((path, offset, raw, reason))
                elif not raw.endswith(b"\n"):
                    repairs.append((path, offset, raw, None))
    identities = {(gap["filename"], gap["offset"], gap["sha256"]) for gap in gaps}
    for path, offset, raw, reason in repairs:
        with path.open("r+b") as stream:
            stream.seek(offset)
            if stream.read() != raw:
                raise RuntimeError("call log changed during exclusively owned recovery")
            if reason is None:
                stream.seek(0, os.SEEK_END)
                stream.write(b"\n")
                stream.flush()
                os.fsync(stream.fileno())
                continue
            tail_hash = sha256(raw).hexdigest()
            quarantine = output / "recovered_call_tails"
            quarantine.mkdir(parents=True, exist_ok=True)
            backup = quarantine / f"{path.name}-{offset}-{tail_hash}.tail.bin"
            if backup.exists():
                if backup.read_bytes() != raw:
                    raise ValueError("isolated call log tail does not match its recorded hash")
            else:
                _durable_replace(backup, raw)
            identity = (path.name, offset, tail_hash)
            if identity not in identities:
                gap = {
                    "filename": path.name,
                    "offset": offset,
                    "bytes": len(raw),
                    "sha256": tail_hash,
                    "reason": reason,
                    "backup_file": backup.relative_to(output).as_posix(),
                    "accounting_status": "unknown",
                }
                gaps.append(gap)
                identities.add(identity)
                _durable_replace(
                    gap_path,
                    (json.dumps(gaps, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
                )
            else:
                existing = next(
                    gap
                    for gap in gaps
                    if (gap["filename"], gap["offset"], gap["sha256"]) == identity
                )
                if (
                    existing["bytes"] != len(raw)
                    or existing["reason"] != reason
                    or existing["backup_file"] != backup.relative_to(output).as_posix()
                ):
                    raise ValueError("existing accounting gap does not match its isolated tail")
            stream.truncate(offset)
            stream.flush()
            os.fsync(stream.fileno())
    return gaps
