from __future__ import annotations

import importlib.util
import json
import os
from hashlib import sha256
from pathlib import Path

import pytest

pytest.importorskip("fcntl", reason="The optional durable local worker uses Unix process locks")

SOURCE = Path(__file__).resolve().parents[1] / "examples" / "longmemeval_worker_support.py"
SPEC = importlib.util.spec_from_file_location("longmemeval_worker_support", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_exclusive_lock_rejects_another_owner_and_releases(tmp_path):
    with MODULE.worker_lock(tmp_path):
        assert json.loads((tmp_path / "worker.lock").read_text())["pid"] == os.getpid()
        with pytest.raises(RuntimeError, match="another worker"):
            with MODULE.worker_lock(tmp_path):
                pytest.fail("recovery must never run concurrently")
    with MODULE.worker_lock(tmp_path):
        pass


def test_package_hash_ignores_bytecode_but_detects_path_or_content_change(tmp_path):
    source = tmp_path / "module.py"
    source.write_text("answer = 42\n")
    first = MODULE.package_digest(tmp_path)
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "module.pyc").write_bytes(b"bytecode")
    assert MODULE.package_digest(tmp_path) == first
    source.write_text("answer = 43\n")
    assert MODULE.package_digest(tmp_path) != first
    source.write_text("answer = 42\n")
    source.rename(tmp_path / "renamed.py")
    assert MODULE.package_digest(tmp_path) != first


def test_empty_package_is_not_a_valid_provenance(tmp_path):
    with pytest.raises(ValueError, match="Python source"):
        MODULE.package_digest(tmp_path)


def call_log(output, name, content):
    calls = output / "calls"
    calls.mkdir(exist_ok=True)
    path = calls / name
    path.write_bytes(content)
    return path


@pytest.mark.parametrize(
    "tail,reason",
    [
        (b'{"model":"fixture","input_tokens":', "incomplete_final_json"),
        (b'{"model":"\xe4\xb8', "incomplete_final_utf8"),
    ],
)
def test_only_incomplete_final_record_is_quarantined_with_explicit_unknown_gap(
    tmp_path, tail, reason
):
    prefix = b'{"input_tokens":13,"output_tokens":2}\n'
    path = call_log(tmp_path, "attempt1.jsonl", prefix + tail)
    with MODULE.worker_lock(tmp_path):
        gaps = MODULE.recover_call_log_tails(tmp_path)
    assert path.read_bytes() == prefix
    assert len(gaps) == 1
    gap = gaps[0]
    assert gap["filename"] == "attempt1.jsonl" and gap["offset"] == len(prefix)
    assert gap["bytes"] == len(tail) and gap["sha256"] == sha256(tail).hexdigest()
    assert gap["reason"] == reason and gap["accounting_status"] == "unknown"
    backup = tmp_path / gap["backup_file"]
    assert backup.read_bytes() == tail and backup.suffix != ".jsonl"
    assert json.loads((tmp_path / "accounting_gaps.json").read_text()) == gaps
    assert list((tmp_path / "calls").glob("*.jsonl")) == [path]
    manifest_bytes = (tmp_path / "accounting_gaps.json").read_bytes()
    with MODULE.worker_lock(tmp_path):
        assert MODULE.recover_call_log_tails(tmp_path) == gaps
    assert (tmp_path / "accounting_gaps.json").read_bytes() == manifest_bytes


def test_valid_final_json_without_newline_is_preserved_without_token_loss(tmp_path):
    content = b'{"input_tokens":13,"output_tokens":null}'
    path = call_log(tmp_path, "attempt1.jsonl", content)
    with MODULE.worker_lock(tmp_path):
        assert MODULE.recover_call_log_tails(tmp_path) == []
    assert path.read_bytes() == content + b"\n"
    assert not (tmp_path / "accounting_gaps.json").exists()
    with path.open("ab") as stream:
        stream.write(b'{"input_tokens":7}\n')
    with MODULE.worker_lock(tmp_path):
        assert MODULE.recover_call_log_tails(tmp_path) == []
    assert len(path.read_text().splitlines()) == 2


@pytest.mark.parametrize(
    "content",
    [
        b'{"broken":\n',
        b'{"ok":1}\n{"broken":\n',
        b'{"broken":\n{"ok":1}\n',
        b'\xff\n{"ok":1}\n',
        b'{"ok":1}\n\xff\n',
        b"\n",
    ],
)
def test_completed_or_middle_invalid_records_are_fatal_and_never_truncated(tmp_path, content):
    path = call_log(tmp_path, "attempt1.jsonl", content)
    with MODULE.worker_lock(tmp_path), pytest.raises(ValueError, match="completed call log"):
        MODULE.recover_call_log_tails(tmp_path)
    assert path.read_bytes() == content
    assert not (tmp_path / "accounting_gaps.json").exists()


def test_directory_validation_is_complete_before_any_tail_is_removed(tmp_path):
    recoverable = call_log(tmp_path, "a.jsonl", b'{"incomplete":')
    call_log(tmp_path, "b.jsonl", b'{"invalid":\n')
    with MODULE.worker_lock(tmp_path), pytest.raises(ValueError):
        MODULE.recover_call_log_tails(tmp_path)
    assert recoverable.read_bytes() == b'{"incomplete":'
    assert not (tmp_path / "accounting_gaps.json").exists()


def test_recovery_after_gap_commit_but_before_truncate_is_idempotent(tmp_path):
    prefix, tail = b'{"ok":1}\n', b'{"unfinished":'
    path = call_log(tmp_path, "attempt1.jsonl", prefix + tail)
    with MODULE.worker_lock(tmp_path):
        original = MODULE.recover_call_log_tails(tmp_path)
    path.write_bytes(prefix + tail)
    with MODULE.worker_lock(tmp_path):
        assert MODULE.recover_call_log_tails(tmp_path) == original
    assert path.read_bytes() == prefix
    assert len(list((tmp_path / "recovered_call_tails").iterdir())) == 1


def test_corrupt_gap_ledger_is_not_silently_replaced(tmp_path):
    path = call_log(tmp_path, "attempt1.jsonl", b'{"unfinished":')
    (tmp_path / "accounting_gaps.json").write_text("{}")
    with MODULE.worker_lock(tmp_path), pytest.raises(ValueError, match="gap ledger"):
        MODULE.recover_call_log_tails(tmp_path)
    assert path.read_bytes() == b'{"unfinished":'


def test_failed_gap_persistence_keeps_original_tail_for_later_recovery(tmp_path, monkeypatch):
    prefix, tail = b'{"ok":1}\n', b'{"unfinished":'
    path = call_log(tmp_path, "attempt1.jsonl", prefix + tail)
    original = MODULE._durable_replace

    def fail_gap_write(destination, content):
        if destination.name == "accounting_gaps.json":
            raise OSError("fixture write failure")
        return original(destination, content)

    with monkeypatch.context() as patch:
        patch.setattr(MODULE, "_durable_replace", fail_gap_write)
        with MODULE.worker_lock(tmp_path), pytest.raises(OSError):
            MODULE.recover_call_log_tails(tmp_path)
    assert path.read_bytes() == prefix + tail
    assert len(list((tmp_path / "recovered_call_tails").iterdir())) == 1
    with MODULE.worker_lock(tmp_path):
        assert len(MODULE.recover_call_log_tails(tmp_path)) == 1
    assert path.read_bytes() == prefix
