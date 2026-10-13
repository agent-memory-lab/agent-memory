"""Offline interruption/replay checks; no model, network, or credentials are used."""

import json
import multiprocessing
from pathlib import Path

import pytest

from experiments.evidence_obligations import checkpoints as module
from experiments.evidence_obligations.checkpoints import (
    CheckpointError,
    CheckpointIncomplete,
    CheckpointMismatch,
    DurableCallCache,
)
from experiments.evidence_obligations.model_client import BudgetLedger

MESSAGES = [{"role": "user", "content": "Unique synthetic request text, never store this."}]


def receipt(number=1, **updates):
    return {
        "content": "Public synthetic answer.", "input_tokens": 4, "output_tokens": 2,
        "latency": 0.25, "receipt_id": f"offline-{number}", **updates,
    }


class FakeClient:
    def __init__(self, result=None):
        self.calls = []
        self.result = result

    def complete(self, messages, *, max_output_tokens, phase):
        self.calls.append((messages, max_output_tokens, phase))
        return dict(self.result) if self.result is not None else receipt(len(self.calls))


class NoDispatch:
    def complete(self, *args, **kwargs):
        raise AssertionError("Replayed or uncertain calls must not dispatch.")


class Interrupted(BaseException):
    pass


class InterruptAt(DurableCallCache):
    def __init__(self, client, path, stage):
        super().__init__(client, path)
        self.stage = stage

    def _on_checkpoint(self, stage):
        if stage == self.stage:
            raise Interrupted


def call(cache, messages=None, max_output_tokens=20, phase="answer"):
    return cache.complete(MESSAGES if messages is None else messages, max_output_tokens, phase)


def test_intent_is_durable_before_dispatch_and_response_before_return(tmp_path, monkeypatch):
    syncs = []
    real_sync = module._fsync_directory

    def observed_sync(path):
        real_sync(path)
        syncs.append(sorted(p.name for p in Path(path).glob("*.json")))

    monkeypatch.setattr(module, "_fsync_directory", observed_sync)

    class ObserveClient(FakeClient):
        def complete(self, *args, **kwargs):
            assert syncs[-1] == ["00000000.intent.json"]
            intent = json.loads((tmp_path / "00000000.intent.json").read_text())
            assert set(intent) == {"version", "call_index", "request_sha256", "phase"}
            return super().complete(*args, **kwargs)

    result = call(DurableCallCache(ObserveClient(), tmp_path))
    assert syncs[-1] == ["00000000.intent.json", "00000000.response.json"]
    assert result["checkpoint_reused"] is False


def test_interrupt_after_intent_before_dispatch_blocks_retry(tmp_path):
    client = FakeClient()
    with pytest.raises(Interrupted):
        call(InterruptAt(client, tmp_path, "after_intent"))
    assert client.calls == []
    assert (tmp_path / "00000000.intent.json").exists()
    assert not (tmp_path / "00000000.response.json").exists()
    with pytest.raises(CheckpointIncomplete, match="manual reconciliation"):
        call(DurableCallCache(client, tmp_path))
    assert client.calls == []


def test_completed_ledger_without_saved_response_never_retries_or_changes_ledger(tmp_path):
    ledger = BudgetLedger(tmp_path / "accounting.json")

    class AccountedClient(FakeClient):
        def complete(self, messages, *, max_output_tokens, phase):
            super().complete(messages, max_output_tokens=max_output_tokens, phase=phase)
            identity = ledger.reserve(100, max_output_tokens, phase)
            ledger._settle(identity, latency=0.25, input_tokens=4, output_tokens=2)
            return receipt(receipt_id=identity)

    client = AccountedClient()
    cache_path = tmp_path / "sequence" / "arm"
    with pytest.raises(Interrupted):
        call(InterruptAt(client, cache_path, "after_dispatch"))
    before = ledger.path.read_bytes()
    assert ledger.snapshot()["receipts"][0]["status"] == "completed"
    with pytest.raises(CheckpointIncomplete, match="completed ledger receipt"):
        call(DurableCallCache(client, cache_path))
    assert len(client.calls) == 1
    assert ledger.path.read_bytes() == before


def test_interrupt_after_saved_response_replays_without_dispatch(tmp_path):
    client = FakeClient()
    with pytest.raises(Interrupted):
        call(InterruptAt(client, tmp_path, "after_response"))
    assert len(client.calls) == 1
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    result = call(DurableCallCache(NoDispatch(), tmp_path))
    assert result["checkpoint_reused"] is True
    assert result["receipt_id"] == "offline-1"
    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before


def test_return_and_disk_response_are_allowlisted_and_preserve_shared_receipt(tmp_path):
    # These are deliberately fake extra fields, not discovered secrets or environment values.
    raw = receipt(
        headers={"provider-debug": "do-not-persist-header"},
        api_key="not-a-real-secret-do-not-persist", environment="do-not-persist-environment",
        extra_debug="do-not-persist-debug", shared_extraction_reused=True, phase="extract",
    )
    client = FakeClient(raw)
    first = call(DurableCallCache(client, tmp_path), phase="extract")
    second = call(DurableCallCache(NoDispatch(), tmp_path), phase="extract")
    assert first == {**second, "checkpoint_reused": False}
    assert first["shared_extraction_reused"] is True
    assert first["receipt_id"] == "offline-1"
    expected = {
        "content", "input_tokens", "output_tokens", "latency", "receipt_id",
        "shared_extraction_reused", "phase",
    }
    assert set(first) == expected | {"checkpoint_reused"}
    artifact = json.loads((tmp_path / "00000000.response.json").read_text())
    assert set(artifact["response"]) == expected
    saved = "\n".join(path.read_text() for path in tmp_path.glob("*.json"))
    assert MESSAGES[0]["content"] not in saved
    assert "do-not-persist" not in saved
    assert "not-a-real-secret" not in saved
    assert "checkpoint_reused" not in saved


@pytest.mark.parametrize("damage", ["content", "checksum", "truncated", "extra", "duplicate"])
def test_corrupt_response_blocks_all_dispatch(tmp_path, damage):
    client = FakeClient()
    call(DurableCallCache(client, tmp_path))
    path = tmp_path / "00000000.response.json"
    data = json.loads(path.read_text())
    if damage == "content":
        data["response"]["content"] = "Corrupted answer."
    elif damage == "checksum":
        data["response_sha256"] = "0" * 64
    elif damage == "extra":
        data["response"]["headers"] = {"unexpected": "data"}
    if damage == "truncated":
        path.write_text('{"version":')
    elif damage == "duplicate":
        path.write_text('{"version": 1, ' + json.dumps(data)[1:])
    else:
        path.write_text(json.dumps(data))
    with pytest.raises(CheckpointError):
        call(DurableCallCache(client, tmp_path))
    assert len(client.calls) == 1


@pytest.mark.parametrize("changed", ["message", "max_output_tokens", "phase", "role"])
def test_changed_request_at_same_index_fails_closed(tmp_path, changed):
    client = FakeClient()
    call(DurableCallCache(client, tmp_path))
    kwargs = {}
    if changed == "message":
        kwargs["messages"] = [{"role": "user", "content": "Different synthetic question."}]
    elif changed == "role":
        kwargs["messages"] = [{"role": "system", "content": MESSAGES[0]["content"]}]
    elif changed == "max_output_tokens":
        kwargs["max_output_tokens"] = 21
    else:
        kwargs["phase"] = "extract"
    with pytest.raises(CheckpointMismatch, match="request mismatch"):
        call(DurableCallCache(client, tmp_path), **kwargs)
    assert len(client.calls) == 1


def test_resume_cached_prefix_then_complete_remaining_calls(tmp_path):
    first_client = FakeClient()
    initial = DurableCallCache(first_client, tmp_path)
    first = call(initial, phase="extract")
    second = call(initial, phase="gap")
    resumed_client = FakeClient(receipt(receipt_id="offline-new"))
    resumed = DurableCallCache(resumed_client, tmp_path)
    assert call(resumed, phase="extract") == {**first, "checkpoint_reused": True}
    assert call(resumed, phase="gap") == {**second, "checkpoint_reused": True}
    third = call(resumed, phase="answer")
    assert not third["checkpoint_reused"] and third["receipt_id"] == "offline-new"
    assert len(resumed_client.calls) == 1
    replay = DurableCallCache(NoDispatch(), tmp_path)
    assert call(replay, phase="extract")["receipt_id"] == first["receipt_id"]
    assert call(replay, phase="gap")["receipt_id"] == second["receipt_id"]
    assert call(replay, phase="answer")["receipt_id"] == third["receipt_id"]


def test_missing_old_artifacts_cannot_silently_restart_call_indices(tmp_path):
    client = FakeClient()
    original = DurableCallCache(client, tmp_path)
    call(original)
    call(original)
    for path in tmp_path.glob("00000000.*.json"):
        path.unlink()
    with pytest.raises(CheckpointMismatch, match="call index mismatch"):
        call(DurableCallCache(client, tmp_path))
    assert len(client.calls) == 2


def test_missing_response_with_later_completed_call_blocks_replay(tmp_path):
    client = FakeClient()
    original = DurableCallCache(client, tmp_path)
    call(original)
    call(original)
    (tmp_path / "00000000.response.json").unlink()
    with pytest.raises(CheckpointIncomplete):
        call(DurableCallCache(client, tmp_path))
    assert len(client.calls) == 2


def test_failed_response_write_keeps_intent_and_blocks_retry(tmp_path, monkeypatch):
    client = FakeClient()
    real_write = module._atomic_json

    def fail_response(path, value):
        if path.name.endswith(".response.json"):
            raise CheckpointError("Injected response persistence failure.")
        real_write(path, value)

    monkeypatch.setattr(module, "_atomic_json", fail_response)
    with pytest.raises(CheckpointError, match="persistence failure"):
        call(DurableCallCache(client, tmp_path))
    monkeypatch.setattr(module, "_atomic_json", real_write)
    with pytest.raises(CheckpointIncomplete):
        call(DurableCallCache(client, tmp_path))
    assert len(client.calls) == 1


@pytest.mark.parametrize("field,value", [
    ("latency", float("nan")), ("input_tokens", True), ("output_tokens", 21),
    ("receipt_id", ""), ("shared_extraction_reused", "yes"), ("phase", "wrong"),
])
def test_invalid_provider_receipt_remains_unresolved(tmp_path, field, value):
    client = FakeClient(receipt(**{field: value}))
    with pytest.raises(CheckpointError):
        call(DurableCallCache(client, tmp_path))
    with pytest.raises(CheckpointIncomplete):
        call(DurableCallCache(client, tmp_path))
    assert len(client.calls) == 1


def test_invalid_request_does_not_create_intent_or_dispatch(tmp_path):
    client = FakeClient()
    cache = DurableCallCache(client, tmp_path)
    with pytest.raises(ValueError):
        call(cache, max_output_tokens=True)
    assert list(tmp_path.glob("*.json")) == []
    assert client.calls == []


def _process_call(path, phase, entered, release, attempted, finished, results):
    class BlockingClient:
        def complete(self, messages, *, max_output_tokens, phase):
            results.put(("dispatch", phase))
            entered.set()
            if not release.wait(10):
                raise RuntimeError("Test synchronization timed out.")
            return receipt()

    try:
        cache = DurableCallCache(BlockingClient(), path)
        attempted.set()
        result = call(cache, phase=phase)
        results.put(("result", result["checkpoint_reused"]))
    except Exception as error:
        results.put(("error", type(error).__name__))
    finally:
        finished.set()


@pytest.mark.parametrize("second_phase", ["answer", "different"])
def test_directory_flock_serializes_different_processes(tmp_path, second_phase):
    context = multiprocessing.get_context("fork")
    entered, release = context.Event(), context.Event()
    first_attempted, second_attempted = context.Event(), context.Event()
    first_finished, second_finished = context.Event(), context.Event()
    results = context.Queue()
    first = context.Process(target=_process_call, args=(
        str(tmp_path), "answer", entered, release, first_attempted, first_finished, results,
    ))
    second = context.Process(target=_process_call, args=(
        str(tmp_path), second_phase, entered, release, second_attempted, second_finished, results,
    ))
    try:
        first.start()
        assert entered.wait(10)
        second.start()
        assert second_attempted.wait(10)
        assert not second_finished.wait(0.1), "A second process bypassed the directory lock."
        release.set()
        assert first_finished.wait(10) and second_finished.wait(10)
        first.join(10)
        second.join(10)
        assert first.exitcode == second.exitcode == 0
        reports = [results.get(timeout=10) for _ in range(3)]
        assert reports.count(("dispatch", "answer")) == 1
        assert ("result", False) in reports
        assert (("result", True) if second_phase == "answer" else (
            "error", "CheckpointMismatch",
        )) in reports
    finally:
        release.set()
        for process in (first, second):
            if process.pid is not None:
                if process.is_alive():
                    process.terminate()
                process.join(10)
        results.close()
        results.join_thread()
