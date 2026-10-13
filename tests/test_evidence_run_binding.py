"""Offline binding guards; no live client or key is constructed."""

import asyncio
import json

import pytest

from experiments.evidence_obligations import runner as r
from experiments.evidence_obligations.dataset import build_dataset
from experiments.evidence_obligations.model_client import ENDPOINT, BudgetLedger, LedgerError


class NeverDispatch:
    def __init__(self, ledger):
        self.ledger = ledger

    def complete(self, *args, **kwargs):
        raise AssertionError("A binding mismatch must fail before dispatch")


def binding(ledger):
    return dict(
        endpoint_sha256=r.digest(ENDPOINT),
        ledger_id=ledger.snapshot()["ledger_id"],
        ledger_path_sha256=r.digest(str(ledger.path.resolve())),
    )


def test_new_ledger_cannot_resume_frozen_run(tmp_path):
    old = BudgetLedger(tmp_path / "old-ledger.json")
    output = tmp_path / "output"
    output.mkdir()
    r.freeze(output, build_dataset(), "dev", 1, ("per_query_gap",), provider_binding=binding(old))
    new = BudgetLedger(tmp_path / "new-ledger.json")
    with pytest.raises(ValueError, match="manifest mismatch"):
        asyncio.run(r.run(output, tmp_path / "work", NeverDispatch(new), arms=("per_query_gap",)))
    assert old.snapshot()["total_calls"] == new.snapshot()["total_calls"] == 0


def test_endpoint_identity_is_part_of_frozen_manifest(tmp_path):
    data = build_dataset()
    r.freeze(
        tmp_path, data, "dev", 1, r.ARMS, provider_binding={"endpoint_sha256": r.digest("one")}
    )
    with pytest.raises(ValueError, match="manifest mismatch"):
        r.freeze(
            tmp_path, data, "dev", 1, r.ARMS, provider_binding={"endpoint_sha256": r.digest("two")}
        )


def test_old_ledger_never_silently_migrated_or_reset(tmp_path):
    path = tmp_path / "ledger.json"
    ledger = BudgetLedger(path)
    state = ledger.snapshot()
    state["version"] = 1
    state.pop("ledger_id")
    path.write_text(json.dumps(state))
    original = path.read_bytes()
    with pytest.raises(LedgerError):
        BudgetLedger(path)
    assert path.read_bytes() == original


def test_run_directory_has_exclusive_process_lock(tmp_path):
    import fcntl

    from test_evidence_obligation_runner import Scripted

    client = Scripted()
    with (tmp_path / ".run.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="already owns"):
            asyncio.run(r.run(tmp_path, tmp_path / "work", client, execution="scripted-test"))
    assert client.requests == []
