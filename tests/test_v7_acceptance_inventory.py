"""The acceptance ledger must not promote planned or inherited evidence to new passes."""

import json
import re
from collections import Counter
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs/design"


def test_frozen_design_is_unchanged():
    assert sha256((DOCS / "AGENT_MEMORY_DESIGN_V7.0.0.md").read_bytes()).hexdigest() == (
        "309104c81b2e89ce74137903312ecbeaa036ba192d47fcfbdd555cdd4dc907b9"
    )


def test_v7_preserves_all_44_original_task_statuses():
    ledger = (DOCS / "v7.0.0/task.md").read_text()
    rows = [
        line.split("|")[1:-1] for line in ledger.splitlines() if re.match(r"\| AM61-T\d\d \|", line)
    ]
    assert len(rows) == 44
    assert {r[0].strip() for r in rows} == {f"AM61-T{i:02d}" for i in range(1, 45)}
    assert Counter(r[2].strip() for r in rows) == {
        "DONE": 1,
        "IN_PROGRESS": 33,
        "TODO": 10,
    }
    assert sha256((DOCS / "v6.1.0/task.md").read_bytes()).hexdigest() == (
        "14f90d83cb2425177aa22ad2dd1cf7fa90e4c56416ae93a02c7ff134ad8810f5"
    )


def test_all_98_acceptance_responsibilities_have_explicit_evidence_state():
    manifest = json.loads((DOCS / "v7.0.0/acceptance-map.json").read_text())
    cases = manifest["cases"]
    assert len(cases) == len({case["id"] for case in cases}) == 98
    assert len([case for case in cases if case["id"].startswith("Q7-")]) == 32
    for case in cases:
        assert case["v7_status"] in {
            "not_run",
            "partial",
            "passed",
            "failed",
            "unsupported",
            "deferred",
            "insufficient_data",
        }
        assert case["owner"].startswith(("AM61-T", "AM70-T"))
        assert isinstance(case["evidence"], list)
        if case["v7_status"] == "passed":
            assert case["evidence"], "A prior status or same-named unit test is not new evidence"
