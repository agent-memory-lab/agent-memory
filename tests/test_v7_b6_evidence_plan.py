"""Planning references do not count as fresh final acceptance evidence."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_b6_plan_preserves_all_responsibilities_without_promoting_evidence():
    folder = ROOT / "docs/design/v7.0.0"
    plan = json.loads((folder / "evidence-plan-b6.json").read_text())
    ledger = json.loads((folder / "acceptance-map.json").read_text())
    assert plan["schema"] == "v7-b6-evidence-plan/1"
    assert plan["full_v7_acceptance"] is plan["production_benefit_claim"] is False
    assert len(plan["cases"]) == 98
    assert {case["id"] for case in plan["cases"]} == {case["id"] for case in ledger["cases"]}
    for case in plan["cases"]:
        assert case["final_verification"] == "not_run"
        assert case["required_review"]
        assert case["candidate_test_paths"] or case["pending_test_paths"]
        for path in case["candidate_test_paths"]:
            assert path.startswith("tests/") and (ROOT / path).is_file()
    quality = next(case for case in plan["cases"] if case["id"] == "Q7-32")
    assert quality["required_review"].startswith("BLOCKED:")
