"""Validate trusted study adaptation and the executable scorer interface."""

import json
import subprocess
import sys
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

from agent_memory.evaluation.acceptance import (
    AcceptanceProfile,
    QualityThresholds,
    acceptance_profile_from_dict,
)
from agent_memory.evaluation.evidence import evidence_dataset_from_dict, evidence_report_from_dict
from agent_memory.serialization import to_jsonable

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/memory_v61"


def test_frozen_study_has_precise_source_spans_and_explicit_deletion_meaning():
    study = json.loads((FIXTURES / "aurora_study.json").read_text())
    dataset = evidence_dataset_from_dict(json.loads((FIXTURES / "aurora_dataset.json").read_text()))
    assert dataset.fingerprint == study["dataset_sha256"]
    assert len(study["events"]) == 17 and len(dataset.cases) == 20
    events = {event["source_revision_id"]: event for event in study["events"]}
    for case in dataset.cases:
        for span in case.evidence:
            content = events[span.source_revision_id]["content"]
            assert sha256(content[span.start : span.end].encode()).hexdigest() == span.quote_sha256
    assert study["events"][14]["operation"] == "correct_source"
    assert study["events"][15]["operation"] == "retract_source"
    assert study["events"][16]["operation"] == "erase_source"
    by_id = {case.case_id: case for case in dataset.cases}
    assert not by_id["Q09"].answerable and not by_id["Q15"].answerable
    assert by_id["Q16"].answerable
    assert by_id["Q20"].diagnostic_only
    assert by_id["Q11"].known_at < by_id["Q12"].known_at
    assert study["end_to_end_executed"] is False


def test_cli_scores_contract_fixture_and_blocks_changed_profile(tmp_path):
    output = tmp_path / "report.json"
    command = [
        sys.executable,
        str(ROOT / "tools/evaluate_memory_evidence.py"),
        "--dataset",
        str(FIXTURES / "aurora_dataset.json"),
        "--observations",
        str(FIXTURES / "scorer_observations.json"),
        "--run-configuration",
        str(FIXTURES / "scorer_configuration.json"),
        "--output",
        str(output),
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)
    payload = json.loads(output.read_text())
    assert payload["metrics"]["case_count"] == 19
    assert payload["metrics"]["answerable_count"] == 17
    assert payload["metrics"]["support_rate"] == 1
    assert payload["production_ready"] is False
    report = evidence_report_from_dict(payload["report"])
    profile = AcceptanceProfile(
        "synthetic-contract",
        "1",
        ("scorer-contract",),
        report.dataset_sha256,
        report.run_configuration_sha256,
        report.fingerprint,
        19,
        17,
        QualityThresholds(1, 1, 1, 1, 0, 0, 0, 100, 1, 100),
        "synthetic-only",
    )
    assert acceptance_profile_from_dict(to_jsonable(profile)) == profile
    frozen_hash = profile.fingerprint
    changed = replace(profile, version="2")
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(to_jsonable(changed)))
    gated = subprocess.run(
        command
        + [
            "--profile",
            str(path),
            "--baseline",
            str(output),
            "--expected-profile-sha256",
            frozen_hash,
        ],
        capture_output=True,
        text=True,
    )
    assert gated.returncode == 1
    assert (
        "profile_changed_after_freeze" in json.loads(output.read_text())["quality_gate"]["reasons"]
    )
