"""Audit tooling contracts; challenge success rates belong in the saved report."""
import json

import pytest

from experiments.public_longmemeval.packing_ablation import evaluate, freeze, main
from experiments.public_longmemeval.tests.packing_fixtures import challenges


def inputs(tmp_path):
    blind, labels = challenges()
    source, gold = tmp_path / "blind.jsonl", tmp_path / "requirements.jsonl"
    source.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in blind), encoding="utf-8")
    gold.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in labels), encoding="utf-8")
    return source, gold


def test_all_four_arms_freeze_before_evaluation_without_model_calls(tmp_path):
    source, gold = inputs(tmp_path)
    output = tmp_path / "run"
    freeze(source, output)
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["evaluator_requirements_opened"] is False
    assert manifest["model_calls"] == 0
    rows = json.loads((output / "arms.json").read_text(encoding="utf-8"))
    expected = {f"packing-{p}/reader-{r}" for p in ("v2", "v3") for r in ("v2", "v3")}
    assert len(rows) == 24
    for row in rows:
        assert set(row["arms"]) == expected
        for packing in ("v2", "v3"):
            left, right = (row["arms"][f"packing-{packing}/reader-{r}"] for r in ("v2", "v3"))
            assert [(v["id"], v["text"]) for v in left["selected"]] == [
                (v["id"], v["text"]) for v in right["selected"]]
            assert left["delivery"]["partial_count"] == right["delivery"]["partial_count"] == 0
    result = evaluate(output, gold)
    assert set(result["summary"]) == expected
    assert all(v["cases"] == 24 for v in result["summary"].values())
    assert result["answer_accuracy"] is None and result["model_calls"] == 0
    assert result["comparison"]["no_new_evidence_loss_gate"] == "rejected"
    assert {row["category"] for row in result["comparison"]["regressions"]} >= {
        "pronoun-bridge", "same-topic-count", "conflict"}
    with pytest.raises(FileExistsError):
        evaluate(output, gold)
    with pytest.raises(FileExistsError):
        freeze(source, output)


def test_tampered_frozen_output_is_rejected_before_labels_are_opened(tmp_path):
    source, gold = inputs(tmp_path)
    output = tmp_path / "run"
    freeze(source, output)
    (output / "arms.json").write_text("[]", encoding="utf-8")
    gold.unlink()
    with pytest.raises(ValueError, match="frozen arms changed"):
        evaluate(output, gold)


def test_unknown_blind_fields_cannot_supply_oracle_requirements(tmp_path):
    source, _ = inputs(tmp_path)
    rows = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines()]
    rows[0]["required_evidence"] = ["oracle"]
    source.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    with pytest.raises(ValueError, match="blind input"):
        freeze(source, tmp_path / "run")
    assert not (tmp_path / "run").exists()


def test_regression_gate_preserves_failed_acceptance_report(tmp_path, monkeypatch, capsys):
    source, gold = inputs(tmp_path)
    output = tmp_path / "run"
    freeze(source, output)
    monkeypatch.setattr("sys.argv", ["packing_ablation", "evaluate", "--output", str(output),
        "--requirements", str(gold), "--require-no-regression"])
    with pytest.raises(SystemExit) as stopped:
        main()
    assert stopped.value.code == 2
    report = json.loads((output / "evaluation.json").read_text(encoding="utf-8"))
    assert report["comparison"]["no_new_evidence_loss_gate"] == "rejected"
    assert json.loads(capsys.readouterr().out)["comparison"]["regressions"]


def test_new_candidate_can_be_compared_without_reinterpreting_old_manifests(tmp_path):
    source, gold = inputs(tmp_path)
    output = tmp_path / "v4-run"
    freeze(source, output, packing_versions=("v2", "v3", "v4"))
    report = evaluate(output, gold, candidate_version="v4")
    assert len(report["summary"]) == 6
    assert report["comparison"]["candidate_version"] == "v4"
    assert report["comparison"]["no_new_evidence_loss_gate"] == "passed"
    assert report["summary"]["packing-v4/reader-v2"]["complete_cases"] == 21
    assert report["summary"]["packing-v4/reader-v3"]["complete_cases"] == 21
    assert report["comparison"]["production_readiness_established"] is False
