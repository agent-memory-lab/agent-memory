import json
from hashlib import sha256

import pytest

from agent_memory.evaluation.real_acceptance import ARTIFACTS, load_inputs


def fixture(tmp_path):
    artifacts = {}
    for name in ARTIFACTS:
        path = tmp_path / (name + ".json")
        path.write_text("{}")
        artifacts[name] = dict(path=path.name, sha256=sha256(path.read_bytes()).hexdigest())
    doc = dict(
        schema="real-memory-acceptance-inputs/1",
        evidence_class="real_business",
        artifacts=artifacts,
        scope_authorization_reference="approved:test",
        corpus_license_reference="license:test",
        gold_independence_reference="gold:test",
        calibration_reference="calibration:test",
    )
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(doc))
    return path, doc


def test_external_inputs_are_hash_bound_not_an_acceptance_claim(tmp_path):
    path, doc = fixture(tmp_path)
    result = load_inputs(path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest())
    assert result.evidence_class == "real_business" and len(result.artifacts) == 4
    (tmp_path / "gold.json").write_text('{"changed":true}')
    with pytest.raises(ValueError, match="artifact_changed"):
        load_inputs(path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest())


def test_synthetic_and_missing_references_do_not_pass_real_preflight(tmp_path):
    path, doc = fixture(tmp_path)
    doc["evidence_class"] = "synthetic"
    path.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="real_input_manifest"):
        load_inputs(path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest())
    doc["evidence_class"] = "real_business"
    doc["calibration_reference"] = ""
    path.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="external_references"):
        load_inputs(path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest())
