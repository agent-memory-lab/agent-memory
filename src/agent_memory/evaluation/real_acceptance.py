"""Pinned external acceptance inputs and explicit host authorization.

File shape/hashes cannot prove a license or gold independence. Those are separate
host checks. Execution delegates to the existing complete four-arm evaluator;
missing prices stay unknown there and prevent unsupported promotion claims.
"""

import argparse
import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from ..retrieval.model_contracts import canonical, digest, hash_value
from .controlled_retrieval import run_controlled_retrieval

ARTIFACTS = ("corpus", "gold", "judge", "pricing")


@dataclass(frozen=True, slots=True)
class RealAcceptanceInputs:
    manifest_path: str
    manifest_sha256: str
    artifacts: tuple[tuple[str, str], ...]
    scope_authorization_reference: str
    corpus_license_reference: str
    gold_independence_reference: str
    calibration_reference: str
    evidence_class: str = "real_business"

    @property
    def fingerprint(self):
        from dataclasses import asdict

        return digest(asdict(self))


def load_inputs(path, *, expected_manifest_sha256):
    """Read only bounded metadata; never print corpus/gold contents or execute files."""
    hash_value(expected_manifest_sha256)
    path = Path(path).resolve(strict=True)
    if path.stat().st_size > 65536:
        raise ValueError("acceptance_manifest_capacity")
    raw = path.read_bytes()
    if sha256(raw).hexdigest() != expected_manifest_sha256:
        raise ValueError("acceptance_manifest_changed")
    doc = json.loads(raw)
    required = {
        "schema",
        "evidence_class",
        "artifacts",
        "scope_authorization_reference",
        "corpus_license_reference",
        "gold_independence_reference",
        "calibration_reference",
    }
    if (
        set(doc) != required
        or doc["schema"] != "real-memory-acceptance-inputs/1"
        or doc["evidence_class"] != "real_business"
    ):
        raise ValueError("acceptance_real_input_manifest_required")
    if set(doc["artifacts"]) != set(ARTIFACTS):
        raise ValueError("acceptance_artifacts_incomplete")
    hashes = []
    for name in ARTIFACTS:
        item = doc["artifacts"][name]
        if set(item) != {"path", "sha256"} or type(item["path"]) is not str:
            raise ValueError("invalid_acceptance_artifact")
        hash_value(item["sha256"])
        artifact = (path.parent / item["path"]).resolve(strict=True)
        if (
            not artifact.is_relative_to(path.parent)
            or not artifact.is_file()
            or artifact.stat().st_size > 1073741824
        ):
            raise ValueError("acceptance_artifact_outside_bounded_root")
        hasher = sha256()
        with artifact.open("rb") as stream:
            for block in iter(lambda: stream.read(1048576), b""):
                hasher.update(block)
        if hasher.hexdigest() != item["sha256"]:
            raise ValueError("acceptance_artifact_changed")
        hashes.append((name, item["sha256"]))
    references = {k: doc[k] for k in required if k.endswith("_reference")}
    if any(type(v) is not str or not 1 <= len(v) <= 2048 for v in references.values()):
        raise ValueError("acceptance_external_references_required")
    return RealAcceptanceInputs(str(path), expected_manifest_sha256, tuple(hashes), **references)


async def run_real_acceptance(inputs, plan, arms, *, authorize_inputs, expected_protocol_sha256):
    """Authority checks include corpus rights, independent gold/judge and calibration."""
    if type(inputs) is not RealAcceptanceInputs or not callable(authorize_inputs):
        raise ValueError("trusted_real_acceptance_inputs_required")
    if plan.fingerprint != expected_protocol_sha256:
        raise ValueError("acceptance_protocol_changed")
    current = load_inputs(inputs.manifest_path, expected_manifest_sha256=inputs.manifest_sha256)
    if current != inputs or await authorize_inputs(inputs, plan) is not True:
        raise PermissionError("real_acceptance_not_authorized")
    hashes = dict(inputs.artifacts)
    if (
        plan.corpus_sha256 != hashes["corpus"]
        or plan.evidence_annotations_sha256 != hashes["gold"]
        or plan.judge_sha256 != hashes["judge"]
    ):
        raise ValueError("acceptance_protocol_input_mismatch")
    report = await run_controlled_retrieval(
        plan, arms, expected_protocol_sha256=expected_protocol_sha256
    )
    if plan.fingerprint != expected_protocol_sha256:
        raise ValueError("acceptance_protocol_changed")
    if (
        load_inputs(inputs.manifest_path, expected_manifest_sha256=inputs.manifest_sha256) != inputs
        or await authorize_inputs(inputs, plan) is not True
    ):
        raise PermissionError("real_acceptance_delivery_not_authorized")
    return report


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()
    inputs = load_inputs(args.manifest, expected_manifest_sha256=args.sha256)
    print(
        canonical(
            dict(
                schema="real-acceptance-preflight/1",
                status="inputs_verified",
                inputs_sha256=inputs.fingerprint,
                acceptance_completed=False,
                next="trusted_host_authorization_and_real_four_arm_execution",
            )
        )
    )


if __name__ == "__main__":
    main()
