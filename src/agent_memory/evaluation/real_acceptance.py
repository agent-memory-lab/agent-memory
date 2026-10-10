"""Pinned external acceptance inputs and explicit host authorization.

File shape/hashes cannot prove a license or gold independence. Those are separate
host checks. Execution delegates to existing retrieval or whole-workflow A9
evaluators; missing prices stay unknown and prevent unsupported benefit claims.
"""

import argparse
import json
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path

from ..retrieval.model_contracts import canonical, digest, hash_value
from .controlled_retrieval import run_controlled_retrieval
from .evidence import _digest
from .question_cost import AnswerOutcome
from .question_experiment import ExperimentArm, ExperimentContrast, ExperimentPlan, run_experiment

ARTIFACTS = ("corpus", "gold", "judge", "pricing")
WORKFLOW_FEATURES = ("shared_proof", "source_omission_audit", "relation_views")
WORKFLOW_SLICES = (
    "shared_proof",
    "omission_recall",
    "omission_false_acceptance",
    "relation_derivation",
    "relation_temporal_retraction",
    "relation_source_retraction",
    "authorization_retraction",
    "erasure",
)


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
        type(doc) is not dict
        or set(doc) != required
        or doc["schema"] != "real-memory-acceptance-inputs/1"
        or doc["evidence_class"] != "real_business"
    ):
        raise ValueError("acceptance_real_input_manifest_required")
    if type(doc["artifacts"]) is not dict or set(doc["artifacts"]) != set(ARTIFACTS):
        raise ValueError("acceptance_artifacts_incomplete")
    hashes = []
    for name in ARTIFACTS:
        item = doc["artifacts"][name]
        if (
            type(item) is not dict
            or set(item) != {"path", "sha256"}
            or type(item["path"]) is not str
            or not item["path"]
        ):
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


def workflow_feature_ablations(plan, *, baseline_arm="exact_cache"):
    """Parameterize A9, retaining its four strategies and optimized baseline.

    These are host-adapter controls, not production switches. A real adapter must
    implement and attest them; the annotation-only Ollama fixture does not.
    """
    if type(plan) is not ExperimentPlan:
        raise TypeError("typed_workflow_plan_required")
    arms = []
    for arm in plan.arms:
        controls = json.loads(arm.controls_json)
        if set(controls) & set(WORKFLOW_FEATURES):
            raise ValueError("workflow_feature_controls_already_present")
        controls.update(dict.fromkeys(WORKFLOW_FEATURES, False))
        arms.append(replace(arm, controls_json=canonical(controls)))
    baseline = next((arm for arm in arms if arm.name == baseline_arm), None)
    if baseline is None or baseline.strategy != "exact_cache":
        raise ValueError("optimized_workflow_baseline_required")
    contrasts = list(plan.contrasts)
    for label, enabled in (
        *((feature, (feature,)) for feature in WORKFLOW_FEATURES),
        ("all_features", WORKFLOW_FEATURES),
    ):
        controls = json.loads(baseline.controls_json)
        controls.update(dict.fromkeys(enabled, True))
        candidate = ExperimentArm(
            f"{baseline.name}+{label}", baseline.strategy, canonical(controls)
        )
        arms.append(candidate)
        contrasts.append(
            ExperimentContrast(
                candidate.name,
                baseline.name,
                label if len(enabled) == 1 else "bundled_workflow_features",
                label if len(enabled) == 1 else None,
            )
        )
    return replace(plan, arms=tuple(arms), contrasts=tuple(contrasts))


@dataclass(frozen=True, slots=True)
class RealWorkflowAcceptancePlan:
    """Frozen input/coverage binding around A9, not a second scoring framework.

    The host authenticates preparation from the hashed raw corpus and held-out
    gold, the meaning of each coverage slice, actual control implementation,
    endpoint scope, source authority and tariffs. Labels alone prove none of it.
    """

    experiment: ExperimentPlan
    corpus_sha256: str
    gold_sha256: str
    pricing_sha256: str
    coverage: tuple[tuple[str, tuple[str, ...]], ...]
    baseline_arm: str = "exact_cache"
    schema: str = "real-memory-workflow-acceptance/1"

    def __post_init__(self):
        plan = self.experiment
        if type(plan) is not ExperimentPlan or self.schema != "real-memory-workflow-acceptance/1":
            raise ValueError("typed_real_workflow_plan_required")
        for name in ("corpus_sha256", "gold_sha256", "pricing_sha256"):
            hash_value(getattr(self, name))
        if (
            plan.dataset_kind != "licensed_real"
            or not plan.license_reference
            or plan.semantic_model.execution != "real"
            or plan.generation_model.execution == "stub"
        ):
            raise ValueError("licensed_raw_source_workflow_with_real_semantic_model_required")
        arms = {arm.name: arm for arm in plan.arms}
        baseline = arms.get(self.baseline_arm)
        if baseline is None or baseline.strategy != "exact_cache":
            raise ValueError("optimized_workflow_baseline_required")
        controls = {name: json.loads(arm.controls_json) for name, arm in arms.items()}
        if any(
            type(value.get(feature)) is not bool
            for value in controls.values()
            for feature in WORKFLOW_FEATURES
        ) or any(controls[self.baseline_arm][feature] for feature in WORKFLOW_FEATURES):
            raise ValueError("explicit_workflow_ablation_controls_required")
        for feature in WORKFLOW_FEATURES:
            if not any(
                contrast.baseline == self.baseline_arm
                and contrast.varied_control == feature
                and controls[contrast.candidate][feature]
                and arms[contrast.candidate].strategy == baseline.strategy
                for contrast in plan.contrasts
            ):
                raise ValueError("isolated_workflow_feature_contrast_required")
        if not any(
            contrast.baseline == self.baseline_arm
            and arms[contrast.candidate].strategy == baseline.strategy
            and controls[contrast.candidate]
            == {**controls[self.baseline_arm], **dict.fromkeys(WORKFLOW_FEATURES, True)}
            for contrast in plan.contrasts
        ):
            raise ValueError("combined_workflow_contrast_required")
        if (
            type(self.coverage) is not tuple
            or any(type(value) is not tuple or len(value) != 2 for value in self.coverage)
            or len(self.coverage) != len(WORKFLOW_SLICES)
            or {name for name, _ in self.coverage} != set(WORKFLOW_SLICES)
        ):
            raise ValueError("complete_workflow_coverage_required")
        requests = {item.identity: item for item in plan.items if item.is_request}
        for name, identities in self.coverage:
            if (
                type(identities) is not tuple
                or not identities
                or any(type(identity) is not str for identity in identities)
                or len(set(identities)) != len(identities)
                or not set(identities) <= requests.keys()
            ):
                raise ValueError("workflow_coverage_requests_required")
            for identity in identities:
                gold = json.loads(requests[identity].gold_json or "{}")
                if type(gold.get("answerable")) is not bool or gold.get("diagnostic_only", False):
                    raise ValueError("independent_nondiagnostic_workflow_gold_required")
                if name in ("omission_recall", "relation_derivation") and not gold["answerable"]:
                    raise ValueError("positive_workflow_gold_required")
                if name == "omission_false_acceptance" and gold["answerable"]:
                    raise ValueError("negative_omission_gold_required")

    @property
    def fingerprint(self):
        return _digest(self)


async def run_real_workflow_acceptance(
    inputs,
    protocol,
    factory,
    judge,
    *,
    authorize_inputs,
    expected_protocol_sha256,
    observer_factory=None,
):
    """Run the existing gold-free A9 workflow, preserving all costs and failures.

    Coverage summaries are descriptive request-level counts, not extraction
    triple recall, confidence intervals or an independent business quality gate.
    The existing paired whole-workload cost/utility gate remains authoritative
    for its own bounded scope; feature-specific promotion is not issued here.
    """
    if (
        type(inputs) is not RealAcceptanceInputs
        or type(protocol) is not RealWorkflowAcceptancePlan
        or not callable(authorize_inputs)
        or not callable(factory)
    ):
        raise ValueError("trusted_real_workflow_inputs_required")
    if protocol.fingerprint != expected_protocol_sha256:
        raise ValueError("acceptance_protocol_changed")
    plan, hashes = protocol.experiment, dict(inputs.artifacts)
    if (
        protocol.corpus_sha256 != hashes["corpus"]
        or protocol.gold_sha256 != hashes["gold"]
        or protocol.pricing_sha256 != hashes["pricing"]
        or plan.judge_sha256 != hashes["judge"]
        or plan.license_reference != inputs.corpus_license_reference
        or plan.acceptance.calibration_reference != inputs.calibration_reference
    ):
        raise ValueError("acceptance_protocol_input_mismatch")

    async def check_authority(*, delivery=False):
        if protocol.fingerprint != expected_protocol_sha256:
            raise ValueError("acceptance_protocol_changed")
        if (
            load_inputs(inputs.manifest_path, expected_manifest_sha256=inputs.manifest_sha256)
            != inputs
            or await authorize_inputs(inputs, protocol) is not True
        ):
            raise PermissionError(
                "real_acceptance_delivery_not_authorized"
                if delivery
                else "real_acceptance_not_authorized"
            )
        if protocol.fingerprint != expected_protocol_sha256:
            raise ValueError("acceptance_protocol_changed")
        # The host may await while authenticating these inputs. Re-read all
        # artifact bytes after that boundary as well as before it; this is not
        # a filesystem lock or a substitute for governed adapter reads.
        if (
            load_inputs(inputs.manifest_path, expected_manifest_sha256=inputs.manifest_sha256)
            != inputs
        ):
            raise PermissionError(
                "real_acceptance_delivery_not_authorized"
                if delivery
                else "real_acceptance_not_authorized"
            )

    await check_authority()

    def checked_factory(execution, arm):
        adapter = factory(execution, arm)
        if getattr(adapter, "acceptance_controls_sha256", None) != digest(
            json.loads(arm.controls_json)
        ):
            raise ValueError("workflow_adapter_controls_not_attested")
        return adapter

    report = await run_experiment(
        plan,
        checked_factory,
        judge,
        expected_plan_sha256=plan.fingerprint,
        observer_factory=observer_factory,
    )
    await check_authority(delivery=True)
    slices = {}
    for arm, run in report["runs"].items():
        requests = {request.request_id: request for request in run.requests}
        slices[arm] = {}
        for name, identities in protocol.coverage:
            selected = [requests[identity] for identity in identities]
            slices[arm][name] = dict(
                requests=len(selected),
                groups=len({request.group_id for request in selected}),
                effective_answers=sum(request.effective_answer for request in selected),
                outcomes={
                    outcome.value: sum(request.outcome == outcome for request in selected)
                    for outcome in AnswerOutcome
                },
                unsafe=sum(not request.safe for request in selected),
                stale=sum(not request.fresh for request in selected),
            )
    report["real_acceptance"] = dict(
        protocol_sha256=protocol.fingerprint,
        inputs_sha256=inputs.fingerprint,
        scope="whole_workflow_cost_utility",
        coverage=slices,
        coverage_is_descriptive=True,
        feature_quality_promotion_assessed=False,
    )
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
