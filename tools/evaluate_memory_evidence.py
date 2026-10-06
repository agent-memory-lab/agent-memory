"""Score exported evidence observations; optionally apply a frozen quality gate.

This command does not run Hindsight, an LLM, or an end-to-end memory adapter.
Observations and independent answer judgments must come from the measured run.
"""

from __future__ import annotations

import argparse
import json
from hashlib import sha256
from pathlib import Path

from agent_memory.evaluation.acceptance import (
    acceptance_profile_from_dict,
    evaluate_quality_gate,
    evidence_metrics,
)
from agent_memory.evaluation.evidence import (
    EvidenceObservation,
    evaluate_evidence,
    evidence_dataset_from_dict,
    evidence_report_from_dict,
)
from agent_memory.serialization import to_jsonable


def _load(path: Path):
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("evaluation input exceeds 32 MiB")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--run-configuration", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--expected-profile-sha256")
    args = parser.parse_args()
    gating = (args.profile, args.baseline, args.expected_profile_sha256)
    if any(value is not None for value in gating) and not all(
        value is not None for value in gating
    ):
        parser.error(
            "--profile, --baseline and --expected-profile-sha256 must be provided together"
        )
    try:
        dataset = evidence_dataset_from_dict(_load(args.dataset))
        raw = _load(args.observations)
        if not isinstance(raw, dict) or len(raw) > 10_000:
            raise ValueError("observations must be a bounded case-ID mapping")
        observations = {key: EvidenceObservation(**value) for key, value in raw.items()}
        _load(args.run_configuration)
        config_hash = sha256(args.run_configuration.read_bytes()).hexdigest()
        report = evaluate_evidence(dataset, observations, run_configuration_sha256=config_hash)
        gate = None
        if args.profile is not None:
            baseline_payload = _load(args.baseline)
            # Accept either the report itself or this command's output envelope.
            baseline = evidence_report_from_dict(baseline_payload.get("report", baseline_payload))
            gate = evaluate_quality_gate(
                acceptance_profile_from_dict(_load(args.profile)),
                report,
                baseline,
                expected_profile_sha256=args.expected_profile_sha256,
            )
        result = {
            "schema": "agent-memory-evidence-run/1",
            "report": to_jsonable(report),
            "metrics": evidence_metrics(report),
            "quality_gate": to_jsonable(gate),
            "production_ready": False,
            "notice": "Scored supplied observations only; operational release checks are separate.",
        }
        output = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        if args.output:
            args.output.write_text(output, encoding="utf-8")
        else:
            print(output, end="")
        return 0 if gate is None or gate.ready else 1
    except (ValueError, TypeError, OSError, KeyError, AttributeError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
