"""Read-only, hash-pinned local authoritative records for domain verification.

The host authenticates and approves the file and issuer. Matching JSON does not
make an untrusted file authoritative. No tool registry is selected by a model.
"""

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from ..domain import MemoryEvent, utc_now
from ..operations.domain_verification import VerificationFinding, VerificationToolSpec, engine_scope
from ..retrieval.model_contracts import canonical, digest


class LocalRecordVerifier:
    def __init__(self, path, *, expected_sha256, spec, clock=utc_now):
        if type(spec) is not VerificationToolSpec:
            raise ValueError("trusted tool registration required")
        self.path = Path(path).resolve(strict=True)
        self.expected_sha256, self.spec, self.clock = expected_sha256, spec, clock
        self._records()

    def _records(self):
        if self.path.stat().st_size > 4_194_304:
            raise ValueError("authoritative record file capacity exceeded")
        data = self.path.read_bytes()
        if sha256(data).hexdigest() != self.expected_sha256:
            raise ValueError("authoritative records changed")
        document = json.loads(data)
        if (
            document.get("schema") != "authoritative-domain-records/1"
            or document.get("issuer") != self.spec.authority.source_id
        ):
            raise ValueError("authoritative record issuer/schema mismatch")
        records = document.get("records")
        if not isinstance(records, list) or len(records) > 4096:
            raise ValueError("invalid authoritative record corpus")
        return records

    async def verify(self, candidate):
        draft = candidate["payload"]["draft"]
        if (
            draft.get("conditions")
            or draft.get("exceptions")
            or draft.get("negated", False)
            or draft.get("modality", "asserted") != "asserted"
        ):
            return VerificationFinding("unknown")
        start = datetime.fromisoformat(candidate["payload"]["valid_from"])
        end = (
            datetime.fromisoformat(candidate["payload"]["valid_to"])
            if candidate["payload"].get("valid_to")
            else None
        )
        matches = []
        for row in self._records():
            if (
                row.get("subject_id") != draft["subject_id"]
                or row.get("predicate") != draft["predicate"]
            ):
                continue
            known = datetime.fromisoformat(row["recorded_at"])
            lower = datetime.fromisoformat(row["valid_from"])
            upper = datetime.fromisoformat(row["valid_to"]) if row.get("valid_to") else None
            if any(
                value.utcoffset() is None for value in (known, lower, *((upper,) if upper else ()))
            ):
                raise ValueError("authoritative times need timezone")
            # Do not expand a point/finite proof into a candidate's open interval.
            if (
                known <= self.clock()
                and lower <= start
                and (upper is None or (end is not None and end <= upper))
            ):
                matches.append((row, known, lower, upper))
        if len(matches) != 1:
            return VerificationFinding("unknown")
        row, known, lower, upper = matches[0]
        body = canonical(row)
        event = MemoryEvent(
            engine_scope(candidate),
            "memory.domain-verification",
            body,
            id="verification-evidence:" + digest([self.expected_sha256, row]),
            occurred_at=known,
            metadata={
                "lifecycle": {
                    "origin": "tool" if self.spec.authority.kind == "tool_observation" else "host"
                }
            },
        )
        return VerificationFinding(
            "supported" if row["value"] == draft["value"] else "refuted",
            event,
            body,
            lower,
            upper,
            ("subject_id", "predicate", "value", "valid_from"),
        )
