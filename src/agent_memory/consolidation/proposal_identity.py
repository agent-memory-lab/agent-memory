"""Pure identity checks shared by native proposal idempotency stores."""

from collections.abc import Mapping
from dataclasses import asdict, fields
from datetime import UTC, datetime

from ..domain import MemoryProposal, MemoryScope, canonical_json


def validate_proposal_replay(
    proposal: MemoryProposal, stored_scope: MemoryScope, payload: Mapping[str, object],
    *, serialized: Mapping[str, object] | None = None,
) -> None:
    """Bind an idempotent result to its exact owner and request, without exposing it."""
    if stored_scope != proposal.scope:
        raise ValueError("proposal ID is already used for a different request")
    if payload.get("erased") is True:
        raise ValueError("proposal was forgotten and cannot be replayed")

    stored = dict(payload)
    incoming = dict(serialized) if serialized is not None else asdict(proposal)
    # A caller may reconstruct the same request later. Creation time is audit
    # metadata, not a change to the operation. Fields added to legacy payloads
    # retain their dataclass defaults; mandatory identity fields must be present.
    for field in fields(MemoryProposal):
        if field.name not in stored and field.name in {
            "operation", "valid_from", "reason",
        }:
            stored[field.name] = field.default
    for candidate in (stored, incoming):
        candidate.pop("created_at", None)
        valid_from = candidate.get("valid_from")
        if valid_from is not None:
            if isinstance(valid_from, str):
                valid_from = datetime.fromisoformat(valid_from)
            if not isinstance(valid_from, datetime) or valid_from.utcoffset() is None:
                raise ValueError("proposal ID is already used for a different request")
            candidate["valid_from"] = valid_from.astimezone(UTC).isoformat()
    if canonical_json(stored) != canonical_json(incoming):
        raise ValueError("proposal ID is already used for a different request")
