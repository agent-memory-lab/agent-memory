"""Request-local span identities without changing public source memory IDs."""

from ..domain import Claim, MemoryItem, MemoryKind
from .evidence import MAX_SOURCE_WINDOW_CHARS as MAX_SOURCE_WINDOW_CHARS
from .evidence import SourceEvidence


def candidate_identity(item):
    """Validate locator geometry; repository/governance still owns authorization."""
    if item.kind != MemoryKind.EVENT:
        return item.id
    evidence = SourceEvidence.from_item(item, require_revision=True)
    return (evidence.indexed_identity(item.metadata) if evidence is not None else None) or item.id


SOURCE_PROOF_FIELDS = ("source_span", "source_revision", "source_chars")


def citation_proof(value: Claim | MemoryItem, sources: tuple[str, ...]) -> dict:
    """Encode the same evidence reference at selection and final authorization."""
    proof = {"memory_id": value.id, "candidate_id": value.id, "source_event_ids": sources}
    if isinstance(value, MemoryItem):
        proof["candidate_id"] = candidate_identity(value)
        proof.update(
            {key: value.metadata[key] for key in SOURCE_PROOF_FIELDS if key in value.metadata}
        )
    return proof
