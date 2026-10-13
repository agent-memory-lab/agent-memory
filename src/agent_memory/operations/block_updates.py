"""Resolve optional block dependencies inside the repository CAS transaction."""

from dataclasses import replace

from ..domain import MemoryBlock


def resolve_block_dependencies(block: MemoryBlock, current: MemoryBlock | None) -> MemoryBlock:
    """Omitted dependencies preserve the current set; a nonempty set replaces it.

    An empty array cannot strip existing claim provenance. A block with unrelated
    event-only content must be created under a new identity instead.
    """
    previous = (current.source_claim_ids or ()) if current else ()
    if block.source_claim_ids is None:
        return replace(block, source_claim_ids=previous)
    if previous and not block.source_claim_ids:
        raise ValueError("source_claim_ids cannot clear existing claim dependencies")
    return block
