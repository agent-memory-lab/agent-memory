"""Request-local span identities without changing public source memory IDs."""

import re
from collections.abc import Mapping
from hashlib import sha256
from json import dumps

from ..domain import MemoryKind

MAX_SOURCE_WINDOW_CHARS = 4096


def candidate_identity(item):
    """Different verified event spans may share their public source ``item.id``.

    Metadata alone is not an authentication boundary. The repository/governance
    layer remains responsible for source access and erasure. This helper checks
    the locator geometry before allowing it to affect request-local deduplication.
    """
    if item.kind != MemoryKind.EVENT:
        return item.id
    metadata = item.metadata
    revision = metadata.get("source_revision")
    span = metadata.get("source_span")
    total = metadata.get("source_chars")
    if not isinstance(span, Mapping) or span.get("unit") != "characters":
        return item.id
    start, end = span.get("start"), span.get("end")
    if (
        type(start) is not int
        or type(end) is not int
        or type(total) is not int
        or not 0 <= start < end <= total
        or end - start != len(item.text)
        or len(item.text) > MAX_SOURCE_WINDOW_CHARS
        or not isinstance(revision, str)
        or not re.fullmatch(r"[0-9a-f]{64}", revision)
    ):
        return item.id
    locator = metadata.get("lexical_chunk_id")
    original = span
    if locator is None:
        # Native packing narrows a retrieved chunk without changing its identity.
        locator = metadata.get("retrieved_lexical_chunk_id")
        original = metadata.get("retrieved_source_span")
        if not isinstance(original, Mapping) or original.get("unit") != "characters":
            return item.id
        lower, upper = original.get("start"), original.get("end")
        if (
            type(lower) is not int
            or type(upper) is not int
            or not 0 <= lower <= start < end <= upper <= total
            or upper - lower > MAX_SOURCE_WINDOW_CHARS
        ):
            return item.id
    expected = sha256(
        dumps(
            [
                "events",
                item.id,
                revision,
                original["start"],
                original["end"],
            ]
        ).encode()
    ).hexdigest()
    if locator != expected:
        return item.id
    return "source-span:" + expected
