"""Canonical source coordinates shared by adapters, packing and delivery.

These values prove representation consistency, never access authorization.
Wire metadata remains backwards compatible; database delivery checks remain final.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from json import dumps
from typing import Self

from ..domain import MemoryItem

MAX_SOURCE_WINDOW_CHARS = 4096


@dataclass(frozen=True, slots=True)
class SourceSpan:
    start: int
    end: int

    def __post_init__(self) -> None:
        if (
            type(self.start) is not int
            or type(self.end) is not int
            or not 0 <= self.start <= self.end
        ):
            raise ValueError("invalid character span")

    @classmethod
    def parse(cls, value: object) -> Self:
        if not isinstance(value, Mapping) or value.get("unit") != "characters":
            raise ValueError("source span must use character offsets")
        return cls(value.get("start"), value.get("end"))

    def to_metadata(self) -> dict[str, int | str]:
        return {"start": self.start, "end": self.end, "unit": "characters"}

    def contains(self, other: Self) -> bool:
        return self.start <= other.start < other.end <= self.end


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    source_id: str
    revision: str | None
    source_chars: int
    span: SourceSpan

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ValueError("source ID must be nonempty")
        if (
            not isinstance(self.span, SourceSpan)
            or type(self.source_chars) is not int
            or self.span.start == self.span.end
            or self.span.end > self.source_chars
        ):
            raise ValueError("source span exceeds source length")
        if self.revision is not None and (
            not isinstance(self.revision, str) or not re.fullmatch(r"[0-9a-f]{64}", self.revision)
        ):
            raise ValueError("source revision must be a SHA-256 digest")

    @classmethod
    def from_item(cls, item: MemoryItem, *, require_revision: bool = False) -> Self | None:
        """Decode one wire envelope; malformed coordinates never gain an identity."""
        try:
            metadata = item.metadata
            if not isinstance(metadata, Mapping):
                return None
            value = cls(
                item.id,
                metadata.get("source_revision"),
                metadata.get("source_chars"),
                SourceSpan.parse(metadata.get("source_span")),
            )
            if value.span.end - value.span.start != len(item.text):
                return None
            if require_revision and value.revision is None:
                return None
            return value
        except (TypeError, ValueError):
            return None

    @classmethod
    def for_stored_event(cls, item: MemoryItem) -> Self | None:
        """Coordinates for native text columns and their signed 32-bit substring offsets."""
        evidence = cls.from_item(item, require_revision=True)
        if evidence is None or evidence.source_chars > 2**31 - 1:
            return None
        return evidence

    def indexed_identity(self, metadata: Mapping, *, allow_retrieved: bool = True) -> str | None:
        if self.revision is None or self.span.end - self.span.start > MAX_SOURCE_WINDOW_CHARS:
            return None
        locator = metadata.get("lexical_chunk_id")
        original = self.span
        if locator is None:
            if not allow_retrieved:
                return None
            locator = metadata.get("retrieved_lexical_chunk_id")
            try:
                original = SourceSpan.parse(metadata.get("retrieved_source_span"))
            except (TypeError, ValueError):
                return None
            if (
                not original.contains(self.span)
                or original.end > self.source_chars
                or original.end - original.start > MAX_SOURCE_WINDOW_CHARS
            ):
                return None
        expected = sha256(
            dumps(
                [
                    "events",
                    self.source_id,
                    self.revision,
                    original.start,
                    original.end,
                ]
            ).encode()
        ).hexdigest()
        return "source-span:" + expected if locator == expected else None

    def matches_source(self, text: str, source: str) -> bool:
        return (
            len(source) == self.source_chars
            and (self.revision is None or sha256(source.encode()).hexdigest() == self.revision)
            and source[self.span.start : self.span.end] == text
        )


def complete_source_metadata_matches(item: MemoryItem) -> bool:
    """Check optional metadata on a whole record before comparing its actual text."""
    revision = item.metadata.get("source_revision")
    size = item.metadata.get("source_chars", len(item.text))
    return (
        type(size) is int
        and size == len(item.text)
        and (revision is None or revision == sha256(item.text.encode()).hexdigest())
    )


def text_matches_source(item: MemoryItem, source: str) -> bool:
    if item.metadata.get("source_span") is None:
        return item.text == source and complete_source_metadata_matches(item)
    evidence = SourceEvidence.from_item(item)
    return evidence is not None and evidence.matches_source(item.text, source)
