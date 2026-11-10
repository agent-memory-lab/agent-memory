"""Opt-in, abstention-only protection for source-actor attribution.

This is a finite risk detector, not a quotation parser or entity resolver. It
never identifies a quoted speaker. Any recognized quote/reporting cue taints the
whole source, so a generated substring cannot hide the surrounding attribution.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

from ..domain import ExtractedAtom, MemoryEvent, MemoryScope

# Fixed language-level cues, not names or patterns derived from an evaluation
# document. Missing a cue is not evidence of direct speech or real-world truth.
_REPORTING = re.compile(
    r"\b(?:say|says|said|saying|tell|tells|told|telling|write|writes|wrote|writing|"
    r"ask|asks|asked|asking|reply|replies|replied|replying|quote|quotes|quoted|quoting)\b"
    r"|说|告[诉訴]|表示|提到|写道|寫道|问|問|回复|回覆|声称|聲稱|转述|轉述",
    re.IGNORECASE,
)
_QUOTES = frozenset('"“”„‟«»‹›「」『』`')


def attribution_risks(content: str) -> tuple[str, ...]:
    """Return finite, observable reasons to abstain; no reasons is not approval."""
    if not isinstance(content, str) or len(content) > 32_000:
        raise ValueError("attribution source must be a string of at most 32000 characters")
    reasons = []
    if any(char in _QUOTES for char in content) or any(
        char in "'‘’" and not (
            index > 0 and index + 1 < len(content)
            and content[index - 1].isalnum() and content[index + 1].isalnum()
        )
        for index, char in enumerate(content)
    ):
        reasons.append("quoted_source_attribution_unknown")
    if _REPORTING.search(content):
        reasons.append("reported_source_attribution_unknown")
    return tuple(reasons)


@dataclass(frozen=True, slots=True)
class ConservativeAttributionGuard:
    """Host-bound, self-report-only guard for an existing extraction pipeline.

    Does not grant source authority, prove identity, approve an alias, or accept
    a fact. Other subjects and recognized attributed speech require another
    verification path. Enable explicitly; the default extraction path is intact.
    """

    scope: MemoryScope
    primary_subject_id: str
    version = "conservative-source-attribution-v1"

    def __post_init__(self) -> None:
        if not isinstance(self.scope, MemoryScope):
            raise TypeError("attribution scope must be a MemoryScope")
        if (
            not isinstance(self.primary_subject_id, str)
            or not self.primary_subject_id.strip()
            or len(self.primary_subject_id) > 256
            or self.primary_subject_id != self.primary_subject_id.strip()
        ):
            raise ValueError("attribution subject must be a bounded exact identity")

    def config_payload(self) -> dict:
        return {"version": self.version, **asdict(self)}

    def validate_event(self, event: MemoryEvent) -> None:
        if event.scope != self.scope or event.actor != self.primary_subject_id:
            raise ValueError("attribution guard scope or source actor mismatch")

    def reasons(self, event: MemoryEvent, candidate: ExtractedAtom) -> tuple[str, ...]:
        self.validate_event(event)
        reasons = attribution_risks(event.content)
        if candidate.draft.subject_id != self.primary_subject_id:
            reasons += ("subject_identity_not_bound_to_source_actor",)
        return reasons
