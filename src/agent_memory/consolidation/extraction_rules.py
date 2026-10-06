"""Offline reference adapters for a deliberately small, whole-message grammar.

These rules establish what a message says, not whether the speaker tells the
truth. Unknown syntax abstains. They never search inside quotes or split away
conditions, attribution, negation, or temporal qualifiers.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from hashlib import sha256

from ..domain import AtomReview, ExtractedAtom, MemoryEvent, canonical_json

_CITIES = {
    "杭州": "Hangzhou",
    "上海": "Shanghai",
    "北京": "Beijing",
    "hangzhou": "Hangzhou",
    "shanghai": "Shanghai",
    "beijing": "Beijing",
}
_LANGUAGES = {"中文": "zh", "英文": "en", "chinese": "zh", "english": "en"}


def _statement(content: str):
    text = content.strip().rstrip("。.!！").strip()
    city = "(" + "|".join(_CITIES) + ")"
    language = "(" + "|".join(_LANGUAGES) + ")"
    rules = (
        (rf"我(?:目前)?住在{city}", "home_city", "fact", "asserted", "durable", _CITIES),
        (rf"I (?:currently )?live in {city}", "home_city", "fact", "asserted", "durable", _CITIES),
        (rf"我不住在{city}", "home_city", "fact", "negated", "transient", _CITIES),
        (
            rf"I (?:do not|don't) live in {city}",
            "home_city",
            "fact",
            "negated",
            "transient",
            _CITIES,
        ),
        (
            rf"我(?:计划|可能)(?:下周)?搬到{city}",
            "home_city",
            "fact",
            "planned",
            "transient",
            _CITIES,
        ),
        (
            rf"I (?:might|plan to) move to {city}(?: next week)?",
            "home_city",
            "fact",
            "planned",
            "transient",
            _CITIES,
        ),
        (
            rf"(?:以后|始终)(?:请)?用{language}回答",
            "response_language",
            "preference",
            "asserted",
            "durable",
            _LANGUAGES,
        ),
        (
            rf"Always answer in {language}",
            "response_language",
            "preference",
            "asserted",
            "durable",
            _LANGUAGES,
        ),
        (
            rf"(?:这次|今天)(?:请)?用{language}回答",
            "response_language",
            "preference",
            "asserted",
            "transient",
            _LANGUAGES,
        ),
        (
            rf"For this reply, (?:please )?answer in {language}",
            "response_language",
            "preference",
            "asserted",
            "transient",
            _LANGUAGES,
        ),
        (
            r"(?:以后|始终)(?:请)?(?:简洁回答|回答简洁)",
            "response_style",
            "preference",
            "asserted",
            "durable",
            None,
        ),
        (
            r"Always (?:be concise|give concise answers)",
            "response_style",
            "preference",
            "asserted",
            "durable",
            None,
        ),
        (r"请简洁回答", "response_style", "preference", "asserted", "session", None),
        (r"Please be concise", "response_style", "preference", "asserted", "session", None),
    )
    for pattern, predicate, kind, modality, retention, aliases in rules:
        match = re.fullmatch(pattern, text, re.IGNORECASE)
        if match:
            value = aliases[match[1].lower()] if aliases else "concise"
            return dict(predicate=predicate, value=value, kind=kind, modality=modality), retention
    return None


class RuleBasedAtomAdapter:
    """Generator and reviewer for explicit residence and response preferences.

    Bind the first-person speaker to a host-authenticated subject. The independent
    review pass reparses the entire source and compares every semantic field.
    Sharing this grammar is an offline reference, not independent model consensus.
    """

    def __init__(self, subject_id: str) -> None:
        if not isinstance(subject_id, str) or not subject_id.strip() or len(subject_id) > 256:
            raise ValueError("subject_id must be a bounded nonempty string")
        self.subject_id = subject_id

    @property
    def version(self) -> str:
        return "whole-message-rules-v1:" + sha256(self.subject_id.encode()).hexdigest()

    async def generate_atoms(self, event: MemoryEvent):
        parsed = _statement(event.content)
        if parsed is None:
            return ()
        statement, _ = parsed
        return (
            {
                **statement,
                "subject_id": self.subject_id,
                "source_quote": event.content,
                "source_start": 0,
                "source_end": len(event.content),
            },
        )

    async def review_atoms(
        self,
        event: MemoryEvent,
        candidates: Sequence[ExtractedAtom],
    ) -> tuple[AtomReview, ...]:
        parsed = _statement(event.content)
        results = []
        for index, candidate in enumerate(candidates):
            if parsed is None:
                results.append(
                    AtomReview(index, "uncertain", "uncertain", ("outside_reference_grammar",))
                )
                continue
            statement, retention = parsed
            draft = candidate.draft
            expected = {
                **statement,
                "subject_id": self.subject_id,
                "valid_from": None,
                "valid_to": None,
                "source_quote": event.content,
            }
            observed = {name: getattr(draft, name) for name in expected}
            supported = canonical_json(expected) == canonical_json(observed)
            results.append(
                AtomReview(
                    index,
                    "supported" if supported else "unsupported",
                    retention,
                    (
                        "whole_source_statement_matches"
                        if supported
                        else "semantic_fields_mismatch",
                    ),
                )
            )
        return tuple(results)
