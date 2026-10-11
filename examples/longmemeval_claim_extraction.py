"""Gold-free LongMemEval extraction contract and body-free outcome accounting.

This module imports no model SDK, so its real contract can be tested offline.
"""

from __future__ import annotations

import json

CLAIM_SCHEMA = {
    "type": "object",
    "properties": {
        "claims": {
            "type": "array",
            "maxItems": 8,
            "items": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                    "text": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                },
                "required": ["key", "value", "text", "confidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["claims"],
    "additionalProperties": False,
}


class BenchmarkClaimGenerator:
    """Bound extraction output so long benchmark sessions produce complete JSON."""

    def __init__(self, client, calls: dict[str, int], *, model: str):
        self.client = client
        self.model = model
        self.calls = calls

    async def generate_claims(self, event):
        self.calls["agent-memory"] += 1
        prompt = (
            "Extract at most eight concise facts useful to answer future questions from this "
            "conversation segment. Include user or assistant facts, named entities, dates, "
            "preferences, plans, and explicit corrections. Skip general advice and filler. "
            "Use stable keys for facts that may be corrected later. "
            "Each fact needs key, value, one short factual text, and explicit numeric confidence "
            "between 0 and 1 describing how clearly the conversation supports the extraction. "
            "This score does not establish independent factual verification. "
            "Return an empty claims array if there are "
            "no useful facts. Do not invent anything.\n\nConversation segment:\n"
            f"{event.content}"
        )
        response = await self.client.chat(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            format=CLAIM_SCHEMA,
            think=False,
            options={"temperature": 0, "num_predict": 1536, "num_ctx": 32768},
        )
        claims = json.loads(response.message.content)["claims"]
        if not isinstance(claims, list):
            raise ValueError("Qwen claim output must contain a claims array")
        return claims


def record_extraction(summary, result):
    """Accumulate capture diagnostics without claiming unknown reports are empty."""
    diagnostic = getattr(result, "extraction", None)
    status = diagnostic.status if diagnostic is not None else "unreported"
    counts = summary.setdefault("status_counts", {})
    counts[status] = counts.get(status, 0) + 1
    summary["events"] = summary.get("events", 0) + 1
    if diagnostic is None or diagnostic.candidate_count is None:
        summary["unknown_candidate_count_events"] = (
            summary.get("unknown_candidate_count_events", 0) + 1
        )
    else:
        summary["candidate_count"] = summary.get("candidate_count", 0) + diagnostic.candidate_count
    if diagnostic is not None:
        summary["accepted_count"] = summary.get("accepted_count", 0) + diagnostic.accepted_count
        rejections = summary.setdefault("rejection_counts", {})
        for rejection in diagnostic.rejections:
            rejections[rejection.reason] = rejections.get(rejection.reason, 0) + rejection.count
