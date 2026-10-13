"""Gold-free LongMemEval extraction contract and body-free outcome accounting.

This module imports no model SDK, so its real contract can be tested offline.
"""

from __future__ import annotations

import json

from agent_memory.extraction_result import (
    ExtractionDiagnostic,
    ExtractionFormatError,
    ExtractionRejection,
)

MODEL = "qwen3.5:9b"
MAX_CLAIMS = 8
NUM_CTX = 32768
EXTRACTION_PROMPT = (
    "Extract at most eight concise facts useful to answer future questions from this conversation "
    "segment. Include user or assistant facts, named entities, dates, preferences, plans, and explicit "
    "corrections. Skip general advice and filler. Use stable keys for facts that may be corrected later. "
    "Each fact needs key, value, one short factual text, and numeric confidence between 0 and 1. "
    "Confidence is your uncertain extraction score, not verified truth; do not automatically use 1. "
    "Return an empty claims array if there are no useful facts. Do not invent anything."
    "\n\nConversation segment:\n"
)


CLAIM_SCHEMA = {
    "type": "object",
    "properties": {
        "claims": {
            "type": "array",
            "maxItems": MAX_CLAIMS,
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


def extraction_options(num_ctx: int = NUM_CTX):
    """Use the same decoding settings for model calls and frozen manifests."""
    return {"temperature": 0, "num_predict": 1536, "num_ctx": num_ctx}


def usage_receipt(response, phase):
    # Missing provider counts are unknown, never inferred as zero or priced free.
    return {"phase": phase, **{
        key: getattr(response, key, None)
        for key in ("prompt_eval_count", "eval_count", "total_duration")
    }}


class BenchmarkClaimFormatError(ExtractionFormatError):
    """A strict schema failure with only body-free, typed diagnostic details."""

    def __init__(self, diagnostic, message=None):
        super().__init__(diagnostic)
        self.args = (message or "Qwen claim output violates the legacy scored-claim schema",)


def _validate_claims(payload):
    if not isinstance(payload, dict) or set(payload) != {"claims"}:
        raise BenchmarkClaimFormatError(ExtractionDiagnostic(
            "error", rejections=(ExtractionRejection(None, "invalid_output"),),
            error_type="BenchmarkClaimFormatError",
        ))
    claims = payload["claims"]
    if not isinstance(claims, list):
        raise BenchmarkClaimFormatError(ExtractionDiagnostic(
            "error", rejections=(ExtractionRejection(None, "invalid_output"),),
            error_type="BenchmarkClaimFormatError",
        ), "Qwen claim output must contain a claims array")
    if len(claims) > MAX_CLAIMS:
        raise BenchmarkClaimFormatError(ExtractionDiagnostic(
            "error", len(claims), rejections=(
                ExtractionRejection(None, "candidate_limit", len(claims) - MAX_CLAIMS),
                ExtractionRejection(None, "batch_aborted", MAX_CLAIMS),
            ), error_type="BenchmarkClaimFormatError",
        ), "Qwen claim output exceeds the frozen claim limit")
    rejected = []
    for index, claim in enumerate(claims):
        reason = None
        if not isinstance(claim, dict):
            reason = "invalid_candidate"
        elif "confidence" not in claim:
            reason = "missing_confidence"
        elif "value" not in claim:
            reason = "missing_value"
        elif set(claim) - {"key", "value", "text", "confidence"}:
            reason = "unsupported_fields"
        elif any(not isinstance(claim.get(key), str) or not claim[key].strip()
                 for key in ("key", "value", "text")):
            reason = "invalid_candidate"
        elif type(claim["confidence"]) not in (int, float) or not 0 <= claim["confidence"] <= 1:
            reason = "invalid_confidence"
        if reason is not None:
            rejected.append(ExtractionRejection(index, reason))
    if rejected:
        # A malformed batch produces no claims, including its schema-valid rows.
        aborted = len(claims) - len(rejected)
        if aborted:
            rejected.append(ExtractionRejection(None, "batch_aborted", aborted))
        raise BenchmarkClaimFormatError(ExtractionDiagnostic(
            "error", len(claims), rejections=tuple(rejected),
            error_type="BenchmarkClaimFormatError",
        ))
    return claims


class BenchmarkClaimGenerator:
    """Bound extraction output so long benchmark sessions produce complete JSON."""

    def __init__(
        self, client, calls: dict[str, int], *, model: str = MODEL,
        prompt: str = EXTRACTION_PROMPT, num_ctx: int = NUM_CTX,
    ):
        self.client = client
        self.model = model
        self.prompt = prompt
        self.num_ctx = num_ctx
        self.calls = calls
        self.generated_claims = 0
        self.receipts = []

    async def generate_claims(self, event):
        self.calls["agent-memory"] += 1
        prompt = self.prompt + event.content
        response = await self.client.chat(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            format=CLAIM_SCHEMA,
            think=False,
            options=extraction_options(self.num_ctx),
        )
        self.receipts.append(usage_receipt(response, "extraction"))
        # Fail visibly before returning any malformed scored-claim batch.
        claims = _validate_claims(json.loads(response.message.content))
        self.generated_claims += len(claims)
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
