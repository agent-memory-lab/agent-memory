"""Explicit, bounded source-month interpretation for approximate month questions.

A recent first-person report can be used as a *declared proxy*, never an exact
event date. This does not establish semantic entailment or source authority; the
answer auditor must still check who reported what and the requested event.
"""

import calendar
import re
from copy import deepcopy
from datetime import date, datetime

POLICY = "source-month-precision/1"
_RECENT_REPORT = re.compile(
    r"\bI(?:[\x27’](?:ve|d))?\s+(?:(?:have|had)\s+)?just\s+(?:completed|finished|graduated)\b"
    r"|我(?:才)?刚(?:刚|才)?(?:完成|结束|毕业)",
    re.IGNORECASE,
)
_EXPLICIT_DATE = re.compile(r"\b(?:19|20)\d{2}\b|\d{1,2}[/-]\d{1,2}|\d+[年月日]")


def _month_bounds(low, high):
    try:
        start, end = date.fromisoformat(low), date.fromisoformat(high)
    except (TypeError, ValueError):
        return False
    return (
        start.isoformat() == low
        and end.isoformat() == high
        and start.day == 1
        and (start.year, start.month) == (end.year, end.month)
        and end.day == calendar.monthrange(start.year, start.month)[1]
    )


def _anchor(operand, quotes, byid):
    anchors, eids = set(), set()
    ranges = {}
    for index in operand["supports"]:
        if type(index) is not int or not 0 <= index < len(quotes):
            return None
        quote = quotes[index]
        block = byid[quote["evidence_id"]]
        start = quote.get("character_start")
        if (
            type(start) is not int
            or start < 0
            or block["text"][start : start + len(quote["text"])] != quote["text"]
        ):
            return None
        ranges.setdefault(quote["evidence_id"], []).append((start, start + len(quote["text"])))
        if block["record_kind"] != "event":
            return None  # A summary is not a new raw temporal anchor.
        details = block["source_details"]
        if not isinstance(details, (tuple, list)) or not details:
            return None
        for detail in details:
            if type(detail) is not dict:
                return None
            roles = detail.get("roles")
            if roles is not None and (not isinstance(roles, (list, tuple)) or "user" not in roles):
                return None
            try:
                instant = datetime.fromisoformat(detail["occurred_at"])
            except (ValueError, TypeError, KeyError):
                return None
            if instant.tzinfo is None:
                return None
            anchors.add((instant.date(), int(instant.utcoffset().total_seconds())))
        eids.add(quote["evidence_id"])
    # Token/quote boundaries must not change temporal meaning. Rejoin only
    # overlapping or adjacent bound spans, never gaps or separate evidence IDs.
    passages = []
    for eid, spans in ranges.items():
        spans.sort()
        low, high = spans[0]
        for start, end in spans[1:]:
            if start <= high:
                high = max(high, end)
            else:
                passages.append(byid[eid]["text"][low:high])
                low, high = start, end
        passages.append(byid[eid]["text"][low:high])
    recent = any(_RECENT_REPORT.search(text) for text in passages)
    if any(_EXPLICIT_DATE.search(text) for text in passages):
        return None
    if not recent or len(anchors) != 1:
        return None
    source_date, offset = next(iter(anchors))
    if operand["value"][:7] != source_date.isoformat()[:7]:
        return None
    return {
        "source_month": source_date.isoformat()[:7],
        "source_calendar_date": source_date.isoformat(),
        "source_offset_seconds": offset,
        "evidence_ids": sorted(eids),
    }


def resolve_temporal_precision(operation, quotes, blocks, *, allow_source_month_proxy=True):
    """Return normalized operands and visible assumptions, without altering raw JSON.

    Legacy full-month operands are recognized only with a bound recent report,
    so old generated plans can be diagnosed without relabeling old outcomes.
    Explicit model declarations must meet the same host checks.
    """
    if type(allow_source_month_proxy) is not bool:
        raise ValueError("invalid_temporal_policy")
    resolved = deepcopy(operation)
    metadata = {
        "policy": POLICY,
        "allow_source_month_proxy": allow_source_month_proxy,
        "assumptions": [],
        "entailment_verified": False,
    }
    if type(operation) is not dict or operation.get("kind") != "elapsed_months":
        return resolved, metadata
    operands = resolved.get("operands")
    if type(operands) is not list or len(operands) != 2:
        return resolved, metadata  # Structural errors are handled by arithmetic.
    byid = {block["evidence_id"]: block for block in blocks}
    for ordinal, operand in enumerate(operands):
        if type(operand) is not dict:
            continue
        declared = operand.get("basis") == "source_month_proxy"
        if not _month_bounds(operand.get("value"), operand.get("end_value")):
            if declared:
                raise ValueError("source_month_proxy_not_grounded")
            continue
        supports = operand.get("supports")
        if type(supports) is not list or not supports:
            continue
        proof = _anchor(operand, quotes, byid)
        if proof is None:
            if declared:
                raise ValueError("source_month_proxy_not_grounded")
            continue
        if not allow_source_month_proxy:
            raise ValueError("source_month_proxy_disabled")
        operand["basis"] = "source_month_proxy"
        metadata["assumptions"].append(
            {"operand_index": ordinal, "basis": "source_month_proxy", **proof}
        )
    return resolved, metadata
