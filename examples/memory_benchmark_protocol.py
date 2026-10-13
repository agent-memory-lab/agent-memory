"""Pure scoring and evidence bounds for the optional local comparison runner."""

from __future__ import annotations

import asyncio
import json
from hashlib import sha256
from time import perf_counter


class BenchmarkTimeout(RuntimeError):
    """A timed-out operation has settled; the experiment must stop."""


async def require_model_accounting(proxy, state, timeout):
    """Fail closed before advancing to another arm if accounting has not settled."""
    state["accounting_complete"] = await asyncio.to_thread(proxy.drain, timeout)
    state["pending_model_requests"] = proxy.pending_requests
    if not state["accounting_complete"]:
        state["status"] = "aborted"
        state["error_type"] = "IncompleteModelAccounting"
        raise BenchmarkTimeout("unfinished_model_accounting")
    return True


async def checked_call(ledger, row, operation, call, timeout):
    ledger.start_phase(row["trial"], row["name"], operation)
    started = perf_counter()
    task = asyncio.ensure_future(call)
    try:
        result = await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except (TimeoutError, asyncio.CancelledError) as error:
        # Cancelling to_thread does not stop synchronous SDK writes. Keep this
        # phase fixed and wait for the bounded HTTP request to actually settle.
        try:
            await task
        except Exception:
            pass
        row["operations"].append(
            {
                "operation": operation,
                "status": "failed",
                "seconds": perf_counter() - started,
                "error_type": type(error).__name__,
            }
        )
        if isinstance(error, asyncio.CancelledError):
            raise
        raise BenchmarkTimeout(operation) from error
    except Exception as error:
        row["operations"].append(
            {
                "operation": operation,
                "status": "failed",
                "seconds": perf_counter() - started,
                "error_type": type(error).__name__,
            }
        )
        raise
    row["operations"].append(
        {
            "operation": operation,
            "status": "completed",
            "seconds": perf_counter() - started,
        }
    )
    return result


def canonical_digest(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def bounded_evidence(items, *, limit=8, byte_budget=8192):
    """Use one UTF-8 byte budget for every arm; this is not a token count."""
    if limit < 1 or byte_budget < 1:
        raise ValueError("positive evidence bounds required")
    parts = []
    remaining = byte_budget
    for item in items[:limit]:
        text = item["text"]
        if not isinstance(text, str):
            raise ValueError("memory text must be a string")
        prefix = "\n- " if parts else "- "
        raw = (prefix + text).encode()
        if len(raw) > remaining:
            raw = raw[:remaining]
        part = raw.decode("utf-8", errors="ignore")
        if part:
            parts.append(part)
            remaining -= len(part.encode())
        if len(raw) == 0 or remaining == 0 or len(raw) < len((prefix + text).encode()):
            break
    return "".join(parts)


def alias_hit(text, groups):
    normalized = text.casefold()
    return all(any(alias.casefold() in normalized for alias in group) for group in groups)


def grade_answer(spec, response):
    """Strict diagnostic rubric, never passed to extraction/retrieval/answering."""
    if not isinstance(response, dict):
        return {"passed": False, "reason": "invalid_answer_schema"}
    text = response.get("answer")
    status = response.get("status")
    if not isinstance(text, str) or status not in ("answered", "unknown"):
        return {"passed": False, "reason": "invalid_answer_schema"}
    if spec.get("unanswerable", False):
        # Requiring both status and a short abstention prevents an answer with
        # invented facts and an appended 'unknown' from passing the rubric.
        clean = text.strip().rstrip("。.!！").casefold()
        passed = status == "unknown" and clean in (
            "未知",
            "不知道",
            "无法确定",
            "信息不足",
            "unknown",
            "i don't know",
        )
        return {"passed": passed, "reason": "abstention" if passed else "unsupported_answer"}
    if status != "answered" or not alias_hit(text, spec["required_aliases"]):
        return {"passed": False, "reason": "missing_required_answer"}
    if any(alias.casefold() in text.casefold() for alias in spec.get("forbidden_aliases", ())):
        return {"passed": False, "reason": "obsolete_value_in_current_only_answer"}
    return {"passed": True, "reason": "required_aliases_present"}


def summarize_cases(cases, *, planned):
    if planned < 0 or len(cases) > planned:
        raise ValueError("invalid planned denominator")
    correct = sum(case.get("grade", {}).get("passed", False) for case in cases)
    return {
        "planned": planned,
        "completed": len(cases),
        "correct": correct,
        "failed_or_missing": planned - len(cases),
        "correct_rate_all_planned": correct / planned if planned else None,
    }
