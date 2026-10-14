"""Pure provider contracts shared by local and budgeted benchmark transports.

No I/O, credentials, retry policy, model-name routing, or accounting lives here.
Metadata helpers retain only allowlisted-format identifiers, never error bodies.
"""
from __future__ import annotations

import copy
import json
import math
import re
from typing import Any, Mapping


STRUCTURED_OUTPUT_MODES = ("json_schema", "json_object")


def strict_json_loads(raw: str | bytes) -> Any:
    """Reject duplicate fields, nonfinite literals, and overflowing JSON numbers."""
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def bad_constant(_):
        raise ValueError("Nonfinite JSON number")

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("Nonfinite JSON number")
        return number

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad_constant,
                      parse_float=finite_float)


def prepare_structured_output(prompt: str, schema: dict | None, *, mode: str) -> tuple[str, dict]:
    """Build the explicitly selected format; JSON mode includes the same schema.

    Provider acceptance and local output validation are separate requirements.
    A failed request never changes this selection or triggers another request.
    """
    if mode not in STRUCTURED_OUTPUT_MODES or not isinstance(prompt, str):
        raise ValueError("Invalid structured-output configuration")
    if schema is not None and not isinstance(schema, dict):
        raise ValueError("Structured-output schema must be an object")
    if mode == "json_schema":
        if schema is None:
            raise ValueError("JSON schema mode requires an explicit schema")
        # Validate JSON representability before a transport reserves any budget.
        json.dumps(schema, allow_nan=False)
        return prompt, {"type": "json_schema", "json_schema": {
            "name": "memory_claims", "strict": True, "schema": copy.deepcopy(schema)}}
    instruction = "Return a JSON object."
    if schema is not None:
        instruction = "Return a JSON object conforming to this schema: " + json.dumps(
            schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return prompt + "\n\n" + instruction, {"type": "json_object"}


def _safe_identifier(value: Any, secrets: tuple[str, ...], *, request_id: bool = False) -> str | None:
    pattern = r"[A-Za-z0-9_.:-]{1,160}" if request_id else r"[A-Za-z][A-Za-z0-9_.-]{0,127}"
    if (not isinstance(value, str) or not re.fullmatch(pattern, value)
            or any(secret and secret in value for secret in secrets)
            or value.lower().startswith(("sk-", "sk_", "bearer"))):
        return None
    return value


def safe_response_metadata(status: int | None, document: Any, *,
                           headers: Mapping[str, Any] | None = None,
                           secrets: tuple[str, ...] = ()) -> dict[str, Any]:
    """Describe HTTP evidence without treating an error as unknown HTTP outcome.

    An HTTP status says nothing by itself about billable usage. The transport
    must separately validate usage and keep its reservation when usage is absent.
    Error categories are diagnostic labels, never permission to retry or resume.
    """
    if status is not None and (type(status) is not int or not 100 <= status <= 599):
        raise ValueError("Invalid HTTP status")
    identifiers = []
    if headers is not None:
        lowered = {name.lower(): value for name, value in headers.items() if isinstance(name, str)}
        identifiers.extend(lowered.get(name) for name in
                           ("x-request-id", "x-dashscope-request-id", "request-id"))
    if isinstance(document, dict):
        identifiers.extend(document.get(name) for name in ("request_id", "id"))
    request_id = next((safe for value in identifiers
                       if (safe := _safe_identifier(value, secrets, request_id=True)) is not None), None)
    error_code = None
    if status is not None and status != 200 and isinstance(document, dict):
        error = document.get("error")
        candidates = [error.get("code"), error.get("type")] if isinstance(error, dict) else []
        candidates.append(document.get("code"))
        error_code = next((safe for value in candidates
                          if (safe := _safe_identifier(value, secrets)) is not None), None)
    if status in (None, 200):
        category = None
    elif status == 400:
        category = "invalid_request"
    elif status == 401:
        category = "authentication"
    elif status == 403:
        category = "quota_or_billing" if error_code and "quota" in error_code.lower() else "permission"
    elif status == 429:
        category = "rate_limit_or_quota"
    elif status >= 500:
        category = "provider_service"
    elif 300 <= status < 400:
        category = "redirect_refused"
    else:
        category = "http_error"
    return {"http_outcome": "not_observed" if status is None else ("success" if status == 200 else "http_error"),
            "provider_request_id": request_id, "provider_error_code": error_code,
            "error_category": category}
