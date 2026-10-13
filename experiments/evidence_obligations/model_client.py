"""Bounded, fail-closed paid-model access for the evidence-obligations experiment.

This module never discovers credentials: a key must be supplied by its caller.  It
has no retry or ledger-reset operation.  The ledger is the cumulative authority
across phases and reruns; do not create a fresh ledger to work around a refusal.
Only the standard-library proxy-aware HTTPS transport makes network requests.
Tests should inject a transport instead of using real credentials or networking.

Money is represented as integer tenths of a micro-yuan, avoiding rounding down.
Input costs 8 units/token (CNY 0.8/million), output costs 27 (CNY 2.7/million).
"""

from __future__ import annotations

import copy
import fcntl
import json
import math
import os
import re
import tempfile
import threading
import time
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

# Non-secret destination supplied by the operator; no workspace-specific host is shipped.
ENDPOINT = os.environ.get(
    "EVIDENCE_MODEL_ENDPOINT",
    "https://configure-your-endpoint.invalid/compatible-mode/v1/chat/completions",
)
MODEL = "qwen3.8-flash"
TOKEN_LIMIT = 1_000_000
CNY_LIMIT = Decimal("5.00")
COST_UNITS_PER_CNY = 10_000_000
INPUT_COST_UNITS = 8
OUTPUT_COST_UNITS = 27
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_OUTPUT_TOKENS = 32_768
# One token per UTF-8 wire byte, with ample chat-template/protocol headroom.
PROTOCOL_OVERHEAD_TOKENS = 4096
PER_MESSAGE_OVERHEAD_TOKENS = 256
_PHASE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_FAILURE_REASONS = {"transport_failure", "invalid_response", "invalid_usage", "usage_overrun"}


class ClientError(RuntimeError):
    """Sanitized error; never includes credentials, request text, or response bodies."""


class BudgetExceeded(ClientError):
    """The proposed reservation would exceed a cumulative limit."""


class LedgerStopped(ClientError):
    """A previous uncertain call needs human accounting review before more spending."""


class LedgerError(ClientError):
    """The durable accounting record could not be safely read or written."""


def validate_endpoint(endpoint: str) -> str:
    """Reject unsafe destinations before persistence or credential transmission.

    Reserved ``.invalid`` hosts remain useful for injected offline transports.
    Never include the rejected value in an error: it may contain credentials.
    """
    try:
        if not isinstance(endpoint, str) or any(c.isspace() or ord(c) < 32 for c in endpoint):
            raise ValueError
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "?" in endpoint
            or "#" in endpoint
            or parsed.port == 0
        ):
            raise ValueError
    except (ValueError, TypeError):
        raise ClientError(
            "Model endpoint must be HTTPS with a host and no userinfo, query, or fragment."
        ) from None
    return endpoint


def _integer(value: Any, *, minimum: int = 0) -> bool:
    return type(value) is int and value >= minimum


def _cost(input_tokens: int, output_tokens: int) -> int:
    return input_tokens * INPUT_COST_UNITS + output_tokens * OUTPUT_COST_UNITS


def _money(units: int) -> str:
    return format(Decimal(units) / COST_UNITS_PER_CNY, ".7f")


class BudgetLedger:
    """Atomic, process-locked JSON ledger with immutable cumulative spending caps.

    Existing files are validated, never initialized over or reset.  An unresolved
    reservation (including one left by process death) blocks every later dispatch.
    There is deliberately no programmatic review, unlock, or reset method.
    ``snapshot()`` supplies sanitized receipts and charged cumulative totals.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        token_limit: int = TOKEN_LIMIT,
        cny_limit: str | Decimal = CNY_LIMIT,
    ) -> None:
        validate_endpoint(ENDPOINT)
        if not _integer(token_limit, minimum=1) or token_limit > TOKEN_LIMIT:
            raise ValueError("Token limit must be positive and no greater than 1000000.")
        try:
            money_limit = Decimal(str(cny_limit))
            scaled = money_limit * COST_UNITS_PER_CNY
            valid = (
                money_limit.is_finite()
                and 0 < money_limit <= CNY_LIMIT
                and scaled == scaled.to_integral_value()
            )
        except (InvalidOperation, ValueError, TypeError):
            valid = False
        if not valid:
            raise ValueError("CNY limit must be positive, at most 5, and exactly representable.")
        self.path = Path(path)
        self.token_limit = token_limit
        self.cost_limit_units = int(scaled)
        self._lock_path = self.path.with_name(self.path.name + ".lock")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._locked():
                if self.path.exists():
                    self._load()
                else:
                    self._write(self._empty())
        except ClientError:
            raise
        except Exception:
            raise LedgerError("Budget ledger could not be initialized.") from None

    def _empty(self) -> dict[str, Any]:
        return {
            "version": 2,
            "ledger_id": uuid.uuid4().hex,
            "endpoint": ENDPOINT,
            "model": MODEL,
            "token_limit": self.token_limit,
            "cost_limit_units": self.cost_limit_units,
            "cost_units_per_cny": COST_UNITS_PER_CNY,
            "input_cost_units": INPUT_COST_UNITS,
            "output_cost_units": OUTPUT_COST_UNITS,
            "total_calls": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_tokens": 0,
            "total_cost_units": 0,
            "total_cost_cny": "0.0000000",
            "stopped": False,
            "stop_reason": None,
            "receipts": [],
        }

    @contextmanager
    def _locked(self):
        try:
            with self._lock_path.open("a+b") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        except ClientError:
            raise
        except Exception:
            raise LedgerError(
                "Budget ledger operation failed; no further dispatch is safe."
            ) from None

    def _load(self) -> dict[str, Any]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self._validate(data)
            return data
        except Exception:
            raise LedgerError(
                "Budget ledger is invalid or unreadable; review is required."
            ) from None

    def _validate(self, data: Any) -> None:
        if type(data) is not dict:
            raise ValueError
        if not isinstance(data.get("ledger_id"), str) or not re.fullmatch(
            r"[0-9a-f]{32}", data["ledger_id"]
        ):
            raise ValueError
        expected = self._empty()
        fixed = (
            "version",
            "endpoint",
            "model",
            "token_limit",
            "cost_limit_units",
            "cost_units_per_cny",
            "input_cost_units",
            "output_cost_units",
        )
        if any(data.get(key) != expected[key] for key in fixed):
            raise ValueError
        if type(data.get("stopped")) is not bool or type(data.get("receipts")) is not list:
            raise ValueError
        if data["stopped"] != (data.get("stop_reason") in _FAILURE_REASONS):
            raise ValueError
        if not data["stopped"] and data.get("stop_reason") is not None:
            raise ValueError
        ids = set()
        for receipt in data["receipts"]:
            if type(receipt) is not dict:
                raise ValueError
            if not isinstance(receipt.get("id"), str) or receipt["id"] in ids:
                raise ValueError
            ids.add(receipt["id"])
            if not isinstance(receipt.get("phase"), str) or not _PHASE.fullmatch(receipt["phase"]):
                raise ValueError
            for field in (
                "reserved_input_tokens",
                "reserved_output_tokens",
                "charged_input_tokens",
                "charged_output_tokens",
                "charged_cost_units",
            ):
                if not _integer(receipt.get(field)):
                    raise ValueError
            reserved_in = receipt["reserved_input_tokens"]
            reserved_out = receipt["reserved_output_tokens"]
            charged_in = receipt["charged_input_tokens"]
            charged_out = receipt["charged_output_tokens"]
            if reserved_in < 1 or reserved_out < 1:
                raise ValueError
            if charged_in > reserved_in or charged_out > reserved_out:
                raise ValueError
            if receipt["charged_cost_units"] != _cost(charged_in, charged_out):
                raise ValueError
            status = receipt.get("status")
            if status not in {"reserved", "completed", "uncertain"}:
                raise ValueError
            if status == "completed":
                if receipt.get("input_tokens") != charged_in:
                    raise ValueError
                if receipt.get("output_tokens") != charged_out:
                    raise ValueError
                if receipt.get("failure_reason") is not None:
                    raise ValueError
            else:
                if (charged_in, charged_out) != (reserved_in, reserved_out):
                    raise ValueError
                if (
                    receipt.get("input_tokens") is not None
                    or receipt.get("output_tokens") is not None
                ):
                    raise ValueError
            if status == "uncertain":
                if not data["stopped"] or receipt.get("failure_reason") not in _FAILURE_REASONS:
                    raise ValueError
        recomputed = copy.deepcopy(data)
        self._totals(recomputed)
        for key in (
            "total_calls",
            "total_input_tokens",
            "total_output_tokens",
            "total_tokens",
            "total_cost_units",
            "total_cost_cny",
        ):
            if data.get(key) != recomputed[key]:
                raise ValueError
        if data["total_tokens"] > self.token_limit:
            raise ValueError
        if data["total_cost_units"] > self.cost_limit_units:
            raise ValueError

    @staticmethod
    def _totals(data: dict[str, Any]) -> None:
        receipts = data["receipts"]
        data["total_calls"] = len(receipts)
        data["total_input_tokens"] = sum(r["charged_input_tokens"] for r in receipts)
        data["total_output_tokens"] = sum(r["charged_output_tokens"] for r in receipts)
        data["total_tokens"] = data["total_input_tokens"] + data["total_output_tokens"]
        data["total_cost_units"] = _cost(data["total_input_tokens"], data["total_output_tokens"])
        data["total_cost_cny"] = _money(data["total_cost_units"])

    def _write(self, data: dict[str, Any]) -> None:
        self._totals(data)
        self._validate(data)
        temporary = None
        try:
            fd, temporary = tempfile.mkstemp(prefix=".budget-", dir=self.path.parent)
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=2, sort_keys=True, allow_nan=False)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
            temporary = None
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except Exception:
            raise LedgerError("Budget ledger could not be persisted; review is required.") from None
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def snapshot(self) -> dict[str, Any]:
        with self._locked():
            return copy.deepcopy(self._load())

    def reserve(self, input_tokens: int, output_tokens: int, phase: str) -> str:
        if not _integer(input_tokens, minimum=1) or not _integer(output_tokens, minimum=1):
            raise ValueError("Reservation token counts must be positive integers.")
        if not isinstance(phase, str) or not _PHASE.fullmatch(phase):
            raise ValueError("Phase must be a short alphanumeric accounting label.")
        with self._locked():
            data = self._load()
            if data["stopped"] or any(r["status"] != "completed" for r in data["receipts"]):
                raise LedgerStopped(
                    "Budget ledger is stopped or has a pending call; review is required."
                )
            if data["total_tokens"] + input_tokens + output_tokens > self.token_limit:
                raise BudgetExceeded("Cumulative token cap would be exceeded; no call was sent.")
            cost_after_call = data["total_cost_units"] + _cost(input_tokens, output_tokens)
            if cost_after_call > self.cost_limit_units:
                raise BudgetExceeded("Cumulative CNY cap would be exceeded; no call was sent.")
            receipt_id = uuid.uuid4().hex
            data["receipts"].append(
                {
                    "id": receipt_id,
                    "phase": phase,
                    "status": "reserved",
                    "reserved_at_unix": time.time(),
                    "reserved_input_tokens": input_tokens,
                    "reserved_output_tokens": output_tokens,
                    "charged_input_tokens": input_tokens,
                    "charged_output_tokens": output_tokens,
                    "charged_cost_units": _cost(input_tokens, output_tokens),
                    "input_tokens": None,
                    "output_tokens": None,
                    "latency": None,
                    "failure_reason": None,
                }
            )
            self._write(data)
            return receipt_id

    def _settle(
        self,
        receipt_id: str,
        *,
        latency: float,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        with self._locked():
            data = self._load()
            matches = [r for r in data["receipts"] if r["id"] == receipt_id]
            if len(matches) != 1 or matches[0]["status"] != "reserved":
                raise LedgerError("Reservation cannot be settled; review is required.")
            receipt = matches[0]
            if failure_reason is not None:
                if failure_reason not in _FAILURE_REASONS:
                    raise LedgerError("Unrecognized accounting failure.")
                receipt["status"] = "uncertain"
                receipt["failure_reason"] = failure_reason
                data["stopped"] = True
                data["stop_reason"] = failure_reason
            else:
                if (
                    not _integer(input_tokens, minimum=1)
                    or not _integer(output_tokens)
                    or input_tokens > receipt["reserved_input_tokens"]
                    or output_tokens > receipt["reserved_output_tokens"]
                ):
                    raise LedgerError("Usage does not fit the reservation; review is required.")
                receipt.update(
                    status="completed",
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    charged_input_tokens=input_tokens,
                    charged_output_tokens=output_tokens,
                    charged_cost_units=_cost(input_tokens, output_tokens),
                )
            receipt["latency"] = latency
            receipt["settled_at_unix"] = time.time()
            self._write(data)


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward the Authorization header, even to a same-host redirect.
        raise ClientError("Model endpoint redirected; request stopped.")


def _https_transport(request: urllib.request.Request, timeout: float, limit: int) -> bytes:
    """Use the configured HTTPS proxy, with standard certificate verification.

    No fallback to direct networking is permitted.  Proxy discovery is solely the
    standard library's default mechanism; no credential environment is consulted.
    """
    host = urlsplit(validate_endpoint(request.full_url)).hostname
    proxies = urllib.request.getproxies()
    proxy = proxies.get("https")
    if not proxy or urllib.request.proxy_bypass(host):
        raise ClientError("A non-bypassed default HTTPS proxy is required.")
    if urlsplit(proxy).scheme not in {"http", "https"}:
        raise ClientError("A supported default HTTPS proxy is required.")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler(proxies), _NoRedirects())
    with opener.open(request, timeout=timeout) as response:
        if response.status != 200:
            raise ClientError("Model endpoint returned an unsuccessful status.")
        body = response.read(limit + 1)
    if len(body) > limit:
        raise ClientError("Model response exceeded the bounded response size.")
    return body


Transport = Callable[[urllib.request.Request, float, int], bytes]


class OpenAICompatibleClient:
    """Single-attempt client.  Every call, phase and rerun shares one ledger."""

    def __init__(
        self,
        api_key: str,
        *,
        ledger: BudgetLedger,
        timeout_seconds: float = 90,
        transport: Transport | None = None,
    ) -> None:
        validate_endpoint(ENDPOINT)
        if not isinstance(api_key, str) or not api_key or any(c.isspace() for c in api_key):
            raise ValueError("A nonempty API key must be passed explicitly.")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise ValueError("Timeout must be a finite positive number no greater than 120.")
        if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 120:
            raise ValueError("Timeout must be a finite positive number no greater than 120.")
        self._api_key = api_key
        self.ledger = ledger
        self.timeout_seconds = float(timeout_seconds)
        self._transport = transport if transport is not None else _https_transport

    def _dispatch(self, request: urllib.request.Request) -> bytes:
        """Impose a wall-clock deadline in addition to urllib's socket timeout.

        A server can otherwise dribble bytes indefinitely.  An over-deadline
        daemon may still finish the already-dispatched request, so its reservation
        is never released and the ledger prevents every subsequent paid call.
        """
        finished = threading.Event()
        result: list[bytes] = []

        def invoke() -> None:
            try:
                result.append(self._transport(request, self.timeout_seconds, MAX_RESPONSE_BYTES))
            except BaseException:
                # Do not retain a provider exception or its potentially secret body.
                pass
            finally:
                finished.set()

        worker = threading.Thread(target=invoke, name="bounded-model-request", daemon=True)
        worker.start()
        if not finished.wait(self.timeout_seconds):
            raise ClientError("Model request exceeded its wall-clock deadline.")
        if not result:
            raise ClientError("Model transport failed or was interrupted.")
        return result[0]

    def complete(
        self,
        messages: Sequence[Mapping[str, str]],
        max_output_tokens: int,
        phase: str,
    ) -> dict[str, Any]:
        validate_endpoint(ENDPOINT)
        if not _integer(max_output_tokens, minimum=1) or max_output_tokens > MAX_OUTPUT_TOKENS:
            raise ValueError("Maximum output tokens must be an integer between 1 and 32768.")
        if not isinstance(messages, (list, tuple)) or not 1 <= len(messages) <= 128:
            raise ValueError("Provide between 1 and 128 text chat messages.")
        normalized = []
        for message in messages:
            if not isinstance(message, Mapping) or set(message) != {"role", "content"}:
                raise ValueError("Each chat message must contain only role and text content.")
            if message["role"] not in {"system", "user", "assistant"}:
                raise ValueError("Unsupported message role.")
            if not isinstance(message["content"], str):
                raise ValueError("Chat message content must be text.")
            normalized.append({"role": message["role"], "content": message["content"]})
        payload = {
            "model": MODEL,
            "messages": normalized,
            "temperature": 0,
            "enable_thinking": False,
            "stream": False,
            "max_tokens": max_output_tokens,
        }
        try:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (ValueError, UnicodeError):
            raise ValueError("Messages could not be encoded as UTF-8 JSON.") from None
        input_bound = (
            len(body) + PROTOCOL_OVERHEAD_TOKENS + PER_MESSAGE_OVERHEAD_TOKENS * len(normalized)
        )
        receipt_id = self.ledger.reserve(input_bound, max_output_tokens, phase)
        started = time.monotonic()
        try:
            request = urllib.request.Request(
                ENDPOINT,
                data=body,
                method="POST",
                headers={
                    "Authorization": "Bearer " + self._api_key,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            raw = self._dispatch(request)
        except BaseException:
            self.ledger._settle(
                receipt_id,
                latency=time.monotonic() - started,
                failure_reason="transport_failure",
            )
            raise ClientError(
                "Model call failed or was interrupted; full reservation retained "
                "and ledger stopped."
            ) from None
        latency = time.monotonic() - started
        failure = "invalid_response"
        try:
            if not isinstance(raw, bytes) or len(raw) > MAX_RESPONSE_BYTES:
                raise ValueError
            response = json.loads(raw)
            if not isinstance(response, dict) or "error" in response:
                raise ValueError
            failure = "invalid_usage"
            usage = response["usage"]
            if not isinstance(usage, dict):
                raise ValueError
            input_tokens = usage["prompt_tokens"]
            output_tokens = usage["completion_tokens"]
            total_tokens = usage["total_tokens"]
            if (
                not _integer(input_tokens, minimum=1)
                or not _integer(output_tokens)
                or not _integer(total_tokens, minimum=1)
                or input_tokens + output_tokens != total_tokens
            ):
                raise ValueError
            failure = "usage_overrun"
            if input_tokens > input_bound or output_tokens > max_output_tokens:
                raise ValueError
            failure = "invalid_response"
            choices = response["choices"]
            if not isinstance(choices, list) or len(choices) != 1:
                raise ValueError
            content = choices[0]["message"]["content"]
            if not isinstance(content, str):
                raise ValueError
        except Exception:
            self.ledger._settle(receipt_id, latency=latency, failure_reason=failure)
            raise ClientError(
                "Model response or usage could not be verified; full reservation retained "
                "and ledger stopped."
            ) from None
        self.ledger._settle(
            receipt_id,
            latency=latency,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        return {
            "content": content,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "latency": latency,
            "receipt_id": receipt_id,
        }
