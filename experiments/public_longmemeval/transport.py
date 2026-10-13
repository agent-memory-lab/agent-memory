"""User-run-only, fail-closed OpenAI-compatible transport (Python 3.13 / stdlib).

Importing this module performs no I/O. Never resets a budget or retries a request.
Use one Client context for the entire experiment. Its state directory is private
local evidence, not a public upload: exact prompts and responses are saved there.
All cost figures are tariff estimates, not invoices. Fresh state starts at zero.
Uncertain calls remain reserved and are never automatically retried.
"""
from __future__ import annotations

import asyncio
import base64
import copy
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import getpass
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import ssl
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.parse import urlsplit
import uuid
import warnings


TOKEN_CAP = 1_000_000
COST_CAP = Decimal("5")
BUDGET_ACCEPTANCE = "I ACCEPT THE CONFIGURED EXPERIMENT BUDGET"
SCHEMA = 2


class TransportError(RuntimeError):
    """Safe error text, with no URL, credentials, or provider error body."""


class RunLocked(TransportError):
    pass


class ResumeBlocked(TransportError):
    pass


class BudgetExceeded(TransportError):
    pass


class UsageOverrun(TransportError):
    pass


class BudgetNotAccepted(TransportError):
    pass


class RequestFailed(TransportError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise TransportError("Invalid decimal configuration") from None
    if not result.is_finite() or result < 0:
        raise TransportError("Invalid decimal configuration")
    return result


def _integer(value: Any) -> bool:
    return type(value) is int and value >= 0


def _sync_directory(directory: Path) -> None:
    # Windows does not provide a portable directory fsync. Every file itself is
    # flushed+fsynced, then os.replace is used on the same local filesystem.
    if os.name != "nt":
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def atomic_write_bytes(path: Path | str, data: bytes) -> None:
    """Durable same-directory replacement; caller owns the state mutation lock."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def atomic_write_json(path: Path | str, value: Any) -> None:
    atomic_write_bytes(path, _canonical(value) + b"\n")


class RunLock:
    """Cross-platform exclusive lock. No stale-lock removal or auto-unlock."""

    def __init__(self, state_dir: Path | str):
        self.path = Path(state_dir) / ".run.lock"
        self.held = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.path.mkdir()
        except FileExistsError:
            raise RunLocked("Run lock exists; concurrent use or interrupted run requires manual review") from None
        self.held = True
        try:
            atomic_write_json(self.path / "owner.json", {"pid": os.getpid(), "created_unix": time.time()})
            _sync_directory(self.path.parent)
        except BaseException:
            # Do not erase an uncertain lock after a failed durable write.
            self.held = False
            raise

    def release(self) -> None:
        if not self.held:
            return
        (self.path / "owner.json").unlink()
        self.path.rmdir()
        _sync_directory(self.path.parent)
        self.held = False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        self.release()


@dataclass(frozen=True)
class Config:
    endpoint: str = field(repr=False)
    model: str = "YOUR_PROVIDER_MODEL"
    max_tokens: int = 1536
    max_calls: int = 160
    timeout_seconds: float = 90.0
    deadline_seconds: float = 1800.0
    input_cny_per_million: str = "0.8"
    output_cny_per_million: str = "2.7"
    tariff_verified: bool = False
    request_overhead_tokens: int = 2048
    max_response_bytes: int = 16 * 1024 * 1024

    def __post_init__(self):
        _validate_endpoint(self.endpoint)
        if not self.model or not isinstance(self.model, str):
            raise TransportError("Invalid model configuration")
        for name in ("max_tokens", "max_calls", "request_overhead_tokens", "max_response_bytes"):
            if not _integer(getattr(self, name)) or getattr(self, name) < 1:
                raise TransportError("Invalid integer configuration")
        if self.request_overhead_tokens < 2048:
            raise TransportError("Input framing reserve must be at least 2048 tokens")
        for value in (self.timeout_seconds, self.deadline_seconds):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise TransportError("Invalid time bound")
        if self.timeout_seconds > 300 or self.deadline_seconds > 86400:
            raise TransportError("Time bounds exceed the permitted per-call or run maximum")
        if _decimal(self.input_cny_per_million) <= 0 or _decimal(self.output_cny_per_million) <= 0:
            raise TransportError("Tariffs must be positive verified conservative rates")
        if type(self.tariff_verified) is not bool:
            raise TransportError("Invalid tariff verification flag")


def _validate_endpoint(endpoint: str):
    try:
        parsed = urlsplit(endpoint)
        valid = (parsed.scheme == "https" and parsed.hostname and not parsed.username
                 and not parsed.password and not parsed.query and not parsed.fragment
                 and parsed.path.endswith("/chat/completions") and not any(c.isspace() for c in endpoint))
        _ = parsed.port
    except (ValueError, TypeError):
        valid = False
    if not valid:
        raise TransportError("Configure an exact HTTPS chat/completions URL without credentials, query, or fragment")
    return parsed


def _safe_error_type(error: BaseException) -> str:
    name = type(error).__name__
    return name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", name) else "RequestError"


def _read_json(body: bytes):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result
    def bad_constant(_):
        raise ValueError("Nonfinite JSON number")
    return json.loads(body.decode("utf-8"), object_pairs_hook=pairs, parse_constant=bad_constant)


def _validated_usage(document: Any) -> dict[str, int] | None:
    usage = document.get("usage") if isinstance(document, dict) else None
    if not isinstance(usage, dict):
        return None
    names = ("prompt_tokens", "completion_tokens", "total_tokens")
    if any(not _integer(usage.get(name)) for name in names):
        return None
    if usage["total_tokens"] != usage["prompt_tokens"] + usage["completion_tokens"]:
        return None
    return {name: usage[name] for name in names}


def _contains_secret(document: Any, secrets: tuple[str, ...]) -> bool:
    """Check decoded JSON as well as bytes, including escaped keys and strings."""
    pending = [document]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if any(secret in item for secret in secrets):
                return True
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return False


class Client:
    """One lock for the full run, persistent reservations, no automatic retries.

    Configuration tariff_verified means the user verified conservative maximum
    rates applicable to this exact endpoint/model, including any surcharges. It
    does not claim the defaults have been checked against a current price list.
    """

    def __init__(self, config: Config | dict[str, Any], key: str, state_dir: Path | str):
        self.config = Config(**config) if isinstance(config, dict) else config
        if not isinstance(self.config, Config):
            raise TransportError("Expected Config or config dictionary")
        if not isinstance(key, str) or not key or any(c in key for c in "\r\n"):
            raise TransportError("A nonempty API key entered interactively is required")
        self._key = key
        self.state_dir = Path(state_dir)
        self._lock = RunLock(self.state_dir)
        self._active = False
        self._closed = True
        self._session_started = time.monotonic()
        self._session_accepted = False
        self._poisoned = False
        self._ledger: dict[str, Any] = {}
        self._lock.acquire()
        try:
            self._open_state()
            self._closed = False
        except BaseException:
            self._key = ""
            self._lock.release()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self) -> None:
        if self._active:
            raise ResumeBlocked("Cannot close a client while a request is active")
        if not self._closed:
            self._key = ""
            self._closed = True
            self._lock.release()

    @property
    def receipts(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._ledger.get("calls", []))

    @property
    def model(self) -> str:
        return self.config.model

    @property
    def ledger(self) -> dict[str, Any]:
        return copy.deepcopy(self._ledger)

    def _public_config(self) -> dict[str, Any]:
        return {
            "model": self.config.model,
            "endpoint_sha256": _sha(self.config.endpoint.encode("utf-8")),
            "input_cny_per_million": str(_decimal(self.config.input_cny_per_million)),
            "output_cny_per_million": str(_decimal(self.config.output_cny_per_million)),
            "max_tokens": self.config.max_tokens,
            "max_calls": self.config.max_calls,
            "request_overhead_tokens": self.config.request_overhead_tokens,
            "deadline_seconds": self.config.deadline_seconds,
        }

    def _open_state(self) -> None:
        ledger_path = self.state_dir / "ledger.json"
        if ledger_path.exists():
            try:
                self._ledger = _read_json(ledger_path.read_bytes())
                self._verify_state()
            except TransportError:
                raise
            except Exception:
                raise ResumeBlocked("State is malformed; manual accounting review is required") from None
        else:
            # Never silently rebuild or initialize over an incomplete state dir.
            existing = {path.name for path in self.state_dir.iterdir()} - {".run.lock"}
            if existing:
                raise ResumeBlocked("State has files but no ledger; do not reset or rebuild accounting")
            self._ledger = {
                "schema": SCHEMA,
                "created_unix": time.time(),
                "deadline_unix": time.time() + self.config.deadline_seconds,
                "budget_acceptance": None,
                "config": self._public_config(),
                "cap_tokens": TOKEN_CAP,
                "cap_cny": str(COST_CAP),
                "cost_basis": "User-verified conservative tariff estimate; not a provider invoice",
                "calls": [],
                "halt_reason": None,
            }
            self._save_ledger()
        self._check_unresolved()

    def _verify_state(self) -> None:
        ledger = self._ledger
        if (ledger.get("schema") != SCHEMA
                or ledger.get("cap_tokens") != TOKEN_CAP or ledger.get("cap_cny") != str(COST_CAP)):
            raise ResumeBlocked("Ledger provenance or global caps differ from the required accounting")
        if ledger.get("config") != self._public_config():
            raise ResumeBlocked("Configuration changed; manual review is required before resuming")
        calls = ledger.get("calls")
        if not isinstance(calls, list):
            raise ResumeBlocked("Malformed calls ledger")
        known = set()
        for entry in calls:
            ident = entry.get("id")
            if not isinstance(ident, str) or not re.fullmatch(r"[a-f0-9]{32}", ident) or ident in known:
                raise ResumeBlocked("Duplicate or invalid call identity")
            known.add(ident)
            path = self.state_dir / "calls" / (ident + ".json")
            raw = path.read_bytes()
            if _sha(raw) != entry.get("receipt_sha256"):
                raise ResumeBlocked("Per-call receipt differs from the ledger; manual review required")
            receipt = _read_json(raw)
            if receipt.get("id") != ident or receipt.get("status") != entry.get("status"):
                raise ResumeBlocked("Per-call receipt identity or status mismatch")
            if not _integer(entry.get("charged_tokens")):
                raise ResumeBlocked("Invalid cumulative token accounting")
            _decimal(entry.get("charged_cny"))
            for name in ("charged_tokens", "charged_cny", "reserved_input_tokens", "reserved_output_tokens", "reserved_cny", "phase"):
                if entry.get(name) != receipt.get(name):
                    raise ResumeBlocked("Receipt and ledger accounting differ")
            request_body = receipt["request_body_utf8"].encode("utf-8")
            if (receipt.get("request_sha256") != _sha(request_body)
                    or receipt.get("request_utf8_bytes") != len(request_body)
                    or entry["reserved_input_tokens"] != len(request_body) + self.config.request_overhead_tokens):
                raise ResumeBlocked("Request reservation differs from its evidence")
            reserved_cost = self._cost(entry["reserved_input_tokens"], entry["reserved_output_tokens"])
            if _decimal(entry["reserved_cny"]) != reserved_cost:
                raise ResumeBlocked("Reservation cost does not match the verified tariff")
            usage = _validated_usage({"usage": receipt.get("usage")})
            if usage is None:
                expected_tokens = entry["reserved_input_tokens"] + entry["reserved_output_tokens"]
                expected_cost = reserved_cost
                if entry["status"] == "completed":
                    raise ResumeBlocked("Completed receipt lacks complete provider usage")
            else:
                expected_tokens = usage["total_tokens"]
                expected_cost = self._cost(usage["prompt_tokens"], usage["completion_tokens"])
            if entry["charged_tokens"] != expected_tokens or _decimal(entry["charged_cny"]) != expected_cost:
                raise ResumeBlocked("Charges differ from provider usage or the unresolved reservation")
            if receipt.get("response_body_base64") is not None:
                response = base64.b64decode(receipt["response_body_base64"], validate=True)
                if _sha(response) != receipt.get("response_sha256"):
                    raise ResumeBlocked("Response body digest mismatch")
                if _validated_usage(_read_json(response)) != usage:
                    raise ResumeBlocked("Receipt usage differs from saved provider response")
        present = {path.stem for path in (self.state_dir / "calls").glob("*.json")}
        if present != known:
            raise ResumeBlocked("Untracked receipt exists; manual review is required")
        if ledger.get("budget") != self.budget_summary():
            raise ResumeBlocked("Cumulative budget mismatch")

    def _check_unresolved(self) -> None:
        if self._poisoned or self._ledger.get("halt_reason") or any(entry["status"] != "completed" for entry in self._ledger["calls"]):
            raise ResumeBlocked("A new reservation is unresolved or the run halted; no automatic retry or resume")

    def budget_summary(self) -> dict[str, Any]:
        calls = self._ledger.get("calls", [])
        tokens = sum(entry["charged_tokens"] for entry in calls)
        cost = sum((_decimal(entry["charged_cny"]) for entry in calls), Decimal(0))
        return {
            "charged_tokens": tokens,
            "charged_tariff_estimate_cny": str(cost),
            "remaining_tokens": TOKEN_CAP - tokens,
            "remaining_tariff_estimate_cny": str(COST_CAP - cost),
            "new_calls": len(calls),
        }

    def _save_ledger(self) -> None:
        self._ledger["budget"] = self.budget_summary()
        atomic_write_json(self.state_dir / "ledger.json", self._ledger)

    def accept_budget(self) -> None:
        """Requires fresh interactive acceptance for each process/run session."""
        if self._closed or self._active:
            raise ResumeBlocked("Client is closed or busy")
        if not sys.stdin.isatty():
            raise BudgetNotAccepted("Budget acceptance requires an interactive terminal")
        print("Cumulative caps: " + str(TOKEN_CAP) + " tokens / CNY " + str(COST_CAP))
        print("Current accounting: " + json.dumps(self.budget_summary()))
        print("Unresolved reservations block further requests; there are no automatic retries.")
        if input(BUDGET_ACCEPTANCE + ": ") != BUDGET_ACCEPTANCE:
            raise BudgetNotAccepted("Budget was not explicitly accepted; no call was made")
        self._ledger["budget_acceptance"] = {"accepted_unix": time.time()}
        self._save_ledger()
        self._session_accepted = True

    def _cost(self, input_tokens: int, output_tokens: int) -> Decimal:
        return (Decimal(input_tokens) * _decimal(self.config.input_cny_per_million)
                + Decimal(output_tokens) * _decimal(self.config.output_cny_per_million)) / Decimal(1_000_000)

    def _receipt_write(self, entry: dict[str, Any], receipt: dict[str, Any]) -> None:
        data = _canonical(receipt) + b"\n"
        atomic_write_bytes(self.state_dir / "calls" / (entry["id"] + ".json"), data)
        entry["receipt_sha256"] = _sha(data)

    def _remaining_seconds(self) -> float:
        return min(self.config.deadline_seconds - (time.monotonic() - self._session_started),
                   self._ledger["deadline_unix"] - time.time())

    async def chat(self, prompt: str, phase: str, format: Any = None, limit: int | None = None) -> str:
        if self._closed or self._active:
            raise ResumeBlocked("Client is closed or another request is active")
        self._check_unresolved()
        if not self._session_accepted:
            raise BudgetNotAccepted("Explicit interactive budget acceptance is required before any API call")
        if not self.config.tariff_verified:
            raise BudgetExceeded("Verify conservative endpoint/model tariff maxima before making any API call")
        if self._remaining_seconds() <= 0:
            raise BudgetExceeded("Run deadline reached; no new API call")
        if len(self._ledger["calls"]) >= self.config.max_calls:
            raise BudgetExceeded("Maximum new-call count reached; no new API call")
        output_limit = self.config.max_tokens if limit is None else limit
        if not _integer(output_limit) or not 1 <= output_limit <= self.config.max_tokens:
            raise TransportError("Output cap must be within the configured per-call maximum")
        if not isinstance(prompt, str) or not isinstance(phase, str) or not phase:
            raise TransportError("Prompt and phase must be strings")
        body = {"model": self.config.model, "messages": [{"role": "user", "content": prompt}],
                "enable_thinking": False, "max_tokens": output_limit, "stream": False, "temperature": 0}
        if format is not None:
            if format == "json":
                body["response_format"] = {"type": "json_object"}
            elif isinstance(format, dict):
                if format.get("type") in ("json_object", "json_schema", "text"):
                    body["response_format"] = format
                else:
                    body["response_format"] = {"type": "json_schema", "json_schema": {
                        "name": "memory_claims", "strict": True, "schema": format}}
            else:
                raise TransportError("Response format must be a dictionary or 'json'")
        payload = _canonical(body)
        if self._key.encode() in payload or self.config.endpoint.encode() in payload or self._key in phase or self.config.endpoint in phase:
            raise TransportError("A credential or endpoint appeared in persisted request content")
        reserve_input = len(payload) + self.config.request_overhead_tokens
        reserve_tokens = reserve_input + output_limit
        reserve_cost = self._cost(reserve_input, output_limit)
        budget = self.budget_summary()
        if reserve_tokens > budget["remaining_tokens"] or reserve_cost > _decimal(budget["remaining_tariff_estimate_cny"]):
            raise BudgetExceeded("The conservative reservation exceeds the cumulative token or CNY cap")
        self._active = True
        ident = uuid.uuid4().hex
        entry = {"id": ident, "phase": phase, "status": "reserved", "reserved_input_tokens": reserve_input,
                 "reserved_output_tokens": output_limit, "charged_tokens": reserve_tokens,
                 "charged_cny": str(reserve_cost), "reserved_cny": str(reserve_cost)}
        receipt = {**entry, "created_unix": time.time(), "prompt": prompt,
                   "request_body_utf8": payload.decode(), "request_sha256": _sha(payload),
                   "request_utf8_bytes": len(payload), "usage": None, "response_body_base64": None}
        try:
            self._receipt_write(entry, receipt)
            self._ledger["calls"].append(entry)
            self._save_ledger()  # Durable reservation BEFORE starting a subprocess or network request.
            remaining = self._remaining_seconds()
            if remaining <= 0:
                self._record_failure(entry, receipt, "DeadlineReached")
                raise RequestFailed("DeadlineReached; reservation held; manual review required")
            started = time.monotonic()
            task = asyncio.create_task(asyncio.to_thread(self._request, payload, min(self.config.timeout_seconds, remaining)))
            cancelled = False
            try:
                try:
                    status, response = await asyncio.shield(task)
                except asyncio.CancelledError:
                    # Keep the lock and await the bounded worker, then persist before propagating cancellation.
                    cancelled = True
                    status, response = await task
            except Exception as error:
                kind = _safe_error_type(error)
                self._record_failure(entry, receipt, kind)
                raise RequestFailed(kind + "; reservation held; manual review required") from None
            receipt["latency_seconds"] = time.monotonic() - started
            result = self._complete(entry, receipt, status, response)
            if cancelled:
                raise asyncio.CancelledError
            return result
        except BaseException:
            # Also block this live object after an uncertain disk write. Reusing
            # the same object must not sidestep restart-time receipt checks.
            self._poisoned = True
            raise
        finally:
            self._active = False

    def _record_failure(self, entry: dict[str, Any], receipt: dict[str, Any], kind: str) -> None:
        entry["status"] = receipt["status"] = "unresolved"
        receipt["error_type"] = kind
        self._ledger["halt_reason"] = kind
        self._receipt_write(entry, receipt)
        self._save_ledger()

    def _complete(self, entry: dict[str, Any], receipt: dict[str, Any], status: int, response: bytes) -> str:
        receipt["http_status"] = status
        receipt["response_sha256"] = _sha(response)
        # Provider failures may echo request headers or URLs. Do not store their
        # bodies. For successful bodies, fail closed if a literal secret occurs.
        try:
            document = _read_json(response)
        except Exception:
            document = None
        reflected = (self._key.encode() in response or self.config.endpoint.encode() in response
                     or _contains_secret(document, (self._key, self.config.endpoint)))
        if status == 200 and document is not None and not reflected:
            receipt["response_body_base64"] = base64.b64encode(response).decode("ascii")
        usage = _validated_usage(document)
        receipt["usage"] = usage
        if usage is not None:
            cost = self._cost(usage["prompt_tokens"], usage["completion_tokens"])
            entry["charged_tokens"] = usage["total_tokens"]
            entry["charged_cny"] = str(cost)
            receipt["charged_tokens"] = usage["total_tokens"]
            receipt["charged_cny"] = str(cost)
            receipt["input_tokens"] = usage["prompt_tokens"]
            receipt["output_tokens"] = usage["completion_tokens"]
            entry["input_tokens"] = usage["prompt_tokens"]
            entry["output_tokens"] = usage["completion_tokens"]
            entry["latency_seconds"] = receipt.get("latency_seconds")
        else:
            cost = _decimal(entry["reserved_cny"])
        overrun = usage is not None and (
            usage["prompt_tokens"] > entry["reserved_input_tokens"]
            or usage["completion_tokens"] > entry["reserved_output_tokens"]
            or cost > _decimal(entry["reserved_cny"]))
        text = None
        finish_reason = None
        if isinstance(document, dict):
            try:
                choices = document["choices"]
                if len(choices) == 1 and choices[0].get("finish_reason") in ("stop", "length"):
                    finish_reason = choices[0]["finish_reason"]
                    candidate = choices[0]["message"]["content"]
                    if isinstance(candidate, str):
                        text = candidate
            except (KeyError, TypeError, IndexError, AttributeError):
                pass
        if overrun:
            kind = "UsageOverrun"
        elif status != 200:
            kind = "HTTPStatusError"
        elif reflected:
            kind = "SecretReflection"
        elif usage is None:
            kind = "MissingOrInvalidUsage"
        elif finish_reason == "length":
            kind = "OutputTruncated"
        elif text is None:
            kind = "InvalidResponseContent"
        else:
            kind = None
        if kind:
            entry["status"] = receipt["status"] = "halted" if usage is not None else "unresolved"
            receipt["error_type"] = kind
            self._ledger["halt_reason"] = kind
        else:
            entry["status"] = receipt["status"] = "completed"
        # Exact response bytes and validated usage are durable BEFORE the ledger
        # advances, the answer returns, or the caller processes the next case.
        self._receipt_write(entry, receipt)
        self._save_ledger()
        if overrun:
            raise UsageOverrun("Provider usage exceeded its reservation; actual usage charged; run stopped")
        if kind:
            raise RequestFailed(kind + "; run stopped; manual accounting review required")
        return text

    def _request(self, payload: bytes, timeout: float) -> tuple[int, bytes]:
        """One killable subprocess, no key/endpoint in argv, stderr, or logs."""
        envelope = _canonical({"endpoint": self.config.endpoint, "key": self._key,
                               "payload_base64": base64.b64encode(payload).decode("ascii"),
                               "timeout": timeout, "max_response_bytes": self.config.max_response_bytes})
        process = subprocess.Popen([sys.executable, "-I", str(Path(__file__).resolve()), "--transport-worker"],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            output, _ = process.communicate(envelope, timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            raise TimeoutError("Worker deadline") from None
        if process.returncode != 0:
            raise RequestFailed("WorkerFailed")
        try:
            result = _read_json(output)
            if result.get("error_type"):
                raise RequestFailed("WorkerRequestError")
            status = result["status"]
            if not _integer(status) or not 100 <= status <= 599:
                raise ValueError
            response = base64.b64decode(result["body_base64"], validate=True)
            if len(response) > self.config.max_response_bytes:
                raise ValueError
            return status, response
        except Exception:
            raise RequestFailed("WorkerResultError") from None


def prompt_api_key() -> str:
    """Call only in the user's actual online run, never in offline preflight."""
    if not sys.stdin.isatty():
        raise TransportError("API key entry requires an interactive terminal")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            return getpass.getpass("API key (not saved): ")
        except getpass.GetPassWarning:
            raise TransportError("Secure hidden key entry is unavailable in this terminal") from None


def _worker() -> int:
    connection = None
    try:
        envelope = _read_json(sys.stdin.buffer.read())
        parsed = _validate_endpoint(envelope["endpoint"])
        # http.client uses no environment proxies and does not follow redirects.
        connection = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443,
                                                 timeout=envelope["timeout"], context=ssl.create_default_context())
        payload = base64.b64decode(envelope["payload_base64"], validate=True)
        connection.request("POST", parsed.path, body=payload,
                           headers={"Authorization": "Bearer " + envelope["key"],
                                    "Content-Type": "application/json", "Accept": "application/json"})
        response = connection.getresponse()
        body = response.read(envelope["max_response_bytes"] + 1)
        if len(body) > envelope["max_response_bytes"]:
            raise ValueError("ResponseTooLarge")
        result = {"status": response.status, "body_base64": base64.b64encode(body).decode("ascii")}
    except Exception as error:
        result = {"error_type": _safe_error_type(error)}
    finally:
        if connection is not None:
            connection.close()
    sys.stdout.buffer.write(_canonical(result))
    sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--transport-worker"]:
        raise SystemExit(_worker())
    print("Import Client from the user-run benchmark. No standalone API action was performed.")
