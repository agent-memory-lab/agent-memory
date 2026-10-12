"""Loopback-only Ollama accounting for local memory experiments.

The gateway records transport and reported token usage, never prompts or output
payloads. Missing usage is unknown, and these measurements are not dollar costs.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from copy import deepcopy
from http.client import HTTPConnection, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import ip_address
from math import isfinite
from pathlib import Path
from threading import Condition, Lock, Thread
from time import perf_counter
from urllib.parse import urlsplit

_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
}
_CHAT_PATHS = {"/api/chat", "/api/generate", "/v1/chat/completions", "/v1/completions"}
_EMBED_PATHS = {"/api/embed", "/api/embeddings", "/v1/embeddings"}


class ModelAccountingError(RuntimeError):
    """A model request could not be durably accounted for; stop this run."""


def _count(value):
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _safe_generation_settings(payload):
    """Only retain recognized scalar configuration; never arbitrary string values."""
    settings = {}
    options = payload.get("options")
    options = options if isinstance(options, dict) else {}
    for key in ("temperature", "num_ctx", "seed", "num_predict"):
        value = options.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value):
            settings[key] = value
    for key in ("temperature", "max_tokens", "max_completion_tokens"):
        value = payload.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value):
            settings[key] = value
    think = payload.get("think")
    if isinstance(think, bool) or think in ("low", "medium", "high"):
        settings["think"] = think
    return settings


def _measured_sum(records, key):
    values = [record[key] for record in records if record[key] is not None]
    return sum(values) if values else None


def _metrics(records):
    return {
        "calls": len(records),
        "successful_calls": sum(record["success"] for record in records),
        "failed_calls": sum(not record["success"] for record in records),
        "input_tokens_measured": _measured_sum(records, "input_tokens"),
        "output_tokens_measured": _measured_sum(records, "output_tokens"),
        "missing_input_usage_calls": sum(record["input_tokens"] is None for record in records),
        "missing_output_usage_calls": sum(record["output_tokens"] is None for record in records),
        "missing_usage_calls": sum(
            record["input_tokens"] is None or record["output_tokens"] is None for record in records
        ),
        "elapsed_seconds": sum(record["elapsed_seconds"] for record in records),
    }


class ModelCallLedger:
    """Thread-safe, append-only request metadata with request-start attribution."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path is not None else None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._records = []
        self._phase = {"trial": None, "arm": "unassigned", "operation": "unassigned"}
        self._error_marker = (
            self.path.with_name(self.path.name + ".accounting-error.json")
            if self.path is not None
            else None
        )
        # Presence alone is sufficient: even a torn marker means accounting is
        # unknown. Never clear it merely because the original path is writable.
        self._accounting_error = (
            {"error_type": "PreviousAccountingFailure", "marker_persisted": True}
            if self._error_marker is not None and self._error_marker.exists()
            else None
        )

    def start_phase(self, trial: int, arm: str, operation: str):
        if isinstance(trial, bool) or not isinstance(trial, int) or trial < 0:
            raise ValueError("trial must be a nonnegative integer")
        if any(
            not isinstance(value, str) or not 1 <= len(value) <= 128 for value in (arm, operation)
        ):
            raise ValueError("arm and operation must be bounded nonempty labels")
        with self._lock:
            self._phase = {"trial": trial, "arm": arm, "operation": operation}
        return self

    def snapshot_phase(self):
        with self._lock:
            return dict(self._phase)

    def append(self, record):
        with self._lock:
            if self._accounting_error is not None:
                raise ModelAccountingError("model accounting has already failed")
            value = {**deepcopy(record), "request_id": len(self._records) + 1}
            try:
                if self.path is not None:
                    with self.path.open("a", encoding="utf-8") as output:
                        output.write(json.dumps(value, ensure_ascii=False) + "\n")
                        output.flush()
                        os.fsync(output.fileno())
                    descriptor = os.open(self.path.parent, os.O_RDONLY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
            except Exception as error:
                self._latch_accounting_error(error)
                raise ModelAccountingError("model call ledger persistence failed") from error
            self._records.append(value)

    def _latch_accounting_error(self, error):
        """Called under _lock; persistence is best effort, the latch is not."""
        if self._accounting_error is not None:
            return
        self._accounting_error = {
            "error_type": type(error).__name__,
            "marker_persisted": False,
        }
        if self._error_marker is None:
            return
        try:
            with self._error_marker.open("w", encoding="utf-8") as output:
                json.dump(
                    {"accounting_status": "unknown", "error_type": type(error).__name__},
                    output,
                )
                output.flush()
                os.fsync(output.fileno())
            descriptor = os.open(self._error_marker.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._accounting_error["marker_persisted"] = True
        except Exception:
            # A full/unwritable filesystem cannot promise a durable gap marker.
            # Durable workers keep the already-claimed attempt running instead
            # of recording successful completion, so recovery marks it unknown.
            pass

    def record_accounting_error(self, error):
        with self._lock:
            self._latch_accounting_error(error)

    @property
    def accounting_error(self):
        with self._lock:
            return deepcopy(self._accounting_error)

    @property
    def records(self):
        with self._lock:
            return deepcopy(self._records)

    @property
    def calls(self):
        with self._lock:
            return len(self._records)

    def summary(self, trial: int | None = None, arm: str | None = None):
        records = [
            record
            for record in self.records
            if (trial is None or record["trial"] == trial) and (arm is None or record["arm"] == arm)
        ]
        phases = defaultdict(list)
        for record in records:
            phases[(record["trial"], record["arm"], record["operation"])].append(record)
        return {
            **_metrics(records),
            "by_kind": {
                kind: _metrics([record for record in records if record["kind"] == kind])
                for kind in ("chat", "embedding", "other")
            },
            "by_phase": [
                {"trial": key[0], "arm": key[1], "operation": key[2], **_metrics(values)}
                for key, values in phases.items()
            ],
            "cost_status": "not_priced",
            "accounting_complete": self.accounting_error is None,
            "accounting_error": self.accounting_error,
        }


def _trusted_upstream(value):
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or parsed.username or parsed.password:
        raise ValueError("upstream must be an HTTP(S) loopback URL without credentials")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError("upstream must be a loopback origin without a path or query")
    hostname = parsed.hostname
    if hostname == "localhost":
        hostname = "127.0.0.1"  # Do not depend on DNS resolution or proxy environment settings.
    try:
        if not ip_address(hostname).is_loopback:
            raise ValueError("non-loopback upstream")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except (ValueError, TypeError) as error:
        raise ValueError("upstream must use a literal loopback address or localhost") from error
    return parsed.scheme, hostname, port


def _filtered_headers(headers):
    connection_tokens = {
        token.strip().lower()
        for key, value in headers
        if key.lower() == "connection"
        for token in value.split(",")
    }
    excluded = _HOP_HEADERS | connection_tokens | {"content-length", "host"}
    return [(key, value) for key, value in headers if key.lower() not in excluded]


class OllamaAccountingProxy:
    """Local non-streaming gateway; budgets count all incoming request attempts."""

    def __init__(
        self, upstream_url="http://127.0.0.1:11434", ledger=None, *, timeout=180, max_requests=1000
    ):
        self._scheme, self._host, self._port = _trusted_upstream(upstream_url)
        if (
            timeout <= 0
            or isinstance(max_requests, bool)
            or not isinstance(max_requests, int)
            or not 1 <= max_requests <= 10000
        ):
            raise ValueError("positive timeout and request budget between 1 and 10000 required")
        self.ledger = ledger if ledger is not None else ModelCallLedger()
        self.timeout, self.max_requests = timeout, max_requests
        self._requests = 0
        self._lock = Lock()
        self._condition = Condition(self._lock)
        self._pending_requests = 0
        self._closed = False
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def _respond(self, status, body, headers=()):
                self.send_response_only(status)
                for key, value in _filtered_headers(headers):
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
                self.close_connection = True

            def _handle(self):
                started = perf_counter()
                phase = proxy.ledger.snapshot_phase()
                with proxy._condition:
                    proxy._pending_requests += 1
                try:
                    path = urlsplit(self.path).path
                except ValueError:
                    path = "/"
                kind = (
                    "chat"
                    if path in _CHAT_PATHS
                    else "embedding"
                    if path in _EMBED_PATHS
                    else "other"
                )
                record = {
                    **phase,
                    "method": self.command,
                    "path": path,
                    "kind": kind,
                    "model": None,
                    "generation_settings": {},
                    "input_tokens": None,
                    "output_tokens": None,
                    "http_status": None,
                    "upstream_status": None,
                    "elapsed_seconds": None,
                    "error_type": None,
                    "success": False,
                }
                connection = None
                try:
                    if proxy.ledger.accounting_error is not None:
                        record["http_status"] = 503
                        record["error_type"] = "ModelAccountingError"
                        self._respond(503, b'{"error":"ModelAccountingError"}')
                        return
                    with proxy._lock:
                        proxy._requests += 1
                        allowed = proxy._requests <= proxy.max_requests
                    if not allowed:
                        record["http_status"], record["error_type"] = 429, "RequestBudgetExceeded"
                        self._respond(429, b'{"error":"RequestBudgetExceeded"}')
                        return
                    if not self.path.startswith("/") or self.headers.get("Transfer-Encoding"):
                        raise ValueError("unsupported request framing")
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 <= length <= 8 * 1024 * 1024:
                        raise ValueError("request body exceeds bounded gateway input")
                    body = self.rfile.read(length)
                    payload = json.loads(body) if body else {}
                    if not isinstance(payload, dict):
                        raise ValueError("request body must be a JSON object")
                    model = payload.get("model")
                    record["model"] = (
                        model if isinstance(model, str) and len(model) <= 512 else None
                    )
                    record["generation_settings"] = _safe_generation_settings(payload)
                    if "stream" in payload and payload["stream"] is not False:
                        raise ValueError("streaming requests are not supported")
                    if (
                        path in ("/api/chat", "/api/generate")
                        and payload.get("stream") is not False
                    ):
                        raise ValueError("Ollama chat/generate must explicitly disable streaming")
                    constructor = HTTPSConnection if proxy._scheme == "https" else HTTPConnection
                    connection = constructor(proxy._host, proxy._port, timeout=proxy.timeout)
                    headers = dict(_filtered_headers(list(self.headers.items())))
                    connection.request(self.command, self.path, body=body, headers=headers)
                    response = connection.getresponse()
                    record["upstream_status"] = response.status
                    output = response.read()
                    record["http_status"] = response.status
                    try:
                        decoded = json.loads(output)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        decoded = None
                    if isinstance(decoded, dict):
                        usage = decoded.get("usage")
                        usage = usage if isinstance(usage, dict) else {}
                        record["input_tokens"] = _count(
                            decoded.get("prompt_eval_count", usage.get("prompt_tokens"))
                        )
                        record["output_tokens"] = _count(
                            decoded.get("eval_count", usage.get("completion_tokens"))
                        )
                        if decoded.get("error"):
                            record["error_type"] = "UpstreamModelError"
                    if not 200 <= response.status < 300:
                        record["error_type"] = "UpstreamHTTPError"
                    record["success"] = (
                        200 <= response.status < 300 and record["error_type"] is None
                    )
                    self._respond(response.status, output, response.getheaders())
                except (ValueError, json.JSONDecodeError) as error:
                    record["http_status"], record["error_type"] = 400, type(error).__name__
                    record["success"] = False
                    self._respond(400, b'{"error":"InvalidNonStreamingRequest"}')
                except Exception as error:
                    record["http_status"], record["error_type"] = 502, type(error).__name__
                    record["success"] = False
                    try:
                        self._respond(502, b'{"error":"UpstreamTransportError"}')
                    except OSError:
                        pass
                finally:
                    if connection is not None:
                        connection.close()
                    record["elapsed_seconds"] = perf_counter() - started
                    try:
                        proxy.ledger.append(record)
                    except Exception as error:
                        # Includes custom ledger failures before its own append
                        # implementation can latch the error. Do not rely on a
                        # server-thread traceback to stop foreground scheduling.
                        proxy.ledger.record_accounting_error(error)
                    finally:
                        with proxy._condition:
                            proxy._pending_requests -= 1
                            proxy._condition.notify_all()

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _handle

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = Thread(
            target=self._server.serve_forever, name="ollama-accounting", daemon=True
        )
        self._thread.start()

    def __enter__(self):
        return self

    @property
    def pending_requests(self):
        with self._condition:
            return self._pending_requests

    def drain(self, timeout=5):
        """Wait until every started request has entered the ledger, or return False."""
        if timeout < 0:
            raise ValueError("drain timeout must be nonnegative")
        with self._condition:
            settled = self._condition.wait_for(
                lambda: self._pending_requests == 0, timeout=timeout
            )
        return settled and self.ledger.accounting_error is None

    def close(self):
        with self._lock:
            shutdown = not self._closed
            self._closed = True
        if shutdown:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=5)
        return self.drain(timeout=self.timeout)

    def __exit__(self, *_args):
        if not self.close():
            if self.ledger.accounting_error is not None:
                raise ModelAccountingError("accounting gateway has unrecorded model usage")
            raise TimeoutError("accounting gateway closed with requests still pending")
