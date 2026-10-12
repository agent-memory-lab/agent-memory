"""Real loopback HTTP contracts; no model, external server, or framework doubles."""

import importlib.util
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep
from urllib.parse import urlsplit

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "memory_benchmark_support",
    Path(__file__).parents[1] / "examples" / "memory_benchmark_support.py",
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
ModelCallLedger = _MODULE.ModelCallLedger
OllamaAccountingProxy = _MODULE.OllamaAccountingProxy
ModelAccountingError = _MODULE.ModelAccountingError


@contextmanager
def upstream(*, status=200, payload=None, entered=None, release=None):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append((self.path, body, dict(self.headers)))
            if entered is not None:
                entered.set()
            if release is not None:
                assert release.wait(3)
            encoded = json.dumps(payload or {}, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Contract", "preserved")
            self.send_header("Connection", "X-Hop")
            self.send_header("X-Hop", "removed")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", received
    finally:
        if release is not None:
            release.set()
        server.shutdown()
        server.server_close()
        thread.join(3)


def request(url, path, payload, *, headers=None):
    address = urlsplit(url)
    connection = HTTPConnection(address.hostname, address.port, timeout=3)
    body = json.dumps(payload, ensure_ascii=False).encode()
    try:
        connection.request(
            "POST", path, body=body, headers=headers or {"Content-Type": "application/json"}
        )
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def completed(ledger, count):
    deadline = monotonic() + 2
    while ledger.calls < count and monotonic() < deadline:
        sleep(0.001)
    assert ledger.calls == count


def test_ollama_chat_preserves_payload_headers_and_accounts_without_leaking(tmp_path):
    response_body = {
        "message": {"content": "private output"},
        "prompt_eval_count": 13,
        "eval_count": 4,
    }
    ledger = ModelCallLedger(tmp_path / "calls.jsonl").start_phase(1, "mem0-oss", "add")
    with (
        upstream(payload=response_body) as (origin, received),
        OllamaAccountingProxy(origin, ledger) as proxy,
    ):
        status, headers, output = request(
            proxy.url,
            "/api/chat",
            {"model": "fixture-model", "stream": False, "messages": [{"content": "private input"}]},
            headers={
                "Content-Type": "application/json",
                "Connection": "X-Secret",
                "X-Secret": "drop",
            },
        )
        completed(ledger, 1)
        assert status == 200 and json.loads(output) == response_body
        assert headers["X-Contract"] == "preserved" and "X-Hop" not in headers
        assert "X-Secret" not in received[0][2]
        assert json.loads(received[0][1])["messages"][0]["content"] == "private input"
    record = ledger.records[0]
    assert (record["trial"], record["arm"], record["operation"]) == (1, "mem0-oss", "add")
    assert (record["input_tokens"], record["output_tokens"]) == (13, 4)
    assert record["kind"] == "chat" and record["http_status"] == 200 and record["success"]
    assert record["elapsed_seconds"] > 0
    assert "private" not in (tmp_path / "calls.jsonl").read_text()
    assert "private" not in json.dumps(ledger.records)


@pytest.mark.parametrize("path", ["/api/embed", "/api/embeddings", "/v1/embeddings"])
def test_embedding_usage_is_separate_and_missing_output_is_unknown(path):
    payload = {"usage": {"prompt_tokens": 7, "total_tokens": 7}, "data": [{"embedding": [0.1]}]}
    ledger = ModelCallLedger().start_phase(0, "graphiti", "search")
    with upstream(payload=payload) as (origin, _), OllamaAccountingProxy(origin, ledger) as proxy:
        assert request(proxy.url, path, {"model": "embedding-model", "input": "sample"})[0] == 200
        completed(ledger, 1)
    record = ledger.records[0]
    assert record["input_tokens"] == 7 and record["output_tokens"] is None
    summary = ledger.summary(trial=0, arm="graphiti")
    assert summary["by_kind"]["embedding"]["calls"] == 1
    assert summary["by_kind"]["chat"]["calls"] == 0
    assert summary["output_tokens_measured"] is None
    assert summary["missing_output_usage_calls"] == 1
    assert summary["by_phase"][0]["operation"] == "search"
    assert summary["cost_status"] == "not_priced"


def test_http_failures_and_absent_usage_preserve_status_without_inventing_zero():
    ledger = ModelCallLedger().start_phase(0, "agent-memory", "add")
    with upstream(status=503, payload={"error": "private service failure"}) as (origin, _):
        with OllamaAccountingProxy(origin, ledger) as proxy:
            status, _, body = request(proxy.url, "/api/chat", {"model": "fixture", "stream": False})
            completed(ledger, 1)
            assert status == 503 and json.loads(body)["error"] == "private service failure"
    record = ledger.records[0]
    assert record["error_type"] == "UpstreamHTTPError" and not record["success"]
    assert record["input_tokens"] is None and record["output_tokens"] is None
    assert ledger.summary()["failed_calls"] == 1
    assert ledger.summary()["input_tokens_measured"] is None
    assert "private" not in json.dumps(ledger.records)


def test_openai_chat_usage_and_measured_zero_are_retained():
    ledger = ModelCallLedger().start_phase(2, "graphiti", "answer")
    with upstream(payload={"usage": {"prompt_tokens": 21, "completion_tokens": 0}}) as (origin, _):
        with OllamaAccountingProxy(origin, ledger) as proxy:
            request(proxy.url, "/v1/chat/completions", {"model": "fixture"})
            completed(ledger, 1)
    assert ledger.records[0]["output_tokens"] == 0
    assert ledger.summary()["output_tokens_measured"] == 0
    assert ledger.summary()["missing_usage_calls"] == 0
    assert ledger.summary(arm="other")["calls"] == 0
    detached = ledger.records
    detached[0]["arm"] = "changed"
    assert ledger.records[0]["arm"] == "graphiti"


@pytest.mark.parametrize(
    "value",
    [
        "https://example.com",
        "http://192.168.0.1:11434",
        "http://0.0.0.0:11434",
        "http://127.0.0.1@evil.example",
        "http://name.local:11434",
        "file:///tmp/model",
        "http://127.0.0.1:11434/api",
        "http://127.0.0.1:11434?secret=private",
    ],
)
def test_gateway_rejects_external_or_ambiguous_origins(value):
    with pytest.raises(ValueError):
        OllamaAccountingProxy(value)


@pytest.mark.parametrize("payload", [{"stream": True}, {"stream": "false"}, {}])
def test_ollama_streaming_requests_never_reach_upstream(payload):
    ledger = ModelCallLedger()
    with upstream() as (origin, received), OllamaAccountingProxy(origin, ledger) as proxy:
        assert request(proxy.url, "/api/chat", payload)[0] == 400
        completed(ledger, 1)
        assert received == []
    assert ledger.records[0]["error_type"] == "ValueError"


def test_request_phase_is_captured_before_concurrent_phase_change():
    entered, release = Event(), Event()
    ledger = ModelCallLedger().start_phase(0, "agent-memory", "add")
    with upstream(entered=entered, release=release) as (origin, _):
        with (
            OllamaAccountingProxy(origin, ledger) as proxy,
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            future = pool.submit(request, proxy.url, "/api/embed", {"model": "fixture"})
            assert entered.wait(2)
            assert proxy.pending_requests == 1
            assert not proxy.drain(timeout=0.01)
            ledger.start_phase(1, "mem0-oss", "search")
            release.set()
            assert future.result(3)[0] == 200
            assert proxy.drain(timeout=2)
            assert proxy.pending_requests == 0 and ledger.calls == 1
    assert ledger.records[0]["arm"] == "agent-memory" and ledger.records[0]["operation"] == "add"
    assert ledger.summary(trial=1)["calls"] == 0


def test_request_budget_shutdown_and_transport_errors_are_visible():
    ledger = ModelCallLedger()
    with upstream() as (origin, received):
        proxy = OllamaAccountingProxy(origin, ledger, max_requests=1)
        assert request(proxy.url, "/api/embed", {"model": "fixture"})[0] == 200
        assert request(proxy.url, "/api/embed", {"model": "fixture"})[0] == 429
        completed(ledger, 2)
        assert len(received) == 1
        proxy.close()
        proxy.close()
        assert not proxy._thread.is_alive()
        with pytest.raises(OSError):
            request(proxy.url, "/api/embed", {})
    assert ledger.records[1]["error_type"] == "RequestBudgetExceeded"
    assert ledger.summary()["successful_calls"] == 1 and ledger.summary()["failed_calls"] == 1
    with OllamaAccountingProxy(origin, ledger, timeout=0.2) as proxy:
        assert request(proxy.url, "/api/embed", {})[0] == 502
        completed(ledger, 3)
    assert ledger.records[2]["error_type"] == "ConnectionRefusedError"


def test_generation_settings_are_safe_and_separate_from_payload():
    ledger = ModelCallLedger()
    with upstream() as (origin, _), OllamaAccountingProxy(origin, ledger) as proxy:
        request(
            proxy.url,
            "/api/chat",
            {
                "model": "fixture",
                "stream": False,
                "think": False,
                "options": {
                    "temperature": 0,
                    "num_ctx": 32768,
                    "seed": 7,
                    "num_predict": 512,
                    "private": "sensitive transcript",
                },
                "messages": [{"content": "sensitive transcript"}],
            },
        )
        request(
            proxy.url,
            "/v1/chat/completions",
            {"model": "fixture", "temperature": 0.2, "max_tokens": 123, "think": "sensitive"},
        )
        assert proxy.drain(timeout=2)
    assert ledger.records[0]["generation_settings"] == {
        "temperature": 0,
        "num_ctx": 32768,
        "seed": 7,
        "num_predict": 512,
        "think": False,
    }
    assert ledger.records[1]["generation_settings"] == {"temperature": 0.2, "max_tokens": 123}
    assert "sensitive" not in json.dumps(ledger.records)
    detached = ledger.records
    detached[0]["generation_settings"]["seed"] = 999
    assert ledger.records[0]["generation_settings"]["seed"] == 7


def test_broken_downstream_response_preserves_usage_and_records_failure(monkeypatch):
    ledger = ModelCallLedger()
    with upstream(payload={"prompt_eval_count": 11, "eval_count": 3}) as (origin, _):
        with OllamaAccountingProxy(origin, ledger) as proxy:
            handler = proxy._server.RequestHandlerClass
            original = handler._respond
            calls = 0

            def break_first_response(self, *args):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise BrokenPipeError("sensitive transport details")
                return original(self, *args)

            monkeypatch.setattr(handler, "_respond", break_first_response)
            assert request(proxy.url, "/api/chat", {"stream": False})[0] == 502
            assert proxy.drain(timeout=2)
    record = ledger.records[0]
    assert record["http_status"] == 502 and record["upstream_status"] == 200
    assert record["error_type"] == "BrokenPipeError" and not record["success"]
    assert (record["input_tokens"], record["output_tokens"]) == (11, 3)
    assert ledger.summary()["failed_calls"] == 1
    assert "sensitive" not in json.dumps(ledger.records)


def test_drain_waits_for_late_accounting_and_close_reports_timeout():
    entered, release = Event(), Event()

    class LateLedger(ModelCallLedger):
        def append(self, record):
            entered.set()
            assert release.wait(3)
            super().append(record)

    ledger = LateLedger()
    with upstream() as (origin, _):
        proxy = OllamaAccountingProxy(origin, ledger, timeout=0.05)
        try:
            assert request(proxy.url, "/api/embed", {})[0] == 200
            assert entered.wait(2)
            assert ledger.calls == 0 and proxy.pending_requests == 1
            assert not proxy.drain(timeout=0.01)
            assert proxy.close() is False
            assert not proxy._thread.is_alive()
        finally:
            release.set()
            assert proxy.drain(timeout=2)
            assert proxy.close() is True
    assert ledger.calls == 1 and proxy.pending_requests == 0
    assert ledger.records[0]["input_tokens"] is None


@pytest.mark.parametrize("budget", [0, -1, 10001, True, False, 1.0])
def test_invalid_request_budget_bounds_and_types_are_rejected(budget):
    with pytest.raises(ValueError, match="request budget"):
        OllamaAccountingProxy(max_requests=budget)


@pytest.mark.parametrize("budget", [1, 1000, 10000])
def test_valid_request_budget_bounds_do_not_change_default(budget):
    with OllamaAccountingProxy(max_requests=budget) as proxy:
        assert proxy.max_requests == budget
        assert proxy.drain(timeout=0)
    with OllamaAccountingProxy() as proxy:
        assert proxy.max_requests == 1000


def test_append_failure_latches_gap_and_stops_upstream_requests_after_path_is_repaired(tmp_path):
    path = tmp_path / "calls.jsonl"
    ledger = ModelCallLedger(path).start_phase(1, "agent-memory", "answer.question")
    # A real filesystem error, not a mocked append result.
    path.mkdir()
    with upstream(payload={"prompt_eval_count": 7, "eval_count": 3}) as (origin, received):
        proxy = OllamaAccountingProxy(origin, ledger)
        try:
            assert request(proxy.url, "/api/chat", {"stream": False})[0] == 200
            assert not proxy.drain(2)
            assert proxy.pending_requests == 0 and ledger.calls == 0
            assert ledger.accounting_error == {
                "error_type": "IsADirectoryError", "marker_persisted": True,
            }
            path.rmdir()
            assert request(proxy.url, "/api/chat", {"stream": False})[0] == 503
            assert len(received) == 1
            assert not proxy.close()
            with pytest.raises(ModelAccountingError, match="unrecorded"):
                proxy.__exit__(None, None, None)
        finally:
            proxy.close()
    marker = path.with_name(path.name + ".accounting-error.json")
    assert json.loads(marker.read_text()) == {
        "accounting_status": "unknown", "error_type": "IsADirectoryError",
    }
    # Restart must preserve the unknown marker, even after the log path recovers.
    reopened = ModelCallLedger(path)
    assert not reopened.summary()["accounting_complete"]
    with pytest.raises(ModelAccountingError, match="already failed"):
        reopened.append({})


def test_total_storage_failure_keeps_durable_attempt_unfinished_and_unknown(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "accounting_test_run_ledger",
        Path(__file__).parents[1] / "examples" / "longmemeval_run_ledger.py",
    )
    run_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(run_module)
    run_path = tmp_path / "run.sqlite3"
    log_path = tmp_path / "calls.jsonl"
    original_open = Path.open

    def unwritable(path, *args, **kwargs):
        if path.name.startswith("calls.jsonl"):
            raise OSError("private disk failure details")
        return original_open(path, *args, **kwargs)

    with run_module.RunLedger(run_path, {"protocol": "fixture"}, ["q1"], ["agent-memory"]) as run:
        task = run.next_task()
        ledger = ModelCallLedger(log_path)
        monkeypatch.setattr(Path, "open", unwritable)
        with upstream(payload={"prompt_eval_count": 4, "eval_count": 2}) as (origin, received):
            with pytest.raises(ModelAccountingError):
                with OllamaAccountingProxy(origin, ledger) as proxy:
                    assert request(proxy.url, "/api/chat", {"stream": False})[0] == 200
                    assert not proxy.drain(2)
                    assert not ledger.accounting_error["marker_persisted"]
                    assert "private" not in json.dumps(ledger.summary())
                    assert request(proxy.url, "/api/chat", {"stream": False})[0] == 503
                    assert len(received) == 1
                # A failed proxy close cannot permit a successful task commit.
                run.complete(task["question_id"], task["arm"], {"judge_label": True})
        assert run.summary()["running"] == 1
        assert run.summary()["completed"] == 0
    assert not log_path.with_name(log_path.name + ".accounting-error.json").exists()
    with run_module.RunLedger(run_path, {"protocol": "fixture"}, ["q1"], ["agent-memory"]) as run:
        assert run.recover() == 1
        assert run.summary()["abandoned_attempts"] == 1


def test_custom_append_failure_is_latched_without_thread_exception_or_sensitive_details():
    class BrokenLedger(ModelCallLedger):
        def append(self, record):
            raise OSError("private user prompt must not be persisted")

    ledger = BrokenLedger()
    with upstream() as (origin, _):
        with pytest.raises(ModelAccountingError):
            with OllamaAccountingProxy(origin, ledger) as proxy:
                assert request(proxy.url, "/api/embed", {})[0] == 200
                assert not proxy.drain(2)
                assert proxy.pending_requests == 0
                assert not ledger.summary()["accounting_complete"]
                assert "private" not in json.dumps(ledger.summary())


def test_fsync_failure_never_treats_visible_json_as_durable_accounting(tmp_path, monkeypatch):
    def fail_sync(_descriptor):
        raise OSError("private filesystem detail")

    monkeypatch.setattr(_MODULE.os, "fsync", fail_sync)
    path = tmp_path / "calls.jsonl"
    ledger = ModelCallLedger(path)
    with upstream(payload={"prompt_eval_count": 3, "eval_count": 2}) as (origin, _):
        with pytest.raises(ModelAccountingError):
            with OllamaAccountingProxy(origin, ledger) as proxy:
                assert request(proxy.url, "/api/embed", {})[0] == 200
                assert not proxy.drain(2)
                assert ledger.calls == 0
                assert not ledger.accounting_error["marker_persisted"]
    # Bytes visible in this process do not imply a successful durability barrier.
    assert json.loads(path.read_text())["input_tokens"] == 3
    assert not ModelCallLedger(path).summary()["accounting_complete"]
