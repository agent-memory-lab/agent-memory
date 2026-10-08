"""HTTP wire tests against a recording fake Ollama, not a real inference engine."""

import asyncio
import json
import threading
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from test_governed_models_v7 import TEMPLATE, configuration

from agent_memory.retrieval.model_contracts import (
    ModelCoordinates,
    ModelError,
    SealedModelInput,
    canonical,
    digest,
)
from agent_memory.retrieval.ollama import OllamaPort


@contextmanager
def server(*, bad=None, pause=None):
    received = []
    show = {
        "template": "test-template",
        "parameters": "num_ctx 8192",
        "capabilities": ["completion"],
    }
    version = {"version": "synthetic-contract-test"}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            result = (
                {"models": [{"name": "qwen3.5:9b", "digest": "a" * 64}]}
                if self.path == "/api/tags"
                else version
            )
            self.send_response(200)
            self.end_headers()
            try:
                self.wfile.write(json.dumps(result).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            received.append((self.path, payload))
            if self.path == "/api/show":
                result = show
            else:
                if pause is not None:
                    pause.wait(30)
                result = {
                    "model": "qwen3.5:9b",
                    "done": True,
                    "done_reason": "stop",
                    "message": {"role": "assistant", "content": "zh-CN"},
                    "prompt_eval_count": 12,
                    "eval_count": 3,
                    "total_duration": 12345,
                }
                if bad == "truncated":
                    result["done_reason"] = "length"
                if bad == "hidden_tools":
                    result["message"]["tool_calls"] = [{"function": "unapproved"}]
                if bad == "private_error":
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write(b"private source body from provider error")
                    return
            self.send_response(200)
            self.end_headers()
            try:
                self.wfile.write(json.dumps(result).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{http.server_port}", received, {"show": show, "version": version}
    finally:
        http.shutdown()
        thread.join()
        http.server_close()


def sealed(cfg):
    coordinates = ModelCoordinates(
        "scope",
        "alice",
        "project",
        "context",
        "alice",
        "q",
        "1",
        "{}",
        "why",
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "2026-10-01T00:00:00+00:00",
        "2026-10-01T00:00:00+00:00",
    )
    payload = canonical(
        dict(
            model=cfg.model,
            messages=[
                {"role": "system", "content": TEMPLATE},
                {"role": "user", "content": "Chinese 🇨🇳. Do not drop the final exception."},
            ],
            stream=False,
            truncate=False,
            shift=False,
            options=json.loads(cfg.options_json),
            think=cfg.think,
            keep_alive=cfg.keep_alive,
        )
    )
    manifest = dict(
        schema="model-input-manifest/1",
        sources={},
        payload_sha256=sha256(payload.encode()).hexdigest(),
        configuration_sha256=cfg.fingerprint,
        coordinates_sha256=digest(coordinates.payload()),
    )
    return SealedModelInput(cfg, coordinates, payload, canonical(manifest))


def test_actual_received_bytes_equal_sealed_request_and_cost_is_unknown():
    with server() as (endpoint, received, runtime):
        cfg = configuration(endpoint=endpoint, runtime_manifest_sha256=digest(runtime))
        request = sealed(cfg)
        response = asyncio.run(OllamaPort(cfg).generate(request))
        assert received[1] == ("/api/chat", request.payload_json.encode())
        assert response.input_tokens == 12 and response.output_tokens == 3
        assert response.total_duration_ns == 12345 and response.actual_microunits is None


@pytest.mark.parametrize("bad", ["truncated", "hidden_tools", "private_error"])
def test_partial_tool_or_sensitive_provider_error_is_never_a_valid_answer(bad):
    with server(bad=bad) as (endpoint, received, runtime):
        cfg = configuration(endpoint=endpoint, runtime_manifest_sha256=digest(runtime))
        with pytest.raises(ModelError) as failure:
            asyncio.run(OllamaPort(cfg).generate(sealed(cfg)))
        assert "private source" not in str(failure.value)


def test_model_revision_or_private_runtime_template_change_blocks_chat():
    with server() as (endpoint, received, runtime):
        for changes in ({"model_revision": "f" * 64}, {"runtime_manifest_sha256": "f" * 64}):
            cfg = configuration(endpoint=endpoint, runtime_manifest_sha256=digest(runtime))
            cfg = replace(cfg, **changes)
            with pytest.raises(ModelError):
                asyncio.run(OllamaPort(cfg).generate(sealed(cfg)))
        assert not any(path == "/api/chat" for path, _ in received)


def test_input_bytes_reject_entire_semantics_instead_of_silent_truncation():
    cfg = configuration(max_input_bytes=10)
    with pytest.raises(ModelError, match="model_input_budget_exceeded"):
        sealed(cfg)
