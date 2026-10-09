"""Real local HTTP + SQLite smoke orchestration; server inference is synthetic.

These tests never claim actual model execution. The separate committed run
artifact records the real installed Ollama run, with unpriced compute retained.
"""

import asyncio
import json
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from agent_memory.evaluation import ollama_smoke as smoke
from agent_memory.retrieval.model_contracts import ModelError, canonical, digest


@contextmanager
def server(*, bad=None):
    calls = []
    show = {"template": "synthetic-server-template", "capabilities": ["completion"]}
    version = {"version": "synthetic-smoke-contract-test"}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def send(self, value, status=200):
            self.send_response(status)
            self.end_headers()
            self.wfile.write(canonical(value).encode())

        def do_GET(self):
            if self.path == "/api/tags":
                self.send({"models": [{"name": "synthetic:1", "digest": "a" * 64}]})
            else:
                self.send(version)

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path == "/api/show":
                self.send(show)
                return
            assert self.path == "/api/chat"
            calls.append(payload)
            if bad == "private_error":
                self.send({"error": "private provider body must never escape"}, 500)
                return
            question = json.loads(payload["messages"][1]["content"])
            values = [
                value
                for row in question["result"]["rows"]
                if row["matches"]
                for field in row["fields"]
                for value in field["known_values"]
            ]
            result = {"answer_status": question["answer_status"], "values": values}
            if bad == "wrong_value":
                result["values"] = ["invented"]
            if bad == "invalid_schema":
                result = {"extra": "not permitted"}
            if bad == "reconfigure":
                version["version"] = "changed-after-dispatch"
            self.send(
                {
                    "model": "synthetic:1",
                    "done": True,
                    "done_reason": "stop",
                    "message": {"role": "assistant", "content": canonical(result)},
                    "prompt_eval_count": 25,
                    "eval_count": 10,
                    "total_duration": 1000,
                }
            )

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{http.server_port}", calls
    finally:
        http.shutdown()
        thread.join()
        http.server_close()


def test_complete_matrix_uses_actual_runtime_cache_reopen_invalidation_and_erasure():
    async def run(endpoint):
        plan = await smoke.freeze_plan(endpoint, "synthetic:1", allow_real_model=True)
        assert not calls  # Freeze inspects installation but never dispatches.
        report = await smoke.run_smoke(
            plan, expected_plan_sha256=digest(plan), allow_real_model=True
        )
        assert report["status"] == "passed_synthetic_runtime_smoke", report.get("failure")
        assert [item["name"] for item in report["checks"]] == list(smoke.STEPS)
        assert len(report["attempts"]) == len(calls) == len(report["model_ledger"]) == 7
        assert all(item["input_tokens"] == 25 for item in report["attempts"])
        assert report["total_cost_microunits"] is None and report["cost_status"] == "unknown"
        assert not report["full_v7_acceptance"] and not report["production_benefit_claim"]
        assert report["source_unchanged"]
        assert all(
            payload["truncate"] is False
            and payload["shift"] is False
            and payload["stream"] is False
            for payload in calls
        )
        assert all(payload["format"] == smoke.OUTPUT_SCHEMA for payload in calls)

    with server() as (endpoint, calls):
        asyncio.run(run(endpoint))


@pytest.mark.parametrize("bad", ["private_error", "wrong_value", "invalid_schema", "reconfigure"])
def test_failure_keeps_actual_attempt_and_ledger_without_leaking_exception_body(bad):
    async def run(endpoint):
        plan = await smoke.freeze_plan(endpoint, "synthetic:1", allow_real_model=True)
        report = await smoke.run_smoke(
            plan, expected_plan_sha256=digest(plan), allow_real_model=True
        )
        assert report["status"] == "failed"
        assert report["failure"]["phase"] == "first_model_answers"
        assert len(report["attempts"]) == len(report["model_ledger"]) == len(calls) == 1
        assert "private provider" not in canonical(report)
        assert report["total_cost_microunits"] is None

    with server(bad=bad) as (endpoint, calls):
        asyncio.run(run(endpoint))


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://example.com:11434",
        "http://192.168.1.1:11434",
        "http://localhost",
        "file:///tmp/model",
        "http://user:password@localhost:11434",
        "http://localhost:11434/path",
        "http://localhost:11434?key=secret",
        "http://localhost:11434#fragment",
        "http://localhost:invalid",
    ],
)
def test_invalid_endpoint_is_rejected_before_any_network_request(endpoint, monkeypatch):
    def network(*_):
        pytest.fail("invalid endpoint must never contact network")

    monkeypatch.setattr(smoke.OllamaPort, "_http", network)
    with pytest.raises(ModelError):
        asyncio.run(smoke.freeze_plan(endpoint, "synthetic:1", allow_real_model=True))


def test_opt_in_and_frozen_plan_are_required_before_execution():
    with pytest.raises(ModelError, match="opt_in_required"):
        asyncio.run(smoke.freeze_plan("http://127.0.0.1:1", "synthetic:1"))
    with pytest.raises(ModelError, match="opt_in_required"):
        asyncio.run(smoke.run_smoke({}, expected_plan_sha256="a" * 64))
    with server() as (endpoint, calls):
        plan = asyncio.run(smoke.freeze_plan(endpoint, "synthetic:1", allow_real_model=True))
        fingerprint = digest(plan)
        plan["expected"]["owner"] = ["tampered"]
        with pytest.raises(ModelError, match="smoke_plan_changed"):
            asyncio.run(
                smoke.run_smoke(plan, expected_plan_sha256=fingerprint, allow_real_model=True)
            )
        with pytest.raises(ModelError, match="configuration_invalid"):
            asyncio.run(
                smoke.run_smoke(plan, expected_plan_sha256=digest(plan), allow_real_model=True)
            )
        assert not calls


@pytest.mark.parametrize(
    "field,value", [("backend", {"name": "postgres"}), ("host", {"platform": "invented"})]
)
def test_frozen_plan_cannot_mislabel_backend_or_execution_host(field, value):
    with server() as (endpoint, calls):
        plan = asyncio.run(smoke.freeze_plan(endpoint, "synthetic:1", allow_real_model=True))
        plan[field] = value
        with pytest.raises(ModelError, match="configuration_invalid"):
            asyncio.run(
                smoke.run_smoke(plan, expected_plan_sha256=digest(plan), allow_real_model=True)
            )
        assert not calls


@pytest.mark.parametrize(
    "metadata", [{"models": None}, {"models": [{"name": "synthetic:1", "digest": None}]}]
)
def test_malformed_installation_metadata_has_stable_failure(metadata, monkeypatch):
    monkeypatch.setattr(smoke.OllamaPort, "_http", lambda *args: metadata)
    with pytest.raises(ModelError, match="invalid_installation_metadata"):
        asyncio.run(smoke.freeze_plan("http://127.0.0.1:1", "synthetic:1", allow_real_model=True))


def test_cli_freezes_plan_before_dispatch_and_does_not_overwrite_artifacts(tmp_path):
    output = tmp_path / "run.json"
    with server() as (endpoint, calls):
        command = [
            sys.executable,
            "-m",
            "agent_memory.evaluation.ollama_smoke",
            "--endpoint",
            endpoint,
            "--model",
            "synthetic:1",
            "--output",
            str(output),
        ]
        refused = subprocess.run(command, capture_output=True, text=True)
        assert refused.returncode == 2 and not calls and not output.exists()
        result = subprocess.run([*command, "--allow-real-model"], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr + result.stdout
        frozen = json.loads(output.with_suffix(".plan.json").read_text())
        report = json.loads(output.read_text())
        assert digest(frozen["plan"]) == frozen["sha256"] == report["plan_sha256"]
        previous = output.read_bytes()
        duplicate = subprocess.run([*command, "--allow-real-model"], capture_output=True, text=True)
        assert duplicate.returncode == 2 and output.read_bytes() == previous
        assert len(calls) == 7


def test_production_never_imports_smoke_runner():
    root = Path(smoke.__file__).parents[1]
    assert not any(
        "evaluation.ollama_smoke" in path.read_text()
        for path in root.rglob("*.py")
        if path.parent.name != "evaluation"
    )
