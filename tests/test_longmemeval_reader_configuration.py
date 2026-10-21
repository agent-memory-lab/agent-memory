"""CLI, manifest and controlled-client settings without model or SDK calls."""

import argparse
import ast
import asyncio
import importlib.util
import json
import sys
from contextlib import nullcontext
from hashlib import sha256
from math import isfinite
from pathlib import Path
from types import SimpleNamespace

import pytest

EXAMPLES = Path(__file__).parents[1] / "examples"
SPEC = importlib.util.spec_from_file_location(
    "reader_configuration_fixture", EXAMPLES / "memory_evidence_reader.py"
)
READER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = READER
SPEC.loader.exec_module(READER)


def definitions(filename, names, namespace):
    tree = ast.parse((EXAMPLES / filename).read_text())
    selected = [node for node in tree.body if getattr(node, "name", None) in names]
    exec(compile(ast.Module(body=selected, type_ignores=[]), filename, "exec"), namespace)
    return namespace


def controlled_client():
    class SDK:
        def __init__(self, *args, **kwargs):
            self.args, self.kwargs, self.calls = args, kwargs, []

        async def chat(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                message=SimpleNamespace(content=getattr(self, "response", "yes"))
            )

    namespace = definitions(
        "unified_memory_benchmark.py",
        {"ControlledClient"},
        {
            "ollama": SimpleNamespace(AsyncClient=SDK),
            "native": SimpleNamespace(MODEL="pinned"),
            "SETTINGS": {"temperature": 0, "seed": 42, "num_ctx": 32768},
        },
    )
    return namespace["ControlledClient"]


def configuration_functions():
    return definitions(
        "longmemeval_full_comparison.py",
        {"answer_reader_manifest", "main"},
        {
            "evidence_reader": READER,
            "canonical_digest": lambda value: sha256(
                json.dumps(value, sort_keys=True).encode()
            ).hexdigest(),
            "argparse": argparse,
            "Path": Path,
            "asyncio": asyncio,
            "isfinite": isfinite,
            "ARMS": ("agent-memory",),
            "__doc__": "Offline CLI probe",
        },
    )


@pytest.mark.parametrize("allow", [False, True])
def test_controlled_client_keeps_unspecified_and_explicit_false_requests_non_thinking(allow):
    client = controlled_client()(host="offline", allow_thinking=allow)
    asyncio.run(client.chat(model="pinned", options={"num_predict": 1536}))
    asyncio.run(client.chat(model="pinned", think=False, options={"num_predict": 16}))
    assert [call["think"] for call in client.calls] == [False, False]
    assert [call["options"]["num_predict"] for call in client.calls] == [1536, 16]
    assert client.kwargs == {"host": "offline"}


def test_thinking_must_be_explicitly_enabled_and_is_never_silently_disabled():
    denied = controlled_client()()
    with pytest.raises(ValueError, match="explicitly enabled"):
        asyncio.run(denied.chat(model="pinned", think=True))
    assert denied.calls == []
    allowed = controlled_client()(allow_thinking=True)
    asyncio.run(allowed.chat(model="pinned", think=True, options={"num_predict": 8192, "seed": 9}))
    assert allowed.calls[0]["think"] is True
    assert allowed.calls[0]["options"] == {
        "num_predict": 8192,
        "temperature": 0,
        "seed": 42,
        "num_ctx": 32768,
    }


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
def test_controlled_client_flags_require_strict_booleans(value):
    with pytest.raises(ValueError, match="allow_thinking"):
        controlled_client()(allow_thinking=value)
    client = controlled_client()(allow_thinking=True)
    with pytest.raises(ValueError, match="think must be a boolean"):
        asyncio.run(client.chat(model="pinned", think=value))
    assert client.calls == []


def test_enabled_thinking_does_not_allow_unpinned_models():
    client = controlled_client()(allow_thinking=True)
    with pytest.raises(ValueError, match="pinned"):
        asyncio.run(client.chat(model="other", think=True))
    assert client.calls == []


def test_one_controlled_client_enables_thinking_only_for_the_structured_reader_call():
    client = controlled_client()(allow_thinking=True)
    client.response = json.dumps(
        {
            "schema_version": READER.SCHEMA_VERSION,
            "requested_attribute": "place",
            "support_kind": "insufficient",
            "answerable": False,
            "answer": "",
            "supporting_quotes": [],
        }
    )

    async def checked(ledger, row, operation, call, timeout):
        return await call

    namespace = definitions(
        "longmemeval_full_comparison.py",
        {"generate_answer"},
        {
            "evidence_reader": READER,
            "checked_call": checked,
            "native": SimpleNamespace(MODEL="pinned"),
            "ANSWER_PROMPT": "unchanged",
        },
    )
    row = {}

    async def run():
        await client.chat(model="pinned", options={"num_predict": 1536})
        answer = await namespace["generate_answer"](
            client,
            object(),
            row,
            question="Where?",
            question_date="2025-01-01",
            evidence="",
            timeout=30,
            reader_mode="structured",
            reader_thinking=True,
            reader_output_tokens=8192,
        )
        await client.chat(model="pinned", think=False, options={"num_predict": 16})
        return answer

    assert asyncio.run(run()) == "I don't know."
    assert [call["think"] for call in client.calls] == [False, True, False]
    assert [call["options"]["num_predict"] for call in client.calls] == [1536, 8192, 16]
    assert row["answer_reader"]["requested_thinking"] is True
    assert row["answer_reader"]["requested_max_output_tokens"] == 8192


@pytest.mark.parametrize(
    "thinking,budget,expected",
    [
        (False, None, 384),
        (True, None, 2048),
        (True, 8192, 8192),
        (False, 1024, 1024),
    ],
)
def test_manifest_binds_the_same_reader_configuration_as_the_helper(thinking, budget, expected):
    manifest = configuration_functions()["answer_reader_manifest"](
        SimpleNamespace(
            reader_mode="structured",
            reader_thinking=thinking,
            reader_output_tokens=budget,
            allow_contextual_inference=True,
        )
    )
    policy = READER.reader_policy_manifest(
        thinking=thinking, max_output_tokens=budget, allow_contextual_inference=True
    )
    assert all(manifest[key] == value for key, value in policy.items())
    assert manifest["thinking"] is thinking
    assert manifest["max_output_tokens"] == manifest["requested_max_output_tokens"] == expected
    assert configuration_functions()["answer_reader_manifest"](SimpleNamespace()) == {
        "mode": "direct"
    }


def run_cli(monkeypatch, options):
    namespace = configuration_functions()
    observed = []

    async def run(args):
        observed.append(args)

    namespace.update(
        {
            "run": run,
            "worker_lock": lambda path: nullcontext(),
            "recover_call_log_tails": lambda path: None,
        }
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "worker",
            "--data",
            "unused.json",
            "--output-dir",
            "unused-output",
            *options,
        ],
    )
    namespace["main"]()
    return observed


def test_cli_packing_selection_is_independent_of_reader_selection(monkeypatch):
    args = run_cli(monkeypatch, ["--packing-version", "v3"])[0]
    assert args.packing_version == "v3" and args.reader_mode == "direct"
    args = run_cli(monkeypatch, ["--reader-mode", "structured-v3"])[0]
    assert args.packing_version == "v2" and args.reader_mode == "structured-v3"
    args = run_cli(monkeypatch, ["--packing-version", "v4", "--reader-mode", "structured-v3"])[0]
    assert args.packing_version == "v4" and args.reader_mode == "structured-v3"


def test_cli_explicit_reader_options_reach_worker_without_model_calls(monkeypatch):
    rows = run_cli(
        monkeypatch,
        [
            "--reader-mode",
            "structured",
            "--reader-thinking",
            "--reader-output-tokens",
            "8192",
            "--allow-contextual-inference",
        ],
    )
    assert len(rows) == 1
    assert rows[0].reader_thinking is True and rows[0].reader_output_tokens == 8192
    assert rows[0].reader_mode == "structured"
    assert rows[0].allow_contextual_inference is True


@pytest.mark.parametrize(
    "options",
    [
        ["--reader-thinking"],
        ["--reader-output-tokens", "8192"],
        ["--reader-mode", "structured", "--reader-output-tokens", "0"],
        ["--reader-mode", "structured", "--reader-output-tokens", "32769"],
    ],
)
def test_cli_rejects_invalid_reader_mode_or_budget_before_worker_runs(monkeypatch, options):
    with pytest.raises(SystemExit) as error:
        run_cli(monkeypatch, options)
    assert error.value.code == 2


def test_cli_defaults_preserve_direct_non_thinking_behavior(monkeypatch):
    args = run_cli(monkeypatch, [])[0]
    assert args.reader_mode == "direct"
    assert args.reader_thinking is False and args.reader_output_tokens is None
