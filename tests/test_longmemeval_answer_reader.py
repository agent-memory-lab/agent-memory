"""Exercise the actual worker answer boundary without optional model SDKs."""

import ast
import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

EXAMPLES = Path(__file__).parents[1] / "examples"
SPEC = importlib.util.spec_from_file_location(
    "worker_reader_fixture", EXAMPLES / "memory_evidence_reader.py"
)
READER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = READER
SPEC.loader.exec_module(READER)


def worker_answer():
    tree = ast.parse((EXAMPLES / "longmemeval_full_comparison.py").read_text())
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "generate_answer"
    )
    observed = []

    async def checked(ledger, row, operation, call, timeout):
        observed.append((operation, timeout))
        return await call

    namespace = {
        "checked_call": checked,
        "evidence_reader": READER,
        "native": SimpleNamespace(MODEL="fixture"),
        "ANSWER_PROMPT": "Direct.\n",
    }
    exec(
        compile(ast.Module(body=[function], type_ignores=[]), "answer-boundary", "exec"), namespace
    )
    return namespace["generate_answer"], observed


class Client:
    def __init__(self, response):
        self.response, self.calls = response, []

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(message=SimpleNamespace(content=self.response))


def invoke(
    client,
    row,
    *,
    reader_mode="direct",
    allow_contextual_inference=False,
    reader_thinking=False,
    reader_output_tokens=None,
    evidence_records=None,
):
    answer, observed = worker_answer()
    value = asyncio.run(
        answer(
            client,
            object(),
            row,
            question="Where are my sketches displayed?",
            question_date="2025-04-02",
            evidence="I joined Elm Gallery.\nMy sketches are on display.",
            timeout=30,
            reader_mode=reader_mode,
            allow_contextual_inference=allow_contextual_inference,
            reader_thinking=reader_thinking,
            reader_output_tokens=reader_output_tokens,
            evidence_records=evidence_records,
        )
    )
    return value, observed


def test_direct_reader_preserves_the_previous_single_call_contract():
    client, row = Client("  At Elm Gallery.  "), {}
    answer, operations = invoke(client, row)
    assert answer == "At Elm Gallery."
    assert operations == [("answer.question", 30)]
    assert len(client.calls) == 1 and "format" not in client.calls[0]
    assert client.calls[0]["options"] == {"num_predict": 384}
    assert "answer_reader" not in row


def test_structured_reader_records_policy_quotes_and_labels_inferred_answers():
    client = Client(
        json.dumps(
            {
                "schema_version": READER.SCHEMA_VERSION,
                "requested_attribute": "place",
                "support_kind": "contextual_inference",
                "answerable": True,
                "answer": "At Elm Gallery.",
                "supporting_quotes": ["I joined Elm Gallery.", "My sketches are on display."],
            }
        )
    )
    row = {}
    answer, operations = invoke(
        client, row, reader_mode="structured", allow_contextual_inference=True
    )
    assert answer.startswith("Based on the conversation context,")
    assert operations == [("answer.question", 30)] and len(client.calls) == 1
    assert row["answer_reader"]["support_kind"] == "contextual_inference"
    assert row["answer_reader"]["allow_contextual_inference"] is True
    assert row["answer_reader"]["entailment_verified"] is False
    assert client.calls[0]["format"] == READER.RESPONSE_SCHEMA


def test_invalid_reader_output_remains_visible_and_is_not_silently_retried():
    client, row = Client("malformed output"), {}
    with pytest.raises(READER.InvalidEvidenceReaderResponse):
        invoke(client, row, reader_mode="structured")
    assert len(client.calls) == 1
    assert row["answer_reader"]["status"] == "invalid"
    assert "answer" not in row


@pytest.mark.parametrize("mode,allow", [("unsupported", False), ("direct", True)])
def test_invalid_reader_policy_fails_before_a_model_call(mode, allow):
    client = Client("unused")
    with pytest.raises(ValueError):
        invoke(client, {}, reader_mode=mode, allow_contextual_inference=allow)
    assert client.calls == []


def test_structured_generation_configuration_matches_request_and_recorded_result():
    client = Client(
        json.dumps(
            {
                "schema_version": READER.SCHEMA_VERSION,
                "requested_attribute": "place",
                "support_kind": "insufficient",
                "answerable": False,
                "answer": "",
                "supporting_quotes": [],
            }
        )
    )
    row = {}
    answer, operations = invoke(
        client, row, reader_mode="structured", reader_thinking=True, reader_output_tokens=8192
    )
    assert answer == "I don't know."
    assert len(client.calls) == 1 and len(operations) == 1
    assert client.calls[0]["think"] is True
    assert client.calls[0]["options"]["num_predict"] == 8192
    assert row["answer_reader"]["requested_thinking"] is True
    assert row["answer_reader"]["requested_max_output_tokens"] == 8192


@pytest.mark.parametrize(
    "kwargs",
    [
        {"reader_thinking": True},
        {"reader_output_tokens": 8192},
        {"reader_thinking": 1},
    ],
)
def test_direct_reader_cannot_silently_ignore_structured_generation_options(kwargs):
    client = Client("unused")
    with pytest.raises(ValueError):
        invoke(client, {}, **kwargs)
    assert client.calls == []


@pytest.mark.parametrize('bad_quote', [False, True])
def test_v3_worker_uses_bound_records_and_preserves_invalid_raw_completion(bad_quote):
    records = [{'text':'user: I exhibit at Maple Workshop.', 'source_ids':['synthetic-session']}]
    raw = json.dumps({'schema_version':'evidence-reader/3','answerable':True,'answer':'Maple Workshop',
        'supporting_quotes':[{'evidence_id':'E1','text':'Fabricated quote' if bad_quote else 'I exhibit at Maple Workshop.'}],
        'missing_information':[]})
    client, row = Client(raw), {}
    if bad_quote:
        with pytest.raises(READER.InvalidEvidenceReaderResponse):
            invoke(client,row,reader_mode='structured-v3',evidence_records=records)
        assert row['answer_reader']['status'] == 'invalid'
    else:
        answer, operations = invoke(client,row,reader_mode='structured-v3',evidence_records=records)
        assert answer == 'Maple Workshop' and len(operations) == 1
    assert row['answer_reader']['raw_response'] == raw
    assert len(client.calls) == 1 and client.calls[0]['format'] == READER.RESPONSE_SCHEMA_V3
    assert client.calls[0]['options']['num_predict'] == 768


def test_v3_worker_requires_source_records_and_does_not_guess_from_flattened_text():
    client = Client('unused')
    with pytest.raises(ValueError,match='requires delivered evidence records'):
        invoke(client,{},reader_mode='structured-v3')
    assert client.calls == []
