"""Structured evidence reader contracts without SDKs, network, or benchmark gold."""

import asyncio
import importlib.util
import inspect
import json
import sys
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE = Path(__file__).parents[1] / "examples" / "memory_evidence_reader.py"
SPEC = importlib.util.spec_from_file_location("memory_evidence_reader", SOURCE)
READER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = READER
SPEC.loader.exec_module(READER)


def response(**changes):
    return {
        "schema_version": READER.SCHEMA_VERSION,
        "requested_attribute": "place",
        "support_kind": "explicit",
        "answerable": True,
        "answer": "Elm Studio",
        "supporting_quotes": ["I attend pottery at Elm Studio."],
        **changes,
    }


def validate(value, evidence="user: I attend pottery at Elm Studio.", **kwargs):
    return READER.validate_reader_response(json.dumps(value), evidence=evidence, **kwargs)


def test_supported_quotes_are_exact_and_locatable_without_demanding_an_address():
    evidence = "user: I attend pottery at Elm Studio.\nassistant: Enjoy your class."
    result = validate(response(), evidence)
    assert result.status == "supported" and READER.render_answer(result) == "Elm Studio"
    assert result.requested_attribute == "place"
    assert result.support_kind == "explicit"
    assert result.allow_contextual_inference is False
    assert result.schema_version == READER.SCHEMA_VERSION
    assert result.prompt_sha256 == sha256(READER.READER_PROMPT.encode()).hexdigest()
    assert result.evidence_sha256 == sha256(evidence.encode()).hexdigest()
    quote = result.supporting_quotes[0]
    assert all(evidence[start:end] == quote.text for start, end in quote.spans)
    assert "granularity actually requested" in READER.READER_PROMPT


def test_multiple_exact_turns_can_jointly_support_a_relationship():
    evidence = (
        "user: My studio is Elm Studio.\nassistant: How was class?\nuser: I enjoyed it yesterday."
    )
    result = validate(
        response(supporting_quotes=["My studio is Elm Studio.", "I enjoyed it yesterday."]),
        evidence,
    )
    assert result.status == "supported" and len(result.supporting_quotes) == 2


def test_repeated_quote_keeps_all_offsets_instead_of_inventing_its_source_occurrence():
    quote = "I attend pottery at Elm Studio."
    result = validate(response(), quote + "\n" + quote)
    assert len(result.supporting_quotes[0].spans) == 2


@pytest.mark.parametrize(
    "quote",
    [
        "I attend pottery at Maple Studio.",
        "I attend pottery ... Elm Studio.",
        "i attend pottery at elm studio.",
        "I attend pottery at Elm Studio!",
    ],
)
def test_fabricated_paraphrased_or_modified_quotes_fail_explicitly(quote):
    result = validate(response(supporting_quotes=[quote]))
    assert result.status == "invalid"
    assert result.errors == ("quote_not_in_delivered_evidence",)
    with pytest.raises(READER.InvalidEvidenceReaderResponse):
        READER.render_answer(result)


def test_quote_must_come_from_delivered_evidence_not_the_question_or_full_source():
    result = validate(response(), "The studio name was outside the delivered evidence budget.")
    assert result.errors == ("quote_not_in_delivered_evidence",)


def test_normal_unsupported_is_distinct_from_invalid_output():
    result = validate(
        response(support_kind="insufficient", answerable=False, answer="", supporting_quotes=[]), ""
    )
    assert result.status == "unsupported" and result.answerable is False
    assert READER.render_answer(result) == "I don't know."
    assert result.errors == ()
    assert result.support_kind == "insufficient"


@pytest.mark.parametrize(
    "change,error",
    [
        ({"answerable": 1}, "answerable_not_boolean"),
        ({"answer": None}, "answer_not_text"),
        ({"requested_attribute": "street_address_only"}, "invalid_requested_attribute"),
        ({"support_kind": "verified_fact"}, "invalid_support_kind"),
        ({"support_kind": "insufficient"}, "support_kind_answerable_mismatch"),
        (
            {"answerable": False, "answer": "", "supporting_quotes": []},
            "support_kind_answerable_mismatch",
        ),
        ({"schema_version": "future"}, "schema_version_mismatch"),
        ({"supporting_quotes": []}, "supported_quotes_empty"),
        ({"supporting_quotes": [""]}, "empty_quote"),
        ({"supporting_quotes": [1]}, "quotes_not_text_array"),
        ({"answerable": False}, "unsupported_response_has_answer_or_quotes"),
        ({"answer": " "}, "supported_answer_empty"),
        ({"supporting_quotes": ["x", "x"]}, "duplicate_quotes"),
        ({"supporting_quotes": [str(index) for index in range(5)]}, "quote_count_exceeded"),
        ({"answer": "汉" * 200}, "answer_byte_budget_exceeded"),
        ({"supporting_quotes": ["汉" * 200]}, "quote_byte_budget_exceeded"),
        (
            {"supporting_quotes": [character * 400 for character in "abcd"]},
            "total_quote_byte_budget_exceeded",
        ),
    ],
)
def test_invalid_contracts_have_body_free_error_codes(change, error):
    result = validate(response(**change))
    assert result.status == "invalid" and result.errors == (error,)
    assert result.answer is None and result.supporting_quotes == ()


@pytest.mark.parametrize("field", READER.RESPONSE_SCHEMA["required"])
def test_missing_required_fields_are_not_normal_abstentions(field):
    value = response()
    del value[field]
    assert validate(value).errors == ("response_fields_mismatch",)


def test_extra_fields_nonfinite_values_duplicate_keys_and_invalid_json_are_rejected():
    assert validate(response(private_metadata="unexpected")).errors == ("response_fields_mismatch",)
    for raw in ["not json", '{"x":NaN}', '{"answerable":true,"answerable":false}']:
        assert READER.validate_reader_response(raw, evidence="").errors == ("invalid_json",)
    assert READER.validate_reader_response("[]", evidence="").errors == ("response_not_object",)
    assert READER.validate_reader_response(None, evidence="").errors == ("response_not_text",)
    assert READER.validate_reader_response("x" * 16385, evidence="").errors == (
        "response_byte_budget_exceeded",
    )


class Client:
    def __init__(self, raw=None, error=None):
        self.raw = raw if raw is not None else json.dumps(response())
        self.error = error
        self.calls = []

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return SimpleNamespace(message=SimpleNamespace(content=self.raw))


@pytest.mark.parametrize("invalid", [False, True])
def test_exactly_one_accountable_call_no_gold_interface_and_no_hidden_retry(invalid):
    client = Client(raw="invalid json" if invalid else None)
    evidence = "user: I attend pottery at Elm Studio."
    result = asyncio.run(
        READER.read_evidence(
            client,
            model="local-fixture",
            question="Where do I attend pottery?",
            evidence=evidence,
            question_date="2024-01-02",
            options={"temperature": 0, "seed": 42, "num_ctx": 32768},
        )
    )
    assert len(client.calls) == 1
    request = client.calls[0]
    assert request["model"] == "local-fixture" and request["think"] is False
    assert request["options"] == {
        "temperature": 0,
        "seed": 42,
        "num_ctx": 32768,
        "num_predict": 384,
    }
    assert request["messages"][0]["content"].endswith("Retrieved memory:\n" + evidence)
    assert request["format"]["additionalProperties"] is False
    assert result.status == ("invalid" if invalid else "supported")
    parameters = set(inspect.signature(READER.read_evidence).parameters)
    assert not parameters & {"gold", "gold_answer", "question_id", "labels", "answer_session_ids"}


def test_transport_failures_propagate_without_retry_or_false_unsupported_result():
    client = Client(error=TimeoutError())
    with pytest.raises(TimeoutError):
        asyncio.run(
            READER.read_evidence(client, model="local-fixture", question="Who?", evidence="")
        )
    assert len(client.calls) == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"evidence": "汉" * 3000},
        {"question": "x" * 4097},
        {"question": " "},
        {"question_date": "x" * 129},
        {"evidence": None},
        {"allow_contextual_inference": "true"},
        {"allow_contextual_inference": 1},
        {"thinking": "true"},
        {"thinking": 1},
        {"thinking": None},
    ],
)
def test_input_budget_and_type_fail_before_any_model_call(kwargs):
    client = Client()
    arguments = {"model": "local-fixture", "question": "Who?", "evidence": "", **kwargs}
    with pytest.raises(ValueError):
        asyncio.run(READER.read_evidence(client, **arguments))
    assert not client.calls


def test_quote_presence_does_not_claim_answer_entailment_or_truth():
    # This intentionally passes structural checks: the independent judge must
    # still reject an answer unsupported by its otherwise authentic quote.
    result = validate(response(answer="An unrelated place"))
    assert result.status == "supported"
    assert result.entailment_verified is False
    assert "traceability, not entailment" in READER.__doc__


def test_invalid_unicode_cannot_escape_structured_failure_handling():
    assert READER.validate_reader_response("\ud800", evidence="").errors == (
        "response_invalid_unicode",
    )
    assert validate(response(answer="\ud800")).errors == ("answer_invalid_unicode",)
    assert validate(response(supporting_quotes=["\ud800"])).errors == ("quote_invalid_unicode",)


CONTEXTUAL_EVIDENCE = (
    "user: I usually attend Elm Studio.\nassistant: How was your next session?\n"
    "user: My lesson there went well yesterday."
)
CONTEXTUAL_QUOTES = ["I usually attend Elm Studio.", "My lesson there went well yesterday."]


def inferred_response(**changes):
    return response(
        support_kind="contextual_inference", supporting_quotes=CONTEXTUAL_QUOTES, **changes
    )


def test_contextual_inference_is_forbidden_by_default_and_cannot_silently_render():
    result = validate(inferred_response(), CONTEXTUAL_EVIDENCE)
    assert result.status == "invalid"
    assert result.errors == ("contextual_inference_disallowed",)
    assert result.allow_contextual_inference is False
    with pytest.raises(READER.InvalidEvidenceReaderResponse):
        READER.render_answer(result)


def test_opt_in_preserves_support_kind_and_always_qualifies_the_rendered_answer():
    result = validate(inferred_response(), CONTEXTUAL_EVIDENCE, allow_contextual_inference=True)
    assert result.status == "supported"
    assert result.support_kind == "contextual_inference"
    assert result.answer == "Elm Studio"
    assert READER.render_answer(result) == "Based on the conversation context, Elm Studio"
    assert result.entailment_verified is False
    assert result.to_dict()["allow_contextual_inference"] is True
    assert result.to_dict()["support_kind"] == "contextual_inference"
    assert len(result.supporting_quotes) == 2


@pytest.mark.parametrize(
    "quotes,error",
    [
        ([CONTEXTUAL_QUOTES[0]], "contextual_inference_needs_distinct_quotes"),
        ([CONTEXTUAL_QUOTES[0]] * 2, "duplicate_quotes"),
        ([CONTEXTUAL_QUOTES[0], "My lesson was at Elm Studio."], "quote_not_in_delivered_evidence"),
    ],
)
def test_inference_requires_two_different_exact_source_quotes(quotes, error):
    result = validate(
        response(support_kind="contextual_inference", supporting_quotes=quotes),
        CONTEXTUAL_EVIDENCE,
        allow_contextual_inference=True,
    )
    assert result.status == "invalid" and result.errors == (error,)
    with pytest.raises(READER.InvalidEvidenceReaderResponse):
        READER.render_answer(result)


@pytest.mark.parametrize("allowed", [False, True])
def test_insufficient_stays_unknown_under_both_policies(allowed):
    result = validate(
        response(support_kind="insufficient", answerable=False, answer="", supporting_quotes=[]),
        CONTEXTUAL_EVIDENCE,
        allow_contextual_inference=allowed,
    )
    assert result.status == "unsupported" and READER.render_answer(result) == READER.UNKNOWN_ANSWER
    assert result.support_kind == "insufficient"
    assert result.allow_contextual_inference is allowed


def test_policy_hashes_are_stable_distinct_and_bound_to_result_and_model_request():
    denied = READER.reader_policy_manifest()
    allowed = READER.reader_policy_manifest(allow_contextual_inference=True)
    assert denied["schema_version"] == "evidence-reader/2"
    assert denied["prompt_version"] == "attribute-grounded-reader/2"
    assert denied["prompt_sha256"] == allowed["prompt_sha256"]
    assert denied["policy_prompt_sha256"] != allowed["policy_prompt_sha256"]
    for policy in (False, True):
        manifest = READER.reader_policy_manifest(allow_contextual_inference=policy)
        prompt = READER.policy_prompt(policy)
        assert manifest["policy_prompt_sha256"] == sha256(prompt.encode()).hexdigest()
        assert prompt.startswith(READER.READER_PROMPT)
        client = Client(raw=json.dumps(inferred_response()) if policy else None)
        result = asyncio.run(
            READER.read_evidence(
                client,
                model="local-fixture",
                question="Where was my lesson?",
                evidence=CONTEXTUAL_EVIDENCE if policy else "I attend pottery at Elm Studio.",
                allow_contextual_inference=policy,
            )
        )
        assert result.status == "supported"
        assert len(client.calls) == 1
        assert client.calls[0]["messages"][0]["content"].startswith(prompt)
        assert result.policy_prompt_sha256 == manifest["policy_prompt_sha256"]


def test_support_classification_limits_are_explicit_without_a_confidence_or_gold_interface():
    prompt = READER.policy_prompt(True)
    for policy_rule in (
        "Isolated co-occurrence",
        "independent suggestion",
        "explicit contradiction",
        "does not require logically excluding every conceivable alternative",
        "not a verified fact",
    ):
        assert policy_rule in prompt
    assert "confidence" not in READER.RESPONSE_SCHEMA["properties"]
    assert validate(response(confidence=0.9)).errors == ("response_fields_mismatch",)
    for function in (
        READER.read_evidence,
        READER.validate_reader_response,
        READER.build_reader_prompt,
        READER.reader_policy_manifest,
    ):
        assert not set(inspect.signature(function).parameters) & {
            "gold",
            "gold_answer",
            "labels",
            "question_id",
            "answer_session_ids",
            "memory_store",
        }
    assert "verified facts or inputs to a memory-write operation" in READER.__doc__


@pytest.mark.parametrize("thinking,budget", [(False, 384), (True, 2048)])
@pytest.mark.parametrize("invalid", [False, True])
def test_thinking_uses_one_call_and_fixed_budget_without_changing_evidence_or_prompt(
    thinking, budget, invalid
):
    client = Client(raw="invalid json" if invalid else None)
    evidence = "user: I attend pottery at Elm Studio."
    result = asyncio.run(
        READER.read_evidence(
            client,
            model="local-fixture",
            question="Where?",
            evidence=evidence,
            thinking=thinking,
            options={"temperature": 0, "seed": 42, "num_ctx": 32768, "num_predict": 9999},
        )
    )
    assert len(client.calls) == 1
    request = client.calls[0]
    assert request["think"] is thinking
    assert request["options"] == {
        "temperature": 0,
        "seed": 42,
        "num_ctx": 32768,
        "num_predict": budget,
    }
    assert request["messages"] == [
        {
            "role": "user",
            "content": READER.build_reader_prompt(question="Where?", evidence=evidence),
        }
    ]
    assert request["format"] == READER.RESPONSE_SCHEMA
    assert result.status == ("invalid" if invalid else "supported")
    assert result.requested_thinking is thinking
    assert result.requested_max_output_tokens == budget
    manifest = READER.reader_policy_manifest(thinking=thinking)
    assert manifest["requested_thinking"] is thinking
    assert manifest["requested_max_output_tokens"] == budget
    assert (
        manifest["policy_prompt_sha256"] == READER.reader_policy_manifest()["policy_prompt_sha256"]
    )
    assert result.to_dict()["requested_thinking"] is thinking


def test_thinking_default_remains_false_and_unsupported_keeps_requested_configuration():
    assert inspect.signature(READER.read_evidence).parameters["thinking"].default is False
    result = validate(
        response(support_kind="insufficient", answerable=False, answer="", supporting_quotes=[]),
        thinking=True,
    )
    assert result.status == "unsupported"
    assert result.requested_thinking is True
    assert result.requested_max_output_tokens == 2048
    assert (
        READER.PROMPT_SHA256 == "787522a7b0ffc0ae5afb8e5945c919ffaae657bd05e0f3417421b332ea05a4bb"
    )


@pytest.mark.parametrize("thinking", [None, 0, 1, "true", []])
def test_manifest_and_validation_require_strict_boolean_thinking(thinking):
    with pytest.raises(ValueError, match="thinking must be a boolean"):
        READER.reader_policy_manifest(thinking=thinking)
    with pytest.raises(ValueError, match="thinking must be a boolean"):
        validate(response(), thinking=thinking)


class ResponseClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def read_generated_response(provider_response, *, thinking=True):
    client = ResponseClient(provider_response)
    result = asyncio.run(
        READER.read_evidence(
            client,
            model="local-fixture",
            question="Where?",
            evidence="I attend pottery at Elm Studio.",
            thinking=thinking,
        )
    )
    assert len(client.calls) == 1
    return result


@pytest.mark.parametrize("as_object", [False, True])
@pytest.mark.parametrize("reason", [None, "length", "stop"])
def test_empty_final_at_budget_is_exhausted_without_exposing_reasoning(as_object, reason):
    message = {"content": "", "thinking": "private reasoning must never be retained"}
    provider = {"message": message, "done": True, "done_reason": reason, "eval_count": 2048}
    if as_object:
        provider["message"] = SimpleNamespace(**message)
        provider = SimpleNamespace(**provider)
    result = read_generated_response(provider)
    assert result.status == "invalid" and result.errors == ("output_budget_exhausted",)
    assert result.generation.done is True
    assert result.generation.done_reason == reason
    assert result.generation.eval_count == 2048
    assert result.generation.final_content_empty is True
    assert result.generation.requested_budget_reached is True
    assert result.requested_max_output_tokens == 2048
    assert "private reasoning" not in json.dumps(result.to_dict())
    with pytest.raises(READER.InvalidEvidenceReaderResponse):
        READER.render_answer(result)


@pytest.mark.parametrize("as_object", [False, True])
def test_completed_dictionary_and_sdk_style_responses_remain_supported(as_object):
    provider = {
        "message": {"content": json.dumps(response())},
        "done": True,
        "done_reason": "stop",
        "eval_count": 384,
    }
    if as_object:
        provider["message"] = SimpleNamespace(**provider["message"])
        provider = SimpleNamespace(**provider)
    result = read_generated_response(provider, thinking=False)
    assert result.status == "supported"
    assert result.generation.final_content_empty is False
    assert result.generation.requested_budget_reached is True
    assert result.generation.eval_count == 384
    assert READER.render_answer(result) == "Elm Studio"


@pytest.mark.parametrize(
    "content,metadata,error",
    [
        (
            "{",
            {"done": True, "done_reason": "length", "eval_count": 2048},
            "output_budget_exhausted",
        ),
        ("{", {"done": True, "done_reason": "stop", "eval_count": 2048}, "invalid_json"),
        ("malformed json", {"done": True, "eval_count": 12}, "invalid_json"),
        ("{", {"done": False}, "generation_incomplete"),
        (json.dumps(response()), {"done": False}, "generation_incomplete"),
        ("", {"done": True, "done_reason": "stop", "eval_count": 12}, "generation_incomplete"),
        ("  \n", {}, "generation_incomplete"),
        (None, {"done_reason": "length"}, "output_budget_exhausted"),
        (None, {}, "response_not_text"),
    ],
)
def test_generation_failures_remain_distinct_from_genuine_malformed_json(content, metadata, error):
    result = read_generated_response({"message": {"content": content}, **metadata})
    assert result.status == "invalid" and result.errors == (error,)


def test_only_allowlisted_runtime_metadata_is_read_or_serialized():
    class Message:
        content = json.dumps(response())

        @property
        def thinking(self):
            raise AssertionError("reader must not inspect reasoning")

    class Provider:
        message = Message()
        done = "true"
        done_reason = "sensitive provider detail"
        eval_count = True

        def model_dump(self):
            raise AssertionError("reader must not dump full provider response")

    result = read_generated_response(Provider())
    assert result.status == "supported"
    assert result.generation.done is None
    assert result.generation.done_reason == "other"
    assert result.generation.eval_count is None
    assert "sensitive provider detail" not in json.dumps(result.to_dict())
    assert set(result.to_dict()["generation"]) == {
        "done",
        "done_reason",
        "eval_count",
        "final_content_empty",
        "requested_budget_reached",
    }


@pytest.mark.parametrize("thinking", [False, True])
@pytest.mark.parametrize("budget", [1, 8192, 32768])
def test_explicit_output_budget_matches_request_manifest_result_and_validation(thinking, budget):
    client = Client()
    evidence = "I attend pottery at Elm Studio."
    result = asyncio.run(
        READER.read_evidence(
            client,
            model="local-fixture",
            question="Where?",
            evidence=evidence,
            thinking=thinking,
            max_output_tokens=budget,
            options={"num_predict": 99999},
        )
    )
    assert len(client.calls) == 1
    request = client.calls[0]
    assert request["options"]["num_predict"] == budget
    assert request["think"] is thinking
    assert request["messages"][0]["content"] == READER.build_reader_prompt(
        question="Where?", evidence=evidence
    )
    assert request["format"] == READER.RESPONSE_SCHEMA
    assert result.requested_max_output_tokens == budget
    manifest = READER.reader_policy_manifest(thinking=thinking, max_output_tokens=budget)
    assert manifest["requested_max_output_tokens"] == budget
    validated = validate(response(), thinking=thinking, max_output_tokens=budget)
    assert validated.requested_max_output_tokens == budget
    assert manifest["policy_prompt_sha256"] == result.policy_prompt_sha256


@pytest.mark.parametrize("budget", [False, True, 0, -1, 32769, 1.0, "8192", []])
def test_invalid_explicit_budgets_fail_before_generation_and_cannot_enter_manifests(budget):
    client = Client()
    with pytest.raises(ValueError, match="max_output_tokens"):
        asyncio.run(
            READER.read_evidence(
                client,
                model="local-fixture",
                question="Where?",
                evidence="",
                max_output_tokens=budget,
            )
        )
    assert not client.calls
    with pytest.raises(ValueError, match="max_output_tokens"):
        READER.reader_policy_manifest(max_output_tokens=budget)
    with pytest.raises(ValueError, match="max_output_tokens"):
        validate(response(), max_output_tokens=budget)


def test_exhaustion_uses_explicit_budget_not_the_default_thinking_budget():
    below = READER.validate_reader_response(
        "",
        evidence="",
        thinking=True,
        max_output_tokens=8192,
        generation_metadata={"done": True, "eval_count": 2048},
    )
    reached = READER.validate_reader_response(
        "",
        evidence="",
        thinking=True,
        max_output_tokens=8192,
        generation_metadata={"done": True, "eval_count": 8192},
    )
    assert below.errors == ("generation_incomplete",)
    assert below.generation.requested_budget_reached is False
    assert reached.errors == ("output_budget_exhausted",)
    assert reached.generation.requested_budget_reached is True
    assert reached.requested_max_output_tokens == 8192
