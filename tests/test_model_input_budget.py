import asyncio
import json
from dataclasses import replace

import pytest
from test_governed_models_v7 import configuration
from test_ollama_port_v7 import sealed

from agent_memory.context.model_input_budget import ModelInputBudget, TokenBudgetPort
from agent_memory.retrieval.model_contracts import ModelError, ModelResponse, canonical


class Port:
    def __init__(self, cfg, tokens):
        self.configuration, self.tokens, self.calls = cfg, tokens, 0

    async def generate(self, request):
        self.calls += 1
        return ModelResponse("ok", self.tokens, 1, 10)


def setup(tokens=40, *, count=40, verify=lambda *_: True):
    cfg = configuration()
    spec = ModelInputBudget(
        cfg.model_revision, "exact-test-tokens/1", "exact-test-renderer/1", 100, 20
    )
    cfg = replace(
        cfg,
        tokenizer_revision=spec.tokenizer_revision,
        overflow_guard_sha256=spec.fingerprint,
        options_json=canonical({"num_ctx": 100, "num_predict": 20}),
    )
    port = Port(cfg, tokens)
    counter_inputs = []

    def counter(payload):
        counter_inputs.append(json.loads(payload))
        return count

    return (
        port,
        TokenBudgetPort(port, spec, counter, verify_binding=verify),
        sealed(cfg),
        counter_inputs,
    )


def test_exact_whole_request_and_output_reservation():
    port, guarded, request, inputs = setup()
    assert guarded.validate_input(request) == 40
    assert inputs[0] == json.loads(request.payload_json)
    assert inputs[0]["messages"]
    assert asyncio.run(guarded.generate(request)).input_tokens == 40
    assert port.calls == 1


@pytest.mark.parametrize("count", [81, True, -1, 1.5])
def test_overflow_and_invalid_count_never_contact_provider(count):
    port, guarded, request, _ = setup(count=count)
    with pytest.raises(ModelError):
        asyncio.run(guarded.generate(request))
    assert port.calls == 0


def test_tokenizer_configuration_and_approval_revocation():
    live = [True]
    port, guarded, request, _ = setup(verify=lambda *_: live[0])
    live[0] = False
    with pytest.raises(ModelError, match="unapproved"):
        guarded.validate_input(request)
    live[0] = True
    port.configuration = replace(port.configuration, tokenizer_revision="another-tokenizer")
    with pytest.raises(ModelError, match="binding_changed"):
        guarded.validate_input(request)


def test_receipt_mismatch_refuses_output():
    port, guarded, request, _ = setup(tokens=41)
    with pytest.raises(ModelError, match="receipt_mismatch"):
        asyncio.run(guarded.generate(request))
    assert port.calls == 1
