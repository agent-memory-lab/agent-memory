"""Exact final-request budgeting with explicit tokenizer/renderer approval.

No tokenizer is guessed. The host approves the exact model/renderer identity;
an unapproved or mutable counter cannot advertise strict token enforcement.
"""

import inspect
import json
from dataclasses import asdict, dataclass

from ..retrieval.model_contracts import ModelError, digest, hash_value, text


@dataclass(frozen=True, slots=True)
class ModelInputBudget:
    model_revision: str
    tokenizer_revision: str
    renderer_revision: str
    context_tokens: int
    output_tokens: int
    framing_tokens: int = 0
    schema: str = "exact-model-input-budget/1"

    def __post_init__(self):
        hash_value(self.model_revision)
        text(self.tokenizer_revision)
        text(self.renderer_revision)
        for name in ("context_tokens", "output_tokens", "framing_tokens"):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= 1_048_576:
                raise ModelError("invalid_model_token_budget")
        if (
            self.schema != "exact-model-input-budget/1"
            or self.output_tokens < 1
            or self.context_tokens <= self.output_tokens + self.framing_tokens
        ):
            raise ModelError("invalid_model_token_budget")

    @property
    def fingerprint(self):
        return digest(asdict(self))


class TokenBudgetPort:
    """Guard before money reservation and again immediately before provider I/O.

    ``count_payload`` receives the canonical *whole* final request, including
    system messages, schemas and generation framing. Its renderer must implement
    the exact configured server behavior, not count JSON characters or messages
    independently. The synchronous host verifier is mandatory and checked after
    callback execution; counted data cannot be retargeted by a callback.
    """

    def __init__(self, port, budget, count_payload, *, verify_binding):
        if type(budget) is not ModelInputBudget or not callable(count_payload):
            raise ModelError("exact_token_counter_required")
        if not callable(verify_binding) or inspect.iscoroutinefunction(verify_binding):
            raise ModelError("token_binding_verifier_required")
        self.port, self.configuration, self.budget = port, port.configuration, budget
        self._counter, self._verifier = count_payload, verify_binding
        self._binding = (port, self.configuration, budget, count_payload, verify_binding)
        self._validate_binding()

    def _validate_binding(self):
        cfg, budget = self.configuration, self.budget
        if (
            (self.port, cfg, budget, self._counter, self._verifier) != self._binding
            or self.port.configuration != cfg
            or cfg.model_revision != budget.model_revision
            or cfg.tokenizer_revision != budget.tokenizer_revision
            or cfg.overflow_guard_sha256 != budget.fingerprint
            or json.loads(cfg.options_json).get("num_ctx") != budget.context_tokens
            or json.loads(cfg.options_json).get("num_predict") != budget.output_tokens
        ):
            raise ModelError("model_token_binding_changed")
        approved = self._verifier(budget, cfg)
        if inspect.isawaitable(approved):
            if inspect.iscoroutine(approved):
                approved.close()
            raise ModelError("model_token_binding_unapproved")
        if approved is not True or (
            (self.port, self.configuration, self.budget, self._counter, self._verifier)
            != self._binding
            or self.port.configuration != cfg
        ):
            raise ModelError("model_token_binding_unapproved")

    def validate_input(self, sealed):
        self._validate_binding()
        if sealed.configuration != self.configuration:
            raise ModelError("model_configuration_mismatch")
        count = self._counter(sealed.payload_json)
        if inspect.isawaitable(count):
            if inspect.iscoroutine(count):
                count.close()
            raise ModelError("synchronous_model_token_counter_required")
        if type(count) is not int or not 0 <= count <= 1_048_576:
            raise ModelError("invalid_model_token_count")
        self._validate_binding()
        if (
            count + self.budget.framing_tokens + self.budget.output_tokens
            > self.budget.context_tokens
        ):
            raise ModelError("model_input_token_budget_exceeded")
        return count + self.budget.framing_tokens

    async def preflight(self):
        self._validate_binding()
        if callable(getattr(self.port, "preflight", None)):
            await self.port.preflight()
        self._validate_binding()

    async def generate(self, sealed):
        expected = self.validate_input(sealed)
        response = await self.port.generate(sealed)
        self._validate_binding()
        if response.input_tokens != expected:
            raise ModelError("model_token_receipt_mismatch")
        return response
