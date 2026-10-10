"""Immutable buffered model contracts. Nothing here grants authority or enables a model."""

import json
from dataclasses import asdict, dataclass
from hashlib import sha256
from urllib.parse import urlsplit


class ModelError(ValueError):
    """Stable, body-free error safe for a public failure result."""

    def __init__(self, code):
        self.code = code
        super().__init__(code)


def canonical(value):
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    except (TypeError, ValueError):
        raise ModelError("invalid_model_json") from None


def digest(value):
    return sha256(canonical(value).encode()).hexdigest()


def text(value, *, limit=512):
    if type(value) is not str or not value or len(value) > limit:
        raise ModelError("invalid_model_identity")
    return value


def count(value, *, optional=False):
    if value is None and optional:
        return value
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ModelError("invalid_model_count")
    return value


def hash_value(value):
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ModelError("invalid_model_digest")
    return value


@dataclass(frozen=True, slots=True)
class ModelConfiguration:
    """All material request and recipient settings; endpoint is never guessed.

    JSON fields are immutable canonical strings, obtained with ``canonical``.
    Bytes are a declared bound, not a claim of exact tokenizer enforcement.
    ``model_revision`` pins the installed Ollama digest or approved local artifact
    manifest; it is never a mutable model tag.
    """

    provider: str
    endpoint: str
    account: str
    region: str
    processing_policy: str
    model: str
    model_revision: str
    runtime_manifest_sha256: str
    overflow_guard_sha256: str
    tokenizer_revision: str
    prompt_revision: str
    template_sha256: str
    options_json: str
    output_schema_json: str
    output_revision: str
    language: str
    max_input_bytes: int
    max_output_bytes: int
    timeout_seconds: int
    think: bool | str = False
    keep_alive: str = "0"
    tools_revision: str = "none/1"
    adapter_revision: str = "ollama-buffered/1"

    def __post_init__(self):
        for key in (
            "provider",
            "endpoint",
            "account",
            "region",
            "processing_policy",
            "model",
            "tokenizer_revision",
            "prompt_revision",
            "output_revision",
            "language",
            "keep_alive",
            "tools_revision",
            "adapter_revision",
        ):
            text(getattr(self, key))
        for key in (
            "model_revision",
            "runtime_manifest_sha256",
            "template_sha256",
            "overflow_guard_sha256",
        ):
            hash_value(getattr(self, key))
        parsed = urlsplit(self.endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ModelError("invalid_model_endpoint")
        object.__setattr__(self, "endpoint", self.endpoint.rstrip("/"))
        for key in ("options_json", "output_schema_json"):
            try:
                value = json.loads(getattr(self, key))
            except (TypeError, ValueError):
                raise ModelError("invalid_model_json") from None
            if not isinstance(value, dict):
                raise ModelError("invalid_model_json")
            object.__setattr__(self, key, canonical(value))
        options = json.loads(self.options_json)
        if any(type(v) not in (int, float, bool, str, list) for v in options.values()):
            raise ModelError("unsupported_model_option")
        if type(options.get("num_predict")) is not int or options["num_predict"] < 1:
            raise ModelError("model_output_token_reservation_required")
        if type(options.get("num_ctx")) is not int or options["num_ctx"] < 1:
            raise ModelError("model_context_limit_required")
        if self.tools_revision != "none/1" or self.adapter_revision not in {
            "ollama-buffered/1",
            "local-transformers/1",
        }:
            raise ModelError("unsupported_model_configuration")
        if type(self.think) not in (bool, str) or isinstance(self.think, str) and not self.think:
            raise ModelError("invalid_model_thinking_mode")
        for key, maximum in (
            ("max_input_bytes", 1048576),
            ("max_output_bytes", 1048576),
            ("timeout_seconds", 600),
        ):
            if not 1 <= count(getattr(self, key)) <= maximum:
                raise ModelError("invalid_model_limit")

    @property
    def fingerprint(self):
        return digest(asdict(self))

    @property
    def recipient(self):
        return digest(
            {
                k: getattr(self, k)
                for k in ("provider", "endpoint", "account", "region", "processing_policy")
            }
        )


@dataclass(frozen=True, slots=True)
class ModelCoordinates:
    """Trusted, exact coordinates. No semantic-similarity or time-bucket keying."""

    scope_key: str
    principal: str
    project: str
    purpose: str
    audience: str
    question_definition: str
    question_version: str
    parameters_json: str
    task_intent: str
    context_sha256: str
    policy_sha256: str
    query_proof_sha256: str
    valid_at: str
    known_at: str

    def __post_init__(self):
        from datetime import datetime

        for key in self.__dataclass_fields__:
            text(getattr(self, key), limit=8192)
        for key in ("context_sha256", "policy_sha256", "query_proof_sha256"):
            hash_value(getattr(self, key))
        try:
            value = json.loads(self.parameters_json)
            if not isinstance(value, dict):
                raise ValueError
            object.__setattr__(self, "parameters_json", canonical(value))
            for key in ("valid_at", "known_at"):
                at = datetime.fromisoformat(getattr(self, key))
                if at.utcoffset() is None:
                    raise ValueError
        except (TypeError, ValueError):
            raise ModelError("invalid_model_coordinates") from None

    def payload(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SealedModelInput:
    """Created by a trusted assembler; the governor must revalidate its proof."""

    configuration: ModelConfiguration
    coordinates: ModelCoordinates
    payload_json: str
    manifest_json: str

    def __post_init__(self):
        if (
            type(self.configuration) is not ModelConfiguration
            or type(self.coordinates) is not ModelCoordinates
        ):
            raise ModelError("invalid_model_input")
        for key in ("payload_json", "manifest_json"):
            try:
                object.__setattr__(self, key, canonical(json.loads(getattr(self, key))))
            except (TypeError, ValueError):
                raise ModelError("invalid_model_input") from None
        if len(self.payload_json.encode()) > self.configuration.max_input_bytes:
            raise ModelError("model_input_budget_exceeded")
        manifest = json.loads(self.manifest_json)
        payload = json.loads(self.payload_json)
        if not isinstance(manifest, dict) or not isinstance(payload, dict):
            raise ModelError("invalid_model_input")
        messages = payload.get("messages")
        if (
            not isinstance(messages, list)
            or not 1 <= len(messages) <= 257
            or any(
                not isinstance(message, dict)
                or set(message) != {"role", "content"}
                or message["role"] not in {"system", "user"}
                or not isinstance(message["content"], str)
                for message in messages
            )
        ):
            raise ModelError("model_hidden_context_unsupported")
        if (
            manifest.get("schema") != "model-input-manifest/1"
            or manifest.get("payload_sha256") != sha256(self.payload_json.encode()).hexdigest()
            or manifest.get("configuration_sha256") != self.configuration.fingerprint
            or manifest.get("coordinates_sha256") != digest(self.coordinates.payload())
            or not isinstance(manifest.get("sources"), dict)
        ):
            raise ModelError("model_manifest_invalid")

    @property
    def key(self):
        return digest(
            {
                "schema": "exact-model-answer/1",
                "configuration": self.configuration.fingerprint,
                "coordinates": self.coordinates.payload(),
                "manifest": json.loads(self.manifest_json),
            }
        )

    @property
    def payload_sha256(self):
        return sha256(self.payload_json.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class ModelResponse:
    text: str
    input_tokens: int | None
    output_tokens: int | None
    total_duration_ns: int | None
    provider_request_id: str | None = None
    actual_microunits: int | None = None
    cost_evidence: str | None = None

    def __post_init__(self):
        if type(self.text) is not str:
            raise ModelError("invalid_model_output")
        for key in ("input_tokens", "output_tokens", "total_duration_ns", "actual_microunits"):
            count(getattr(self, key), optional=True)
        if self.actual_microunits is not None and not self.cost_evidence:
            raise ModelError("model_cost_evidence_required")
        if self.provider_request_id is not None:
            text(self.provider_request_id)


@dataclass(frozen=True, slots=True)
class ModelAnswer:
    text: str
    key: str
    call_id: str
    delivery_id: str
    cache_hit: bool
    cost_status: str
