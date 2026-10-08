"""Opt-in, stateless, buffered Ollama HTTP port using only the Python standard library.

No default endpoint, pull/download, remote session, tools, fallback, redirects,
ambient proxy, or hidden conversation state. Installation/real evaluation is a
separate host action. API contract: https://docs.ollama.com/api/chat .
"""

import asyncio
import json
from contextvars import ContextVar
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .model_contracts import ModelError, ModelResponse, canonical, digest


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ModelError("model_redirect_denied")


class OllamaPort:
    """Receives the exact already-authorized request bytes; never adds context.

    The host must exclusively manage the configured local endpoint and pin the
    model installation during execution. Pre/postflight catches ordinary model
    replacement; Ollama's tag API does not provide atomic revision execution.
    Consequently this port does not advertise strict immutable-server execution
    or exact input-token budgeting without a separately validated tokenizer.
    """

    def __init__(self, configuration):
        if configuration.provider != "ollama":
            raise ModelError("unsupported_model_provider")
        self.configuration = configuration
        # Every request, including hooks and worker-thread I/O, inherits one
        # immutable call-local recipient. Host reconfiguration affects only a
        # later call and cannot retarget a currently authorized payload.
        self._call_configuration = ContextVar("ollama_call_configuration", default=None)

    def _configuration(self):
        return self._call_configuration.get() or self.configuration

    def _http(self, route, payload=None):
        cfg = self._configuration()
        data = payload.encode() if payload is not None else None
        request = Request(
            cfg.endpoint + route, data=data, headers={"Content-Type": "application/json"}
        )
        try:
            with build_opener(ProxyHandler({}), _NoRedirect()).open(
                request, timeout=cfg.timeout_seconds
            ) as response:
                # Bound the entire provider envelope, including undisplayed thinking.
                body = response.read(2 * cfg.max_output_bytes + 65537)
            if len(body) > 2 * cfg.max_output_bytes + 65536:
                raise ModelError("model_response_budget_exceeded")
            result = json.loads(body)
            if not isinstance(result, dict):
                raise ModelError("invalid_model_response")
            return result
        except ModelError:
            raise
        except HTTPError as error:
            error.close()
            raise ModelError("model_transport_unavailable") from None
        except (URLError, OSError, TimeoutError):
            # Provider error bodies may repeat protected prompts; do not expose them.
            raise ModelError("model_transport_unavailable") from None
        except (ValueError, UnicodeError):
            raise ModelError("invalid_model_response") from None

    def _verify_installation(self):
        cfg = self._configuration()
        models = self._http("/api/tags").get("models", [])
        matches = [m for m in models if isinstance(m, dict) and m.get("name") == cfg.model]
        if (
            len(matches) != 1
            or matches[0].get("digest", "").removeprefix("sha256:") != cfg.model_revision
        ):
            raise ModelError("model_revision_mismatch")
        show = self._http("/api/show", canonical({"model": cfg.model}))
        version = self._http("/api/version")
        if digest({"show": show, "version": version}) != cfg.runtime_manifest_sha256:
            raise ModelError("model_runtime_configuration_changed")

    def _preflight(self):
        token = self._call_configuration.set(self.configuration)
        try:
            self._verify_installation()
        finally:
            self._call_configuration.reset(token)

    async def preflight(self):
        await asyncio.to_thread(self._preflight)

    def _generate(self, sealed):
        cfg = self.configuration
        if sealed.configuration != cfg:
            raise ModelError("model_configuration_mismatch")
        token = self._call_configuration.set(cfg)
        try:
            return self._generate_bound(sealed, cfg)
        finally:
            self._call_configuration.reset(token)

    def _generate_bound(self, sealed, cfg):
        payload = json.loads(sealed.payload_json)
        expected = {
            "model",
            "messages",
            "stream",
            "options",
            "think",
            "keep_alive",
            "truncate",
            "shift",
        }
        if json.loads(cfg.output_schema_json):
            expected.add("format")
        if (
            set(payload) != expected
            or payload.get("model") != cfg.model
            or payload.get("truncate") is not False
            or payload.get("shift") is not False
            or payload.get("stream") is not False
            or payload.get("options") != json.loads(cfg.options_json)
            or payload.get("think") != cfg.think
            or payload.get("keep_alive") != cfg.keep_alive
            or payload.get("format", {}) != json.loads(cfg.output_schema_json)
        ):
            raise ModelError("model_payload_configuration_mismatch")
        self._verify_installation()
        result = self._http("/api/chat", sealed.payload_json)
        self._verify_installation()
        message = result.get("message", {})
        if (
            result.get("model") != cfg.model
            or result.get("done") is not True
            or result.get("done_reason") != "stop"
            or not isinstance(message, dict)
            or message.get("role") != "assistant"
            or message.get("tool_calls")
            or not isinstance(message.get("content"), str)
        ):
            raise ModelError("model_output_incomplete_or_unsupported")
        if len(message["content"].encode()) > cfg.max_output_bytes:
            raise ModelError("model_output_budget_exceeded")
        return ModelResponse(
            message["content"],
            result.get("prompt_eval_count"),
            result.get("eval_count"),
            result.get("total_duration"),
            # Local execution has no invoice. Unpriced compute remains UNKNOWN.
        )

    async def generate(self, sealed):
        return await asyncio.to_thread(self._generate, sealed)
