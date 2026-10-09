"""Optional local rank-only pair scoring; no models, downloads, or transports.

The final-token adapter consumes actual yes/no logits from a pinned host runtime.
Generated numbers from a chat model are not a compatible scoring interface.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from typing import Protocol

from ..domain import MemoryScope
from .feature_gate import (
    RetrievalFeatureApproval,
    checked_digest,
    validate_feature_enablement,
)
from .fusion import FusedCandidate


def _digest(value) -> str:
    return sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def _text(value, field, maximum=256):
    if type(value) is not str or not value.strip() or len(value) > maximum:
        raise ValueError(f"invalid {field}")
    return value


def _finite(value, field):
    if type(value) not in (float, int) or not math.isfinite(value):
        raise ValueError(f"{field} must be finite numeric data, not a boolean")
    return float(value)


@dataclass(frozen=True, slots=True)
class PairModelSpec:
    """Immutable installed model/tokenizer/prompt/runtime/quantization identity."""

    model: str
    model_sha256: str
    tokenizer_sha256: str
    prompt_sha256: str
    runtime_sha256: str
    quantization_sha256: str
    yes_token_id: int
    no_token_id: int
    execution: str = "host-local"
    scoring: str = "final-token-yes-no-logits/1"

    def __post_init__(self):
        _text(self.model, "model")
        for name in (
            "model_sha256",
            "tokenizer_sha256",
            "prompt_sha256",
            "runtime_sha256",
            "quantization_sha256",
        ):
            checked_digest(getattr(self, name))
        for name in ("yes_token_id", "no_token_id"):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value < 2**31:
                raise ValueError("invalid pinned answer token identity")
        if self.yes_token_id == self.no_token_id:
            raise ValueError("yes and no tokens must be distinct")
        if self.execution != "host-local" or self.scoring != "final-token-yes-no-logits/1":
            raise ValueError("only authorized host-local final-token scoring is supported")

    @property
    def fingerprint(self):
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class PairCandidate:
    memory_id: str
    text: str
    source_event_ids: tuple[str, ...]

    def __post_init__(self):
        _text(self.memory_id, "memory_id")
        _text(self.text, "candidate text", 1_000_000)
        if type(self.source_event_ids) is not tuple or not 1 <= len(self.source_event_ids) <= 32:
            raise ValueError("bounded immutable source identities required")
        if len(set(self.source_event_ids)) != len(self.source_event_ids):
            raise ValueError("duplicate source identity")
        for value in self.source_event_ids:
            _text(value, "source identity")


@dataclass(frozen=True, slots=True)
class PairScore:
    memory_id: str
    score: float
    probability: float | None = None

    def __post_init__(self):
        _text(self.memory_id, "memory_id")
        _finite(self.score, "pair score")
        if self.probability is not None and not 0 <= _finite(self.probability, "probability") <= 1:
            raise ValueError("pair probability must be between zero and one")


@dataclass(frozen=True, slots=True)
class FinalTokenLogits:
    memory_id: str
    yes_logit: float
    no_logit: float

    def __post_init__(self):
        _text(self.memory_id, "memory_id")
        _finite(self.yes_logit, "yes logit")
        _finite(self.no_logit, "no logit")


class PairScorer(Protocol):
    @property
    def model_spec(self) -> PairModelSpec: ...

    async def score(
        self,
        query_text: str,
        candidates: tuple[PairCandidate, ...],
    ) -> tuple[PairScore, ...]: ...


def _exact_scores(values, candidates, expected_type):
    if type(values) is not tuple or len(values) != len(candidates):
        raise ValueError("scorer must return exactly the bounded candidate tuple")
    if any(type(value) is not expected_type for value in values):
        raise ValueError("scorer returned an invalid rank-only result")
    ids = tuple(value.memory_id for value in values)
    if len(set(ids)) != len(ids) or set(ids) != {value.memory_id for value in candidates}:
        raise ValueError("scorer returned duplicate, missing, or extra identities")
    return values


class FinalTokenPairScorer:
    """Real-score adapter for an explicitly supplied local inference callable.

    The host callable must apply the pinned prompt/tokenizer and obtain logits at
    the actual final answer position. It receives the exact spec to verify runtime
    identity. The core never fabricates scores, parses chat answers, or fetches a
    model. The logit margin determines order; sigmoid is retained only as telemetry
    because floating-point probabilities can saturate and are not calibrated.
    """

    def __init__(
        self,
        model_spec: PairModelSpec,
        infer: Callable[
            [PairModelSpec, str, tuple[PairCandidate, ...]],
            Awaitable[tuple[FinalTokenLogits, ...]],
        ],
    ):
        if type(model_spec) is not PairModelSpec or not callable(infer):
            raise TypeError("pinned model spec and host-local inference callable required")
        self._model_spec, self._infer = model_spec, infer

    @property
    def model_spec(self):
        return self._model_spec

    async def score(self, query_text, candidates):
        if type(candidates) is not tuple or not 1 <= len(candidates) <= 64:
            raise ValueError("pair input must be an immutable tuple of 1 to 64 candidates")
        if any(type(value) is not PairCandidate for value in candidates):
            raise TypeError("invalid pair candidate")
        if len({value.memory_id for value in candidates}) != len(candidates):
            raise ValueError("duplicate pair candidate")
        _text(query_text, "query", 8192)
        logits = _exact_scores(
            await self._infer(self.model_spec, query_text, candidates), candidates, FinalTokenLogits
        )
        result = []
        for value in logits:
            # Validate again at the trust boundary even if a host bypassed a
            # frozen dataclass constructor. Overflowing differences are rejected.
            margin = _finite(value.yes_logit, "yes logit") - _finite(value.no_logit, "no logit")
            _finite(margin, "logit margin")
            if margin >= 0:
                probability = 1 / (1 + math.exp(-margin))
            else:
                exponent = math.exp(margin)
                probability = exponent / (1 + exponent)
            result.append(PairScore(value.memory_id, margin, probability))
        return tuple(result)


@dataclass(frozen=True, slots=True)
class PairProcessingRequest:
    scope_key: str
    candidate_ids: tuple[str, ...]
    source_event_ids: tuple[str, ...]
    payload_sha256: str
    configuration_sha256: str


class PairAuthorizationError(PermissionError):
    """Authorization, revocation, and authority failures never fall back."""


@dataclass(frozen=True, slots=True)
class PairRerankTrace:
    status: str
    activation_mode: str
    configuration_sha256: str
    scored_count: int = 0
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class PairRankingResult:
    candidates: tuple[FusedCandidate, ...]
    trace: PairRerankTrace


class CompactPairReranker:
    def __init__(
        self,
        scorer: PairScorer,
        *,
        authorize: Callable[[PairProcessingRequest], Awaitable[bool]],
        enabled: bool = False,
        max_candidates: int = 32,
        max_input_characters: int = 131_072,
        timeout_seconds: float = 10.0,
        approval: RetrievalFeatureApproval | None = None,
        verify_approval: Callable[[RetrievalFeatureApproval], bool] | None = None,
        contract_test_only: bool = False,
    ):
        if type(scorer.model_spec) is not PairModelSpec or not callable(authorize):
            raise TypeError("pinned model and explicit host processing authorization required")
        if type(max_candidates) is not int or not 1 <= max_candidates <= 64:
            raise ValueError("max_candidates must be between 1 and 64")
        if type(max_input_characters) is not int or not 1 <= max_input_characters <= 1_000_000:
            raise ValueError("invalid pair input bound")
        if not 0 < _finite(timeout_seconds, "timeout") <= 60:
            raise ValueError("timeout must be between zero and 60 seconds")
        self._scorer, self._authorize = scorer, authorize
        self._original_scorer = scorer
        self._model_spec = scorer.model_spec
        self._model_fingerprint = scorer.model_spec.fingerprint
        self._max_candidates, self._max_input = max_candidates, max_input_characters
        self._timeout = timeout_seconds
        self.configuration_sha256 = _digest(
            dict(
                model=asdict(self._model_spec),
                max_candidates=max_candidates,
                max_input_characters=max_input_characters,
                timeout_seconds=float(timeout_seconds),
                rank_policy="margin-desc-rrf-order-ties/1",
            )
        )
        self._configuration_identity = self.configuration_sha256
        self._enabled, self._contract_test_only = enabled, contract_test_only
        self._enabled_identity = enabled
        self._approval, self._verify_approval = approval, verify_approval
        self._original_verifier = verify_approval
        self._approval_identity = _digest(asdict(approval)) if approval is not None else None
        self.activation_mode = validate_feature_enablement(
            feature="pair-reranker",
            configuration_sha256=self.configuration_sha256,
            enabled=enabled,
            approval=approval,
            verify_approval=verify_approval,
            contract_test_only=contract_test_only,
        )

        self._activation_identity = self.activation_mode

    @property
    def enabled(self):
        return self._enabled_identity

    def _check_configuration_binding(self):
        current = _digest(
            dict(
                model=asdict(self._scorer.model_spec),
                max_candidates=self._max_candidates,
                max_input_characters=self._max_input,
                timeout_seconds=float(self._timeout),
                rank_policy="margin-desc-rrf-order-ties/1",
            )
        )
        if (
            self._scorer is not self._original_scorer
            or self._verify_approval is not self._original_verifier
            or current != self._configuration_identity
            or self.configuration_sha256 != self._configuration_identity
            or self.activation_mode != self._activation_identity
            or (_digest(asdict(self._approval)) if self._approval is not None else None)
            != self._approval_identity
        ):
            raise PairAuthorizationError("pair promotion configuration or approval changed")

    def require_current_approval(self):
        """Recheck live host qualification; revocation never becomes fallback.

        Called at entry, after every external await, and by the governed pipeline
        after its last source-policy await. Reconfiguration requires a new instance.
        """
        if self._enabled != self._enabled_identity:
            raise PairAuthorizationError("pair promotion activation changed")
        if not self.enabled:
            return
        try:
            self._check_configuration_binding()
            mode = validate_feature_enablement(
                feature="pair-reranker",
                configuration_sha256=self._configuration_identity,
                enabled=True,
                approval=self._approval,
                verify_approval=self._verify_approval,
                contract_test_only=self._contract_test_only,
            )
            if mode != self._activation_identity:
                raise PairAuthorizationError("pair promotion activation mode changed")
            # The synchronous verifier is host code too; it cannot mutate the
            # object it just qualified or rebind the configuration during review.
            self._check_configuration_binding()
        except PairAuthorizationError:
            raise
        except Exception as error:
            raise PairAuthorizationError(
                "pair promotion approval unavailable or revoked"
            ) from error

    async def _require_authorized(self, request):
        try:
            allowed = await self._authorize(request)
        except Exception as error:
            raise PairAuthorizationError("pair processing authority unavailable") from error
        if allowed is not True:
            raise PairAuthorizationError("pair processing unauthorized")
        self.require_current_approval()

    async def rank(self, scope, query_text, candidates):
        if type(candidates) is not tuple or len(candidates) > 256:
            raise ValueError("ranking requires a bounded candidate tuple")
        if not isinstance(scope, MemoryScope):
            raise TypeError("ranking requires a trusted scope")
        if any(type(value) is not FusedCandidate for value in candidates):
            raise TypeError("ranking requires fused candidates")
        for candidate in candidates:
            _text(candidate.item.id, "candidate identity")
            _finite(candidate.score, "fusion score")
        if len({value.item.id for value in candidates}) != len(candidates):
            raise ValueError("ranking candidate identities must be unique")
        self.require_current_approval()
        trace_args = dict(
            activation_mode=self.activation_mode, configuration_sha256=self.configuration_sha256
        )
        if not self.enabled or not candidates:
            return PairRankingResult(
                candidates,
                PairRerankTrace("disabled" if not self.enabled else "empty", **trace_args),
            )
        candidate_snapshot = deepcopy(candidates)
        head = candidates[: self._max_candidates]
        if (type(query_text) is str and len(query_text) > 8192) or any(
            len(value.source_event_ids) > 32
            or type(value.item.text) is str
            and len(value.item.text) > 1_000_000
            for value in head
        ):
            return PairRankingResult(
                candidates, PairRerankTrace("degraded", reason="input_bound", **trace_args)
            )
        pairs = tuple(
            PairCandidate(value.item.id, value.item.text, value.source_event_ids) for value in head
        )
        _text(query_text, "query", 8192)
        if sum(len(value.text) for value in pairs) + len(query_text) > self._max_input:
            return PairRankingResult(
                candidates, PairRerankTrace("degraded", reason="input_bound", **trace_args)
            )
        request = PairProcessingRequest(
            scope.partition_key(),
            tuple(value.memory_id for value in pairs),
            tuple(sorted({source for value in pairs for source in value.source_event_ids})),
            _digest(dict(query=query_text, candidates=[asdict(value) for value in pairs])),
            self.configuration_sha256,
        )
        request_fingerprint = _digest(asdict(request))
        await self._require_authorized(request)
        if _digest(asdict(request)) != request_fingerprint:
            raise PairAuthorizationError("pair authorization request changed")
        reason = None
        scores = ()
        try:
            if self._scorer.model_spec.fingerprint != self._model_fingerprint:
                raise PairAuthorizationError("scorer model configuration changed")
            async with asyncio.timeout(self._timeout):
                scores = _exact_scores(
                    await self._scorer.score(query_text, pairs), pairs, PairScore
                )
            self.require_current_approval()
            if request.payload_sha256 != _digest(
                dict(
                    query=query_text,
                    candidates=[asdict(value) for value in pairs],
                )
            ):
                raise PairAuthorizationError("pair input was altered during scoring")
            for value in scores:
                _finite(value.score, "pair score")
                if (
                    value.probability is not None
                    and not 0 <= _finite(value.probability, "probability") <= 1
                ):
                    raise ValueError("invalid pair probability")
            if self._scorer.model_spec.fingerprint != self._model_fingerprint:
                raise PairAuthorizationError("scorer model configuration changed")
        except (PairAuthorizationError, PermissionError):
            raise
        except Exception as error:
            reason = type(error).__name__
        self.require_current_approval()
        if (
            request.payload_sha256
            != _digest(
                dict(
                    query=query_text,
                    candidates=[asdict(value) for value in pairs],
                )
            )
            or self._scorer.model_spec.fingerprint != self._model_fingerprint
        ):
            raise PairAuthorizationError("pair input or configuration changed during scoring")
        # Copy validated primitives before any further external await. Even a
        # host bypassing frozen dataclasses cannot mutate our retained scores.
        score_snapshot = (
            tuple(
                PairScore(
                    value.memory_id,
                    float(value.score),
                    float(value.probability) if value.probability is not None else None,
                )
                for value in scores
            )
            if reason is None
            else ()
        )
        # Ordinary scoring errors degrade only after a fresh authorization check.
        await self._require_authorized(request)
        if (
            candidates != candidate_snapshot
            or _digest(asdict(request)) != request_fingerprint
            or self._scorer.model_spec.fingerprint != self._model_fingerprint
            or request.payload_sha256
            != _digest(dict(query=query_text, candidates=[asdict(value) for value in pairs]))
        ):
            raise PairAuthorizationError("pair authority, input or configuration changed")
        if reason:
            return PairRankingResult(
                candidates, PairRerankTrace("degraded", reason=reason, **trace_args)
            )
        by_id = {value.memory_id: value for value in score_snapshot}
        original_order = {value.item.id: i for i, value in enumerate(head)}
        ranked = (
            tuple(
                sorted(
                    head,
                    key=lambda value: (
                        -by_id[value.item.id].score,
                        original_order[value.item.id],
                        value.item.id,
                    ),
                )
            )
            + candidates[self._max_candidates :]
        )
        output = []
        for rank, candidate in enumerate(ranked, 1):
            value = by_id.get(candidate.item.id)
            output.append(
                replace(
                    candidate,
                    score=float(len(ranked) - rank + 1),
                    original_fusion_score=(
                        candidate.original_fusion_score
                        if candidate.original_fusion_score is not None
                        else candidate.score
                    ),
                    pair_score=value.score if value else None,
                    pair_probability=value.probability if value else None,
                    pair_rank=rank if value else None,
                )
            )
        return PairRankingResult(
            tuple(output), PairRerankTrace("ranked", scored_count=len(scores), **trace_args)
        )
