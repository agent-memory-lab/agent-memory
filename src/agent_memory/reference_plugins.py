from __future__ import annotations

from typing import Any

from .domain import MemoryEvent, MemoryQuery
from .plugin_protocol import (
    ConsolidationRequest,
    ConsolidationResult,
    ExtractionResult,
    PluginContext,
    PluginHealth,
    PluginHealthStatus,
)
from .plugins import (
    PluginError,
    PluginErrorCode,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)
from .ports import MemoryProvider


def _manifest(
    name: str,
    kind: PluginKind,
    capabilities: tuple[str, ...],
    *,
    failure_mode: PluginFailureMode = PluginFailureMode.FALLBACK,
) -> PluginManifest:
    return PluginManifest(
        name=name,
        version="0.1.0",
        kind=kind,
        capabilities=capabilities,
        requires={"core": ">=0.1,<1.0"},
        config_schema={"type": "object", "additionalProperties": False},
        resource_limits=PluginResourceLimits(),
        failure_mode=failure_mode,
    )


class _ReferencePlugin:
    def __init__(self, descriptor: PluginManifest) -> None:
        self._descriptor = descriptor
        self._context: PluginContext | None = None
        self._closed = False

    def plugin_manifest(self) -> PluginManifest:
        return self._descriptor

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None and not self._closed:
            raise PluginError(
                "reference plugin is already initialized",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        self._context = context
        self._closed = False

    async def health(self) -> PluginHealth:
        if self._context is None or self._closed:
            return PluginHealth(PluginHealthStatus.UNAVAILABLE, "plugin is not active")
        return PluginHealth(PluginHealthStatus.READY)

    async def close(self) -> None:
        self._closed = True

    def _require_context(self, context: PluginContext) -> None:
        if self._closed or self._context is None:
            raise PluginError(
                "reference plugin is not active",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        if context is not self._context:
            raise PluginError(
                "plugin operation used a context not issued during initialization",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if context.cancelled or context.expired:
            raise PluginError(
                "plugin operation was cancelled or expired",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )


class ReferenceCaptureAdapter(_ReferencePlugin):
    def __init__(self) -> None:
        super().__init__(
            _manifest(
                "reference-capture",
                PluginKind.CAPTURE_ADAPTER,
                ("lifecycle.capture",),
            )
        )

    async def capture(
        self,
        event: MemoryEvent,
        context: PluginContext,
    ) -> tuple[MemoryEvent, ...]:
        self._require_context(context)
        if event.scope != context.scope:
            raise PluginError(
                "capture event is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        return (event,)


class ReferenceExtractor(_ReferencePlugin):
    def __init__(self) -> None:
        super().__init__(
            _manifest(
                "reference-extractor",
                PluginKind.EXTRACTOR,
                ("claim.extract",),
            )
        )

    async def extract(
        self,
        event: MemoryEvent,
        context: PluginContext,
    ) -> ExtractionResult:
        self._require_context(context)
        if event.scope != context.scope:
            raise PluginError(
                "extract event is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        return ExtractionResult()


class ReferenceRetriever(_ReferencePlugin):
    def __init__(self) -> None:
        super().__init__(
            _manifest(
                "reference-retriever",
                PluginKind.RETRIEVER,
                ("lexical.search",),
            )
        )

    async def retrieve(
        self,
        query: MemoryQuery,
        context: PluginContext,
    ) -> tuple[()]:
        self._require_context(context)
        if query.scope != context.scope:
            raise PluginError(
                "retrieval query is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        return ()


class ReferenceConsolidator(_ReferencePlugin):
    def __init__(self) -> None:
        super().__init__(
            _manifest(
                "reference-consolidator",
                PluginKind.CONSOLIDATOR,
                ("memory.consolidate",),
            )
        )

    async def consolidate(
        self,
        request: ConsolidationRequest,
        context: PluginContext,
    ) -> ConsolidationResult:
        self._require_context(context)
        if request.scope != context.scope:
            raise PluginError(
                "consolidation request is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        return ConsolidationResult()


class ReferenceStorageProvider(_ReferencePlugin):
    def __init__(self, provider: MemoryProvider) -> None:
        super().__init__(
            _manifest(
                "reference-storage",
                PluginKind.STORAGE_PROVIDER,
                ("storage.provider",),
                failure_mode=PluginFailureMode.FAIL_CLOSED,
            )
        )
        self._provider = provider

    async def create_provider(self, context: PluginContext) -> MemoryProvider:
        self._require_context(context)
        return self._provider


class ReferenceEvaluator(_ReferencePlugin):
    def __init__(self) -> None:
        super().__init__(
            _manifest(
                "reference-evaluator",
                PluginKind.EVALUATOR,
                ("candidate.evaluate",),
                failure_mode=PluginFailureMode.FAIL_CLOSED,
            )
        )

    async def evaluate(
        self,
        request: Any,
        context: PluginContext,
    ) -> dict[str, Any]:
        self._require_context(context)
        return {
            "passed": True,
            "evaluator": "reference-evaluator",
            "request": request,
        }
