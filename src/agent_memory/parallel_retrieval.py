"""Bounded parallel execution and deterministic fusion of loaded retrievers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Sequence

from .candidate_fusion import CandidateFusionResult, fuse_candidates
from .domain import MemoryQuery
from .plugin_loader import LoadedPlugin
from .plugin_protocol import RetrievalCandidate
from .plugins import PluginError, PluginErrorCode, PluginFailureMode, PluginKind


@dataclass(frozen=True, slots=True)
class RetrieverExecutionTrace:
    name: str
    status: str
    candidate_count: int
    elapsed_ms: float
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ParallelRetrievalResult:
    fusion: CandidateFusionResult
    traces: tuple[RetrieverExecutionTrace, ...]
    degraded: bool


class ParallelRetrieverOrchestrator:
    """Run one bounded retrieval wave; no retries or recursive retrieval."""

    def __init__(
        self,
        *,
        max_plugins: int = 4,
        max_candidates: int = 256,
        max_results: int = 8,
    ) -> None:
        if type(max_plugins) is not int or not 1 <= max_plugins <= 8:
            raise ValueError("max_plugins must be between 1 and 8")
        if type(max_candidates) is not int or not 1 <= max_candidates <= 256:
            raise ValueError("max_candidates must be between 1 and 256")
        if type(max_results) is not int or not 1 <= max_results <= max_candidates:
            raise ValueError("max_results must be between 1 and max_candidates")
        self._max_plugins = max_plugins
        self._max_candidates = max_candidates
        self._max_results = max_results

    async def retrieve(
        self,
        query: MemoryQuery,
        plugins: Sequence[LoadedPlugin],
    ) -> ParallelRetrievalResult:
        if not isinstance(plugins, Sequence) or isinstance(plugins, (str, bytes)):
            raise TypeError("plugins must be a bounded sequence")
        if not plugins or len(plugins) > self._max_plugins:
            raise ValueError("plugin count is outside the configured bound")
        names: set[str] = set()
        for loaded in plugins:
            if not isinstance(loaded, LoadedPlugin) or loaded.manifest.kind is not PluginKind.RETRIEVER:
                raise ValueError("all plugins must be loaded retrievers")
            if loaded.context.scope != query.scope:
                raise PluginError(
                    "retriever context is outside the query scope",
                    code=PluginErrorCode.INVALID_IMPLEMENTATION,
                    field="scope",
                )
            if loaded.manifest.name in names:
                raise ValueError("retriever names must be unique")
            names.add(loaded.manifest.name)

        tasks = [asyncio.create_task(self._run(query, loaded)) for loaded in plugins]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        candidates: list[RetrievalCandidate] = []
        traces: list[RetrieverExecutionTrace] = []
        fatal: tuple[LoadedPlugin, BaseException] | None = None
        for loaded, result in zip(plugins, results, strict=True):
            if isinstance(result, BaseException):
                traces.append(
                    RetrieverExecutionTrace(
                        name=loaded.manifest.name,
                        status="failed",
                        candidate_count=0,
                        elapsed_ms=0.0,
                        reason=type(result).__name__,
                    )
                )
                if loaded.manifest.failure_mode is PluginFailureMode.FAIL_CLOSED:
                    fatal = (loaded, result)
                continue
            batch, trace = result
            candidates.extend(batch)
            traces.append(trace)
        if fatal is not None:
            loaded, error = fatal
            raise PluginError(
                f"required retriever {loaded.manifest.name!r} failed",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            ) from error
        if len(candidates) > self._max_candidates:
            raise PluginError(
                "retrievers exceeded the global candidate budget",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        traces.sort(key=lambda trace: trace.name)
        return ParallelRetrievalResult(
            fusion=fuse_candidates(
                candidates,
                max_candidates=self._max_candidates,
                max_results=self._max_results,
            ),
            traces=tuple(traces),
            degraded=any(trace.status != "ready" for trace in traces),
        )

    @staticmethod
    async def _run(
        query: MemoryQuery, loaded: LoadedPlugin
    ) -> tuple[tuple[RetrievalCandidate, ...], RetrieverExecutionTrace]:
        started = monotonic()
        limits = loaded.context.resource_limits
        bounded_query = MemoryQuery(
            scope=query.scope,
            text=query.text,
            limit=min(query.limit, limits.max_candidates),
            token_budget=query.token_budget,
            trace_enabled=query.trace_enabled,
        )
        values = await asyncio.wait_for(
            loaded.instance.retrieve(bounded_query, loaded.context),
            timeout=limits.timeout_ms / 1_000,
        )
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            raise TypeError("retriever returned a non-sequence")
        if len(values) > bounded_query.limit:
            raise ValueError("retriever exceeded its candidate limit")
        if any(not isinstance(value, RetrievalCandidate) for value in values):
            raise TypeError("retriever returned an invalid candidate")
        elapsed = (monotonic() - started) * 1_000
        return tuple(values), RetrieverExecutionTrace(
            name=loaded.manifest.name,
            status="ready",
            candidate_count=len(values),
            elapsed_ms=elapsed,
        )
