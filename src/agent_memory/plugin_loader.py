from __future__ import annotations

import asyncio
from collections.abc import Callable, Collection
from dataclasses import dataclass
from importlib.metadata import EntryPoint, PackageNotFoundError, entry_points, version
import inspect
import re
from typing import Any

from .plugin_protocol import (
    CaptureAdapterPlugin,
    ConsolidatorPlugin,
    EvaluatorPlugin,
    ExtractorPlugin,
    PluginContext,
    PluginHealth,
    PluginHealthStatus,
    RetrieverPlugin,
    StorageProviderPlugin,
)
from .plugins import (
    PLUGIN_API_VERSION,
    PluginError,
    PluginErrorCode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)

PLUGIN_ENTRY_POINT_GROUPS = {
    PluginKind.CAPTURE_ADAPTER: "agent_memory.capture",
    PluginKind.EXTRACTOR: "agent_memory.extractors",
    PluginKind.RETRIEVER: "agent_memory.retrievers",
    PluginKind.CONSOLIDATOR: "agent_memory.consolidators",
    PluginKind.STORAGE_PROVIDER: "agent_memory.storage",
    PluginKind.EVALUATOR: "agent_memory.evaluators",
}

_PROTOCOLS = {
    PluginKind.CAPTURE_ADAPTER: CaptureAdapterPlugin,
    PluginKind.EXTRACTOR: ExtractorPlugin,
    PluginKind.RETRIEVER: RetrieverPlugin,
    PluginKind.CONSOLIDATOR: ConsolidatorPlugin,
    PluginKind.STORAGE_PROVIDER: StorageProviderPlugin,
    PluginKind.EVALUATOR: EvaluatorPlugin,
}

PluginFactory = Callable[[], Any]
_VERSION_PATTERN = re.compile(
    r"(?P<major>0|[1-9]\d*)"
    r"(?:\.(?P<minor>0|[1-9]\d*))?"
    r"(?:\.(?P<patch>0|[1-9]\d*))?"
    r"(?:-[0-9A-Za-z.-]+)?\Z"
)
_SPECIFIER_PATTERN = re.compile(r"(?P<operator>==|!=|>=|<=|>|<|~=)(?P<version>.+)\Z")


@dataclass(frozen=True, slots=True)
class PluginCandidateReference:
    name: str
    kind: PluginKind
    group: str
    target: str
    distribution: str | None
    priority: int = 0


@dataclass(frozen=True, slots=True)
class PluginLoadRequest:
    name: str
    kind: PluginKind
    context: PluginContext
    required_capabilities: tuple[str, ...] = ()
    distribution: str | None = None


@dataclass(frozen=True, slots=True)
class LoadedPlugin:
    reference: PluginCandidateReference
    manifest: PluginManifest
    instance: Any
    context: PluginContext
    health: PluginHealth


@dataclass(frozen=True, slots=True)
class _RegisteredPlugin:
    reference: PluginCandidateReference
    factory: PluginFactory


def installed_core_version() -> str:
    try:
        return version("agent-memory")
    except PackageNotFoundError:
        return "0.1.0"


def _parse_version(value: str) -> tuple[int, int, int]:
    matched = _VERSION_PATTERN.fullmatch(value)
    if matched is None:
        raise ValueError(f"unsupported version {value!r}")
    return (
        int(matched.group("major")),
        int(matched.group("minor") or 0),
        int(matched.group("patch") or 0),
    )


def version_satisfies(value: str, constraint: str) -> bool:
    candidate = _parse_version(value)
    for clause in constraint.split(","):
        matched = _SPECIFIER_PATTERN.fullmatch(clause)
        if matched is None:
            return False
        operator = matched.group("operator")
        raw_required = matched.group("version")
        required = _parse_version(raw_required)
        if operator == "==" and candidate != required:
            return False
        if operator == "!=" and candidate == required:
            return False
        if operator == ">=" and candidate < required:
            return False
        if operator == "<=" and candidate > required:
            return False
        if operator == ">" and candidate <= required:
            return False
        if operator == "<" and candidate >= required:
            return False
        if operator == "~=":
            release_parts = raw_required.split("-")[0].split(".")
            upper = (
                (required[0] + 1, 0, 0)
                if len(release_parts) < 3
                else (required[0], required[1] + 1, 0)
            )
            if candidate < required or candidate >= upper:
                return False
    return True


def _effective_limits(
    host: PluginResourceLimits,
    plugin: PluginResourceLimits,
) -> PluginResourceLimits:
    return PluginResourceLimits(
        timeout_ms=min(host.timeout_ms, plugin.timeout_ms),
        max_candidates=min(host.max_candidates, plugin.max_candidates),
        max_batch_size=min(host.max_batch_size, plugin.max_batch_size),
        max_concurrency=min(host.max_concurrency, plugin.max_concurrency),
    )


class PluginLoader:
    """Lazy, bounded loader for Plugin Protocol v1 implementations."""

    def __init__(self, *, core_version: str | None = None) -> None:
        resolved_version = core_version or installed_core_version()
        try:
            _parse_version(resolved_version)
        except ValueError as error:
            raise PluginError(
                "core version must be a semantic release",
                code=PluginErrorCode.INCOMPATIBLE_PLUGIN_API,
            ) from error
        self._core_version = resolved_version
        self._registered: list[_RegisteredPlugin] = []
        self._loaded: list[LoadedPlugin] = []

    @property
    def core_version(self) -> str:
        return self._core_version

    @property
    def loaded(self) -> tuple[LoadedPlugin, ...]:
        return tuple(self._loaded)

    def register(
        self,
        *,
        name: str,
        kind: PluginKind,
        factory: PluginFactory,
        distribution: str | None = None,
        priority: int = 100,
    ) -> None:
        try:
            resolved_kind = PluginKind(kind)
        except (TypeError, ValueError) as error:
            raise PluginError(
                f"unknown plugin kind {kind!r}",
                code=PluginErrorCode.INVALID_MANIFEST,
                field="kind",
            ) from error
        if not isinstance(name, str) or not name.strip():
            raise PluginError(
                "plugin registration name must be non-empty",
                code=PluginErrorCode.INVALID_MANIFEST,
                field="name",
            )
        if not callable(factory):
            raise PluginError(
                f"plugin factory for {name!r} is not callable",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if type(priority) is not int:
            raise PluginError(
                "plugin priority must be an integer",
                code=PluginErrorCode.INVALID_MANIFEST,
                field="priority",
            )
        group = PLUGIN_ENTRY_POINT_GROUPS[resolved_kind]
        target = f"{getattr(factory, '__module__', '<unknown>')}:"
        target += getattr(factory, "__qualname__", type(factory).__qualname__)
        reference = PluginCandidateReference(
            name=name,
            kind=resolved_kind,
            group=group,
            target=target,
            distribution=distribution,
            priority=priority,
        )
        if any(item.reference == reference for item in self._registered):
            raise PluginError(
                f"plugin {name!r} is already registered for {resolved_kind.value}",
                code=PluginErrorCode.DUPLICATE_PLUGIN,
            )
        self._registered.append(_RegisteredPlugin(reference, factory))

    def references(
        self,
        *,
        kind: PluginKind | None = None,
    ) -> tuple[PluginCandidateReference, ...]:
        kinds = (PluginKind(kind),) if kind is not None else tuple(PluginKind)
        references = [
            item.reference for item in self._registered if item.reference.kind in kinds
        ]
        for candidate_kind in kinds:
            group = PLUGIN_ENTRY_POINT_GROUPS[candidate_kind]
            for item in entry_points(group=group):
                distribution = item.dist.name if item.dist is not None else None
                references.append(
                    PluginCandidateReference(
                        name=item.name,
                        kind=candidate_kind,
                        group=group,
                        target=item.value,
                        distribution=distribution,
                    )
                )
        return tuple(
            sorted(
                references,
                key=lambda item: (
                    item.kind.value,
                    item.name,
                    -item.priority,
                    item.distribution or "",
                    item.target,
                ),
            )
        )

    async def load(
        self,
        name: str,
        kind: PluginKind,
        context: PluginContext,
        *,
        required_capabilities: Collection[str] = (),
        distribution: str | None = None,
    ) -> LoadedPlugin:
        resolved_kind = PluginKind(kind)
        reference, factory = self._select(name, resolved_kind, distribution)
        instance = await self._create_instance(reference, factory)
        protocol = _PROTOCOLS[resolved_kind]
        if not isinstance(instance, protocol):
            raise PluginError(
                f"plugin {name!r} does not implement the {resolved_kind.value} protocol",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        try:
            manifest = instance.plugin_manifest()
        except Exception as error:
            raise PluginError(
                f"plugin {name!r} failed to provide its manifest",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            ) from error
        if not isinstance(manifest, PluginManifest):
            raise PluginError(
                f"plugin {name!r} returned an invalid manifest type",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        self._validate_manifest(reference, manifest, required_capabilities)
        effective_context = PluginContext(
            scope=context.scope,
            resource_limits=_effective_limits(context.resource_limits, manifest.resource_limits),
            config=context.config,
            request_id=context.request_id,
            deadline=context.deadline,
            logger=context.logger,
            clock=context.clock,
            cancellation=context.cancellation,
        )
        timeout = effective_context.resource_limits.timeout_ms / 1_000
        try:
            await asyncio.wait_for(instance.initialize(effective_context), timeout=timeout)
            health = await asyncio.wait_for(instance.health(), timeout=timeout)
            if not isinstance(health, PluginHealth):
                raise TypeError("health() did not return PluginHealth")
            if health.status is PluginHealthStatus.UNAVAILABLE:
                raise RuntimeError("plugin reported unavailable")
        except Exception as error:
            await self._close_instance(instance, timeout=timeout)
            raise PluginError(
                f"plugin {name!r} failed initialization or health checks",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            ) from error
        loaded = LoadedPlugin(reference, manifest, instance, effective_context, health)
        self._loaded.append(loaded)
        return loaded

    async def load_many(
        self,
        requests: Collection[PluginLoadRequest],
    ) -> tuple[LoadedPlugin, ...]:
        loaded_now: list[LoadedPlugin] = []
        try:
            for request in requests:
                loaded_now.append(
                    await self.load(
                        request.name,
                        request.kind,
                        request.context,
                        required_capabilities=request.required_capabilities,
                        distribution=request.distribution,
                    )
                )
        except BaseException as error:
            rollback_failures: list[str] = []
            for loaded in reversed(loaded_now):
                try:
                    await self._close_loaded(loaded)
                except Exception:
                    rollback_failures.append(loaded.manifest.name)
            if rollback_failures:
                error.add_note(
                    "plugin rollback also failed for: " + ", ".join(rollback_failures)
                )
            raise
        return tuple(loaded_now)

    async def health(self) -> dict[str, PluginHealth]:
        results: dict[str, PluginHealth] = {}
        for loaded in self._loaded:
            key = f"{loaded.manifest.kind.value}:{loaded.manifest.name}"
            timeout = loaded.context.resource_limits.timeout_ms / 1_000
            try:
                result = await asyncio.wait_for(loaded.instance.health(), timeout=timeout)
                if not isinstance(result, PluginHealth):
                    raise TypeError("health() did not return PluginHealth")
            except Exception:
                result = PluginHealth(
                    PluginHealthStatus.UNAVAILABLE,
                    "plugin health check failed",
                )
            results[key] = result
        return results

    async def close(self) -> None:
        failures: list[str] = []
        for loaded in reversed(tuple(self._loaded)):
            try:
                await self._close_loaded(loaded)
            except Exception:
                failures.append(loaded.manifest.name)
        if failures:
            raise PluginError(
                f"failed to close plugins: {', '.join(failures)}",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )

    def _select(
        self,
        name: str,
        kind: PluginKind,
        distribution: str | None,
    ) -> tuple[PluginCandidateReference, PluginFactory | EntryPoint]:
        candidates: list[tuple[PluginCandidateReference, PluginFactory | EntryPoint]] = []
        for item in self._registered:
            if item.reference.name == name and item.reference.kind is kind:
                candidates.append((item.reference, item.factory))
        group = PLUGIN_ENTRY_POINT_GROUPS[kind]
        for item in entry_points(group=group):
            if item.name != name:
                continue
            item_distribution = item.dist.name if item.dist is not None else None
            reference = PluginCandidateReference(
                name=item.name,
                kind=kind,
                group=group,
                target=item.value,
                distribution=item_distribution,
            )
            candidates.append((reference, item))
        if distribution is not None:
            candidates = [
                candidate
                for candidate in candidates
                if candidate[0].distribution == distribution
            ]
        if not candidates:
            raise PluginError(
                f"unknown {kind.value} plugin {name!r}",
                code=PluginErrorCode.UNKNOWN_PLUGIN,
            )
        highest_priority = max(reference.priority for reference, _ in candidates)
        selected = [item for item in candidates if item[0].priority == highest_priority]
        if len(selected) != 1:
            distributions = sorted(
                {reference.distribution or "<unknown>" for reference, _ in selected}
            )
            raise PluginError(
                f"multiple {kind.value} plugins use {name!r}: {', '.join(distributions)}",
                code=PluginErrorCode.DUPLICATE_PLUGIN,
            )
        return selected[0]

    async def _create_instance(
        self,
        reference: PluginCandidateReference,
        source: PluginFactory | EntryPoint,
    ) -> Any:
        try:
            factory = source.load() if isinstance(source, EntryPoint) else source
            created = factory() if callable(factory) else factory
            return await created if inspect.isawaitable(created) else created
        except Exception as error:
            raise PluginError(
                f"failed to load plugin {reference.name!r}",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            ) from error

    def _validate_manifest(
        self,
        reference: PluginCandidateReference,
        manifest: PluginManifest,
        required_capabilities: Collection[str],
    ) -> None:
        if manifest.plugin_api != PLUGIN_API_VERSION:
            raise PluginError(
                f"plugin {manifest.name!r} requires plugin API {manifest.plugin_api}; "
                f"host provides {PLUGIN_API_VERSION}",
                code=PluginErrorCode.INCOMPATIBLE_PLUGIN_API,
                field="plugin_api",
            )
        if manifest.name != reference.name or manifest.kind is not reference.kind:
            raise PluginError(
                "plugin manifest identity does not match its registration",
                code=PluginErrorCode.INVALID_MANIFEST,
            )
        if not version_satisfies(self._core_version, manifest.requires["core"]):
            raise PluginError(
                f"plugin {manifest.name!r} is incompatible with core {self._core_version}",
                code=PluginErrorCode.INCOMPATIBLE_PLUGIN_API,
                field="requires.core",
            )
        requested = tuple(required_capabilities)
        if any(not isinstance(capability, str) for capability in requested):
            raise PluginError(
                "required capabilities must be strings",
                code=PluginErrorCode.INVALID_MANIFEST,
                field="capabilities",
            )
        missing = sorted(set(requested) - set(manifest.capabilities))
        if missing:
            raise PluginError(
                f"plugin {manifest.name!r} lacks capabilities: {', '.join(missing)}",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="capabilities",
            )

    async def _close_loaded(self, loaded: LoadedPlugin) -> None:
        timeout = loaded.context.resource_limits.timeout_ms / 1_000
        await asyncio.wait_for(loaded.instance.close(), timeout=timeout)
        if loaded in self._loaded:
            self._loaded.remove(loaded)

    @staticmethod
    async def _close_instance(instance: Any, *, timeout: float) -> None:
        try:
            await asyncio.wait_for(instance.close(), timeout=timeout)
        except Exception:
            return None
