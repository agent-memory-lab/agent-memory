from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass
from enum import StrEnum
from importlib.metadata import entry_points
import math
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Mapping

from .composition import build_local_kernel
from .ports import ClaimExtractor, EmbeddingProvider, MemoryPolicy, MemoryProvider, Reranker

PROVIDER_GROUP = "agent_memory.providers"
INTEGRATION_GROUP = "agent_memory.integrations"
PLUGIN_API_VERSION = 1

_NAME_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*\Z")
_CAPABILITY_PATTERN = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\Z")
_SEMVER_PATTERN = re.compile(
    r"(?:0|[1-9]\d*)\."
    r"(?:0|[1-9]\d*)\."
    r"(?:0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
_VERSION_SPECIFIER_PATTERN = re.compile(
    r"(?:==|!=|>=|<=|>|<|~=)"
    r"(?:0|[1-9]\d*)"
    r"(?:\.(?:0|[1-9]\d*)){0,2}"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)


class PluginKind(StrEnum):
    CAPTURE_ADAPTER = "capture_adapter"
    EXTRACTOR = "extractor"
    RETRIEVER = "retriever"
    CONSOLIDATOR = "consolidator"
    STORAGE_PROVIDER = "storage_provider"
    EVALUATOR = "evaluator"


class PluginFailureMode(StrEnum):
    FALLBACK = "fallback"
    FAIL_CLOSED = "fail_closed"


class PluginErrorCode(StrEnum):
    INVALID_MANIFEST = "invalid_manifest"
    INCOMPATIBLE_PLUGIN_API = "incompatible_plugin_api"
    UNKNOWN_PLUGIN = "unknown_plugin"
    DUPLICATE_PLUGIN = "duplicate_plugin"
    INVALID_IMPLEMENTATION = "invalid_implementation"
    PLUGIN_LOAD_FAILED = "plugin_load_failed"


class PluginError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: PluginErrorCode = PluginErrorCode.PLUGIN_LOAD_FAILED,
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.field = field

    def to_dict(self) -> dict[str, str]:
        payload = {"code": self.code.value, "message": str(self)}
        if self.field is not None:
            payload["field"] = self.field
        return payload


class PluginManifestError(PluginError):
    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message, code=PluginErrorCode.INVALID_MANIFEST, field=field)


def _manifest_error(field: str, message: str) -> PluginManifestError:
    return PluginManifestError(f"invalid plugin manifest field {field!r}: {message}", field=field)


def _validate_name(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _NAME_PATTERN.fullmatch(value):
        raise _manifest_error(
            field,
            "must start with a lowercase letter and contain lowercase letters, digits, '.', "
            "'_' or '-'",
        )
    return value


def _validate_version_spec(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise _manifest_error(field, "must be a non-empty version constraint")
    clauses = value.split(",")
    if any(not clause or not _VERSION_SPECIFIER_PATTERN.fullmatch(clause) for clause in clauses):
        raise _manifest_error(
            field,
            "must be comma-separated constraints such as '>=0.2,<1.0'",
        )
    return value


def _freeze_json(value: Any, *, field: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _manifest_error(field, "must contain only finite JSON numbers")
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _manifest_error(field, "must contain only string object keys")
            frozen[key] = _freeze_json(item, field=field)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, field=field) for item in value)
    raise _manifest_error(field, f"contains non-JSON value of type {type(value).__name__}")


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class PluginResourceLimits:
    timeout_ms: int = 1_000
    max_candidates: int = 100
    max_batch_size: int = 100
    max_concurrency: int = 4

    def __post_init__(self) -> None:
        limits = {
            "timeout_ms": (self.timeout_ms, 1, 300_000),
            "max_candidates": (self.max_candidates, 1, 10_000),
            "max_batch_size": (self.max_batch_size, 1, 10_000),
            "max_concurrency": (self.max_concurrency, 1, 256),
        }
        for field, (value, minimum, maximum) in limits.items():
            if type(value) is not int or not minimum <= value <= maximum:
                raise _manifest_error(
                    f"resource_limits.{field}",
                    f"must be an integer between {minimum} and {maximum}",
                )

    def to_dict(self) -> dict[str, int]:
        return {
            "timeout_ms": self.timeout_ms,
            "max_candidates": self.max_candidates,
            "max_batch_size": self.max_batch_size,
            "max_concurrency": self.max_concurrency,
        }


@dataclass(frozen=True, slots=True)
class PluginManifest:
    name: str
    version: str
    kind: PluginKind
    capabilities: tuple[str, ...]
    requires: Mapping[str, str]
    config_schema: Mapping[str, Any]
    resource_limits: PluginResourceLimits
    failure_mode: PluginFailureMode
    plugin_api: int = PLUGIN_API_VERSION

    def __post_init__(self) -> None:
        _validate_name(self.name, field="name")
        if not isinstance(self.version, str) or not _SEMVER_PATTERN.fullmatch(self.version):
            raise _manifest_error("version", "must be a semantic version such as '1.2.3'")
        if type(self.plugin_api) is not int or self.plugin_api < 1:
            raise _manifest_error("plugin_api", "must be a positive integer")

        try:
            kind = PluginKind(self.kind)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in PluginKind)
            raise _manifest_error("kind", f"must be one of: {allowed}") from exc
        object.__setattr__(self, "kind", kind)

        if isinstance(self.capabilities, str):
            raise _manifest_error("capabilities", "must be a collection, not a string")
        try:
            capabilities = tuple(self.capabilities)
        except TypeError as exc:
            raise _manifest_error("capabilities", "must be a collection of strings") from exc
        if not capabilities:
            raise _manifest_error("capabilities", "must contain at least one capability")
        for capability in capabilities:
            if not isinstance(capability, str) or not _CAPABILITY_PATTERN.fullmatch(capability):
                raise _manifest_error(
                    "capabilities",
                    "entries must use lowercase dotted names such as 'semantic.search'",
                )
        if len(set(capabilities)) != len(capabilities):
            raise _manifest_error("capabilities", "must not contain duplicate entries")
        object.__setattr__(self, "capabilities", capabilities)

        if not isinstance(self.requires, Mapping):
            raise _manifest_error("requires", "must be an object")
        requirements: dict[str, str] = {}
        for dependency, constraint in self.requires.items():
            name = _validate_name(dependency, field="requires")
            requirements[name] = _validate_version_spec(
                constraint,
                field=f"requires.{name}",
            )
        if "core" not in requirements:
            raise _manifest_error("requires", "must declare a 'core' version constraint")
        object.__setattr__(self, "requires", MappingProxyType(requirements))

        if not isinstance(self.config_schema, Mapping):
            raise _manifest_error("config_schema", "must be a JSON object")
        config_schema = _freeze_json(self.config_schema, field="config_schema")
        if config_schema.get("type", "object") != "object":
            raise _manifest_error("config_schema", "top-level schema type must be 'object'")
        object.__setattr__(self, "config_schema", config_schema)

        if not isinstance(self.resource_limits, PluginResourceLimits):
            raise _manifest_error(
                "resource_limits",
                "must be a PluginResourceLimits instance",
            )

        try:
            failure_mode = PluginFailureMode(self.failure_mode)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in PluginFailureMode)
            raise _manifest_error("failure_mode", f"must be one of: {allowed}") from exc
        object.__setattr__(self, "failure_mode", failure_mode)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PluginManifest:
        if not isinstance(value, Mapping):
            raise PluginManifestError("plugin manifest must be an object")
        if any(not isinstance(key, str) for key in value):
            raise PluginManifestError("plugin manifest must contain only string field names")
        required = {
            "plugin_api",
            "name",
            "version",
            "kind",
            "capabilities",
            "requires",
            "config_schema",
            "resource_limits",
            "failure_mode",
        }
        unknown = set(value) - required
        missing = required - set(value)
        if unknown:
            raise PluginManifestError(
                f"plugin manifest contains unknown fields: {', '.join(sorted(unknown))}"
            )
        if missing:
            raise PluginManifestError(
                f"plugin manifest is missing fields: {', '.join(sorted(missing))}"
            )
        raw_capabilities = value["capabilities"]
        if not isinstance(raw_capabilities, (list, tuple)):
            raise _manifest_error("capabilities", "must be a JSON array of strings")
        raw_limits = value["resource_limits"]
        if not isinstance(raw_limits, Mapping):
            raise _manifest_error("resource_limits", "must be an object")
        if any(not isinstance(key, str) for key in raw_limits):
            raise _manifest_error("resource_limits", "must contain only string field names")
        limit_fields = {
            "timeout_ms",
            "max_candidates",
            "max_batch_size",
            "max_concurrency",
        }
        unknown_limits = set(raw_limits) - limit_fields
        missing_limits = limit_fields - set(raw_limits)
        if unknown_limits:
            raise _manifest_error(
                "resource_limits",
                f"contains unknown fields: {', '.join(sorted(unknown_limits))}",
            )
        if missing_limits:
            raise _manifest_error(
                "resource_limits",
                f"is missing fields: {', '.join(sorted(missing_limits))}",
            )
        return cls(
            plugin_api=value["plugin_api"],
            name=value["name"],
            version=value["version"],
            kind=value["kind"],
            capabilities=tuple(raw_capabilities),
            requires=value["requires"],
            config_schema=value["config_schema"],
            resource_limits=PluginResourceLimits(**dict(raw_limits)),
            failure_mode=value["failure_mode"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_api": self.plugin_api,
            "name": self.name,
            "version": self.version,
            "kind": self.kind.value,
            "capabilities": list(self.capabilities),
            "requires": dict(self.requires),
            "config_schema": _thaw_json(self.config_schema),
            "resource_limits": self.resource_limits.to_dict(),
            "failure_mode": self.failure_mode.value,
        }


@dataclass(frozen=True, slots=True)
class PluginReference:
    name: str
    group: str
    target: str
    distribution: str | None = None


def build_sqlite_plugin(
    *,
    database_path: str | Path = "agent-memory.db",
    extractor: ClaimExtractor | None = None,
    policy: MemoryPolicy | None = None,
    reranker: Reranker | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    trusted_evaluator_ids: Collection[str] | None = None,
    **_: Any,
) -> MemoryProvider:
    return build_local_kernel(
        database_path,
        extractor=extractor,
        policy=policy,
        reranker=reranker,
        embedding_provider=embedding_provider,
        trusted_evaluator_ids=trusted_evaluator_ids,
    )


class PluginRegistry:
    """Discover metadata without importing optional plugin packages."""

    _builtin_providers: dict[str, Callable[..., MemoryProvider]] = {"sqlite": build_sqlite_plugin}

    def references(self, group: str) -> tuple[PluginReference, ...]:
        discovered: dict[str, PluginReference] = {}
        if group == PROVIDER_GROUP:
            discovered["sqlite"] = PluginReference(
                "sqlite", group, "agent_memory.plugins:build_sqlite_plugin", "agent-memory"
            )
        for item in entry_points(group=group):
            distribution = item.dist.name if item.dist is not None else None
            discovered.setdefault(
                item.name,
                PluginReference(item.name, group, item.value, distribution),
            )
        return tuple(discovered[name] for name in sorted(discovered))

    def create_provider(self, name: str, **config: Any) -> MemoryProvider:
        factory = self._builtin_providers.get(name)
        if factory is None:
            matches = [item for item in entry_points(group=PROVIDER_GROUP) if item.name == name]
            if not matches:
                available = ", ".join(item.name for item in self.references(PROVIDER_GROUP))
                raise PluginError(
                    f"unknown provider {name!r}; available: {available}",
                    code=PluginErrorCode.UNKNOWN_PLUGIN,
                )
            if len(matches) > 1:
                raise PluginError(
                    f"multiple installed providers use the name {name!r}",
                    code=PluginErrorCode.DUPLICATE_PLUGIN,
                )
            factory = matches[0].load()
        provider = factory(**config)
        required = ("initialize", "ingest_event", "retrieve", "manifest")
        if any(not hasattr(provider, attribute) for attribute in required):
            raise PluginError(
                f"provider plugin {name!r} does not implement MemoryProvider",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        return provider

    def load_integration(self, name: str) -> Any:
        matches = [item for item in entry_points(group=INTEGRATION_GROUP) if item.name == name]
        if not matches:
            available = ", ".join(item.name for item in self.references(INTEGRATION_GROUP))
            raise PluginError(
                f"unknown integration {name!r}; available: {available}",
                code=PluginErrorCode.UNKNOWN_PLUGIN,
            )
        if len(matches) > 1:
            raise PluginError(
                f"multiple installed integrations use the name {name!r}",
                code=PluginErrorCode.DUPLICATE_PLUGIN,
            )
        return matches[0].load()
