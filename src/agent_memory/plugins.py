from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any

from .composition import build_local_kernel
from .ports import ClaimExtractor, EmbeddingProvider, MemoryPolicy, MemoryProvider, Reranker

PROVIDER_GROUP = "agent_memory.providers"
INTEGRATION_GROUP = "agent_memory.integrations"


class PluginError(RuntimeError):
    pass


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
    **_: Any,
) -> MemoryProvider:
    return build_local_kernel(
        database_path,
        extractor=extractor,
        policy=policy,
        reranker=reranker,
        embedding_provider=embedding_provider,
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
                raise PluginError(f"unknown provider {name!r}; available: {available}")
            if len(matches) > 1:
                raise PluginError(f"multiple installed providers use the name {name!r}")
            factory = matches[0].load()
        provider = factory(**config)
        required = ("initialize", "ingest_event", "retrieve", "manifest")
        if any(not hasattr(provider, attribute) for attribute in required):
            raise PluginError(f"provider plugin {name!r} does not implement MemoryProvider")
        return provider

    def load_integration(self, name: str) -> Any:
        matches = [item for item in entry_points(group=INTEGRATION_GROUP) if item.name == name]
        if not matches:
            available = ", ".join(item.name for item in self.references(INTEGRATION_GROUP))
            raise PluginError(f"unknown integration {name!r}; available: {available}")
        if len(matches) > 1:
            raise PluginError(f"multiple installed integrations use the name {name!r}")
        return matches[0].load()
