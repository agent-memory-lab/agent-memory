from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from agent_memory import (
    CaptureAdapterPlugin,
    ConsolidationResult,
    ConsolidatorPlugin,
    EvaluatorPlugin,
    ExtractionResult,
    ExtractorPlugin,
    MemoryScope,
    NeverCancelled,
    PluginContext,
    PluginError,
    PluginErrorCode,
    PluginFailureMode,
    PluginHealth,
    PluginHealthStatus,
    PluginKind,
    PluginLoader,
    PluginLoadRequest,
    PluginManifest,
    PluginManifestError,
    PluginResourceLimits,
    RetrieverPlugin,
    StorageProviderPlugin,
    version_satisfies,
)


def run(coroutine):
    return asyncio.run(coroutine)


def manifest(
    name: str,
    *,
    kind: PluginKind = PluginKind.RETRIEVER,
    plugin_api: int = 1,
    core: str = ">=0.1,<1.0",
    capabilities: tuple[str, ...] = ("semantic.search",),
    limits: PluginResourceLimits | None = None,
) -> PluginManifest:
    return PluginManifest(
        plugin_api=plugin_api,
        name=name,
        version="0.1.0",
        kind=kind,
        capabilities=capabilities,
        requires={"core": core},
        config_schema={"type": "object"},
        resource_limits=limits or PluginResourceLimits(),
        failure_mode=PluginFailureMode.FALLBACK,
    )


def context(
    *,
    limits: PluginResourceLimits | None = None,
    deadline: datetime | None = None,
    clock=None,
) -> PluginContext:
    values = {
        "scope": MemoryScope("tenant-a", user_id="user-a"),
        "resource_limits": limits or PluginResourceLimits(),
        "config": {"endpoint": "https://memory.invalid"},
        "request_id": "request-1",
        "deadline": deadline,
    }
    if clock is not None:
        values["clock"] = clock
    return PluginContext(**values)


class FakeRetriever:
    def __init__(
        self,
        descriptor: PluginManifest,
        *,
        label: str = "plugin",
        log: list[str] | None = None,
        initialize_error: Exception | None = None,
        close_error: Exception | None = None,
        initialize_delay: float = 0,
        health: PluginHealth | None = None,
    ) -> None:
        self.descriptor = descriptor
        self.label = label
        self.log = log if log is not None else []
        self.initialize_error = initialize_error
        self.close_error = close_error
        self.initialize_delay = initialize_delay
        self.health_result = health or PluginHealth(PluginHealthStatus.READY)
        self.context = None

    def plugin_manifest(self):
        return self.descriptor

    async def initialize(self, plugin_context):
        self.log.append(f"initialize:{self.label}")
        self.context = plugin_context
        if self.initialize_delay:
            await asyncio.sleep(self.initialize_delay)
        if self.initialize_error is not None:
            raise self.initialize_error

    async def health(self):
        self.log.append(f"health:{self.label}")
        return self.health_result

    async def close(self):
        self.log.append(f"close:{self.label}")
        if self.close_error is not None:
            raise self.close_error

    async def retrieve(self, query, plugin_context):
        return ()


class AllProtocolMethods(FakeRetriever):
    async def capture(self, event, plugin_context):
        return ()

    async def extract(self, event, plugin_context):
        return ExtractionResult()

    async def consolidate(self, request, plugin_context):
        return ConsolidationResult()

    async def create_provider(self, plugin_context):
        return None

    async def evaluate(self, request, plugin_context):
        return {"passed": True}


class FixedClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


def test_six_protocols_are_runtime_checkable():
    plugin = AllProtocolMethods(manifest("all-methods"))

    assert isinstance(plugin, CaptureAdapterPlugin)
    assert isinstance(plugin, ExtractorPlugin)
    assert isinstance(plugin, RetrieverPlugin)
    assert isinstance(plugin, ConsolidatorPlugin)
    assert isinstance(plugin, StorageProviderPlugin)
    assert isinstance(plugin, EvaluatorPlugin)


def test_context_is_scoped_immutable_and_time_bounded():
    now = datetime(2026, 9, 15, tzinfo=timezone.utc)
    plugin_context = context(deadline=now - timedelta(seconds=1), clock=FixedClock(now))

    assert plugin_context.scope.tenant_id == "tenant-a"
    assert plugin_context.expired is True
    assert plugin_context.cancelled is False
    assert isinstance(plugin_context.cancellation, NeverCancelled)
    with pytest.raises(TypeError):
        plugin_context.config["endpoint"] = "changed"  # type: ignore[index]


def test_context_rejects_naive_deadline():
    with pytest.raises(PluginManifestError):
        context(deadline=datetime(2026, 9, 15))


@pytest.mark.parametrize(
    ("value", "constraint", "expected"),
    [
        ("0.1.0", ">=0.1,<1.0", True),
        ("1.0.0", ">=0.1,<1.0", False),
        ("0.2.4", "~=0.2", True),
        ("1.0.0", "~=0.2", False),
        ("0.2.4", "~=0.2.3", True),
        ("0.3.0", "~=0.2.3", False),
        ("0.1.0", "!=0.1.0", False),
    ],
)
def test_version_constraints(value, constraint, expected):
    assert version_satisfies(value, constraint) is expected


def test_loader_negotiates_capabilities_and_tightens_limits():
    async def scenario():
        loader = PluginLoader(core_version="0.1.0")
        descriptor = manifest(
            "hybrid",
            capabilities=("semantic.search", "temporal.search"),
            limits=PluginResourceLimits(
                timeout_ms=500,
                max_candidates=50,
                max_batch_size=80,
                max_concurrency=8,
            ),
        )
        plugin = FakeRetriever(descriptor)
        loader.register(
            name="hybrid",
            kind=PluginKind.RETRIEVER,
            factory=lambda: plugin,
        )
        loaded = await loader.load(
            "hybrid",
            PluginKind.RETRIEVER,
            context(
                limits=PluginResourceLimits(
                    timeout_ms=800,
                    max_candidates=20,
                    max_batch_size=40,
                    max_concurrency=2,
                )
            ),
            required_capabilities=("semantic.search",),
        )

        assert loaded.instance is plugin
        assert loaded.health.status is PluginHealthStatus.READY
        assert loaded.context.resource_limits == PluginResourceLimits(
            timeout_ms=500,
            max_candidates=20,
            max_batch_size=40,
            max_concurrency=2,
        )
        await loader.close()

    run(scenario())


@pytest.mark.parametrize(
    ("descriptor", "code"),
    [
        (manifest("candidate", plugin_api=2), PluginErrorCode.INCOMPATIBLE_PLUGIN_API),
        (
            manifest("candidate", core=">=2.0,<3.0"),
            PluginErrorCode.INCOMPATIBLE_PLUGIN_API,
        ),
        (
            manifest("different-name"),
            PluginErrorCode.INVALID_MANIFEST,
        ),
        (
            manifest("candidate", kind=PluginKind.EXTRACTOR),
            PluginErrorCode.INVALID_MANIFEST,
        ),
    ],
)
def test_loader_rejects_incompatible_or_mismatched_manifest(descriptor, code):
    async def scenario():
        loader = PluginLoader(core_version="0.1.0")
        loader.register(
            name="candidate",
            kind=PluginKind.RETRIEVER,
            factory=lambda: FakeRetriever(descriptor),
        )
        with pytest.raises(PluginError) as caught:
            await loader.load("candidate", PluginKind.RETRIEVER, context())
        assert caught.value.code is code

    run(scenario())


def test_loader_rejects_missing_required_capability():
    async def scenario():
        loader = PluginLoader(core_version="0.1.0")
        loader.register(
            name="lexical",
            kind=PluginKind.RETRIEVER,
            factory=lambda: FakeRetriever(
                manifest("lexical", capabilities=("lexical.search",))
            ),
        )
        with pytest.raises(PluginError) as caught:
            await loader.load(
                "lexical",
                PluginKind.RETRIEVER,
                context(),
                required_capabilities=("semantic.search",),
            )
        assert caught.value.code is PluginErrorCode.INVALID_IMPLEMENTATION
        assert caught.value.field == "capabilities"

    run(scenario())


def test_unique_highest_priority_registration_wins():
    async def scenario():
        loader = PluginLoader(core_version="0.1.0")
        low = FakeRetriever(manifest("ranked"), label="low")
        high = FakeRetriever(manifest("ranked"), label="high")
        loader.register(
            name="ranked",
            kind=PluginKind.RETRIEVER,
            factory=lambda: low,
            distribution="low-package",
            priority=10,
        )
        loader.register(
            name="ranked",
            kind=PluginKind.RETRIEVER,
            factory=lambda: high,
            distribution="high-package",
            priority=20,
        )

        loaded = await loader.load("ranked", PluginKind.RETRIEVER, context())

        assert loaded.instance is high
        await loader.close()

    run(scenario())


def test_equal_priority_duplicate_is_rejected():
    async def scenario():
        loader = PluginLoader(core_version="0.1.0")
        for distribution in ("package-a", "package-b"):
            loader.register(
                name="duplicate",
                kind=PluginKind.RETRIEVER,
                factory=lambda: FakeRetriever(manifest("duplicate")),
                distribution=distribution,
                priority=10,
            )
        with pytest.raises(PluginError) as caught:
            await loader.load("duplicate", PluginKind.RETRIEVER, context())
        assert caught.value.code is PluginErrorCode.DUPLICATE_PLUGIN

    run(scenario())


def test_initialize_timeout_closes_partial_plugin():
    async def scenario():
        log: list[str] = []
        loader = PluginLoader(core_version="0.1.0")
        plugin = FakeRetriever(
            manifest(
                "slow",
                limits=PluginResourceLimits(timeout_ms=1),
            ),
            label="slow",
            log=log,
            initialize_delay=0.05,
        )
        loader.register(
            name="slow",
            kind=PluginKind.RETRIEVER,
            factory=lambda: plugin,
        )

        with pytest.raises(PluginError) as caught:
            await loader.load("slow", PluginKind.RETRIEVER, context())

        assert caught.value.code is PluginErrorCode.PLUGIN_LOAD_FAILED
        assert log == ["initialize:slow", "close:slow"]
        assert loader.loaded == ()

    run(scenario())


def test_unavailable_health_closes_plugin():
    async def scenario():
        log: list[str] = []
        loader = PluginLoader(core_version="0.1.0")
        plugin = FakeRetriever(
            manifest("unavailable"),
            label="unavailable",
            log=log,
            health=PluginHealth(PluginHealthStatus.UNAVAILABLE, "offline"),
        )
        loader.register(
            name="unavailable",
            kind=PluginKind.RETRIEVER,
            factory=lambda: plugin,
        )

        with pytest.raises(PluginError):
            await loader.load("unavailable", PluginKind.RETRIEVER, context())

        assert log == ["initialize:unavailable", "health:unavailable", "close:unavailable"]

    run(scenario())


def test_batch_failure_rolls_back_in_reverse_order():
    async def scenario():
        log: list[str] = []
        loader = PluginLoader(core_version="0.1.0")
        for name in ("first", "second"):
            plugin = FakeRetriever(manifest(name), label=name, log=log)
            loader.register(
                name=name,
                kind=PluginKind.RETRIEVER,
                factory=lambda plugin=plugin: plugin,
            )
        failing = FakeRetriever(
            manifest("failing"),
            label="failing",
            log=log,
            initialize_error=RuntimeError("initialize failed"),
        )
        loader.register(
            name="failing",
            kind=PluginKind.RETRIEVER,
            factory=lambda: failing,
        )

        requests = tuple(
            PluginLoadRequest(name, PluginKind.RETRIEVER, context())
            for name in ("first", "second", "failing")
        )
        with pytest.raises(PluginError):
            await loader.load_many(requests)

        assert log[-3:] == ["close:failing", "close:second", "close:first"]
        assert loader.loaded == ()

    run(scenario())


def test_rollback_close_failure_does_not_hide_original_load_error():
    async def scenario():
        loader = PluginLoader(core_version="0.1.0")
        sticky = FakeRetriever(
            manifest("sticky"),
            close_error=RuntimeError("close failed"),
        )
        failing = FakeRetriever(
            manifest("failing"),
            initialize_error=RuntimeError("original initialize failure"),
        )
        loader.register(
            name="sticky",
            kind=PluginKind.RETRIEVER,
            factory=lambda: sticky,
        )
        loader.register(
            name="failing",
            kind=PluginKind.RETRIEVER,
            factory=lambda: failing,
        )
        requests = (
            PluginLoadRequest("sticky", PluginKind.RETRIEVER, context()),
            PluginLoadRequest("failing", PluginKind.RETRIEVER, context()),
        )

        with pytest.raises(PluginError) as caught:
            await loader.load_many(requests)

        assert "failing" in str(caught.value)
        assert caught.value.__notes__ == ["plugin rollback also failed for: sticky"]
        assert tuple(item.manifest.name for item in loader.loaded) == ("sticky",)

    run(scenario())


def test_loader_closes_plugins_in_reverse_order():
    async def scenario():
        log: list[str] = []
        loader = PluginLoader(core_version="0.1.0")
        for name in ("first", "second", "third"):
            plugin = FakeRetriever(manifest(name), label=name, log=log)
            loader.register(
                name=name,
                kind=PluginKind.RETRIEVER,
                factory=lambda plugin=plugin: plugin,
            )
            await loader.load(name, PluginKind.RETRIEVER, context())

        await loader.close()

        closes = [entry for entry in log if entry.startswith("close:")]
        assert closes == ["close:third", "close:second", "close:first"]
        assert loader.loaded == ()

    run(scenario())
