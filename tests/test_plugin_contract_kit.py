from __future__ import annotations

import asyncio

import pytest

from agent_memory import (
    ConsolidationRequest,
    ConsolidationResult,
    ExtractionResult,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
    PluginContext,
    PluginContractError,
    PluginFailureMode,
    PluginHealth,
    PluginHealthStatus,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
    ReferenceCaptureAdapter,
    ReferenceConsolidator,
    ReferenceEvaluator,
    ReferenceExtractor,
    ReferenceRetriever,
    ReferenceStorageProvider,
    assert_plugin_contract,
    build_local_kernel,
    verify_plugin_contract,
)


def run(coroutine):
    return asyncio.run(coroutine)


def context(scope: MemoryScope) -> PluginContext:
    return PluginContext(
        scope=scope,
        resource_limits=PluginResourceLimits(timeout_ms=1_000),
    )


def test_reference_capture_contract():
    async def scenario():
        scope = MemoryScope("tenant-a", user_id="user-a")
        event = MemoryEvent(scope, "user.message", "Remember this.")

        async def exercise(plugin, plugin_context):
            assert await plugin.capture(event, plugin_context) == (event,)

        report = await assert_plugin_contract(
            name="reference-capture",
            kind=PluginKind.CAPTURE_ADAPTER,
            factory=ReferenceCaptureAdapter,
            context=context(scope),
            required_capabilities=("lifecycle.capture",),
            exercise=exercise,
            core_version="0.1.0",
        )
        assert report.passed
        assert report.checks[-1] == "close"

    run(scenario())


def test_reference_extractor_contract():
    async def scenario():
        scope = MemoryScope("tenant-a", user_id="user-a")
        event = MemoryEvent(scope, "user.message", "Remember this.")

        async def exercise(plugin, plugin_context):
            assert await plugin.extract(event, plugin_context) == ExtractionResult()

        report = await assert_plugin_contract(
            name="reference-extractor",
            kind=PluginKind.EXTRACTOR,
            factory=ReferenceExtractor,
            context=context(scope),
            exercise=exercise,
            core_version="0.1.0",
        )
        assert report.passed

    run(scenario())


def test_reference_retriever_contract():
    async def scenario():
        scope = MemoryScope("tenant-a", user_id="user-a")
        query = MemoryQuery(scope, "What should be remembered?")

        async def exercise(plugin, plugin_context):
            assert await plugin.retrieve(query, plugin_context) == ()

        report = await assert_plugin_contract(
            name="reference-retriever",
            kind=PluginKind.RETRIEVER,
            factory=ReferenceRetriever,
            context=context(scope),
            exercise=exercise,
            core_version="0.1.0",
        )
        assert report.passed

    run(scenario())


def test_reference_consolidator_contract():
    async def scenario():
        scope = MemoryScope("tenant-a", user_id="user-a")
        request = ConsolidationRequest(scope)

        async def exercise(plugin, plugin_context):
            assert await plugin.consolidate(request, plugin_context) == ConsolidationResult()

        report = await assert_plugin_contract(
            name="reference-consolidator",
            kind=PluginKind.CONSOLIDATOR,
            factory=ReferenceConsolidator,
            context=context(scope),
            exercise=exercise,
            core_version="0.1.0",
        )
        assert report.passed

    run(scenario())


def test_reference_storage_contract(tmp_path):
    async def scenario():
        scope = MemoryScope("tenant-a", user_id="user-a")
        provider = build_local_kernel(tmp_path / "memory.db")

        async def exercise(plugin, plugin_context):
            assert await plugin.create_provider(plugin_context) is provider

        report = await assert_plugin_contract(
            name="reference-storage",
            kind=PluginKind.STORAGE_PROVIDER,
            factory=lambda: ReferenceStorageProvider(provider),
            context=context(scope),
            exercise=exercise,
            core_version="0.1.0",
        )
        assert report.passed

    run(scenario())


def test_reference_evaluator_contract():
    async def scenario():
        scope = MemoryScope("tenant-a", user_id="user-a")

        async def exercise(plugin, plugin_context):
            result = await plugin.evaluate({"candidate_id": "candidate-1"}, plugin_context)
            assert result["passed"] is True
            assert result["request"]["candidate_id"] == "candidate-1"

        report = await assert_plugin_contract(
            name="reference-evaluator",
            kind=PluginKind.EVALUATOR,
            factory=ReferenceEvaluator,
            context=context(scope),
            exercise=exercise,
            core_version="0.1.0",
        )
        assert report.passed

    run(scenario())


def test_report_is_machine_readable_for_broken_plugin():
    class BrokenRetriever:
        def plugin_manifest(self):
            return PluginManifest(
                name="broken",
                version="0.1.0",
                kind=PluginKind.RETRIEVER,
                capabilities=("lexical.search",),
                requires={"core": ">=0.1,<1.0"},
                config_schema={"type": "object"},
                resource_limits=PluginResourceLimits(),
                failure_mode=PluginFailureMode.FALLBACK,
            )

        async def initialize(self, plugin_context):
            return None

        async def health(self):
            return PluginHealth(PluginHealthStatus.READY)

        async def close(self):
            return None

    report = run(
        verify_plugin_contract(
            name="broken",
            kind=PluginKind.RETRIEVER,
            factory=BrokenRetriever,
            context=context(MemoryScope("tenant-a")),
            core_version="0.1.0",
        )
    )

    assert report.passed is False
    assert report.to_dict()["failures"][0]["code"] == "invalid_implementation"
    with pytest.raises(PluginContractError):
        report.raise_for_errors()


def test_operation_failure_is_reported_and_plugin_is_closed():
    async def scenario():
        plugin = ReferenceRetriever()

        async def fail_operation(instance, plugin_context):
            raise RuntimeError("operation failed")

        report = await verify_plugin_contract(
            name="reference-retriever",
            kind=PluginKind.RETRIEVER,
            factory=lambda: plugin,
            context=context(MemoryScope("tenant-a")),
            exercise=fail_operation,
            core_version="0.1.0",
        )

        assert report.passed is False
        assert report.failures[0].check == "operation"
        assert report.checks[-1] == "close"
        assert (await plugin.health()).status is PluginHealthStatus.UNAVAILABLE

    run(scenario())
