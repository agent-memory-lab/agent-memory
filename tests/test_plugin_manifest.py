from __future__ import annotations

import math

import pytest

from agent_memory import (
    PLUGIN_API_VERSION,
    PluginErrorCode,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginManifestError,
    PluginRegistry,
    PluginResourceLimits,
)


def manifest_dict() -> dict[str, object]:
    return {
        "plugin_api": PLUGIN_API_VERSION,
        "name": "agent-memory-hybrid",
        "version": "1.2.3",
        "kind": "retriever",
        "capabilities": ["lexical.search", "semantic.search"],
        "requires": {"core": ">=0.1,<1.0"},
        "config_schema": {
            "type": "object",
            "properties": {
                "endpoint": {"type": "string"},
                "top_k": {"type": "integer", "minimum": 1},
            },
            "required": ["endpoint"],
            "additionalProperties": False,
        },
        "resource_limits": {
            "timeout_ms": 500,
            "max_candidates": 50,
            "max_batch_size": 20,
            "max_concurrency": 2,
        },
        "failure_mode": "fallback",
    }


def test_manifest_round_trip_is_stable_and_typed():
    raw = manifest_dict()

    manifest = PluginManifest.from_dict(raw)

    assert manifest.plugin_api == PLUGIN_API_VERSION
    assert manifest.kind is PluginKind.RETRIEVER
    assert manifest.failure_mode is PluginFailureMode.FALLBACK
    assert manifest.capabilities == ("lexical.search", "semantic.search")
    assert manifest.to_dict() == raw
    assert PluginManifest.from_dict(manifest.to_dict()) == manifest


def test_manifest_copies_and_freezes_nested_configuration():
    raw = manifest_dict()
    schema = raw["config_schema"]
    assert isinstance(schema, dict)
    manifest = PluginManifest.from_dict(raw)

    schema["type"] = "array"

    assert manifest.config_schema["type"] == "object"
    assert manifest.config_schema["required"] == ("endpoint",)
    with pytest.raises(TypeError):
        manifest.config_schema["type"] = "array"  # type: ignore[index]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "Agent Memory"),
        ("version", "1.2"),
        ("plugin_api", True),
        ("kind", "unknown"),
        ("capabilities", "semantic.search"),
        ("capabilities", []),
        ("capabilities", ["Semantic Search"]),
        ("capabilities", ["semantic.search", "semantic.search"]),
        ("requires", {}),
        ("requires", {"core": "0.1"}),
        ("config_schema", {"type": "array"}),
        ("failure_mode", "ignore"),
    ],
)
def test_manifest_rejects_invalid_top_level_values(field, value):
    raw = manifest_dict()
    raw[field] = value

    with pytest.raises(PluginManifestError) as caught:
        PluginManifest.from_dict(raw)

    assert caught.value.code is PluginErrorCode.INVALID_MANIFEST


@pytest.mark.parametrize(
    "value",
    [
        None,
        7,
        {1: "not-a-string-key"},
        {"plugin_api": PLUGIN_API_VERSION},
        {**manifest_dict(), "unexpected": True},
    ],
)
def test_manifest_rejects_invalid_document_shapes(value):
    with pytest.raises(PluginManifestError) as caught:
        PluginManifest.from_dict(value)

    assert caught.value.code is PluginErrorCode.INVALID_MANIFEST


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_ms", 0),
        ("timeout_ms", 300_001),
        ("max_candidates", True),
        ("max_candidates", 10_001),
        ("max_batch_size", 0),
        ("max_concurrency", 257),
    ],
)
def test_manifest_rejects_resource_limits_outside_hard_bounds(field, value):
    raw = manifest_dict()
    limits = raw["resource_limits"]
    assert isinstance(limits, dict)
    limits[field] = value

    with pytest.raises(PluginManifestError) as caught:
        PluginManifest.from_dict(raw)

    assert caught.value.field == f"resource_limits.{field}"


def test_manifest_rejects_unknown_or_missing_resource_limit_fields():
    unknown = manifest_dict()
    unknown_limits = unknown["resource_limits"]
    assert isinstance(unknown_limits, dict)
    unknown_limits["unbounded"] = True

    missing = manifest_dict()
    missing_limits = missing["resource_limits"]
    assert isinstance(missing_limits, dict)
    del missing_limits["timeout_ms"]

    with pytest.raises(PluginManifestError):
        PluginManifest.from_dict(unknown)
    with pytest.raises(PluginManifestError):
        PluginManifest.from_dict(missing)


def test_manifest_rejects_non_json_or_non_finite_schema_values():
    for bad_value in (object(), math.inf, math.nan):
        raw = manifest_dict()
        schema = raw["config_schema"]
        assert isinstance(schema, dict)
        schema["default"] = bad_value

        with pytest.raises(PluginManifestError):
            PluginManifest.from_dict(raw)


def test_resource_limits_can_be_constructed_directly():
    limits = PluginResourceLimits(
        timeout_ms=250,
        max_candidates=25,
        max_batch_size=10,
        max_concurrency=1,
    )

    assert limits.to_dict() == {
        "timeout_ms": 250,
        "max_candidates": 25,
        "max_batch_size": 10,
        "max_concurrency": 1,
    }


def test_registry_unknown_plugin_uses_stable_error_code():
    with pytest.raises(Exception) as caught:
        PluginRegistry().create_provider("definitely-not-installed")

    assert caught.value.code is PluginErrorCode.UNKNOWN_PLUGIN

