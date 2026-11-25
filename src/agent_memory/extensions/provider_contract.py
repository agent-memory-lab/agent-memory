"""Validate provider contracts before initialization or application work."""

from dataclasses import fields
from typing import get_protocol_members

from ..domain import MemoryCapabilities, ProviderManifest
from ..ports import (
    BitemporalMemoryProvider,
    MemoryProvider,
    implements_port_method,
    supports_pipeline_guard,
)


def validate_provider(
    provider: MemoryProvider, *, require_delivery_guard: bool = False
) -> ProviderManifest:
    missing = sorted(
        name
        for name in get_protocol_members(MemoryProvider)
        if not implements_port_method(provider, MemoryProvider, name)
    )
    if missing:
        raise ValueError("provider lacks callable methods: " + ", ".join(missing))
    manifest = provider.manifest()
    if not isinstance(manifest, ProviderManifest):
        raise ValueError("provider manifest must be a ProviderManifest")
    for identity in (manifest.name, manifest.version, manifest.protocol_version):
        if not isinstance(identity, str) or not identity.strip():
            raise ValueError("provider manifest requires a name, version and protocol version")
    if type(manifest.schema_version) is not int or manifest.schema_version < 0:
        raise ValueError("provider manifest requires a nonnegative schema version")
    capabilities = manifest.capabilities
    if not isinstance(capabilities, MemoryCapabilities):
        raise ValueError("provider capabilities must be MemoryCapabilities")
    for field in fields(capabilities):
        if isinstance(field.default, bool) and type(getattr(capabilities, field.name)) is not bool:
            raise ValueError(f"provider capability {field.name} must be a bool")
    if not isinstance(capabilities.packing_versions, tuple) or any(
        not isinstance(version, str) or not version for version in capabilities.packing_versions
    ):
        raise ValueError("provider packing_versions must be a tuple of nonempty strings")
    if capabilities.bitemporal_claims and not implements_port_method(
        provider, BitemporalMemoryProvider, "get_state_at",
    ):
        raise ValueError("provider declares bitemporal_claims but lacks get_state_at")
    if capabilities.final_delivery_guard and not supports_pipeline_guard(provider):
        raise ValueError("provider declares final_delivery_guard but lacks guard_pipeline_delivery")
    if require_delivery_guard and not capabilities.final_delivery_guard:
        raise ValueError("provider does not support required final delivery guard")
    return manifest
