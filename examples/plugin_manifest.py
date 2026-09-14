from __future__ import annotations

import json

from agent_memory import (
    PLUGIN_API_VERSION,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)


manifest = PluginManifest(
    plugin_api=PLUGIN_API_VERSION,
    name="example-retriever",
    version="0.1.0",
    kind=PluginKind.RETRIEVER,
    capabilities=("example.search",),
    requires={"core": ">=0.1,<1.0"},
    config_schema={
        "type": "object",
        "properties": {
            "endpoint": {"type": "string"},
        },
        "additionalProperties": False,
    },
    resource_limits=PluginResourceLimits(
        timeout_ms=500,
        max_candidates=50,
        max_batch_size=20,
        max_concurrency=2,
    ),
    failure_mode=PluginFailureMode.FALLBACK,
)

print(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
