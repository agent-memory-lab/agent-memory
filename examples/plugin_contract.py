from __future__ import annotations

import asyncio
import json

from agent_memory import (
    MemoryEvent,
    MemoryScope,
    PluginContext,
    PluginKind,
    PluginResourceLimits,
    ReferenceCaptureAdapter,
    verify_plugin_contract,
)


async def main() -> None:
    scope = MemoryScope("example-tenant", user_id="example-user")
    context = PluginContext(scope, PluginResourceLimits())
    event = MemoryEvent(scope, "user.message", "Remember this preference.")

    async def exercise(plugin, plugin_context) -> None:
        captured = await plugin.capture(event, plugin_context)
        if captured != (event,):
            raise AssertionError("capture plugin changed the canonical event")

    report = await verify_plugin_contract(
        name="reference-capture",
        kind=PluginKind.CAPTURE_ADAPTER,
        factory=ReferenceCaptureAdapter,
        context=context,
        required_capabilities=("lifecycle.capture",),
        exercise=exercise,
        core_version="0.1.0",
    )
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    report.raise_for_errors()


asyncio.run(main())
