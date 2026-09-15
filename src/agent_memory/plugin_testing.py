from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from typing import Any

from .plugin_loader import PluginLoader
from .plugin_protocol import PluginContext, PluginHealthStatus
from .plugins import PluginError, PluginErrorCode, PluginKind

PluginExercise = Callable[[Any, PluginContext], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class PluginContractFailure:
    check: str
    code: str
    message: str
    field: str | None = None

    def to_dict(self) -> dict[str, str]:
        result = {"check": self.check, "code": self.code, "message": self.message}
        if self.field is not None:
            result["field"] = self.field
        return result


@dataclass(frozen=True, slots=True)
class PluginContractReport:
    name: str
    kind: PluginKind
    checks: tuple[str, ...]
    failures: tuple[PluginContractFailure, ...]

    @property
    def passed(self) -> bool:
        return not self.failures

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind.value,
            "passed": self.passed,
            "checks": list(self.checks),
            "failures": [failure.to_dict() for failure in self.failures],
        }

    def raise_for_errors(self) -> None:
        if not self.passed:
            raise PluginContractError(self)


class PluginContractError(PluginError):
    def __init__(self, report: PluginContractReport) -> None:
        checks = ", ".join(failure.check for failure in report.failures)
        super().__init__(
            f"plugin {report.name!r} failed contract checks: {checks}",
            code=PluginErrorCode.INVALID_IMPLEMENTATION,
        )
        self.report = report


async def verify_plugin_contract(
    *,
    name: str,
    kind: PluginKind,
    factory: Callable[[], Any],
    context: PluginContext,
    required_capabilities: Collection[str] = (),
    exercise: PluginExercise | None = None,
    core_version: str | None = None,
) -> PluginContractReport:
    """Run bounded lifecycle and optional behavior checks without a test dependency."""

    checks: list[str] = []
    failures: list[PluginContractFailure] = []
    loader = PluginLoader(core_version=core_version)
    current_check = "registration"
    try:
        loader.register(name=name, kind=kind, factory=factory)
        checks.append(current_check)

        current_check = "load_and_initialize"
        loaded = await loader.load(
            name,
            kind,
            context,
            required_capabilities=required_capabilities,
        )
        checks.append(current_check)

        current_check = "manifest_stability"
        if loaded.instance.plugin_manifest() != loaded.manifest:
            raise PluginError(
                "plugin_manifest() changed after initialization",
                code=PluginErrorCode.INVALID_MANIFEST,
            )
        checks.append(current_check)

        current_check = "effective_resource_limits"
        host_limits = context.resource_limits
        effective = loaded.context.resource_limits
        if (
            effective.timeout_ms > host_limits.timeout_ms
            or effective.max_candidates > host_limits.max_candidates
            or effective.max_batch_size > host_limits.max_batch_size
            or effective.max_concurrency > host_limits.max_concurrency
        ):
            raise PluginError(
                "plugin effective limits exceed host limits",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        checks.append(current_check)

        if exercise is not None:
            current_check = "operation"
            timeout = loaded.context.resource_limits.timeout_ms / 1_000
            await asyncio.wait_for(
                exercise(loaded.instance, loaded.context),
                timeout=timeout,
            )
            checks.append(current_check)

        current_check = "health"
        health = await loader.health()
        key = f"{loaded.manifest.kind.value}:{loaded.manifest.name}"
        if health[key].status is PluginHealthStatus.UNAVAILABLE:
            raise PluginError(
                "plugin became unavailable during contract checks",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        checks.append(current_check)
    except PluginError as error:
        failures.append(
            PluginContractFailure(
                check=current_check,
                code=error.code.value,
                message=str(error),
                field=error.field,
            )
        )
    except asyncio.TimeoutError:
        failures.append(
            PluginContractFailure(
                check=current_check,
                code="plugin_timeout",
                message=f"plugin contract check {current_check!r} exceeded its time limit",
            )
        )
    except Exception as error:
        failures.append(
            PluginContractFailure(
                check=current_check,
                code="plugin_contract_exception",
                message=f"{type(error).__name__}: {error}",
            )
        )
    finally:
        if loader.loaded:
            current_check = "close"
            try:
                await loader.close()
                checks.append(current_check)
            except PluginError as error:
                failures.append(
                    PluginContractFailure(
                        check=current_check,
                        code=error.code.value,
                        message=str(error),
                        field=error.field,
                    )
                )
    return PluginContractReport(name, PluginKind(kind), tuple(checks), tuple(failures))


async def assert_plugin_contract(**options: Any) -> PluginContractReport:
    report = await verify_plugin_contract(**options)
    report.raise_for_errors()
    return report
