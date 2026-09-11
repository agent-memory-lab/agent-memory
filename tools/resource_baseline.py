#!/usr/bin/env python3
"""Reproducible lightweight resource baseline for the local memory plugin."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import resource
import subprocess
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

from agent_memory import AgentMemory, MemoryLimits

COMPONENT_MODULES = {
    "core": "agent_memory",
    "evolution": "agent_memory_evolution",
    "postgres": "agent_memory_postgres",
}


def _component_imports() -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}
    program = (
        "import importlib,json,resource,sys;"
        "scale=1 if sys.platform=='darwin' else 1024;"
        "before=int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*scale);"
        "importlib.import_module(sys.argv[1]);"
        "after=int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*scale);"
        "print(json.dumps({'before':before,'after':after,'delta':max(0,after-before)}))"
    )
    for name, module in COMPONENT_MODULES.items():
        completed = subprocess.run(
            [sys.executable, "-c", program, module],
            check=True,
            capture_output=True,
            text=True,
        )
        results[name] = json.loads(completed.stdout)
    return results


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


async def _measure(event_count: int, recall_count: int) -> dict[str, object]:
    limits = MemoryLimits(
        max_recall_items=8,
        max_context_tokens=1_200,
        max_state_claims=64,
    )
    started = time.perf_counter()
    tracemalloc.start()
    initial_rss = _peak_rss_bytes()

    with tempfile.TemporaryDirectory(prefix="agent-memory-baseline-") as directory:
        database = Path(directory) / "memory.db"
        async with AgentMemory.local(database, limits=limits) as memory:
            initialized_at = time.perf_counter()
            initialized_rss = _peak_rss_bytes()
            for index in range(event_count):
                await memory.remember(
                    f"Preference {index}: keep response {index % 7} concise.",
                    idempotency_key=f"baseline-{index}",
                    claims=(
                        {
                            "key": f"preference.{index}",
                            "value": index % 7,
                            "text": f"Response preference {index % 7}",
                            "scope_level": "session",
                        },
                    ),
                )
            write_finished_at = time.perf_counter()
            after_write_rss = _peak_rss_bytes()

            token_estimates: list[int] = []
            for index in range(recall_count):
                bundle = await memory.recall(f"response preference {index % 7}")
                token_estimates.append(bundle.token_estimate)
            recall_finished_at = time.perf_counter()
            after_recall_rss = _peak_rss_bytes()
            database_bytes = database.stat().st_size

    _, python_heap_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    result: dict[str, object] = {
        "environment": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
        },
        "workload": {
            "events": event_count,
            "recalls": recall_count,
            "concurrency": 1,
            "max_recall_items": limits.max_recall_items,
            "max_context_tokens": limits.max_context_tokens,
        },
        "timing_ms": {
            "initialization": round((initialized_at - started) * 1000, 3),
            "writes_total": round((write_finished_at - initialized_at) * 1000, 3),
            "recalls_total": round((recall_finished_at - write_finished_at) * 1000, 3),
        },
        "memory_bytes": {
            "process_peak_at_start": initial_rss,
            "process_peak_after_initialize": initialized_rss,
            "process_peak_after_writes": after_write_rss,
            "process_peak_after_recalls": after_recall_rss,
            "python_heap_peak": python_heap_peak,
            "database": database_bytes,
            "database_per_event": round(database_bytes / event_count, 3),
        },
        "component_imports": _component_imports(),
        "retrieval": {
            "maximum_token_estimate": max(token_estimates, default=0),
            "budget_respected": all(
                value <= limits.max_context_tokens for value in token_estimates
            ),
        },
    }
    timing = result["timing_ms"]
    memory = result["memory_bytes"]
    assert isinstance(timing, dict) and isinstance(memory, dict)
    thresholds = {
        "initialization_ms_max": 100.0,
        "writes_total_ms_max": 5_000.0,
        "recalls_total_ms_max": 5_000.0,
        "process_peak_bytes_max": 128 * 1024 * 1024,
        "python_heap_peak_bytes_max": 16 * 1024 * 1024,
        "database_bytes_max": 8 * 1024 * 1024,
    }
    checks = {
        "initialization": timing["initialization"] <= thresholds["initialization_ms_max"],
        "writes": timing["writes_total"] <= thresholds["writes_total_ms_max"],
        "recalls": timing["recalls_total"] <= thresholds["recalls_total_ms_max"],
        "process_peak": (
            memory["process_peak_after_recalls"] <= thresholds["process_peak_bytes_max"]
        ),
        "python_heap": memory["python_heap_peak"] <= thresholds["python_heap_peak_bytes_max"],
        "database": memory["database"] <= thresholds["database_bytes_max"],
        "token_budget": result["retrieval"]["budget_respected"],
    }
    result["thresholds"] = thresholds
    result["checks"] = checks
    result["passed"] = all(checks.values())
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=500)
    parser.add_argument("--recalls", type=int, default=50)
    args = parser.parse_args()
    if args.events < 1 or args.recalls < 1:
        parser.error("--events and --recalls must be positive")
    print(json.dumps(asyncio.run(_measure(args.events, args.recalls)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
