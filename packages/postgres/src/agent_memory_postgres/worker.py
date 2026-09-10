from __future__ import annotations

import argparse
import asyncio
import os
import socket
from collections.abc import Sequence

from .composition import build_postgres_kernel
from .consolidation import BlockConsolidationPolicy, TrajectoryBlockConsolidator
from .jobs import ConsolidationWorker, PostgresConsolidationQueue


async def run_consolidation_worker(
    dsn: str,
    *,
    worker_id: str,
    once: bool = False,
    idle_seconds: float = 1.0,
    policy: BlockConsolidationPolicy | None = None,
) -> bool | None:
    """Run the PostgreSQL consolidation loop outside the agent request process."""
    memory = build_postgres_kernel(dsn)
    await memory.initialize()
    queue = memory.consolidation_scheduler
    if not isinstance(queue, PostgresConsolidationQueue):
        raise RuntimeError("PostgreSQL memory kernel did not expose a consolidation queue")

    consolidator = TrajectoryBlockConsolidator(memory, policy)
    worker = ConsolidationWorker(
        queue,
        {"memory.consolidate": consolidator.as_handler()},
        worker_id=worker_id,
    )
    if once:
        return await worker.run_once()

    stop = asyncio.Event()
    await worker.run_forever(stop, idle_seconds=idle_seconds)
    return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run agent-memory PostgreSQL background consolidation."
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv("AGENT_MEMORY_POSTGRES_DSN"),
        help="PostgreSQL DSN; defaults to AGENT_MEMORY_POSTGRES_DSN.",
    )
    parser.add_argument(
        "--worker-id",
        default=f"{socket.gethostname()}-{os.getpid()}",
        help="Unique worker identity used for queue leases.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process at most one job, then exit.",
    )
    parser.add_argument(
        "--idle-seconds",
        type=float,
        default=1.0,
        help="Polling delay while the queue is empty.",
    )
    args = parser.parse_args(argv)
    if not args.dsn:
        parser.error("--dsn or AGENT_MEMORY_POSTGRES_DSN is required")
    if args.idle_seconds <= 0:
        parser.error("--idle-seconds must be positive")

    try:
        asyncio.run(
            run_consolidation_worker(
                args.dsn,
                worker_id=args.worker_id,
                once=args.once,
                idle_seconds=args.idle_seconds,
            )
        )
    except KeyboardInterrupt:
        return 0
    return 0
