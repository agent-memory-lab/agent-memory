"""Bounded waits over one immutable server target; a timeout never cancels work."""

import asyncio
import math
import time

from agent_memory.serialization import to_jsonable


async def wait_until(
    client, session, target_id, *, stage="l1_decided", timeout=30, poll_interval=0.1
):
    for value, minimum, maximum in ((timeout, 0, 30), (poll_interval, 0.01, 5)):
        if (
            type(value) not in {int, float}
            or not math.isfinite(value)
            or not minimum <= value <= maximum
        ):
            raise ValueError("invalid readiness wait bounds")
    binding = to_jsonable(session)
    deadline = time.monotonic() + timeout
    status = None
    while True:
        remaining = deadline - time.monotonic()
        if status is not None and remaining <= 0:
            return {**status, "state": "timed_out", "last_state": "processing"}
        try:
            call = client.durable_readiness(session, target_id, stage=stage)
            status = await asyncio.wait_for(call, 30 if timeout == 0 else max(remaining, 0.001))
        except TimeoutError:
            return {
                **(status or {}),
                "schema": "durable-readiness/1",
                "target_id": target_id,
                "stage": stage,
                "state": "timed_out",
                "last_state": (status or {}).get("state"),
            }
        if (
            not isinstance(status, dict)
            or status.get("schema") != "durable-readiness/1"
            or status.get("target_id") != target_id
            or status.get("stage") != stage
            or status.get("producer_id") != binding["producer_id"]
            or type(status.get("epoch")) is not int
            or status["epoch"] != binding["epoch"]
            or status.get("state")
            not in {"reached", "processing", "blocked", "failed", "unsupported"}
        ):
            raise ValueError("invalid readiness response")
        if status["state"] != "processing":
            return status
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return {**status, "state": "timed_out", "last_state": "processing"}
        await asyncio.sleep(min(poll_interval, remaining))
