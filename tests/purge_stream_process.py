"""Bounded progress protocol for the streamed-restore SIGKILL tests."""

import asyncio
import signal
from contextlib import suppress


async def wait_for_boundary(reader, head, *, inactivity_timeout=90, total_timeout=300):
    # A large replay can exceed 90s on a slow runner. Only completed imports
    # renew the inactivity budget; a separate five-minute deadline bounds the
    # 4097-entry replay (the same live CI run completed its sibling in 113s).
    progress = 0
    async with asyncio.timeout(total_timeout):
        while True:
            line = await asyncio.wait_for(reader.readline(), inactivity_timeout)
            if line == b"ready\n":
                return
            if not line:
                raise RuntimeError("restore child exited before the commit boundary")
            try:
                tag, cursor = line.decode().strip().split()
                cursor = int(cursor)
            except (UnicodeError, ValueError) as error:
                raise RuntimeError(f"invalid restore child progress: {line!r}") from error
            if tag != "progress" or cursor <= progress or cursor > head or cursor % 128:
                raise RuntimeError(f"invalid restore child progress: {line!r}")
            progress = cursor


async def kill_at_boundary(process, head, **timeouts):
    failure = None
    try:
        await wait_for_boundary(process.stdout, head, **timeouts)
    except (TimeoutError, RuntimeError) as error:
        failure = error
    finally:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        # communicate drains both pipes while reaping. Waiting for exit first
        # can deadlock on a full pipe even after SIGKILL.
        # Preserve the protocol/timeout error if cleanup itself fails.
        try:
            _, stderr = await asyncio.wait_for(process.communicate(), 10)
        except Exception as cleanup_error:
            if failure is not None:
                raise RuntimeError(
                    f"restore child failed ({type(failure).__name__}): {failure}; "
                    f"cleanup failed: {cleanup_error}"
                ) from failure
            raise
    if failure is not None:
        raise RuntimeError(
            f"restore child failed ({type(failure).__name__}): {failure}; "
            f"stderr: {stderr.decode(errors='replace')}"
        ) from failure
    if process.returncode != -signal.SIGKILL:
        raise RuntimeError(f"restore child was not SIGKILLed: {process.returncode}")
