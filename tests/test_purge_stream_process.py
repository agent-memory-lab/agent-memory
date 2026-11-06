"""The crash harness must wait for a boundary, not merely a progress report."""

import asyncio
import signal
import sys

import pytest
from purge_stream_process import kill_at_boundary, wait_for_boundary


def test_progress_does_not_signal_commit_boundary():
    async def run():
        reader = asyncio.StreamReader()
        task = asyncio.create_task(wait_for_boundary(reader, 4097))
        try:
            reader.feed_data(b"progress 128\nprogress 256\n")
            await asyncio.sleep(0.01)
            assert not task.done()
            reader.feed_data(b"ready\n")
            await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


@pytest.mark.parametrize("data", [
    b"progress 128\nprogress 128\n", b"progress 256\nprogress 128\n",
    b"progress 129\n", b"progress 512\n", b"unexpected\n", b"",
])
def test_invalid_or_missing_progress_fails(data):
    async def run():
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        with pytest.raises(RuntimeError):
            await wait_for_boundary(reader, 256)
    asyncio.run(run())


def test_inactivity_deadline_is_enforced():
    async def run():
        reader = asyncio.StreamReader()
        reader.feed_data(b"progress 128\n")
        with pytest.raises(TimeoutError):
            await wait_for_boundary(reader, 4097, inactivity_timeout=0.01, total_timeout=1)
    asyncio.run(run())


def test_overall_deadline_bounds_continuing_progress():
    async def run():
        reader = asyncio.StreamReader()
        async def feed():
            for cursor in range(128, 4097, 128):
                reader.feed_data(f"progress {cursor}\n".encode())
                await asyncio.sleep(0.005)
        producer = asyncio.create_task(feed())
        try:
            with pytest.raises(TimeoutError):
                await wait_for_boundary(reader, 4097, inactivity_timeout=1, total_timeout=0.03)
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
    asyncio.run(run())


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="requires real SIGKILL")
@pytest.mark.parametrize("mode", ["ready", "eof", "stall", "duplicate", "cancel"])
def test_real_child_is_reaped_and_errors_include_stderr(mode):
    async def run():
        output = {"ready": "ready", "duplicate": "progress 128\\nprogress 128"}.get(mode, "")
        script = "import sys,time; print('diagnostic', file=sys.stderr, flush=True); "
        script += f"print('{output}', flush=True); " if output else ""
        script += "sys.exit(3)" if mode == "eof" else "time.sleep(30)"
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", script,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        task = asyncio.create_task(kill_at_boundary(
            process, 4097, inactivity_timeout=0.2, total_timeout=2,
        ))
        if mode == "cancel":
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        elif mode == "ready":
            await task
        else:
            with pytest.raises(RuntimeError, match="diagnostic"):
                await task
        assert process.returncode is not None
        if mode != "eof":
            assert process.returncode == -signal.SIGKILL
    asyncio.run(run())


def test_exit_race_keeps_original_failure_and_reaps():
    async def run():
        class ExitedProcess:
            returncode = None
            stdout = asyncio.StreamReader()
            reaped = False

            def kill(self):
                raise ProcessLookupError("already exited")

            async def wait(self):
                self.reaped = True
                self.returncode = 3
                return 3

            async def communicate(self):
                await self.wait()
                return b"", b"exit diagnostic"

        process = ExitedProcess()
        process.stdout.feed_eof()
        with pytest.raises(RuntimeError, match="exit diagnostic") as caught:
            await kill_at_boundary(process, 4097)
        assert "exited before the commit boundary" in str(caught.value)
        assert process.reaped
    asyncio.run(run())


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="requires real SIGKILL")
def test_full_stderr_pipe_is_drained_before_waiting_for_exit():
    async def run():
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c",
            "import sys,time; sys.stderr.write('diagnostic:' + 'x' * 1048576); "
            "sys.stderr.flush(); time.sleep(30)",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            with pytest.raises(RuntimeError, match="diagnostic:") as caught:
                await asyncio.wait_for(kill_at_boundary(
                    process, 4097, inactivity_timeout=0.2, total_timeout=1,
                ), 2)
            assert isinstance(caught.value.__cause__, TimeoutError)
            assert "cleanup failed" not in str(caught.value)
            assert process.returncode == -signal.SIGKILL
        finally:
            if process.returncode is None:
                process.kill()
            await process.communicate()
    asyncio.run(run())
