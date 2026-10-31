"""Real MCP process checks for the smoke script used against published images."""

import asyncio
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("container_smoke", ROOT / "tools/container_smoke.py")
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


def test_mcp_restart_identity_http_and_erase_lifecycle(tmp_path):
    # Every stdio phase starts a fresh installed CLI process against the same database.
    # Docker/UID/volume/SIGKILL checks run separately in the container workflow.
    for phase in ("seed", "verify"):
        assert asyncio.run(smoke.stdio_phase(tmp_path, phase))["phase"] == phase
    assert asyncio.run(smoke.http_checks(tmp_path))["gateway_identity_cases"] == 5
    for phase in ("erase", "erased"):
        assert asyncio.run(smoke.stdio_phase(tmp_path, phase))["phase"] == phase


def test_verification_fails_without_durable_seed(tmp_path):
    with pytest.raises(ExceptionGroup) as error:
        asyncio.run(smoke.stdio_phase(tmp_path, "verify"))
    assert "AssertionError" in repr(error.value)


@pytest.mark.parametrize(
    "output,exception",
    [
        ("print('{}')", RuntimeError),
        ("import time; print('{', end='', flush=True); time.sleep(10)", TimeoutError),
    ],
)
def test_checkpoint_never_accepts_eof_or_hangs_on_partial_output(output, exception):
    spec = importlib.util.spec_from_file_location(
        "container_lifecycle_smoke",
        ROOT / "tools/container_lifecycle_smoke.py",
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    process = subprocess.Popen([sys.executable, "-c", output], stdout=subprocess.PIPE)
    try:
        with pytest.raises(exception):
            runner.wait_for_checkpoint(process, timeout=0.5)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        process.stdout.close()
