"""Check the same image across forced termination, restart, isolation and erase.

Creates only uniquely named temporary Docker resources, removed even on failure.
No external service/model is used; container networking is disabled (HTTP is loopback).
"""

import argparse
import json
import os
import selectors
import subprocess
import time
import uuid
from pathlib import Path


def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], check=True, timeout=600, **kwargs)


def wait_for_checkpoint(process, timeout=180):
    """Bound the wait even when a failed server never writes a stdout line."""
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + timeout
        pending = b""
        while time.monotonic() < deadline:
            if not selector.select(timeout=max(0, deadline - time.monotonic())):
                break
            chunk = os.read(process.stdout.fileno(), 65536)
            if not chunk:
                raise RuntimeError("seed container exited before its committed-write checkpoint")
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                try:
                    message = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if message == {"status": "checkpoint", "phase": "seed-hold"}:
                    return
            if len(pending) > 65536:
                raise RuntimeError("seed output exceeded the checkpoint line limit")
    raise TimeoutError("seed container did not acknowledge its committed write")


def run(image, platform, expected_version, *, seed_image=None, seed_version=None):
    if (seed_image is None) != (seed_version is None):
        raise ValueError("seed image and version must be supplied together")
    name = f"agent-memory-smoke-{uuid.uuid4().hex}"
    volume = name + "-data"
    script = Path(__file__).with_name("container_smoke.py").read_text(encoding="utf-8")
    docker("volume", "create", volume, stdout=subprocess.DEVNULL)
    process = None
    try:
        command = [
            "run",
            "--rm",
            "-i",
            "--network",
            "none",
            "--platform",
            platform,
            "--mount",
            f"type=volume,source={volume},target=/data",
            "--entrypoint",
            "python",
        ]
        options = [
            "-I",
            "-",
            "--expected-version",
            expected_version,
            "--data-dir",
            "/data/lifecycle",
            "--phase",
        ]
        seed_options = [
            "-I",
            "-",
            "--expected-version",
            seed_version or expected_version,
            "--data-dir",
            "/data/lifecycle",
            "--phase",
            "seed-hold",
        ]
        process = subprocess.Popen(
            ["docker", *command, "--name", name, seed_image or image, *seed_options],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        process.stdin.write(script.encode("utf-8"))
        process.stdin.close()
        wait_for_checkpoint(process)
        docker("kill", "--signal", "KILL", name, stdout=subprocess.DEVNULL)
        assert process.wait(timeout=30) == 137, "seed must exit because of SIGKILL"
        for phase in ("verify", "erase", "erased"):
            docker(*command, image, *options, phase, input=script, text=True)
        print(
            json.dumps(
                {
                    "status": "passed",
                    "image": image,
                    "platform": platform,
                    "seed_image": seed_image or image,
                    "seed_version": seed_version or expected_version,
                    "scenarios": [
                        "committed_write_sigkill",
                        "named_volume_restart",
                        "tenant_user_isolation",
                        "scope_forgery_denied",
                        "unauthorized_erase_denied",
                        "http_gateway_identity",
                        "authorized_erase",
                        "no_resurrection_after_restart",
                    ],
                }
            )
        )
    finally:
        subprocess.run(
            ["docker", "rm", "-f", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
            check=False,
        )
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=30)
            process.stdout.close()
        docker("volume", "rm", "-f", volume, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--platform", required=True, choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--expected-version", required=True)
    parser.add_argument(
        "--seed-image", help="Optional previous release digest for upgrade verification"
    )
    parser.add_argument("--seed-version")
    args = parser.parse_args()
    if bool(args.seed_image) != bool(args.seed_version):
        parser.error("--seed-image and --seed-version must be supplied together")
    run(
        args.image,
        args.platform,
        args.expected_version,
        seed_image=args.seed_image,
        seed_version=args.seed_version,
    )
