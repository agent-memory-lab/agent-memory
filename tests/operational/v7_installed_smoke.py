"""Validate every release distribution from a clean non-editable install.

Run with the target environment's Python -I, from outside the checkout. This
checks packaging, not runtime feature completeness or production model quality.
"""

import argparse
import asyncio
import importlib
import importlib.metadata
import importlib.util
import json
import runpy
import sys
import sysconfig
import tempfile
from hashlib import sha256
from pathlib import Path


async def sqlite_roundtrip():
    from agent_memory import AgentMemory, MemoryScope

    with tempfile.TemporaryDirectory(prefix="v7-installed-smoke-") as directory:
        scope = MemoryScope(tenant_id="packaging-test", user_id="alice", agent_id="smoke")
        async with AgentMemory.local(Path(directory) / "memory.db", scope=scope) as memory:
            await memory.remember(
                "I prefer concise answers.", event_type="user.message", actor="user",
                idempotency_key="installed-smoke", claims=({
                    "key": "answer.style", "value": "concise", "text": "Use concise answers",
                    "scope": "user", "confidence": 0.98,
                },),
            )
            bundle = await memory.recall("How should I answer Alice?", token_budget=600)
            assert any(claim.value == "concise" for claim in bundle.current_state)


def run(source_root):
    assert sys.flags.isolated, "use Python -I to exclude PYTHONPATH and the current directory"
    assert sys.version_info[:2] == (3, 13), "release acceptance is pinned to Python 3.13"
    installed = Path(sysconfig.get_paths()["purelib"]).resolve()
    assert not installed.is_relative_to(source_root), "install in a fresh external virtualenv"
    inventory = runpy.run_path(str(source_root / "tools/package_inventory.py"))[
        "package_inventory"
    ](source_root)
    evidence = []
    for package in inventory:
        distribution_name, module_name = package["distribution"], package["module"]
        distribution = importlib.metadata.distribution(distribution_name)
        assert distribution.version == package["version"]
        direct = distribution.read_text("direct_url.json")
        assert not (direct and json.loads(direct).get("dir_info", {}).get("editable")), (
            "editable install is not built-artifact evidence"
        )
        module = importlib.import_module(module_name)
        assert Path(module.__file__).resolve().is_relative_to(installed), module_name
        for entry in distribution.entry_points:
            assert callable(entry.load()), entry.name
        evidence.append({
            "distribution": distribution_name, "version": distribution.version,
            "entry_points": sorted(entry.name for entry in distribution.entry_points),
            "installed_from_artifact": True,
        })
    # Import the real adapter modules too, not just the inert package namespace.
    # A base wheel install must not pull heavy inference dependencies or weights.
    for dependency in ("torch", "transformers", "tokenizers", "safetensors"):
        assert importlib.util.find_spec(dependency) is None, dependency
    for name in ("agent_memory_local_models.models", "agent_memory_local_models.smoke"):
        module = importlib.import_module(name)
        assert Path(module.__file__).resolve().is_relative_to(installed), name
    assert not {"torch", "transformers", "tokenizers", "safetensors"} & sys.modules.keys()
    source_migrations = source_root / "packages/postgres/migrations"
    packaged_migrations = installed / "agent_memory_postgres/migrations"
    expected = {path.name: sha256(path.read_bytes()).hexdigest()
                for path in source_migrations.glob("*.sql")}
    actual = {path.name: sha256(path.read_bytes()).hexdigest()
              for path in packaged_migrations.glob("*.sql")}
    assert expected and actual == expected, "wheel migration contents differ from the tested source"
    asyncio.run(sqlite_roundtrip())
    from agent_memory.evaluation.resources import ProcessResourceProbe

    try:
        rss = {"status": "measured", "peak_bytes": ProcessResourceProbe().rss_bytes()}
    except NotImplementedError as exc:
        assert sys.platform == "win32", "RSS unexpectedly unavailable on a Unix runner"
        rss = {"status": "unsupported", "reason": str(exc)}
    else:
        assert rss["peak_bytes"] > 0
    return {"status": "passed",
            "scope": f"{len(inventory)} installed distributions and SQLite smoke only",
            "platform": sys.platform, "rss": rss, "local_model_inference": "not_run",
            "packages": evidence, "migration_sha256": actual,
            "production_benefit_claim": False, "full_v7_acceptance": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.source_root.resolve()), sort_keys=True, indent=2))
