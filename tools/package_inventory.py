"""Discover release distributions from their build metadata, without importing them."""

import tomllib
from pathlib import Path


def package_inventory(source_root: Path) -> tuple[dict, ...]:
    """One inventory for builds, installed imports, and archive verification.

    A new immediate child of packages with a pyproject is automatically gated.
    All current projects publish one Python package using Hatch's src layout.
    Fail closed if a new project's layout needs an explicit smoke contract.
    """
    root = source_root.resolve()
    projects = [root / "pyproject.toml", *sorted((root / "packages").glob("*/pyproject.toml"))]
    inventory = []
    for project_path in projects:
        metadata = tomllib.loads(project_path.read_text(encoding="utf-8"))
        packages = metadata["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
        if len(packages) != 1 or not packages[0].startswith("src/"):
            raise ValueError(f"Add an installed import contract for {project_path}")
        module = packages[0].removeprefix("src/")
        if not module.isidentifier():
            raise ValueError(f"Expected one top-level Python package in {project_path}")
        inventory.append({
            "path": project_path.parent.relative_to(root).as_posix(),
            "distribution": metadata["project"]["name"],
            "version": metadata["project"]["version"],
            "module": module,
        })
    names = [package["distribution"] for package in inventory]
    if len(set(names)) != len(names):
        raise ValueError("Duplicate release distribution name")
    return tuple(inventory)
