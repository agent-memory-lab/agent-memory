"""Verify local or downloaded release archives against the checked-out source.

Never extract archives or execute their content. The same gate runs before upload
and after downloading the published attachments; manifests use portable basenames.
"""

import argparse
import hashlib
import json
import re
import runpy
import subprocess
import tarfile
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

package_inventory = runpy.run_path(str(Path(__file__).with_name("package_inventory.py")))[
    "package_inventory"
]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def archive_files(path):
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            pairs = [(name, archive.read(name)) for name in archive.namelist()
                     if not name.endswith("/")]
    else:
        with tarfile.open(path) as archive:
            pairs = [(item.name, archive.extractfile(item).read())
                     for item in archive.getmembers() if item.isfile()]
    if len({name for name, _ in pairs}) != len(pairs):
        raise ValueError(f"Duplicate archive member: {path.name}")
    if any(PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts
           for name, _ in pairs):
        raise ValueError(f"Unsafe archive member: {path.name}")
    return dict(pairs)


def source_files(root, package):
    project = root / package["path"]
    files = {path.relative_to(project / "src").as_posix(): path.read_bytes()
             for path in (project / "src" / package["module"]).rglob("*")
             if path.is_file() and "__pycache__" not in path.parts
             and path.suffix not in (".pyc", ".pyo")}
    # Hatch force-include paths are part of the installed contract (SQL migrations).
    import tomllib
    metadata = tomllib.loads((project / "pyproject.toml").read_text())
    forced = metadata["tool"]["hatch"]["build"]["targets"]["wheel"].get("force-include", {})
    for origin, destination in forced.items():
        for path in (project / origin).rglob("*"):
            if path.is_file():
                target = PurePosixPath(destination) / path.relative_to(project / origin)
                files[target.as_posix()] = path.read_bytes()
    return files, forced


def verify(source_root, artifacts, *, write_manifest=False, require_manifest=False):
    root, artifacts = Path(source_root).resolve(), Path(artifacts).resolve()
    inventory = {canonical(p["distribution"]): p for p in package_inventory(root)}
    paths = sorted([*artifacts.rglob("*.whl"), *artifacts.rglob("*.tar.gz")])
    if len(paths) != 2 * len(inventory) or len({p.name for p in paths}) != len(paths):
        raise ValueError("Expected exactly one wheel and sdist per distribution, with unique names")
    license_text = (root / "LICENSE").read_bytes()
    seen, requirements, entries = set(), {}, []
    source_count = 0
    for path in paths:
        files = archive_files(path)
        wheel = path.suffix == ".whl"
        metadata_names = [name for name in files if (
            name.endswith(".dist-info/METADATA") if wheel
            else len(PurePosixPath(name).parts) == 2 and name.endswith("/PKG-INFO")
        )]
        if len(metadata_names) != 1:
            raise ValueError(f"Expected one distribution metadata file: {path.name}")
        metadata = BytesParser().parsebytes(files[metadata_names[0]])
        name = canonical(metadata["Name"])
        package = inventory.get(name)
        identity = (name, "wheel" if wheel else "sdist")
        if package is None or identity in seen or metadata["Version"] != package["version"]:
            raise ValueError(f"Unexpected distribution/version: {path.name}")
        basename = package["distribution"].replace("-", "_") + "-" + package["version"]
        if not (path.name.startswith(basename + "-") if wheel
                else path.name == basename + ".tar.gz"):
            raise ValueError(f"Archive filename differs from distribution/version: {path.name}")
        seen.add(identity)
        if not any(PurePosixPath(key).name == "LICENSE" and value == license_text
                   for key, value in files.items()):
            raise ValueError(f"Missing or different full LICENSE: {path.name}")
        expected, forced = source_files(root, package)
        if wheel:
            metadata_prefix = metadata_names[0].removesuffix("METADATA")
            actual = {key: value for key, value in files.items()
                      if not key.startswith(metadata_prefix)}
            source_count += len(expected)
        else:
            prefix = metadata_names[0].removesuffix("PKG-INFO")
            actual = {key.removeprefix(prefix + "src/"): value for key, value in files.items()
                      if key.startswith(prefix + "src/" + package["module"] + "/")}
            for origin, destination in forced.items():
                actual.update({destination + "/" + key.removeprefix(prefix + origin + "/"): value
                               for key, value in files.items()
                               if key.startswith(prefix + origin + "/")})
        if actual != expected:
            raise ValueError(f"Archive source differs from checkout: {path.name}")
        contract = (metadata["Requires-Python"], sorted(metadata.get_all("Requires-Dist", [])))
        if name in requirements and contract != requirements[name]:
            raise ValueError(f"Wheel/sdist dependency metadata differs: {name}")
        requirements[name] = contract
        entries.append({"name": path.name, "bytes": path.stat().st_size,
                        "sha256": digest(path.read_bytes())})
    entries.sort(key=lambda entry: entry["name"])
    manifest_path = artifacts / "release-manifest.json"
    sums_path = artifacts / "SHA256SUMS"
    if write_manifest:
        def git(ref):
            return subprocess.check_output(["git", "rev-parse", ref], cwd=root, text=True).strip()
        dirty = bool(subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=root, text=True,
        ).strip())
        manifest = {"schema": "agent-memory-release/2",
                    "version": inventory["agent-memory"]["version"],
                    "source_commit": git("HEAD"), "source_dirty": dirty,
                    "source_tree": None if dirty else git("HEAD^{tree}"), "files": entries}
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        sums_path.write_text("".join(f"{entry['sha256']}  {entry['name']}\n" for entry in entries)
                             + f"{digest(manifest_path.read_bytes())}  release-manifest.json\n",
                             encoding="utf-8")
    if require_manifest or write_manifest:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (manifest["files"] != entries
                or manifest["version"] != inventory["agent-memory"]["version"]):
            raise ValueError("Release manifest does not match the actual archives")
        if require_manifest:
            commit = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=root, text=True,
            ).strip()
            tree = subprocess.check_output(
                ["git", "rev-parse", "HEAD^{tree}"], cwd=root, text=True,
            ).strip()
            if (manifest.get("source_dirty") or manifest.get("source_tree") != tree
                    or manifest.get("source_commit") != commit):
                raise ValueError("Release manifest must bind the clean source tree")
        expected_sums = {entry["name"]: entry["sha256"] for entry in entries}
        expected_sums["release-manifest.json"] = digest(manifest_path.read_bytes())
        actual_sums = {}
        for line in sums_path.read_text(encoding="utf-8").splitlines():
            checksum, name = line.split("  ", 1)
            if name in actual_sums:
                raise ValueError("Duplicate checksum entry")
            actual_sums[name] = checksum
        if actual_sums != expected_sums:
            raise ValueError("SHA256SUMS does not match the actual release files")
    return {"status": "passed", "archive_count": len(paths), "source_files": source_count,
            "licenses_verified": len(paths),
            "manifest_verified": require_manifest or write_manifest}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--artifacts", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write-manifest", action="store_true")
    mode.add_argument("--require-manifest", action="store_true")
    args = parser.parse_args()
    print(json.dumps(verify(args.source_root, args.artifacts, write_manifest=args.write_manifest,
                            require_manifest=args.require_manifest)))
