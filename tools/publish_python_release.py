"""Upload verified archives to a draft; never replace an existing attachment."""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from verify_release_artifacts import verify


def missing_assets(paths, release):
    if not release["draft"]:
        raise ValueError("An already published release cannot be replaced")
    local = {path.name: "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
             for path in paths}
    remote = {asset["name"]: asset["digest"] for asset in release["assets"]}
    if any(name not in local or checksum != local[name] for name, checksum in remote.items()):
        raise ValueError("Existing draft assets differ; refuse to overwrite")
    return [path for path in paths if path.name not in remote]


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("action", choices=("stage",))
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    verify(root, args.artifacts, require_manifest=True)
    manifest = json.loads((args.artifacts / "release-manifest.json").read_text())
    if manifest["version"] != args.version:
        raise ValueError("Release version differs from verified archives")
    paths = sorted([*args.artifacts.rglob("*.whl"), *args.artifacts.rglob("*.tar.gz"),
                    args.artifacts / "release-manifest.json", args.artifacts / "SHA256SUMS"])
    tag = f"v{args.version}"
    notes = root / "docs/releases" / f"{args.version}.md"
    if not notes.is_file():
        raise ValueError("A reviewed release note file is required")
    # This handles draft releases too; REST's release-by-tag endpoint does not.
    result = subprocess.run(["gh", "release", "view", tag, "--json", "apiUrl"],
                            capture_output=True, text=True)
    if result.returncode:
        if "release not found" not in result.stderr.lower() and "404" not in result.stderr:
            raise RuntimeError(result.stderr)
        subprocess.run(["gh", "release", "create", tag, "--draft", "--prerelease",
                        "--verify-tag", "--title", f"Agent Memory {args.version} Alpha",
                        "--notes-file", str(notes)], check=True)
        result = subprocess.run(["gh", "release", "view", tag, "--json", "apiUrl"],
                                capture_output=True, text=True, check=True)
    release = json.loads(subprocess.check_output(
        ["gh", "api", json.loads(result.stdout)["apiUrl"]], text=True))
    missing = missing_assets(paths, release)
    if missing:
        subprocess.run(["gh", "release", "upload", tag, *map(str, missing)], check=True)
    print(json.dumps({"tag": tag, "draft": True, "uploaded": len(missing)}))


if __name__ == "__main__":
    main()
