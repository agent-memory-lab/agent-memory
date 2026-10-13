"""Resolve the checkout core and record exactly which source files were used."""
import hashlib
from pathlib import Path
import subprocess
import sys

REPOSITORY = Path(__file__).resolve().parents[2]

def core_identity():
    source = REPOSITORY / "src" / "agent_memory"
    if not source.is_dir():
        raise RuntimeError("Run from the source repository checkout")
    sys.path.insert(0, str(source.parent))
    import agent_memory
    if Path(agent_memory.__file__).resolve().parent != source:
        raise RuntimeError("A different agent_memory package was already imported")
    try:
        revision = subprocess.check_output(
            ["git", "-C", str(REPOSITORY), "rev-parse", "HEAD"], text=True, encoding="utf-8",
            stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = None
    return {"git_revision": revision, "files": {
        p.relative_to(REPOSITORY).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(source.rglob("*")) if p.suffix in (".py", ".sql")},
        "experiment_files": {
            p.relative_to(REPOSITORY).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(Path(__file__).parent.glob("*.py"))}}


def input_identity(directory):
    """Verify and bind input bytes, including the artifact manifest itself."""
    import json
    directory = Path(directory)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifacts = {}
    for name, expected in manifest["artifacts"].items():
        if Path(name).name != name or name in (".", ".."):
            raise ValueError("Artifact names must be local filenames")
        actual = hashlib.sha256((directory / name).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError("Dataset artifact integrity check failed: " + name)
        artifacts[name] = actual
    return {"manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            "artifacts": artifacts}
