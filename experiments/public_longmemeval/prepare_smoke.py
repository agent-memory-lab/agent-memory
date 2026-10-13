"""Explicitly reconstruct the pinned optional smoke from downloaded public data.

No network or model calls. Conversational artifacts are ignored local outputs.
"""
from __future__ import annotations

if __package__ in (None, ""):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import argparse
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace

from experiments.public_longmemeval.public_retrieval import prepare

ROOT = Path(__file__).resolve().parent


def select_prepared(prepared, output, manifest):
    """Extract only the frozen case; verify all bytes before writing any output."""
    artifacts = {}
    for source, target in (("corpus.jsonl", "corpus.json"),
                           ("queries.jsonl", "query.json"),
                           ("gold.evaluator-only.jsonl", "gold.evaluator-only.json")):
        matches = []
        with (Path(prepared) / source).open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                if row["case_id"] == manifest["case_id"]:
                    matches.append(row)
        if len(matches) != 1:
            raise ValueError("Expected exactly one frozen sample case in " + source)
        data = (json.dumps(matches[0], ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        if hashlib.sha256(data).hexdigest() != manifest["artifacts"][target]:
            raise ValueError("Reconstructed sample does not match the pinned bytes: " + target)
        if (Path(output) / target).exists():
            raise FileExistsError("Refusing to overwrite sample: " + target)
        artifacts[target] = data
    Path(output).mkdir(parents=True, exist_ok=True)
    for name, data in artifacts.items():
        with (Path(output) / name).open("xb") as destination:
            destination.write(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=ROOT / "data/longmemeval_s_cleaned.json")
    args = parser.parse_args()
    if not args.data.is_file():
        parser.error("Download the pinned source explicitly with the download_full module first")
    manifest = json.loads((ROOT / "smoke/manifest.json").read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="longmemeval-prepare-") as temporary:
        prepared = Path(temporary) / "prepared"
        prepare(SimpleNamespace(data=args.data, output=prepared))
        select_prepared(prepared, ROOT / "smoke", manifest)
    print("Prepared ignored local sample; hashes match smoke/manifest.json. No model calls made.")


if __name__ == "__main__":
    main()
