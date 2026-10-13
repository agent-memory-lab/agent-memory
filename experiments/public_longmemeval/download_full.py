"""Download only the pinned MIT LongMemEval-S dataset, never model weights."""
if __package__ in (None, ""):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import hashlib
from pathlib import Path
import urllib.request
from experiments.public_longmemeval.public_retrieval import DATASET_REVISION, DATASET_SHA256

def main():
    target = Path(__file__).parent / "data" / "longmemeval_s_cleaned.json"
    target.parent.mkdir(exist_ok=True)
    if target.exists():
        raise SystemExit("Target already exists; verify it instead of overwriting it.")
    url = f"https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/{DATASET_REVISION}/longmemeval_s_cleaned.json"
    digest = hashlib.sha256()
    temporary = target.with_suffix(".download")
    with temporary.open("xb") as destination, urllib.request.urlopen(url, timeout=120) as source:
        while block := source.read(1048576):
            destination.write(block)
            digest.update(block)
    if digest.hexdigest() != DATASET_SHA256 or temporary.stat().st_size != 277383467:
        raise SystemExit("Hash/size mismatch; file is not accepted as official data.")
    temporary.rename(target)
    print(target, DATASET_SHA256)


if __name__ == "__main__":
    main()
