"""Authored synthetic fixture, unrelated to the public benchmark conversations."""
import hashlib
import json
from pathlib import Path


def write_smoke(root):
    smoke = Path(root) / "smoke"
    smoke.mkdir(parents=True)
    docs = {
        "corpus.json": {"case_id": "synthetic-test", "sessions": [{"source_id": "session-0000",
            "occurred_at": "2024-01-01T00:00:00+00:00",
            "turns": [{"role": "user", "content": "The fictional test robot stores a blue cube."}]}]},
        "query.json": {"case_id": "synthetic-test", "question": "What color is the fictional robot's cube?",
            "question_date": "2024-01-02T00:00:00+00:00"},
        "gold.evaluator-only.json": {"case_id": "synthetic-test", "answer": "blue",
            "answer_source_ids": ["session-0000"]},
    }
    hashes = {}
    for name, value in docs.items():
        data = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
        (smoke / name).write_bytes(data)
        hashes[name] = hashlib.sha256(data).hexdigest()
    (smoke / "manifest.json").write_text(json.dumps({"dataset_revision": "authored-synthetic-fixture",
                                                   "artifacts": hashes}), encoding="utf-8", newline="\n")
    return smoke
