"""Frozen public sample and custom split invariants; no downloads or model calls."""
import importlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from experiments.public_longmemeval import public_retrieval as retrieval
from experiments.public_longmemeval.tests.fixtures import write_smoke
from experiments.public_longmemeval.core import REPOSITORY, core_identity, input_identity

ROOT = Path(__file__).resolve().parents[1]


class Artifacts(unittest.TestCase):
    def test_sample_and_split_hashes(self):
        for directory in ("smoke", "protocol"):
            manifest = json.loads((ROOT / directory / "manifest.json").read_text(encoding="utf-8"))
            for name, digest in manifest["artifacts"].items():
                path = ROOT / directory / name
                if path.exists():
                    self.assertEqual(retrieval.sha(path), digest, name)
        split = json.loads((ROOT / "protocol/split.json").read_text(encoding="utf-8"))
        groups = {}
        for name, expected in (("development", 50), ("holdout", 450), ("smoke", 10)):
            groups[name] = json.loads((ROOT / "protocol" / (name + ".case-ids.json")).read_text(encoding="utf-8"))
            self.assertEqual(len(set(groups[name])), expected)
            self.assertEqual(groups[name], ["case-" + retrieval.stable(qid)[:24] for qid in split[name]])
        self.assertFalse(set(groups["development"]) & set(groups["holdout"]))
        self.assertTrue(set(groups["smoke"]) <= set(groups["development"]))
        sample = json.loads((ROOT / "smoke/manifest.json").read_text(encoding="utf-8"))
        self.assertIn(sample["case_id"], groups["smoke"])

    def test_source_identity_uses_checkout(self):
        identity = core_identity()
        import agent_memory
        self.assertEqual(Path(agent_memory.__file__).resolve().parent, REPOSITORY / "src/agent_memory")
        self.assertTrue(identity["files"])
        self.assertIn("src/agent_memory/__init__.py", identity["files"])

    def test_source_identity_includes_experiment_and_sql(self):
        identity = core_identity()
        for name in ("extraction.py", "public_retrieval.py", "transport.py"):
            self.assertIn("experiments/public_longmemeval/" + name, identity["experiment_files"])
        expected = {p.relative_to(REPOSITORY).as_posix() for p in (REPOSITORY / "src/agent_memory").rglob("*.sql")}
        self.assertTrue(expected <= set(identity["files"]))

    def test_input_binding_rejects_tampering(self):
        import shutil
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            copied = write_smoke(Path(directory))
            original = input_identity(copied)
            query = copied / "query.json"
            query.write_bytes(query.read_bytes() + b" ")
            with self.assertRaises(ValueError):
                input_identity(copied)
            manifest_path = copied / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["artifacts"]["query.json"] = retrieval.sha(query)
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8", newline="\n")
            self.assertNotEqual(input_identity(copied), original)

    def test_evaluator_rejects_missing_or_changed_binding(self):
        import tempfile
        from experiments.public_longmemeval import evaluate
        arms = {name: {"status": "completed"} for name in
                ("no-memory", "bm25-raw-chunk", "recent-raw-chunk", "agent-memory")}
        with tempfile.TemporaryDirectory() as directory:
            fixture_root = Path(directory) / "fixtures"
            write_smoke(fixture_root)
            self.enterContext(patch.object(evaluate, "ROOT", fixture_root))
            run = Path(directory)
            result = {"status": "completed", "arms": arms}
            for binding in (None, {"manifest_sha256": "wrong", "artifacts": {}}):
                result["input_artifacts"] = binding
                (run / "result.json").write_text(json.dumps(result), encoding="utf-8", newline="\n")
                with self.assertRaisesRegex(ValueError, "input binding"):
                    evaluate.evaluate(run)
                self.assertFalse((run / "manual_review.json").exists())

    def test_smoke_preparation_reconstructs_and_pins_synthetic_bytes(self):
        import tempfile
        from experiments.public_longmemeval.prepare_smoke import select_prepared
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = write_smoke(root / "fixture")
            manifest = json.loads((fixture / "manifest.json").read_text(encoding="utf-8"))
            manifest["case_id"] = "synthetic-test"
            prepared = root / "prepared"
            prepared.mkdir()
            for source, target in (("corpus.json", "corpus.jsonl"),
                                   ("query.json", "queries.jsonl"),
                                   ("gold.evaluator-only.json", "gold.evaluator-only.jsonl")):
                (prepared / target).write_text(json.dumps(json.loads((fixture / source).read_text(encoding="utf-8"))) + "\n", encoding="utf-8", newline="\n")
            output = root / "output"
            select_prepared(prepared, output, manifest)
            for name in manifest["artifacts"]:
                self.assertEqual((fixture / name).read_bytes(), (output / name).read_bytes())
            with self.assertRaises(FileExistsError):
                select_prepared(prepared, output, manifest)
            manifest["artifacts"]["query.json"] = "incorrect"
            with self.assertRaises(ValueError):
                select_prepared(prepared, root / "mismatched", manifest)
            self.assertFalse((root / "mismatched").exists())

    def test_downloader_import_has_no_network_or_filesystem_side_effects(self):
        with patch("urllib.request.urlopen", side_effect=AssertionError("No network")):
            module = importlib.import_module("experiments.public_longmemeval.download_full")
            importlib.reload(module)


if __name__ == "__main__":
    unittest.main()
