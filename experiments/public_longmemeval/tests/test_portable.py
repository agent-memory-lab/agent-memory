"""Offline contract tests. Model responses here are mocks, never benchmark scores."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from experiments.public_longmemeval import mac_smoke
from experiments.public_longmemeval import public_retrieval as retrieval


class Contracts(unittest.TestCase):
    def test_split(self):
        rows = [{"question_id": f"{kind}-{n}" + ("_abs" if n < 5 else ""),
                 "question_type": kind} for kind in ("a", "b", "c", "d", "e") for n in range(100)]
        result = retrieval.make_split(rows)
        self.assertEqual(len(result["development"]), 50)
        self.assertEqual(len(result["holdout"]), 450)
        self.assertFalse(set(result["development"]) & set(result["holdout"]))
        self.assertTrue(set(result["smoke"]) <= set(result["development"]))

    def test_leakage_fields_rejected(self):
        with self.assertRaises(ValueError):
            retrieval.blind_sessions({"case_id": "a", "sessions": [{"source_id": "a", "occurred_at": "d",
                "turns": [{"role": "user", "content": "x", "has_answer": True}]}]})

    def test_metrics(self):
        actual = retrieval.metrics(["b", "a", "c"], ["a", "b"], 5)
        self.assertEqual(actual["recall_all@5"], 1)
        self.assertEqual(actual["upstream_ndcg@5"], 1)
        self.assertEqual(retrieval.metrics(["a"], ["a", "b"], 5)["recall_all@5"], 0)
        with self.assertRaises(ValueError):
            retrieval.metrics([], [], 5)

    def test_unicode_chunks(self):
        original = "中文🙂abc" * 10000
        parts = list(mac_smoke.chunks(original))
        self.assertEqual("".join(parts), original)
        self.assertTrue(all(len(part.encode()) <= 9000 for part in parts))

    def test_only_bare_loopback_hosts(self):
        for host in ("https://example.com", "http://user:pass@localhost:11434", "http://localhost:11434/?a=1",
                     "http://localhost:11434/#fragment", "http://127.0.0.1:11434/path"):
            with self.assertRaises(ValueError):
                mac_smoke.LocalOllama(host, "model", "embedding", 1, 1)
        self.assertEqual(mac_smoke.LocalOllama("http://localhost:11434", "m", "e", 1, 1).host,
                         "http://127.0.0.1:11434")

    def test_empty_budget_rejects_without_network(self):
        client = mac_smoke.LocalOllama("http://127.0.0.1:11434", "m", "e", 0, 30)
        with self.assertRaises(RuntimeError):
            asyncio.run(client.generate("/api/chat", {}, "test"))
        self.assertEqual(client.receipts, [])

    def test_confidence_is_required(self):
        class Client:
            async def chat(self, *args, **kwargs):
                return json.dumps({"claims": [{"key": "a", "value": "b", "text": "c"}]})
        generator = mac_smoke.ClaimGenerator(Client())
        with self.assertRaises(ValueError):
            asyncio.run(generator.generate_claims(argparse.Namespace(content="test")))

    def test_real_core_with_mock_models(self):
        class MockClient:
            model = "mock-never-reported-as-benchmark"
            receipts = None
            def __init__(self):
                self.receipts = []
            async def chat(self, prompt, phase, **kwargs):
                self.receipts.append({"phase": phase, "status": "completed"})
                if phase == "extraction":
                    return json.dumps({"claims": [{"key": "drink", "value": "tea",
                                                   "text": "The user prefers tea.", "confidence": .95}]})
                return "Mock answer; not a benchmark response."
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            smoke = root / "smoke"
            smoke.mkdir()
            documents = {
                "corpus.json": {"case_id": "synthetic-test", "sessions": [{"source_id": "s0",
                    "occurred_at": "2024-01-01T00:00:00+00:00", "turns": [{"role": "user", "content": "I prefer tea."}]}]},
                "query.json": {"case_id": "synthetic-test", "question": "Which drink does the user prefer?",
                               "question_date": "2024-01-02T00:00:00+00:00"},
            }
            for name, value in documents.items():
                (smoke / name).write_text(json.dumps(value))
            (smoke / "manifest.json").write_text(json.dumps({"artifacts": {name: retrieval.sha(smoke / name) for name in documents}}))
            args = argparse.Namespace(smoke=smoke, output=root / "result", max_calls=160, max_seconds=1800)
            result = asyncio.run(mac_smoke.run(args, MockClient(), {"embedding": None}))
            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(result["arms"]), 4)
            self.assertGreater(result["extraction_claims_meeting_confidence_threshold"], 0)
            self.assertIsNone(result["answer_accuracy"])
            self.assertTrue((args.output / "result.json").exists())


if __name__ == "__main__":
    unittest.main()
