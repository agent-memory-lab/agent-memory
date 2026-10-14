"""Strict local Claims validation, independent of provider output mode."""
import asyncio
import copy
import json
from types import SimpleNamespace
import unittest

from experiments.public_longmemeval.extraction import CLAIM_SCHEMA, ClaimGenerator, validate_claims_output


CLAIM = {"key": "drink", "value": "tea", "text": "The user prefers tea.", "confidence": .95}


class ExtractionContracts(unittest.TestCase):
    def test_valid_empty_and_bounded_claims(self):
        self.assertEqual(validate_claims_output('{"claims":[]}'), [])
        claims = [copy.deepcopy(CLAIM) for _ in range(8)]
        self.assertEqual(validate_claims_output(json.dumps({"claims": claims})), claims)
        for confidence in (0, 1, .5):
            self.assertEqual(validate_claims_output(json.dumps({"claims": [{**CLAIM, "confidence": confidence}]}))[0]["confidence"], confidence)

    def test_entire_schema_is_enforced_without_repair_or_coercion(self):
        malformed = [[], {}, {"claims": None}, {"claims": {}}, {"claims": [], "extra": 1},
                     {"claims": [CLAIM] * 9}, {"claims": [None]},
                     {"claims": [{key: val for key, val in CLAIM.items() if key != "confidence"}]},
                     {"claims": [{**CLAIM, "extra": "not in schema"}]}]
        malformed += [{"claims": [{**CLAIM, "confidence": value}]} for value in
                      (True, False, None, "0.9", -1, 1.01, float("nan"), float("inf"), 10**400)]
        malformed += [{"claims": [{**CLAIM, name: value}]} for name in ("key", "value", "text")
                      for value in ("", " \n\t", 1, None, [])]
        for value in malformed:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_claims_output(json.dumps(value))

    def test_duplicates_nonfinite_json_and_wrapped_prose_are_rejected(self):
        for raw in ('{"claims":[],"claims":[]}',
                    '{"claims":[{"key":"x","key":"y","value":"v","text":"t","confidence":1}]}',
                    '{"claims":[{"key":"x","value":"v","text":"t","confidence":1e9999}]}',
                    '```json\n{"claims":[]}\n```'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                validate_claims_output(raw)

    def test_generator_only_counts_validated_claims_and_never_retries(self):
        class Client:
            def __init__(self):
                self.calls = []
                self.output = json.dumps({"claims": [CLAIM, {**CLAIM, "confidence": .5}]})

            async def chat(self, prompt, phase, **kwargs):
                self.calls.append((prompt, phase, kwargs))
                return self.output

        client = Client()
        generator = ClaimGenerator(client)
        result = asyncio.run(generator.generate_claims(SimpleNamespace(content="synthetic")))
        self.assertEqual(len(result), 2)
        self.assertEqual(generator.proposed, 2)
        self.assertEqual(generator.meeting_confidence_threshold, 1)
        self.assertEqual(client.calls[0][2]["format"], CLAIM_SCHEMA)
        client.output = '{"claims":[{}]}'
        with self.assertRaises(ValueError):
            asyncio.run(generator.generate_claims(SimpleNamespace(content="synthetic")))
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(generator.proposed, 2)
        self.assertEqual(generator.meeting_confidence_threshold, 1)


if __name__ == "__main__":
    unittest.main()
