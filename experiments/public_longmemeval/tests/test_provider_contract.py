"""Pure synthetic contracts; no network, credentials, or live run state."""
import copy
import json
import unittest

from experiments.public_longmemeval.provider_contract import (
    prepare_structured_output, safe_response_metadata, strict_json_loads,
)


class ProviderContracts(unittest.TestCase):
    def test_mode_is_explicit_and_schema_is_identical_without_mutation(self):
        schema = {"type": "object", "properties": {"x": {"type": "number"}}, "required": ["x"]}
        original = copy.deepcopy(schema)
        prompt, wire = prepare_structured_output("synthetic", schema, mode="json_schema")
        self.assertEqual(prompt, "synthetic")
        self.assertEqual(wire["json_schema"]["schema"], original)
        wire["json_schema"]["schema"]["required"].append("other")
        self.assertEqual(schema, original)
        prompt, wire = prepare_structured_output("synthetic", schema, mode="json_object")
        self.assertEqual(wire, {"type": "json_object"})
        self.assertEqual(json.loads(prompt.split("this schema: ", 1)[1]), original)
        with self.assertRaises(ValueError):
            prepare_structured_output("synthetic", schema, mode="auto")
        with self.assertRaises(ValueError):
            prepare_structured_output("synthetic", None, mode="json_schema")

    def test_strict_json_rejects_duplicates_and_all_nonfinite_numbers(self):
        for raw in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}', '{"x":1e9999}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                strict_json_loads(raw)
        self.assertEqual(strict_json_loads(b'{"x":0.5}'), {"x": .5})

    def test_request_ids_survive_http_errors_with_header_precedence(self):
        for status in (200, 400, 403, 503):
            with self.subTest(status=status):
                result = safe_response_metadata(status, {"request_id": "body-request", "id": "chat-id"},
                                                headers={"X-Request-Id": "header-request"})
                self.assertEqual(result["provider_request_id"], "header-request")
                self.assertEqual(result["http_outcome"], "success" if status == 200 else "http_error")
        result = safe_response_metadata(503, {"request_id": "body-request", "id": "chat-id"})
        self.assertEqual(result["provider_request_id"], "body-request")
        self.assertEqual(safe_response_metadata(400, {"id": "chat-id"})["provider_request_id"], "chat-id")

    def test_safe_metadata_excludes_secrets_body_and_arbitrary_headers(self):
        secret = "synthetic-secret-unique"
        result = safe_response_metadata(400, {"request_id": secret, "error": {
            "code": secret, "message": "full body "+secret}},
            headers={"x-request-id": secret, "authorization": secret}, secrets=(secret,))
        self.assertIsNone(result["provider_request_id"])
        self.assertIsNone(result["provider_error_code"])
        self.assertNotIn(secret, json.dumps(result))
        for unsafe in ("spaces are not identifiers", "a\nb", "sk-provider-secret", "https://example.invalid/path"):
            with self.subTest(unsafe=unsafe):
                result = safe_response_metadata(400, {"request_id": unsafe, "error": {"code": unsafe}})
                self.assertIsNone(result["provider_request_id"])
                self.assertIsNone(result["provider_error_code"])

    def test_categories_classify_evidence_without_retry_instructions(self):
        for status, code, category in (
            (400, "invalid_parameter_error", "invalid_request"),
            (401, "InvalidApiKey", "authentication"),
            (403, "AllocationQuota.FreeTierOnly", "quota_or_billing"),
            (403, "AccessDenied", "permission"),
            (429, "Throttling", "rate_limit_or_quota"),
            (503, "ServiceUnavailable", "provider_service"),
            (302, "Redirect", "redirect_refused"),
        ):
            with self.subTest(status=status):
                result = safe_response_metadata(status, {"error": {"code": code}})
                self.assertEqual(result["error_category"], category)
                self.assertEqual(result["provider_error_code"], code)
                self.assertNotIn("retry", result)
        self.assertEqual(safe_response_metadata(None, None)["http_outcome"], "not_observed")


if __name__ == "__main__":
    unittest.main()
