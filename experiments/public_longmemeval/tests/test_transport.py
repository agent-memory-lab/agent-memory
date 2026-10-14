"""Offline tests. All network paths are mocked; no credentials are read."""
import asyncio
import base64
from decimal import Decimal
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from experiments.public_longmemeval import transport as t

KEY = "offline-test-key-never-a-real-credential"
ENDPOINT = "https://example.invalid/v1/chat/completions"


def response(input_tokens=10, output_tokens=4, *, usage=True, finish="stop", content="answer"):
    document = {"choices": [{"finish_reason": finish, "message": {"content": content}}]}
    if usage:
        document["usage"] = {"prompt_tokens": input_tokens, "completion_tokens": output_tokens,
                             "total_tokens": input_tokens + output_tokens}
    return json.dumps(document, ensure_ascii=False).encode("utf-8")


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "state"
        self.config = t.Config(endpoint=ENDPOINT, tariff_verified=True)
        self.clients = []
        # Fail the test instead of accidentally using any real network.
        self.network = patch.object(t.http.client, "HTTPSConnection", side_effect=AssertionError("Network forbidden"))
        self.network.start()

    def tearDown(self):
        for client in reversed(self.clients):
            if not client._active:
                client.close()
        self.network.stop()
        self.temp.cleanup()

    def client(self, config=None, accept=True):
        client = t.Client(config or self.config, KEY, self.state)
        self.clients.append(client)
        if accept:
            with patch.object(t.sys.stdin, "isatty", return_value=True), patch("builtins.input", return_value=t.BUDGET_ACCEPTANCE), patch("sys.stdout", new=io.StringIO()):
                client.accept_budget()
        return client

    def chat(self, client, body=None, **kwargs):
        with patch.object(client, "_request", return_value=(200, body or response())):
            return asyncio.run(client.chat("Public document: café", "answer", **kwargs))

    def ledger(self):
        return json.loads((self.state / "ledger.json").read_text(encoding="utf-8"))

    def receipt(self, ident):
        return json.loads((self.state / "calls" / (ident + ".json")).read_text(encoding="utf-8"))

    def test_fresh_accounting_is_zero(self):
        client = self.client()
        self.assertEqual(client.budget_summary()["charged_tokens"], 0)
        self.assertEqual(client.budget_summary()["charged_tariff_estimate_cny"], "0")
        self.assertEqual(set(self.state.iterdir()), {self.state / ".run.lock", self.state / "ledger.json"})

    def test_explicit_interactive_acceptance_is_required(self):
        client = self.client(accept=False)
        with patch.object(client, "_request") as network:
            with self.assertRaises(t.BudgetNotAccepted):
                asyncio.run(client.chat("x", "answer"))
            with patch.object(t.sys.stdin, "isatty", return_value=False):
                with self.assertRaises(t.BudgetNotAccepted):
                    client.accept_budget()
            with patch.object(t.sys.stdin, "isatty", return_value=True), patch("builtins.input", return_value="yes"), patch("sys.stdout", new=io.StringIO()):
                with self.assertRaises(t.BudgetNotAccepted):
                    client.accept_budget()
            network.assert_not_called()
        self.assertEqual(client.receipts, [])

    def test_tariff_must_be_user_verified(self):
        client = self.client(t.Config(endpoint=ENDPOINT))
        with self.assertRaises(t.BudgetExceeded):
            self.chat(client)
        self.assertEqual(client.receipts, [])

    def test_reservation_precedes_network_and_evidence_precedes_return(self):
        client = self.client()
        raw = response(content="exact café\ntext")
        def fake_request(payload, timeout):
            ledger = self.ledger()
            entry = ledger["calls"][0]
            receipt = self.receipt(entry["id"])
            self.assertEqual(entry["status"], "reserved")
            self.assertEqual(receipt["prompt"], "Public document: café")
            self.assertEqual(entry["reserved_input_tokens"], len(payload) + 2048)
            self.assertEqual(receipt["request_body_utf8"].encode(), payload)
            request = json.loads(payload)
            self.assertFalse(request["enable_thinking"])
            self.assertEqual(request["temperature"], 0)
            self.assertEqual(request["max_tokens"], 100)
            self.assertLessEqual(timeout, 90)
            return 200, raw
        with patch.object(client, "_request", side_effect=fake_request):
            result = asyncio.run(client.chat("Public document: café", "answer", limit=100))
        self.assertEqual(result, "exact café\ntext")
        entry = self.ledger()["calls"][0]
        receipt = self.receipt(entry["id"])
        self.assertEqual(entry["status"], "completed")
        self.assertEqual(base64.b64decode(receipt["response_body_base64"]), raw)
        self.assertEqual(receipt["usage"]["total_tokens"], 14)
        self.assertEqual(client.budget_summary()["charged_tokens"], 14)
        self.assertEqual(Decimal(client.budget_summary()["charged_tariff_estimate_cny"]), Decimal("0.0000188"))
        for path in self.state.rglob("*.json"):
            self.assertNotIn(KEY, path.read_text(encoding="utf-8"))
            self.assertNotIn(ENDPOINT, path.read_text(encoding="utf-8"))

    def test_completed_restart_keeps_cumulative_accounting(self):
        first = self.client()
        self.chat(first)
        first.close()
        second = self.client(accept=False)
        self.assertEqual(second.budget_summary()["charged_tokens"], 14)
        with self.assertRaises(t.BudgetNotAccepted):
            self.chat(second)
        self.assertEqual(len(second.receipts), 1)

    def test_new_outstanding_reservation_hard_blocks_restart(self):
        client = self.client()
        with patch.object(client, "_request", side_effect=TimeoutError("contains " + KEY + ENDPOINT)) as network:
            with self.assertRaises(t.RequestFailed) as error:
                asyncio.run(client.chat("x", "answer"))
            self.assertEqual(network.call_count, 1)
            self.assertNotIn(KEY, str(error.exception))
            with self.assertRaises(t.ResumeBlocked):
                asyncio.run(client.chat("x", "answer"))
        held = client.budget_summary()["charged_tokens"]
        self.assertGreater(held, 0)
        client.close()
        with self.assertRaises(t.ResumeBlocked):
            self.client()
        self.assertEqual(self.ledger()["budget"]["charged_tokens"], held)
        self.assertNotIn(KEY, (self.state / "ledger.json").read_text(encoding="utf-8"))

    def test_missing_usage_keeps_full_reservation_and_body(self):
        client = self.client()
        raw = response(usage=False)
        with self.assertRaises(t.RequestFailed):
            self.chat(client, raw)
        entry = client.receipts[0]
        self.assertEqual(entry["status"], "unresolved")
        self.assertEqual(entry["charged_tokens"], entry["reserved_input_tokens"] + entry["reserved_output_tokens"])
        self.assertEqual(base64.b64decode(self.receipt(entry["id"])["response_body_base64"]), raw)
        client.close()
        with self.assertRaises(t.ResumeBlocked):
            self.client()

    def test_usage_is_complete_strict_nonnegative_integer_and_consistent(self):
        base = {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        self.assertEqual(t._validated_usage({"usage": base}), base)
        for bad in (None, {}, {**base, "prompt_tokens": True}, {**base, "completion_tokens": 2.0},
                    {**base, "total_tokens": 4}, {**base, "prompt_tokens": -1}, {**base, "prompt_tokens": "1"}):
            self.assertIsNone(t._validated_usage({"usage": bad}), bad)
        with self.assertRaises(ValueError):
            t._read_json(b'{"usage":{},"usage":{}}')

    def test_overrun_charges_actual_and_stops(self):
        client = self.client()
        raw = response(1_000_001, 10)
        with self.assertRaises(t.UsageOverrun):
            self.chat(client, raw)
        self.assertEqual(client.budget_summary()["charged_tokens"], 1_000_011)
        self.assertLess(client.budget_summary()["remaining_tokens"], 0)
        self.assertEqual(client.receipts[0]["charged_cny"], "0.8000278")
        with self.assertRaises(t.ResumeBlocked):
            self.chat(client)

    def test_truncation_is_saved_charged_and_not_scored(self):
        client = self.client()
        raw = response(finish="length")
        with self.assertRaisesRegex(t.RequestFailed, "OutputTruncated"):
            self.chat(client, raw)
        entry = client.receipts[0]
        self.assertEqual(entry["charged_tokens"], 14)
        self.assertEqual(base64.b64decode(self.receipt(entry["id"])["response_body_base64"]), raw)
        with self.assertRaises(t.ResumeBlocked):
            self.chat(client)

    def test_malformed_choices_still_persist_body_and_usage(self):
        client = self.client()
        raw = json.dumps({"choices": ["wrong shape"], "usage": {
            "prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14}}).encode()
        with self.assertRaisesRegex(t.RequestFailed, "InvalidResponseContent"):
            self.chat(client, raw)
        entry = client.receipts[0]
        self.assertEqual(entry["charged_tokens"], 14)
        self.assertEqual(base64.b64decode(self.receipt(entry["id"])["response_body_base64"]), raw)

    def test_duplicate_lock_blocks_before_any_state_mutation(self):
        client = self.client()
        before = (self.state / "ledger.json").read_bytes()
        with self.assertRaises(t.RunLocked):
            t.Client(self.config, KEY, self.state)
        self.assertEqual(before, (self.state / "ledger.json").read_bytes())
        self.assertTrue((self.state / ".run.lock").exists())
        client.close()
        self.assertFalse((self.state / ".run.lock").exists())

    def test_config_changes_do_not_reset_budget(self):
        client = self.client()
        self.chat(client)
        client.close()
        with self.assertRaises(t.ResumeBlocked):
            self.client(t.Config(endpoint=ENDPOINT, max_calls=999, tariff_verified=True))
        self.assertEqual(self.ledger()["budget"]["charged_tokens"], 14)

    def test_endpoint_changes_require_manual_review(self):
        client = self.client()
        client.close()
        with self.assertRaises(t.ResumeBlocked):
            self.client(t.Config(endpoint="https://other.example.invalid/v1/chat/completions", tariff_verified=True))

    def test_deadline_and_max_calls_survive_restart(self):
        config = t.Config(endpoint=ENDPOINT, max_calls=1, tariff_verified=True)
        client = self.client(config)
        self.chat(client)
        client.close()
        second = self.client(config)
        with self.assertRaises(t.BudgetExceeded):
            self.chat(second)
        deadline = second.ledger["deadline_unix"]
        with patch.object(t.time, "time", return_value=deadline + 1):
            with self.assertRaises(t.BudgetExceeded):
                self.chat(second)

    def test_global_caps_check_full_reservation_before_call(self):
        client = self.client()
        with patch.object(client, "_request") as network:
            with self.assertRaises(t.BudgetExceeded):
                asyncio.run(client.chat("x" * 1_000_000, "answer"))
            network.assert_not_called()
        self.assertEqual(client.receipts, [])

    def test_cost_cap_is_independent_of_token_cap(self):
        config = t.Config(endpoint=ENDPOINT, input_cny_per_million="100000", tariff_verified=True)
        client = self.client(config)
        with self.assertRaises(t.BudgetExceeded):
            self.chat(client)
        self.assertEqual(client.receipts, [])

    def test_subprocess_timeout_is_killed_without_retry(self):
        client = self.client()
        process = Mock(returncode=-9)
        process.communicate.side_effect = [subprocess.TimeoutExpired("worker", 0.01), (b"", b"")]
        with patch.object(t.subprocess, "Popen", return_value=process) as popen:
            with self.assertRaises(TimeoutError):
                client._request(b"{}", 0.01)
        self.assertEqual(popen.call_count, 1)
        process.kill.assert_called_once_with()
        argv = popen.call_args.args[0]
        self.assertNotIn(KEY, str(argv))
        self.assertNotIn(ENDPOINT, str(argv))
        envelope = json.loads(process.communicate.call_args_list[0].args[0])
        self.assertEqual(envelope["key"], KEY)
        self.assertEqual(popen.call_args.kwargs["stderr"], subprocess.DEVNULL)

    def test_atomic_write_failure_never_returns_answer_or_allows_next_call(self):
        client = self.client()
        original_write = t.atomic_write_bytes
        completed_evidence_seen = []
        def fail_completed_ledger(path, data):
            if Path(path).name == "ledger.json" and b'"status":"completed"' in data:
                ident = client.receipts[0]["id"]
                completed_evidence_seen.append(self.receipt(ident)["usage"])
                raise OSError("Simulated disk failure")
            return original_write(path, data)
        with patch.object(t, "atomic_write_bytes", side_effect=fail_completed_ledger):
            with self.assertRaises(OSError):
                self.chat(client)
        self.assertEqual(completed_evidence_seen[0]["total_tokens"], 14)
        with self.assertRaises(t.ResumeBlocked):
            self.chat(client)
        client.close()
        with self.assertRaises(t.ResumeBlocked):
            self.client()

    def test_receipt_tampering_or_missing_ledger_hard_blocks(self):
        client = self.client()
        self.chat(client)
        ident = client.receipts[0]["id"]
        client.close()
        path = self.state / "calls" / (ident + ".json")
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaises(t.ResumeBlocked):
            self.client()
        (self.state / "ledger.json").unlink()
        with self.assertRaises(t.ResumeBlocked):
            self.client()

    def test_ledger_charge_cannot_shrink_independently_of_receipt(self):
        client = self.client()
        self.chat(client)
        client.close()
        ledger = self.ledger()
        ledger["calls"][0]["charged_tokens"] = 0
        ledger["budget"]["charged_tokens"] = 0
        ledger["budget"]["remaining_tokens"] = 1000000
        t.atomic_write_json(self.state / "ledger.json", ledger)
        with self.assertRaises(t.ResumeBlocked):
            self.client()

    def test_key_reflection_is_omitted_but_usage_charged(self):
        client = self.client()
        with self.assertRaisesRegex(t.RequestFailed, "SecretReflection"):
            self.chat(client, response(content=KEY))
        entry = client.receipts[0]
        self.assertEqual(entry["charged_tokens"], 14)
        receipt = self.receipt(entry["id"])
        self.assertIsNone(receipt["response_body_base64"])
        for path in self.state.rglob("*.json"):
            self.assertNotIn(KEY, path.read_text(encoding="utf-8"))

    def test_json_escaped_key_reflection_is_also_omitted(self):
        client = self.client()
        raw = response(content=KEY).replace(KEY.encode(), ("\\u006f" + KEY[1:]).encode())
        self.assertNotIn(KEY.encode(), raw)
        with self.assertRaisesRegex(t.RequestFailed, "SecretReflection"):
            self.chat(client, raw)
        entry = client.receipts[0]
        self.assertEqual(entry["charged_tokens"], 14)
        self.assertIsNone(self.receipt(entry["id"])["response_body_base64"])

    def test_malformed_json_escaped_secret_body_is_not_saved(self):
        client = self.client()
        escaped = "\\u006f" + KEY[1:]
        raw = ('{"x":"' + escaped + '","x":"duplicate"}').encode()
        with self.assertRaises(t.RequestFailed):
            self.chat(client, raw)
        self.assertIsNone(self.receipt(client.receipts[0]["id"])["response_body_base64"])

    def test_json_escaped_endpoint_in_nested_key_is_detected(self):
        document = {"nested": [{ENDPOINT: "ordinary"}]}
        encoded = json.dumps(document).replace("https", "\\u0068ttps").encode()
        self.assertTrue(t._contains_secret(t._read_json(encoded), (KEY, ENDPOINT)))

    def test_endpoint_reflection_and_provider_errors_are_not_persisted(self):
        client = self.client()
        raw = ("Authentication error " + KEY + " " + ENDPOINT).encode()
        with patch.object(client, "_request", return_value=(401, raw)):
            with self.assertRaisesRegex(t.RequestFailed, "HTTPStatusError"):
                asyncio.run(client.chat("x", "answer"))
        for path in self.state.rglob("*.json"):
            self.assertNotIn(KEY, path.read_text(encoding="utf-8"))
            self.assertNotIn(ENDPOINT, path.read_text(encoding="utf-8"))

    def test_raw_schema_is_wrapped_and_model_exposed(self):
        client = self.client()
        schema = {"type": "object", "properties": {"claims": {"type": "array"}}, "required": ["claims"]}
        self.chat(client, format=schema)
        body = json.loads(self.receipt(client.receipts[0]["id"])["request_body_utf8"])
        self.assertEqual(body["response_format"]["type"], "json_schema")
        self.assertEqual(body["response_format"]["json_schema"]["schema"], schema)
        self.assertEqual(client.model, "YOUR_PROVIDER_MODEL")

    def test_json_object_mode_embeds_schema_and_binds_mode_to_ledger(self):
        client = self.client(t.Config(endpoint=ENDPOINT, tariff_verified=True, structured_output_mode="json_object"))
        schema = {"type": "object", "properties": {"claims": {"type": "array"}}, "required": ["claims"]}
        self.chat(client, format=schema)
        receipt = self.receipt(client.receipts[0]["id"])
        body = json.loads(receipt["request_body_utf8"])
        self.assertEqual(body["response_format"], {"type": "json_object"})
        actual_schema = json.loads(body["messages"][0]["content"].split("this schema: ", 1)[1])
        self.assertEqual(actual_schema, schema)
        self.assertEqual(receipt["reserved_input_tokens"], len(receipt["request_body_utf8"].encode()) + 2048)
        self.assertEqual(client.ledger["config"]["structured_output_mode"], "json_object")
        client.close()
        with self.assertRaises(t.ResumeBlocked):
            self.client(self.config)

    def test_format_conflicts_fail_before_reservation_or_network(self):
        client = self.client()
        with patch.object(client, "_request") as request:
            with self.assertRaises(t.TransportError):
                asyncio.run(client.chat("synthetic", "extraction", format={"type": "json_object"}))
            request.assert_not_called()
        self.assertEqual(client.receipts, [])
        with self.assertRaises(t.TransportError):
            t.Config(endpoint=ENDPOINT, structured_output_mode="auto")

    def test_http_error_preserves_safe_id_and_code_without_body_or_retry(self):
        client = self.client()
        raw = json.dumps({"request_id": "body-safe-id", "error": {
            "code": "ServiceUnavailable", "message": "never save "+KEY}}).encode()
        with patch.object(client, "_request", return_value=(503, raw, {"x-request-id": "header-safe-id"})) as request:
            with self.assertRaises(t.RequestFailed) as caught:
                asyncio.run(client.chat("synthetic", "extraction"))
            self.assertEqual(caught.exception.request_id, "header-safe-id")
            self.assertEqual(caught.exception.provider_error_code, "ServiceUnavailable")
            self.assertEqual(caught.exception.diagnostics["error_category"], "provider_service")
            with self.assertRaises(t.ResumeBlocked):
                asyncio.run(client.chat("synthetic", "extraction"))
            self.assertEqual(request.call_count, 1)
        entry = client.receipts[0]
        receipt = self.receipt(entry["id"])
        self.assertEqual(entry["request_outcome"], "http_error")
        self.assertEqual(entry["http_outcome"], "http_error")
        self.assertEqual(entry["usage_status"], "unknown")
        self.assertEqual(entry["cost_basis"], "conservative_reservation")
        self.assertEqual(entry["provider_request_id"], "header-safe-id")
        self.assertIsNone(receipt["response_body_base64"])
        self.assertIsNotNone(entry["latency_seconds"])
        budget = client.budget_summary()
        self.assertEqual(Decimal(budget["known_usage_tariff_estimate_cny"]), 0)
        self.assertEqual(budget["unknown_usage_reserved_cny"], entry["reserved_cny"])
        for path in self.state.rglob("*.json"):
            self.assertNotIn(KEY, path.read_text())
            self.assertNotIn("never save", path.read_text())

    def test_error_with_known_usage_is_charged_without_unknown_reservation(self):
        client = self.client()
        document = json.loads(response())
        document.update(request_id="request-body-id", error={"code": "invalid_parameter_error"})
        with patch.object(client, "_request", return_value=(400, json.dumps(document).encode())):
            with self.assertRaises(t.RequestFailed):
                asyncio.run(client.chat("synthetic", "extraction"))
        entry = client.receipts[0]
        self.assertEqual(entry["request_outcome"], "http_error")
        self.assertEqual(entry["usage_status"], "known")
        self.assertEqual(entry["provider_request_id"], "request-body-id")
        self.assertEqual(entry["cost_basis"], "provider_usage_tariff_estimate")
        self.assertEqual(entry["status"], "halted")
        budget = client.budget_summary()
        self.assertEqual(Decimal(budget["unknown_usage_reserved_cny"]), 0)
        self.assertEqual(budget["known_usage_tokens"], 14)

    def test_missing_usage_preserves_successful_http_outcome_and_request_id(self):
        client = self.client()
        document = json.loads(response(usage=False))
        document["request_id"] = "known-response-id"
        with self.assertRaises(t.RequestFailed):
            self.chat(client, json.dumps(document).encode())
        entry = client.receipts[0]
        self.assertEqual(entry["http_outcome"], "success")
        self.assertEqual(entry["request_outcome"], "response_rejected")
        self.assertEqual(entry["usage_status"], "unknown")
        self.assertEqual(entry["provider_request_id"], "known-response-id")

    def test_transport_timeout_is_not_reported_as_observed_http_failure(self):
        client = self.client()
        with patch.object(client, "_request", side_effect=TimeoutError("private detail")):
            with self.assertRaises(t.RequestFailed):
                asyncio.run(client.chat("synthetic", "extraction"))
        entry = client.receipts[0]
        self.assertEqual(entry["http_outcome"], "not_observed")
        self.assertEqual(entry["request_outcome"], "transport_error")
        self.assertEqual(entry["error_category"], "transport_failure")
        self.assertEqual(entry["usage_status"], "unknown")

    def test_budget_summary_separates_success_cost_and_error_reservation(self):
        client = self.client()
        self.chat(client)
        with patch.object(client, "_request", return_value=(400, b'{"error":{"code":"bad_request"}}')):
            with self.assertRaises(t.RequestFailed):
                asyncio.run(client.chat("synthetic", "extraction"))
        budget = client.budget_summary()
        self.assertEqual(budget["known_usage_tokens"], 14)
        self.assertEqual(Decimal(budget["known_usage_tariff_estimate_cny"]), Decimal("0.0000188"))
        self.assertEqual(Decimal(budget["charged_tariff_estimate_cny"]),
                         Decimal(budget["known_usage_tariff_estimate_cny"])+Decimal(budget["unknown_usage_reserved_cny"]))

    def test_worker_id_is_forwarded_but_parent_revalidates_it(self):
        client = self.client()
        envelope = {"status": 400, "body_base64": base64.b64encode(b'{}').decode(), "provider_request_id": "worker-safe-id"}
        process = Mock(returncode=0)
        process.communicate.return_value = (json.dumps(envelope).encode(), b"")
        with patch.object(t.subprocess, "Popen", return_value=process):
            status, body, headers = client._request(b'{}', 1)
        self.assertEqual((status, body, headers), (400, b'{}', {"x-request-id": "worker-safe-id"}))
        with patch.object(client, "_request", return_value=(400, b'{}', {"x-request-id": KEY})):
            with self.assertRaises(t.RequestFailed) as error:
                asyncio.run(client.chat("synthetic", "extraction"))
        self.assertIsNone(error.exception.request_id)

    def test_endpoint_validation_rejects_insecure_or_ambiguous_routes(self):
        for endpoint in ("http://example.invalid/v1/chat/completions", "https://name:password@example.invalid/chat/completions",
                         "https://example.invalid/chat/completions?key=x", "https://example.invalid/v1", "https://example.invalid/chat/completions#x"):
            with self.assertRaises(t.TransportError):
                t.Config(endpoint=endpoint)

    def test_worker_uses_direct_https_and_does_not_follow_redirects(self):
        connection = Mock()
        connection.getresponse.return_value.status = 302
        connection.getresponse.return_value.read.return_value = b"redirect"
        connection.getresponse.return_value.getheader.side_effect = lambda name: "worker-header-id" if name == "x-request-id" else None
        envelope = t._canonical({"endpoint": ENDPOINT, "key": KEY, "payload_base64": base64.b64encode(b"{}").decode(),
                                 "timeout": 2, "max_response_bytes": 1000})
        stdin = Mock(buffer=io.BytesIO(envelope))
        stdout = Mock(buffer=io.BytesIO())
        with patch.object(t.http.client, "HTTPSConnection", return_value=connection) as https, patch.object(t.sys, "stdin", stdin), patch.object(t.sys, "stdout", stdout):
            self.assertEqual(t._worker(), 0)
        self.assertEqual(https.call_count, 1)
        self.assertEqual(connection.request.call_count, 1)
        self.assertEqual(connection.request.call_args.args[:2], ("POST", "/v1/chat/completions"))
        self.assertEqual(json.loads(stdout.buffer.getvalue())["status"], 302)
        self.assertEqual(json.loads(stdout.buffer.getvalue())["provider_request_id"], "worker-header-id")

    def test_secure_key_prompt_refuses_non_tty_and_echo_fallback(self):
        with patch.object(t.sys.stdin, "isatty", return_value=False), patch.object(t.getpass, "getpass") as getpass:
            with self.assertRaises(t.TransportError):
                t.prompt_api_key()
            getpass.assert_not_called()
        with patch.object(t.sys.stdin, "isatty", return_value=True), patch.object(t.getpass, "getpass", side_effect=t.getpass.GetPassWarning("echo")):
            with self.assertRaises(t.TransportError):
                t.prompt_api_key()


if __name__ == "__main__":
    unittest.main()
