"""Offline accounting and transport tests: no credentials or network are required."""

import json
import tempfile
import threading
import traceback
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

from experiments.evidence_obligations import model_client as module
from experiments.evidence_obligations.model_client import (
    ENDPOINT,
    MAX_RESPONSE_BYTES,
    MODEL,
    BudgetExceeded,
    BudgetLedger,
    ClientError,
    LedgerError,
    LedgerStopped,
    OpenAICompatibleClient,
)

MESSAGES = [{"role": "user", "content": "Summarize this synthetic document."}]
FAKE_KEY = "offline-test-key-not-a-credential"


def response(input_tokens=20, output_tokens=10, content="test answer"):
    return json.dumps({
        "choices": [{"message": {"role": "assistant", "content": content}}],
        "usage": {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    }).encode()


class ModelClientTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "budget.json"
        self.ledger = BudgetLedger(self.path)
        self.calls = []

    def transport(self, request, timeout, limit):
        self.calls.append((request, timeout, limit))
        return response()

    def client(self, transport=None, ledger=None):
        return OpenAICompatibleClient(
            FAKE_KEY, ledger=ledger or self.ledger, transport=transport or self.transport,
        )

    def assert_stopped_with_full_charge(self, reason):
        snapshot = self.ledger.snapshot()
        self.assertTrue(snapshot["stopped"])
        self.assertEqual(snapshot["stop_reason"], reason)
        receipt = snapshot["receipts"][-1]
        self.assertEqual(receipt["status"], "uncertain")
        self.assertEqual(receipt["charged_input_tokens"], receipt["reserved_input_tokens"])
        self.assertEqual(receipt["charged_output_tokens"], receipt["reserved_output_tokens"])
        self.assertIsNone(receipt["input_tokens"])
        self.assertIsNone(receipt["output_tokens"])
        with self.assertRaises(LedgerStopped):
            self.client().complete(MESSAGES, 100, "retry")
        reopened = BudgetLedger(self.path)
        with self.assertRaises(LedgerStopped):
            self.client(ledger=reopened).complete(MESSAGES, 100, "rerun")

    def test_success_has_fixed_request_and_complete_receipt(self):
        result = self.client().complete(MESSAGES, 100, "answer")
        self.assertEqual(result["content"], "test answer")
        self.assertEqual(result["input_tokens"], 20)
        self.assertEqual(result["output_tokens"], 10)
        self.assertGreaterEqual(result["latency"], 0)
        request, timeout, limit = self.calls[0]
        self.assertEqual(request.full_url, ENDPOINT)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer " + FAKE_KEY)
        self.assertEqual(timeout, 90)
        self.assertEqual(limit, MAX_RESPONSE_BYTES)
        payload = json.loads(request.data)
        self.assertEqual(payload, {
            "model": MODEL, "messages": MESSAGES, "temperature": 0,
            "enable_thinking": False, "stream": False, "max_tokens": 100,
        })
        snapshot = self.ledger.snapshot()
        self.assertEqual(snapshot["total_calls"], 1)
        self.assertEqual(snapshot["total_tokens"], 30)
        self.assertEqual(snapshot["total_cost_cny"], "0.0000430")
        self.assertEqual(snapshot["receipts"][0]["id"], result["receipt_id"])
        self.assertEqual(snapshot["receipts"][0]["status"], "completed")
        persisted = self.path.read_text()
        self.assertNotIn(FAKE_KEY, persisted)
        self.assertNotIn(MESSAGES[0]["content"], persisted)
        self.assertNotIn("test answer", persisted)

    def test_reserved_before_dispatch_with_utf8_and_protocol_bound(self):
        messages = [{"role": "user", "content": "中文 🐱"}]

        def inspect(request, timeout, limit):
            snapshot = BudgetLedger(self.path).snapshot()
            receipt = snapshot["receipts"][0]
            self.assertEqual(receipt["status"], "reserved")
            self.assertEqual(
                receipt["reserved_input_tokens"],
                len(request.data) + module.PROTOCOL_OVERHEAD_TOKENS
                + module.PER_MESSAGE_OVERHEAD_TOKENS,
            )
            self.assertGreater(len(request.data), len(request.data.decode("utf-8")))
            self.assertEqual(snapshot["total_tokens"], receipt["reserved_input_tokens"] + 100)
            return response()

        self.client(transport=inspect).complete(messages, 100, "unicode")

    def test_cumulative_accounting_survives_new_client_and_rerun(self):
        self.client().complete(MESSAGES, 100, "extract")
        reopened = BudgetLedger(self.path)
        self.client(ledger=reopened).complete(MESSAGES, 100, "answer")
        snapshot = self.ledger.snapshot()
        self.assertEqual(snapshot["total_calls"], 2)
        self.assertEqual(snapshot["total_input_tokens"], 40)
        self.assertEqual(snapshot["total_output_tokens"], 20)
        self.assertEqual(snapshot["total_tokens"], 60)
        self.assertEqual(snapshot["total_cost_cny"], "0.0000860")
        self.assertEqual([r["phase"] for r in snapshot["receipts"]], ["extract", "answer"])

    def test_token_cap_blocks_dispatch_without_charging(self):
        ledger = BudgetLedger(Path(self.temp.name) / "tiny.json", token_limit=100)
        with self.assertRaises(BudgetExceeded):
            self.client(ledger=ledger).complete(MESSAGES, 100, "answer")
        self.assertEqual(self.calls, [])
        self.assertEqual(ledger.snapshot()["total_calls"], 0)

    def test_money_cap_blocks_dispatch_without_charging(self):
        ledger = BudgetLedger(Path(self.temp.name) / "cheap.json", cny_limit="0.0001")
        with self.assertRaises(BudgetExceeded):
            self.client(ledger=ledger).complete(MESSAGES, 100, "answer")
        self.assertEqual(self.calls, [])
        self.assertEqual(ledger.snapshot()["total_calls"], 0)

    def test_exact_cap_allowed_and_next_reservation_refused(self):
        ledger = BudgetLedger(Path(self.temp.name) / "exact.json", token_limit=30)
        identifier = ledger.reserve(20, 10, "first")
        ledger._settle(identifier, latency=0, input_tokens=20, output_tokens=10)
        with self.assertRaises(BudgetExceeded):
            ledger.reserve(1, 1, "second")
        self.assertEqual(ledger.snapshot()["total_tokens"], 30)

    def test_exact_money_cap_and_integer_pricing(self):
        ledger = BudgetLedger(Path(self.temp.name) / "money.json", cny_limit="0.000043")
        identifier = ledger.reserve(20, 10, "first")
        ledger._settle(identifier, latency=0, input_tokens=20, output_tokens=10)
        with self.assertRaises(BudgetExceeded):
            ledger.reserve(1, 1, "second")
        self.assertEqual(ledger.snapshot()["total_cost_units"], 430)

    def test_crash_reservation_survives_and_blocks_rerun(self):
        self.ledger.reserve(7000, 500, "interrupted")
        reopened = BudgetLedger(self.path)
        with self.assertRaises(LedgerStopped):
            self.client(ledger=reopened).complete(MESSAGES, 100, "rerun")
        self.assertEqual(reopened.snapshot()["total_tokens"], 7500)
        self.assertEqual(self.calls, [])

    def test_failure_is_sanitized_no_retry_and_charged_in_full(self):
        called = []

        def broken(request, timeout, limit):
            called.append(True)
            raise urllib.error.URLError("sensitive body " + FAKE_KEY)

        try:
            self.client(transport=broken).complete(MESSAGES, 100, "answer")
        except ClientError as error:
            formatted = "".join(traceback.format_exception(error))
            self.assertNotIn(FAKE_KEY, formatted)
            self.assertNotIn("sensitive body", formatted)
        else:
            self.fail("A failed model call must raise.")
        self.assertEqual(called, [True])
        self.assert_stopped_with_full_charge("transport_failure")

    def test_interruption_retains_full_charge_and_latches_stop(self):
        def interrupted(request, timeout, limit):
            raise KeyboardInterrupt(FAKE_KEY)

        with self.assertRaises(ClientError):
            self.client(transport=interrupted).complete(MESSAGES, 100, "answer")
        self.assert_stopped_with_full_charge("transport_failure")

    def test_missing_and_malformed_usage_all_fail_closed(self):
        usages = [
            None, {}, [], {"prompt_tokens": 20, "completion_tokens": 10},
            {"prompt_tokens": "20", "completion_tokens": 10, "total_tokens": 30},
            {"prompt_tokens": 20.0, "completion_tokens": 10, "total_tokens": 30},
            {"prompt_tokens": True, "completion_tokens": 10, "total_tokens": 11},
            {"prompt_tokens": 0, "completion_tokens": 10, "total_tokens": 10},
            {"prompt_tokens": -1, "completion_tokens": 10, "total_tokens": 9},
            {"prompt_tokens": 20, "completion_tokens": -1, "total_tokens": 19},
            {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 31},
            {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": True},
        ]
        for index, usage in enumerate(usages):
            with self.subTest(usage=usage):
                ledger = BudgetLedger(Path(self.temp.name) / f"usage-{index}.json")
                data = json.loads(response())
                data["usage"] = usage
                def transport(request, timeout, limit, d=data):
                    return json.dumps(d).encode()

                with self.assertRaises(ClientError):
                    self.client(transport=transport, ledger=ledger).complete(MESSAGES, 100, "bad")
                snapshot = ledger.snapshot()
                self.assertTrue(snapshot["stopped"])
                receipt = snapshot["receipts"][0]
                self.assertEqual(receipt["failure_reason"], "invalid_usage")
                self.assertEqual(receipt["charged_input_tokens"], receipt["reserved_input_tokens"])

    def test_input_overrun_retains_reservation_and_stops(self):
        def oversized(request, timeout, limit):
            bound = self.ledger.snapshot()["receipts"][0]["reserved_input_tokens"]
            return response(input_tokens=bound + 1)

        with self.assertRaises(ClientError):
            self.client(transport=oversized).complete(MESSAGES, 100, "answer")
        self.assert_stopped_with_full_charge("usage_overrun")

    def test_output_overrun_retains_reservation_and_stops(self):
        with self.assertRaises(ClientError):
            self.client(transport=lambda *args: response(output_tokens=101)).complete(
                MESSAGES, 100, "answer",
            )
        self.assert_stopped_with_full_charge("usage_overrun")

    def test_bad_body_or_choice_retains_full_charge(self):
        bodies = [b"bad private error body", b"[]", b"{}", b'{"error":"private"}',
                  b"x" * (MAX_RESPONSE_BYTES + 1)]
        data = json.loads(response())
        data["choices"] = []
        bodies.append(json.dumps(data).encode())
        for index, body in enumerate(bodies):
            with self.subTest(index=index):
                ledger = BudgetLedger(Path(self.temp.name) / f"body-{index}.json")
                with self.assertRaises(ClientError):
                    self.client(transport=lambda *args, b=body: b, ledger=ledger).complete(
                        MESSAGES, 100, "bad",
                    )
                self.assertTrue(ledger.snapshot()["stopped"])

    def test_empty_output_is_valid_when_accounting_is_valid(self):
        client = self.client(transport=lambda *args: response(output_tokens=0, content=""))
        result = client.complete(MESSAGES, 100, "empty")
        self.assertEqual(result["output_tokens"], 0)
        self.assertEqual(self.ledger.snapshot()["total_tokens"], 20)

    def test_existing_invalid_ledger_is_never_replaced(self):
        path = Path(self.temp.name) / "invalid.json"
        path.write_text("existing invalid data")
        with self.assertRaises(LedgerError):
            BudgetLedger(path)
        self.assertEqual(path.read_text(), "existing invalid data")

    def test_existing_limits_cannot_be_raised_or_silently_reset(self):
        path = Path(self.temp.name) / "limited.json"
        BudgetLedger(path, token_limit=5000, cny_limit="1")
        original = path.read_bytes()
        with self.assertRaises(LedgerError):
            BudgetLedger(path)
        self.assertEqual(path.read_bytes(), original)

    def test_corrupt_totals_fail_closed(self):
        self.client().complete(MESSAGES, 100, "answer")
        data = json.loads(self.path.read_text())
        data["total_tokens"] = 0
        self.path.write_text(json.dumps(data))
        with self.assertRaises(LedgerError):
            self.client().complete(MESSAGES, 100, "rerun")
        self.assertEqual(len(self.calls), 1)

    def test_failure_to_persist_reservation_prevents_dispatch(self):
        original = self.path.read_bytes()
        with patch.object(module.os, "replace", side_effect=OSError("private path")):
            with self.assertRaises(LedgerError):
                self.client().complete(MESSAGES, 100, "answer")
        self.assertEqual(self.calls, [])
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(list(self.path.parent.glob(".budget-*")), [])

    def test_concurrent_reservation_cannot_double_dispatch(self):
        entered = threading.Event()
        release = threading.Event()
        results = []

        def slow(request, timeout, limit):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test transport timed out")
            return response()

        def run():
            try:
                results.append(self.client(transport=slow).complete(MESSAGES, 100, "first"))
            except BaseException as error:
                results.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            with self.assertRaises(LedgerStopped):
                self.client(ledger=BudgetLedger(self.path)).complete(MESSAGES, 100, "second")
            self.assertEqual(self.calls, [])
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], dict)
        self.assertEqual(self.ledger.snapshot()["total_calls"], 1)

    def test_wall_clock_deadline_retains_reservation_while_transport_finishes(self):
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def slow(request, timeout, limit):
            entered.set()
            release.wait(5)
            finished.set()
            return response()

        client = OpenAICompatibleClient(
            FAKE_KEY, ledger=self.ledger, timeout_seconds=0.05, transport=slow,
        )
        try:
            with self.assertRaises(ClientError):
                client.complete(MESSAGES, 100, "deadline")
            self.assertTrue(entered.is_set())
            self.assertFalse(finished.is_set())
            self.assert_stopped_with_full_charge("transport_failure")
            reserved = self.ledger.snapshot()
        finally:
            release.set()
            self.assertTrue(finished.wait(5))
        # A late answer cannot refund the uncertain charge or reopen the ledger.
        self.assertEqual(self.ledger.snapshot(), reserved)

    def test_invalid_arguments_never_dispatch(self):
        for maximum in [0, -1, True, 1.5, 32769]:
            with self.subTest(maximum=maximum), self.assertRaises(ValueError):
                self.client().complete(MESSAGES, maximum, "answer")
        for messages in [[], [{"role": "tool", "content": "x"}],
                         [{"role": "user", "content": {}}],
                         [{"role": "user", "content": "x", "extra": "x"}]]:
            with self.subTest(messages=messages), self.assertRaises(ValueError):
                self.client().complete(messages, 100, "answer")
        for phase in ["", "has spaces", "../unsafe", "x" * 65]:
            with self.subTest(phase=phase), self.assertRaises(ValueError):
                self.client().complete(MESSAGES, 100, phase)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.ledger.snapshot()["total_calls"], 0)

    def test_no_constructor_configuration_can_expand_hard_limits(self):
        for token_limit in [0, True, 1000001]:
            with self.subTest(token_limit=token_limit), self.assertRaises(ValueError):
                BudgetLedger(self.path, token_limit=token_limit)
        for cny_limit in ["0", "5.1", "NaN", "Infinity", "0.00000001"]:
            with self.subTest(cny_limit=cny_limit), self.assertRaises(ValueError):
                BudgetLedger(self.path, cny_limit=cny_limit)
        for timeout in [0, -1, True, float("inf"), float("nan"), 121]:
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                OpenAICompatibleClient(FAKE_KEY, ledger=self.ledger, timeout_seconds=timeout)


class DefaultTransportTests(unittest.TestCase):
    def request(self):
        return urllib.request.Request(ENDPOINT, data=b"{}", method="POST")

    def test_proxy_required_and_no_direct_fallback(self):
        for proxies, bypass in [({}, False), ({"http": "http://proxy:80"}, False),
                                ({"https": "http://proxy:80"}, True),
                                ({"https": "socks5://proxy:80"}, False)]:
            with self.subTest(proxies=proxies, bypass=bypass):
                with patch.object(module.urllib.request, "getproxies", return_value=proxies), \
                     patch.object(module.urllib.request, "proxy_bypass", return_value=bypass), \
                     patch.object(module.urllib.request, "build_opener") as build:
                    with self.assertRaises(ClientError):
                        module._https_transport(self.request(), 90, 1024)
                    build.assert_not_called()

    def test_default_https_proxy_is_preserved_and_response_read_is_bounded(self):
        proxies = {"https": "http://proxy.invalid:8080", "http": "http://proxy.invalid:8080"}
        with patch.object(module.urllib.request, "getproxies", return_value=proxies), \
             patch.object(module.urllib.request, "proxy_bypass", return_value=False), \
             patch.object(module.urllib.request, "build_opener") as build:
            network_response = build.return_value.open.return_value.__enter__.return_value
            network_response.status = 200
            network_response.read.return_value = b"{}"
            request = self.request()
            self.assertEqual(module._https_transport(request, 45, 1024), b"{}")
            handlers = build.call_args.args
            self.assertIsInstance(handlers[0], urllib.request.ProxyHandler)
            self.assertEqual(handlers[0].proxies, proxies)
            self.assertIsInstance(handlers[1], module._NoRedirects)
            build.return_value.open.assert_called_once_with(request, timeout=45)
            network_response.read.assert_called_once_with(1025)

    def test_redirect_is_never_followed_with_authorization(self):
        handler = module._NoRedirects()
        with self.assertRaises(ClientError):
            handler.redirect_request(self.request(), None, 302, "", {}, "https://other.invalid")

    def test_unsuccessful_http_status_and_oversized_body_are_rejected(self):
        with patch.object(module.urllib.request, "getproxies",
                          return_value={"https": "http://proxy.invalid:8080"}), \
             patch.object(module.urllib.request, "proxy_bypass", return_value=False), \
             patch.object(module.urllib.request, "build_opener") as build:
            network_response = build.return_value.open.return_value.__enter__.return_value
            network_response.status = 503
            with self.assertRaises(ClientError):
                module._https_transport(self.request(), 45, 1024)
            network_response.read.assert_not_called()
            network_response.status = 200
            network_response.read.return_value = b"x" * 1025
            with self.assertRaises(ClientError):
                module._https_transport(self.request(), 45, 1024)


class EndpointSafetyTests(unittest.TestCase):
    UNSAFE = (
        "http://example.invalid/chat",
        "https://private-user:private-password@example.invalid/chat",
        "https://example.invalid/chat?api_key=fake",
        "https://example.invalid/chat#private-fragment",
        "https:///chat",
        "https://example.invalid:invalid/chat",
        "https://example.invalid:0/chat",
        "https://example.invalid/\nchat",
    )

    def test_unsafe_endpoint_never_creates_ledger_or_reveals_value(self):
        with tempfile.TemporaryDirectory() as directory:
            for endpoint in self.UNSAFE:
                path = Path(directory) / "never-created" / "budget.json"
                with self.subTest(endpoint=endpoint), patch.object(module, "ENDPOINT", endpoint):
                    with self.assertRaises(ClientError) as caught:
                        BudgetLedger(path)
                    self.assertNotIn(endpoint, str(caught.exception))
                    self.assertNotIn("private-", str(caught.exception))
                    self.assertFalse(path.parent.exists())

    def test_unsafe_endpoint_rejected_by_client_even_with_existing_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = BudgetLedger(Path(directory) / "budget.json")
            client = OpenAICompatibleClient(FAKE_KEY, ledger=ledger, transport=lambda *a: response())
            original = ledger.path.read_bytes()
            for endpoint in self.UNSAFE:
                with self.subTest(endpoint=endpoint), patch.object(module, "ENDPOINT", endpoint):
                    with self.assertRaises(ClientError):
                        OpenAICompatibleClient(FAKE_KEY, ledger=ledger)
                    with self.assertRaises(ClientError):
                        client.complete(MESSAGES, 100, "answer")
                    self.assertEqual(ledger.path.read_bytes(), original)

    def test_default_transport_validates_before_proxy_lookup_or_open(self):
        # Request strips some control characters, so exercise those via validation above.
        for endpoint in self.UNSAFE[:-1]:
            with self.subTest(endpoint=endpoint), \
                 patch.object(module.urllib.request, "getproxies") as proxies, \
                 patch.object(module.urllib.request, "build_opener") as build:
                with self.assertRaises(ClientError):
                    module._https_transport(urllib.request.Request(endpoint), 1, 100)
                proxies.assert_not_called()
                build.assert_not_called()

    def test_reserved_offline_endpoint_is_accepted(self):
        endpoint = "https://configure-your-endpoint.invalid/compatible-mode/v1/chat/completions"
        self.assertEqual(module.validate_endpoint(endpoint), endpoint)


if __name__ == "__main__":
    unittest.main()
