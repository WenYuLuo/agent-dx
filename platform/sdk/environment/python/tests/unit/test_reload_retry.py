"""Reload transport retries retain the operation identity across node outages."""
import unittest
from unittest.mock import patch

import httpx

from adx_environment import ConnectionConfig, Environment
from adx_environment._transport import EnvironmentClient


class ReloadRetryTests(unittest.TestCase):
    def environment(self, handler):
        client = EnvironmentClient(connection=ConnectionConfig(
            server_address="ingress:8888", token="token", use_tls=False,
        ))
        self.addCleanup(client.close)
        client._http.close()
        client._http = httpx.Client(transport=httpx.MockTransport(handler))
        environment = object.__new__(Environment)
        environment._closed = False
        environment._sid = "env"
        environment._client = client
        return environment

    def test_node_restart_retries_same_reload_until_confirmed(self):
        requests = []

        def handler(request):
            requests.append(request)
            if len(requests) == 1:
                raise httpx.ReadError("node restarted", request=request)
            if len(requests) == 2:
                return httpx.Response(503, json={"error": {
                    "code": "UNAVAILABLE", "retry": "same_operation", "outcome": "unknown",
                }})
            return httpx.Response(200, json={"code": 200, "data": {"success": True}})

        with patch("adx_environment._transport.time.sleep"):
            self.assertTrue(self.environment(handler).reload())
        self.assertEqual(len(requests), 3)
        self.assertEqual(len({r.headers["X-ADX-Request-ID"] for r in requests}), 1)
        self.assertTrue(all(r.url.path.endswith("/environments/env/reload") for r in requests))
        self.assertEqual(len({r.content for r in requests}), 1)

    def test_terminal_rejection_is_not_retried(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(409, json={"error": {
                "code": "CONFLICT", "retry": "never", "outcome": "not_started",
            }})

        self.assertFalse(self.environment(handler).reload())
        self.assertEqual(len(requests), 1)
