"""Initialization rollback must preserve reused remote Environments."""
import json
import unittest
from unittest.mock import patch

import httpx
from adx_environment import ConnectionConfig, Environment


class CreationOwnershipTests(unittest.TestCase):
    def exercise(self, ownership, *, tunnel=False, fail=True):
        requests = []
        payload = {"environmentId": "tenant-shared", "status": "running"}
        if ownership is not None:
            payload["newlyCreated"] = ownership

        def send(_client, request, **_kwargs):
            requests.append(request.method)
            if request.method == "POST":
                return httpx.Response(
                    200, headers={"content-type": "text/event-stream"},
                    content="event: final\ndata: " + json.dumps(payload) + "\n\n", request=request,
                )
            return httpx.Response(200, json={}, request=request)

        with patch.object(httpx.Client, "send", autospec=True, side_effect=send):
            with (
                patch("adx_environment.tunnel_client.TunnelClient") as client,
                patch("adx_environment.environment_api.Pty") as pty,
            ):
                client.return_value.start.return_value = False
                if fail and not tunnel:
                    pty.side_effect = RuntimeError("local initialization failed")
                kwargs = {
                    "name": "shared", "detached": True,
                    "connection": ConnectionConfig(server_address="localhost:8888", token="test", use_tls=False),
                }
                if tunnel:
                    kwargs["upstream"] = "http://127.0.0.1:9000"
                if fail:
                    with self.assertRaises(RuntimeError):
                        Environment(**kwargs)
                else:
                    environment = Environment(**kwargs)
                    self.assertEqual(environment.newly_created, ownership is True)
                    environment.close()
        return requests

    def test_failed_initialization_only_deletes_confirmed_new_instances(self):
        for tunnel in (False, True):
            for ownership in (True, False, None, "true"):
                with self.subTest(tunnel=tunnel, ownership=ownership):
                    calls = self.exercise(ownership, tunnel=tunnel)
                    self.assertEqual(calls.count("DELETE"), int(ownership is True))

    def test_success_exposes_ownership_without_remote_cleanup_on_close(self):
        for ownership in (True, False, None):
            with self.subTest(ownership=ownership):
                self.assertEqual(self.exercise(ownership, fail=False), ["POST"])
