"""Public workload-probe creation and failure contracts."""
import json
import unittest

import httpx
from unittest.mock import patch

from adx_environment import HealthcheckProbe, ReadinessProbe, Environment, EnvironmentError
from adx_environment import ConnectionConfig
from adx_environment._transport import EnvironmentClient, EnvironmentHTTPError


class _FakeClient:
    created = []

    def __init__(self, **_kwargs):
        pass

    def create_info(self, body):
        self.created.append(body)
        return {"environmentId": "environment-1", "status": "running"}

    def set_direct_enabled(self, enabled):
        pass

    def close(self):
        pass

    def environment_info(self, environment_id):
        return {"id": environment_id, "status": "running"}


class ProbeContractTests(unittest.TestCase):
    def setUp(self):
        _FakeClient.created.clear()
        self.capability = patch.object(_FakeClient, "require_workload_probes", create=True, return_value=None)
        self.capability.start()
        self.addCleanup(self.capability.stop)

    def test_probe_create_serialization_and_initialization_budget(self):
        with patch("adx_environment.environment_api.EnvironmentClient", _FakeClient):
            environment = Environment(
                image="server:latest", inherit_entrypoint=True, init_timeout=120,
                readiness_probe=ReadinessProbe.http_get(port=8080, path="/ready"),
                healthcheck_probe=HealthcheckProbe.exec(command=["/app/check"], failure_threshold=4),
            )
        body = _FakeClient.created[-1]
        self.assertEqual(body["initCallTimeoutSeconds"], 120)
        self.assertEqual(body["createTimeoutSeconds"], 180)
        self.assertEqual(body["readinessProbe"]["httpGet"], {"port": 8080, "path": "/ready"})
        self.assertNotIn("failureThreshold", body["readinessProbe"])
        self.assertEqual(body["healthcheckProbe"]["failureThreshold"], 4)
        self.assertEqual(body["healthcheckProbe"]["exec"]["command"], ["/app/check"])
        self.assertEqual(environment.get_info().state, "running")

    def test_probes_require_managed_entrypoint_and_invalid_inputs_never_submit(self):
        with patch("adx_environment.environment_api.EnvironmentClient", _FakeClient):
            with self.assertRaises(ValueError):
                Environment(image="server", readiness_probe=ReadinessProbe.tcp_socket(port=8080))
            with self.assertRaises(TypeError):
                Environment(image="server", inherit_entrypoint=True, readiness_probe={})
            with self.assertRaises(ValueError):
                Environment(image="server", inherit_entrypoint=True, init_timeout=True)
        self.assertEqual(_FakeClient.created, [])

    def test_validates_actions_and_timing_and_copies_command(self):
        for port in (0, 65536, True, "8080"):
            with self.assertRaises((TypeError, ValueError)):
                ReadinessProbe.tcp_socket(port=port)
        for command in ([], "true", [""]):
            with self.assertRaises((TypeError, ValueError)):
                HealthcheckProbe.exec(command=command)
        with self.assertRaises(ValueError):
            ReadinessProbe.http_get(port=8080, path="http://other/ready")
        with self.assertRaises(ValueError):
            HealthcheckProbe.tcp_socket(port=8080, failure_threshold=0)
        with self.assertRaises(ValueError):
            ReadinessProbe.tcp_socket(port=8080, period_seconds=0)
        command = ["/app/check"]
        probe = HealthcheckProbe.exec(command=command)
        command.append("modified")
        self.assertEqual(probe.to_dict()["exec"]["command"], ["/app/check"])

    def test_absent_probes_preserve_create_body(self):
        with patch("adx_environment.environment_api.EnvironmentClient", _FakeClient):
            Environment(image="server", inherit_entrypoint=True)
        self.assertNotIn("readinessProbe", _FakeClient.created[-1])
        self.assertNotIn("healthcheckProbe", _FakeClient.created[-1])
        self.assertEqual(_FakeClient.created[-1]["createTimeoutSeconds"], 90)

    def test_final_failure_is_available_on_info(self):
        failure = {"kind": "healthcheck_failed", "message": "connection refused", "consecutive_failures": 3}
        with patch("adx_environment.environment_api.EnvironmentClient", _FakeClient):
            environment = Environment(image="server", inherit_entrypoint=True)
        with patch.object(_FakeClient, "environment_info", return_value={"id": "environment-1", "status": "failed", "failure": {"reason": "ProbeFailed", "probe_failure": failure}}):
            self.assertEqual(environment.get_info().failure.probe_failure, failure)
        error = EnvironmentError("readiness failed", probe_failure=failure)
        self.assertEqual(error.probe_failure, failure)


class ProbeTransportTests(unittest.TestCase):
    def create_with_transport(self, handler):
        http = httpx.Client(transport=httpx.MockTransport(handler))
        self.addCleanup(http.close)
        with patch("adx_environment._transport.acquire_shared_http_client", return_value=http):
            return Environment(
                image="server", inherit_entrypoint=True,
                readiness_probe=ReadinessProbe.tcp_socket(port=8080),
                connection=ConnectionConfig(server_address="probe.adx.test:7443", token="probe-token"),
            )

    def test_unsupported_server_never_receives_create(self):
        requests = []
        def answer(request):
            requests.append(request)
            self.assertEqual(request.headers["X-Auth"], "probe-token")
            return httpx.Response(404)
        with self.assertRaises(EnvironmentError) as raised:
            self.create_with_transport(answer)
        self.assertEqual(raised.exception.code, "UNSUPPORTED")
        self.assertEqual(raised.exception.outcome, "not_started")
        self.assertEqual([r.method for r in requests], ["GET"])

    def test_sse_probe_failure_is_terminal_and_preserves_metadata(self):
        requests = []
        failure = {"kind": "readiness_timeout", "message": "not ready", "consecutive_failures": 4, "exit_code": 7}
        def answer(request):
            requests.append(request)
            if request.method == "GET":
                return httpx.Response(200, json={"capabilities": ["workload-probes-v1"]})
            event = {"status": "failed", "message": "not ready", "error": {
                "code": "UNAVAILABLE", "retry": "never", "outcome": "terminal",
                "requestId": request.headers["X-Request-Id"], "probeFailure": failure,
            }}
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"},
                                  text="event: final\ndata: " + json.dumps(event) + "\n\n")
        with self.assertRaises(EnvironmentError) as raised:
            self.create_with_transport(answer)
        self.assertEqual(raised.exception.probe_failure, failure)
        self.assertEqual(raised.exception.retry, "never")
        self.assertEqual(raised.exception.outcome, "terminal")
        self.assertEqual([r.method for r in requests], ["GET", "POST"])

    def test_json_error_preserves_probe_metadata(self):
        failure = {"kind": "readiness_timeout", "message": "not ready", "consecutive_failures": 2}
        response = httpx.Response(409, json={"message": "not ready", "error": {
            "code": "UNAVAILABLE", "retry": "never", "outcome": "terminal", "probeFailure": failure,
        }})
        with self.assertRaises(EnvironmentHTTPError) as raised:
            EnvironmentClient._json(response)
        self.assertEqual(raised.exception.probe_failure, failure)


if __name__ == "__main__":
    unittest.main()
