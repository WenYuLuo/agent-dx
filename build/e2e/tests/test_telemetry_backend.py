"""Exercise the E2E OTLP receiver over real HTTP sockets."""

import gzip
import http.client
import importlib.util
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

SPEC = importlib.util.spec_from_file_location('otlp_backend', Path(__file__).resolve().parents[1] / 'telemetry.py')
telemetry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(telemetry)


class TelemetryBackendTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        for name, value in [('P', self.root), ('D', self.root), ('BATCHES', self.root / 'collected-logs.jsonl')]:
            patch = mock.patch.object(telemetry, name, value)
            patch.start()
            self.addCleanup(patch.stop)
        self.server = telemetry.make_backend_server(('127.0.0.1', 0), ['test-secret'])
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 3)
        self.addCleanup(self.server.shutdown)

    def request(self, path, value, *, compressed=False, method='POST'):
        body = json.dumps(value).encode()
        headers = {'Content-Type': 'application/json'}
        if compressed:
            body = gzip.compress(body)
            headers['Content-Encoding'] = 'gzip'
        client = http.client.HTTPConnection(*self.server.server_address, timeout=5)
        try:
            client.request(method, path, body, headers)
            response = client.getresponse()
            return response.status, response.read()
        finally:
            client.close()

    def test_logs_and_gzip_traces_are_stored_separately(self):
        logs = {'resourceLogs': []}
        traces = {'resourceSpans': []}
        self.assertEqual(self.request('/v1/logs', logs), (200, b'{}'))
        self.assertEqual(self.request('/v1/traces', traces, compressed=True), (200, b'{}'))
        self.assertEqual(json.loads((self.root / 'collected-logs.jsonl').read_text()), logs)
        self.assertEqual(json.loads((self.root / 'collected-traces.jsonl').read_text()), traces)

    def test_outage_rejects_without_persisting_then_recovers(self):
        paused = self.root / 'sink-paused'
        paused.touch()
        self.assertEqual(self.request('/v1/logs', {'sequence': 1})[0], 503)
        self.assertTrue((self.root / 'collector-backend-unavailable').exists())
        self.assertFalse((self.root / 'collected-logs.jsonl').exists())
        paused.unlink()
        self.assertEqual(self.request('/v1/logs', {'sequence': 1})[0], 200)
        self.assertEqual((self.root / 'collected-logs.jsonl').read_text().count('\n'), 1)

    def test_plain_and_compressed_credentials_are_rejected_before_storage(self):
        for compressed in [False, True]:
            self.assertEqual(self.request('/v1/traces', {'token': 'test-secret'}, compressed=compressed)[0], 400)
        self.assertTrue((self.root / 'collector-secret-leak').exists())
        self.assertFalse((self.root / 'collected-traces.jsonl').exists())

    def test_unsupported_paths_and_methods_do_not_write_batches(self):
        self.assertEqual(self.request('/other', {})[0], 404)
        self.assertEqual(self.request('/v1/logs', {}, method='GET')[0], 405)
        self.assertFalse((self.root / 'collected-logs.jsonl').exists())

    def test_parallel_requests_preserve_complete_json_lines(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(lambda number: self.request('/v1/logs', {'sequence': number})[0], range(32)))
        self.assertEqual(statuses, [200] * 32)
        rows = [json.loads(line) for line in (self.root / 'collected-logs.jsonl').read_text().splitlines()]
        self.assertEqual(sorted(row['sequence'] for row in rows), list(range(32)))


if __name__ == '__main__':
    unittest.main()
