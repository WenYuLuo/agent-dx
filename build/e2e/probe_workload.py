"""Managed HTTP workload for readiness and healthcheck acceptance."""

import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

started = time.monotonic()
ready_delay = float(os.environ.get('PROBE_READY_DELAY', '0'))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/ready':
            status = 200 if time.monotonic() - started >= ready_delay else 503
            if status == 200:
                Path('/tmp/adx-probe-ready').touch()
        elif self.path == '/health':
            status = 503 if Path('/tmp/adx-probe-unhealthy').exists() else 200
        else:
            status = 404
        self.send_response(status)
        self.send_header('Content-Length', '0')
        self.end_headers()


if __name__ == '__main__':
    HTTPServer(('127.0.0.1', 8080), Handler).serve_forever()
