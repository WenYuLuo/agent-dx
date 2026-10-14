"""Inherited image process used to prove real Agent data-plane forwarding."""
import http.server
import json
import os
import uuid

BOOT_ID = uuid.uuid4().hex


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({'boot_id': BOOT_ID, 'marker': os.environ.get('AGENT_E2E_MARKER'), 'path': self.path}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    http.server.ThreadingHTTPServer(('0.0.0.0', 18092), Handler).serve_forever()
