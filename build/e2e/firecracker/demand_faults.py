"""External fault adapters; never substitute a lifecycle or runtime implementation."""

import http.client
import json
import select
import socket
import ssl
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


class ResumeResponseCut:
    """Consume a successful upstream resume, observe its commit, lose one reply."""

    def __init__(self, upstream, secrets, observe):
        self.upstream = urlsplit(upstream)
        self.secrets = secrets
        self.observe = observe
        self.attempts = []
        self.commit = None
        self.error = None
        self.lock = threading.Lock()

    def __enter__(self):
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                with proxy.lock:
                    first = len(proxy.attempts) == 0
                    proxy.attempts.append(self.headers.get('X-ADX-Request-ID'))
                context = ssl.create_default_context(cafile=str(proxy.secrets / 'tls/ca.pem'))
                upstream = http.client.HTTPSConnection(proxy.upstream.hostname, proxy.upstream.port, context=context, timeout=180)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in ('host', 'connection', 'content-length')}
                try:
                    upstream.request('POST', self.path, body, headers)
                    response = upstream.getresponse()
                    payload = response.read()
                    if first:
                        if response.status != 200:
                            raise AssertionError('resume did not succeed before response cut: ' + str(response.status))
                        proxy.commit = proxy.observe()
                        proxy.commit['reply_cut_at'] = time.time()
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    self.send_response(response.status)
                    self.send_header('Content-Type', response.getheader('Content-Type', 'application/json'))
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except Exception as error:
                    proxy.error = repr(error)
                    self.close_connection = True
                finally:
                    upstream.close()

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self.secrets / 'tls/ingress.pem'), str(self.secrets / 'tls/ingress.key'))
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


def prepare_proxy(control, port):
    """Transparent HTTP control proxy, run as non-root behind owner-scoped REDIRECT."""
    control = Path(control)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def forward(self):
            original = self.connection.getsockopt(socket.SOL_IP, 80, 16)
            destination = socket.inet_ntoa(original[4:8])
            destination_port = struct.unpack('!H', original[2:4])[0]
            upstream = http.client.HTTPConnection(destination, destination_port, timeout=180)
            body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
            headers = {k: v for k, v in self.headers.items() if k.lower() not in ('host', 'connection', 'content-length')}
            if self.headers.get('Upgrade'):
                # Execd command watches are streaming upgrades; preserve the real socket.
                peer = socket.create_connection((destination, destination_port), timeout=180)
                request = f'{self.command} {self.path} HTTP/1.1\r\n' + ''.join(f'{k}: {v}\r\n' for k, v in self.headers.items()) + '\r\n'
                peer.sendall(request.encode() + body)
                try:
                    while True:
                        readable, _, _ = select.select([peer, self.connection], [], [], 180)
                        if not readable:
                            return
                        for source in readable:
                            data = source.recv(65536)
                            if not data:
                                return
                            (self.connection if source is peer else peer).sendall(data)
                finally:
                    peer.close()
                    self.close_connection = True
                return
            try:
                upstream.request(self.command, self.path, body or None, headers)
                response = upstream.getresponse()
                payload = response.read()
                with (control / 'events.jsonl').open('a') as events:
                    events.write(json.dumps({'path': self.path, 'status': response.status, 'at': time.time()}) + '\n')
                if self.path == '/control/v1/checkpoint/prepare' and response.status == 200 and (control / 'arm').exists():
                    operation = json.loads(body).get('operation_id', '')
                    if operation.startswith('idle-'):
                        (control / 'prepared.json').write_text(json.dumps({'operation': operation, 'prepared_at': time.time(), 'response': json.loads(payload)}))
                        deadline = time.monotonic() + 120
                        while not (control / 'release').exists():
                            if time.monotonic() >= deadline:
                                raise TimeoutError('prepared response was not released')
                            time.sleep(0.05)
                        (control / 'arm').unlink(missing_ok=True)
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ('content-length', 'transfer-encoding', 'connection'):
                        self.send_header(key, value)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            finally:
                upstream.close()

        do_GET = forward
        do_POST = forward
        do_DELETE = forward

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    (control / 'ready').touch()
    server.serve_forever()


if __name__ == '__main__':
    import sys
    prepare_proxy(sys.argv[1], int(sys.argv[2]))
