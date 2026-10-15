#!/usr/bin/env python3
"""Verify early backend deadlines through an isolated standalone public API.

The container must carry adx.e2e.backend-timeout=true and contain an idle runc
backend. Faults affect only this disposable container and one runc run per case.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid

import httpx


TLS_SERVER = r'''
import socket, threading
import http.server, json, ssl, subprocess
from pathlib import Path
root = Path('/tmp/adx-backend-timeout')
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(('127.0.0.1', 5051))
listener.listen()
subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
    '-subj', '/CN=image-timeout.test', '-keyout', str(root/'key.pem'),
    '-out', str(root/'cert.pem')], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
class Rejections(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        cases = {'missing': (404, 'MANIFEST_UNKNOWN'), 'forbidden': (403, 'DENIED'),
                 'unauthorized': (401, 'UNAUTHORIZED'), 'limited': (429, 'TOOMANYREQUESTS')}
        name = self.path.split('/')[2] if self.path.startswith('/v2/') and self.path != '/v2/' else ''
        code, category = cases.get(name, (200, ''))
        body = json.dumps({'errors': [{'code': category, 'message': 'controlled image ' + name}]}).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
server = http.server.ThreadingHTTPServer(('127.0.0.1', 5052), Rejections)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain(str(root/'cert.pem'), str(root/'key.pem'))
server.socket = tls.wrap_socket(server.socket, server_side=True)
threading.Thread(target=server.serve_forever, daemon=True).start()
(root/'ready').touch()
def hold(connection):
    with connection:
        while connection.recv(4096):
            pass
while True:
    connection, _ = listener.accept()
    threading.Thread(target=hold, args=(connection,), daemon=True).start()
'''

RUNC_WRAPPER = r'''#!/usr/bin/python3
import ctypes, json, os, signal, sys, time
from pathlib import Path
root = Path('/tmp/adx-backend-timeout')
if 'run' in sys.argv[1:]:
    try:
        (root/'inject-once').unlink()
    except FileNotFoundError:
        pass
    else:
        parent = os.getppid()
        if ctypes.CDLL(None).prctl(1, signal.SIGKILL) != 0:
            raise RuntimeError('cannot fence injected process to its shim')
        if os.getppid() != parent:
            os.kill(os.getpid(), signal.SIGKILL)
        (root/'claimed.json').write_text(json.dumps({'pid': os.getpid(), 'args': sys.argv}))
        time.sleep(90)
        raise SystemExit(125)
os.execv('/usr/local/bin/runc.backend-timeout-real', sys.argv)
'''


def docker(container: str, script: str, *, detached: bool = False) -> str:
    args = ['docker', 'exec'] + (['-d'] if detached else ['-i'])
    args += [container, 'python3', '-c', script]
    return subprocess.check_output(args, text=True)


def install(container: str) -> str:
    setup = r'''
import hashlib, json
from pathlib import Path
root = Path('/tmp/adx-backend-timeout')
root.mkdir(exist_ok=False)
with Path('/etc/hosts').open('a') as stream:
    stream.write('\n127.0.0.1 image-timeout.test\n')
print(hashlib.sha256(Path('/usr/local/bin/runc').read_bytes()).hexdigest())
'''
    digest = docker(container, setup).strip()
    docker(container, TLS_SERVER, detached=True)
    for _ in range(50):
        ready = docker(container, "from pathlib import Path; print(Path('/tmp/adx-backend-timeout/ready').exists())")
        if ready.strip() == 'True':
            return digest
        time.sleep(.1)
    raise RuntimeError('TLS timeout registry did not start')


def inject_runtime(container: str) -> None:
    setup = (
        "from pathlib import Path\n"
        "binary = Path('/usr/local/bin/runc')\n"
        "binary.rename('/usr/local/bin/runc.backend-timeout-real')\n"
        f"binary.write_text({RUNC_WRAPPER!r})\n"
        "binary.chmod(0o755)\n"
        "Path('/tmp/adx-backend-timeout/inject-once').touch()\n"
    )
    docker(container, setup)


def restore_runtime(container: str, digest: str) -> None:
    restore = r'''
import hashlib
from pathlib import Path
binary = Path('/usr/local/bin/runc')
original = binary.with_name('runc.backend-timeout-real')
if original.exists():
    original.replace(binary)
print(hashlib.sha256(binary.read_bytes()).hexdigest())
'''
    assert docker(container, restore).strip() == digest, 'runc restoration checksum mismatch'


def create(client: httpx.Client, body: dict, request_id: str, sse: bool) -> dict:
    started = time.monotonic()
    response = client.post('/api/environment/v1/environments', json=body, headers={
        'X-Request-Id': request_id,
        'Accept': 'text/event-stream' if sse else 'application/json',
    })
    if sse:
        finals = [json.loads(block.split('data: ', 1)[1]) for block in response.text.split('\n\n')
                  if block.startswith('event: final\n')]
        assert len(finals) == 1, response.text
        value = finals[0]
        status = value.get('errorCode', response.status_code)
    else:
        value, status = response.json(), response.status_code
    return {'seconds': time.monotonic() - started, 'http_status': response.status_code,
            'status': status, 'body': value}


def assert_timeout(result: dict, stage: str, budget: int) -> None:
    assert result['status'] == 504, result
    error = result['body']['error']
    assert (error['code'], error['retry'], error['outcome']) == ('DEADLINE_EXCEEDED', 'never', 'terminal'), result
    assert stage in result['body']['message'], result
    assert result['seconds'] < budget - 10, result


def run_case(args: argparse.Namespace, client: httpx.Client, stage: str, sse: bool, digest: str) -> dict:
    name = 'backend-timeout-' + uuid.uuid4().hex[:12]
    environment_id = 'default-' + name
    request_id = 'create-' + str(uuid.uuid4())
    body = {'namespace': 'default', 'name': name, 'cpu': 1000, 'memory': 2048,
            'rootfs': {'type': 'image', 'runtime': 'runc', 'imageurl': args.image if stage == 'runtime start'
                       else 'image-timeout.test:5051/image:latest'},
            'createTimeoutSeconds': args.budget, 'scheduleTimeoutSeconds': 1,
            'initCallTimeoutSeconds': 10, 'inheritEntrypoint': False, 'failover': False,
            'lifecycle': 'detached', 'idleTimeoutSeconds': 300, 'idleAction': 'delete'}
    if stage == 'runtime start':
        inject_runtime(args.container)
    result = {}
    failure = None
    try:
        result = create(client, body, request_id, sse)
        result.update(stage=stage, sse=sse, environment_id=environment_id)
        (args.evidence / (name + '.json')).write_text(json.dumps(result, indent=2))
        assert_timeout(result, stage, args.budget)
        if stage == 'runtime start':
            claimed = json.loads(docker(args.container,
                "from pathlib import Path; print(Path('/tmp/adx-backend-timeout/claimed.json').read_text())"))
            result['injected_pid'] = claimed['pid']
        # Both original-request replay and authoritative failed-record replay
        # must retain the exact internal phase without issuing another Start.
        for replay_id in [request_id, 'create-' + str(uuid.uuid4())]:
            replay = create(client, body, replay_id, sse)
            assert_timeout(replay, stage, args.budget)
            assert replay['body']['message'] == result['body']['message'], replay
            assert replay['seconds'] < 3, replay
    except Exception as error:
        failure = error
        result['failure'] = str(error)
        raise
    finally:
        cleanup_errors = []
        try:
            restore_runtime(args.container, digest)
        except Exception as error:
            cleanup_errors.append(str(error))
        try:
            deletion = client.delete(f'/api/environment/v1/environments/{environment_id}')
            result['delete_status'] = deletion.status_code
            assert deletion.status_code in (200, 404), deletion.text
        except Exception as error:
            cleanup_errors.append(str(error))
        result['cleanup_errors'] = cleanup_errors
        (args.evidence / (name + '.json')).write_text(json.dumps(result, indent=2))
        if cleanup_errors and failure is None:
            raise RuntimeError('; '.join(cleanup_errors))
    if stage == 'runtime start':
        for _ in range(50):
            alive = docker(args.container, f"from pathlib import Path; print(Path('/proc/{claimed['pid']}').exists())")
            if alive.strip() == 'False':
                break
            time.sleep(.1)
        else:
            raise AssertionError('injected runtime process outlived rollback')
    print(json.dumps(result), flush=True)
    return result




def rejected_image(args: argparse.Namespace, client: httpx.Client, kind: str, sse: bool) -> dict:
    expected = {'missing': (404, 'NOT_FOUND'), 'forbidden': (403, 'PERMISSION_DENIED'),
                'unauthorized': (401, 'UNAUTHENTICATED'), 'limited': (429, 'RESOURCE_EXHAUSTED')}
    name = 'backend-rejected-' + uuid.uuid4().hex[:12]
    body = {'namespace': 'default', 'name': name, 'cpu': 1000, 'memory': 2048,
            'rootfs': {'type': 'image', 'runtime': 'runc',
                       'imageurl': f'image-timeout.test:5052/{kind}:latest'},
            'createTimeoutSeconds': args.budget, 'scheduleTimeoutSeconds': 1,
            'inheritEntrypoint': False, 'failover': False, 'lifecycle': 'detached'}
    result = {}
    try:
        result = create(client, body, 'create-' + str(uuid.uuid4()), sse)
        result.update(kind=kind, sse=sse)
        assert result['status'] == expected[kind][0], result
        assert result['body']['error']['code'] == expected[kind][1], result
        assert result['body']['error']['retry'] == 'never', result
        if kind == 'limited':
            assert result['body']['error']['outcome'] == 'terminal', result
        assert 'controlled image ' + kind in result['body']['message'], result
        assert 'image pull' in result['body']['message'], result
        assert result['seconds'] < args.budget - 10, result
        print(json.dumps(result), flush=True)
        return result
    finally:
        (args.evidence / (name + '.json')).write_text(json.dumps(result, indent=2))
        deletion = client.delete(f'/api/environment/v1/environments/default-{name}')
        assert deletion.status_code in (200, 404), deletion.text


def healthy_create(args: argparse.Namespace, client: httpx.Client) -> dict:
    name = 'backend-healthy-' + uuid.uuid4().hex[:12]
    body = {'namespace': 'default', 'name': name, 'cpu': 1000, 'memory': 2048,
            'rootfs': {'type': 'image', 'runtime': 'runc', 'imageurl': args.image},
            'createTimeoutSeconds': args.budget, 'scheduleTimeoutSeconds': 1,
            'initCallTimeoutSeconds': 10, 'inheritEntrypoint': False, 'failover': False,
            'lifecycle': 'detached', 'idleTimeoutSeconds': 300, 'idleAction': 'delete'}
    try:
        result = create(client, body, 'create-' + str(uuid.uuid4()), False)
        assert result['status'] == 200, result
        return result
    finally:
        response = client.delete(f'/api/environment/v1/environments/default-{name}')
        assert response.status_code in (200, 404), response.text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', required=True)
    parser.add_argument('--api-url', required=True)
    parser.add_argument('--token-file', required=True, type=Path)
    parser.add_argument('--image', required=True, help='Reachable OCI image for native runc')
    parser.add_argument('--budget', type=int, default=180)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    assert args.budget >= 90, 'budget must leave room beyond internal TLS/runtime timers'
    labels = json.loads(subprocess.check_output([
        'docker', 'inspect', '--format', '{{json .Config.Labels}}', args.container], text=True))
    assert labels.get('adx.e2e.backend-timeout') == 'true', 'requires a disposable, labelled fixture'
    args.evidence.mkdir(parents=True, exist_ok=False)
    digest = install(args.container)
    with httpx.Client(base_url=args.api_url, verify=False, trust_env=False, timeout=args.budget + 10,
                      headers={'X-Auth': args.token_file.read_text().strip()}) as client:
        results = []
        before = healthy_create(args, client)
        try:
            for stage in ['image pull', 'runtime start']:
                for sse in [False, True]:
                    results.append(run_case(args, client, stage, sse, digest))
        finally:
            restore_runtime(args.container, digest)
        rejections = [rejected_image(args, client, kind, sse)
                      for kind in ['missing', 'forbidden', 'unauthorized', 'limited'] for sse in [False, True]]
        after = healthy_create(args, client)
        summary = {'passed': True, 'cases': results, 'rejections': rejections,
                   'healthy_before': before, 'healthy_after': after}
        (args.evidence / 'result.json').write_text(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
