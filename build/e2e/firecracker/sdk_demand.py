#!/usr/bin/env python3
"""Real public HTTP + Redis + two Firecracker backends + external OBS fault suite."""

import hashlib
import json
import os
from pathlib import Path
import signal
import ssl
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from demand_contract import CASES
from demand_faults import ResumeResponseCut
from s3_client import Client
from storage_fixture import public as public_storage

root = Path(sys.argv[1])
image = sys.argv[2]
base = Path(os.environ['ADX_FC_BASE'])
evidence = root / 'evidence'
os.environ['SSL_CERT_FILE'] = str(root / 'secrets/tls/ca.pem')
from adx_sandbox import ConnectionConfig, PausePolicy, Sandbox
from adx_sandbox._transport import SandboxClient

connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(root / 'secrets/api-key').read_text().strip(), use_tls=True, verify_tls=True)
redis_env = {**os.environ, 'REDISCLI_AUTH': (root / 'secrets/redis-key').read_text().strip()}
namespaces = json.loads((evidence / 'topology.json').read_text())['namespaces']
result = {'status': 'failed', 'cases': [], 'storage': public_storage(root), 'cleanup_errors': []}
sandboxes = []
stopped = set()
rules = []
proxies = []
executor = ThreadPoolExecutor(max_workers=16)

GUEST = '''import json, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
seen = {}
lock = threading.Lock()
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  query = parse_qs(urlsplit(self.path).query)
  nonce = query['nonce'][0]
  with lock:
   seen[nonce] = seen.get(nonce, 0) + 1
   count = seen[nonce]
   open('/tmp/demand-started', 'w').write(nonce)
  time.sleep(float(query.get('hold', ['0'])[0]))
  payload = json.dumps({'pid': os.getpid(), 'nonce': nonce, 'count': count, 'memory': dict(seen), 'file': open('/tmp/demand-file').read()}).encode()
  self.send_response(200)
  self.send_header('Content-Length', str(len(payload)))
  self.end_headers()
  self.wfile.write(payload)
 def log_message(self, *args): pass
server = ThreadingHTTPServer(('0.0.0.0', 8088), Handler)
open('/tmp/demand-ready', 'w').write(str(os.getpid()))
server.serve_forever()
'''


def catalog():
    raw = subprocess.check_output([str(base / 'tools/redis-cli'), '--json', 'HGETALL', 'adx:{acceptance}:control:v1'], env=redis_env, text=True, timeout=10)
    return {key: json.loads(value) for key, value in json.loads(raw).items()}


def record(sandbox):
    return catalog().get('environment:' + sandbox.id)


def wait(predicate, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise TimeoutError('demand condition timed out')


def inventory(node):
    raw = subprocess.check_output(['/opt/adx-fc/bin/sbox', '-a', str(root / node / 'sandboxd/sandboxd.sock'), 'list'], text=True, timeout=10)
    return raw.strip().splitlines()[1:]


def physical(node):
    rows = inventory(node)
    if len(rows) != 1:
        raise AssertionError(('expected exactly one physical VM', node, rows))
    return rows[0].split()[0]


def pid(folder, role):
    status = json.loads(subprocess.check_output([str(base / 'package/bin/adxctl'), 'status', '--config', str(folder / 'deployment.yaml')], text=True))
    return next(item['pid'] for item in status['services'] if item['role'] == role)


def pass_case(index, **details):
    item = {'name': CASES[index], 'passed': True, **details}
    result['cases'].append(item)
    (evidence / 'sdk-demand.json').write_text(json.dumps(result, indent=2))
    print('PASS', json.dumps(item), flush=True)


def command(sandbox, text):
    output = sandbox.commands.run(text)
    if output.exit_code != 0:
        raise AssertionError(('guest command failed', output.stderr))
    return output.stdout.strip()


def create(idle=0, ttl=900):
    sandbox = Sandbox(image=image, runtime='firecracker', cpu=1000, memory=128, storage_mb=32,
                      env={'PYTHONDONTWRITEBYTECODE': '1'}, idle_timeout=idle,
                      idle_action='pause', pause_policy=PausePolicy(ttl_seconds=ttl, timeout_seconds=1800, storage='shared'),
                      auto_resume=True, port_forwardings=[8088], node_id='node1', connection=connection, create_timeout=180)
    sandboxes.append(sandbox)
    sandbox.files.write('/tmp/demand-server.py', GUEST)
    sandbox.files.write('/tmp/demand-file', 'durable-' + sandbox.id)
    command(sandbox, 'nohup python3 /tmp/demand-server.py >/tmp/demand-server.log 2>&1 </dev/null &')
    command(sandbox, 'for attempt in $(seq 1 100); do test ! -f /tmp/demand-ready || break; sleep 0.1; done; cat /tmp/demand-ready')
    return sandbox


def request(sandbox, nonce=None, hold=0, single_attempt=False):
    nonce = nonce or uuid.uuid4().hex
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context(cafile=str(root / 'secrets/tls/ca.pem'))))
    started = time.time()
    transient = []
    while True:
        try:
            with opener.open(Request(sandbox.get_port_url(8088) + f'/?nonce={nonce}&hold={hold}', headers=sandbox.get_port_auth_headers()), timeout=330) as response:
                data = json.loads(response.read())
            break
        except HTTPError as error:
            failure = {'status': error.code, 'at': time.time(), 'body': error.read(4096).decode(errors='replace')}
            with (evidence / 'http-failures.jsonl').open('a') as output:
                output.write(json.dumps({'nonce': nonce, **failure}) + '\n')
            if single_attempt or error.code != 503 or time.time() - started > 1800:
                raise
            transient.append(failure)
            time.sleep(1)
    data.update(request_started_at=started, request_completed_at=time.time(), transient_statuses=transient)
    if data['nonce'] != nonce or data['count'] != 1 or data['file'] != 'durable-' + sandbox.id:
        raise AssertionError(('HTTP executed incorrectly', data))
    return data


def continuity(before, after):
    if before['pid'] != after['pid'] or after['memory'].get(before['nonce']) != 1 or before['file'] != after['file']:
        raise AssertionError(('checkpoint continuity failed', before, after))


def paused(sandbox):
    value = wait(lambda: (lambda r: r if r and r['result']['state'] == 'Paused' else None)(record(sandbox)))
    artifact = value['result']['checkpoint']['artifact']
    if artifact['storage'] != 'obs':
        raise AssertionError(('checkpoint is not in OBS', artifact))
    client = Client(root)
    raw = client.request('GET', '/' + client.config['bucket'] + '/' + client.config['prefix'] + '/' + artifact['location'] + '/manifest.json')
    manifest = json.loads(raw)
    if manifest['size'] != artifact['size_bytes'] or not manifest['files']:
        raise AssertionError('OBS commit manifest mismatch')
    (evidence / ('obs-manifest-' + artifact['location'] + '.json')).write_bytes(raw)
    result.setdefault('obs_manifests', []).append({'artifact': artifact['location'], 'sha256': hashlib.sha256(raw).hexdigest(), 'size': manifest['size'], 'request_id': client.last_request_id})
    return value


def delete(sandbox):
    sandbox.kill()
    wait(lambda: record(sandbox) is None)
    wait(lambda: not inventory('node1') and not inventory('node2'))
    sandboxes.remove(sandbox)


def block(node, port, action='DROP', redirect=None):
    prefix = ['nsenter', '--net=/var/run/netns/' + namespaces[node], 'iptables']
    table = ['-t', 'nat'] if redirect else []
    rule = ['OUTPUT', '-p', 'tcp', '--dport', str(port), '-m', 'owner', '--uid-owner', '0', '-m', 'comment', '--comment', root.name, '-j', action]
    if redirect:
        rule += ['--to-ports', str(redirect)]
    subprocess.run([*prefix, *table, '-I', *rule], check=True)
    item = (prefix, table, rule)
    rules.append(item)
    return item


def unblock(item):
    prefix, table, rule = item
    subprocess.run([*prefix, *table, '-D', *rule], check=True)
    rules.remove(item)


def stop_source(sandbox):
    source = record(sandbox)['assignment']['node_id']
    manager = pid(root / source, 'adxlet')
    os.kill(manager, signal.SIGSTOP)
    stopped.add(manager)
    wait(lambda: record(sandbox).get('invalidated'))
    return source, manager


def return_source(source, manager):
    os.kill(manager, signal.SIGCONT)
    stopped.remove(manager)
    wait(lambda: catalog()['node:' + source]['session']['routable'] and not inventory(source))


def restart_case(index, role):
    sandbox = create()
    baseline = request(sandbox)
    sandbox.pause(ttl_seconds=900, timeout_seconds=1800)
    paused(sandbox)
    source, manager = stop_source(sandbox)
    target = 'node2'
    blocked = block(target, 50090)
    future = executor.submit(request, sandbox)
    try:
        def uncommitted():
            r = record(sandbox)
            rows = inventory(target)
            if r['assignment']['node_id'] == target and r.get('recovery', {}).get('pending') and r['result']['state'] == 'Paused' and len(rows) == 1 and 'SANDBOX_STATE_RUNNING' in rows[0]:
                return r, rows[0].split()[0]
            return None
        planned, old_backend = wait(uncommitted)
        state = catalog()
        epoch = state['header']['epoch']
        session = state['node:' + target]['session']['id']
        crashed = pid(root if role == 'coordinator' else root / target, role)
        (evidence / (role + '-uncommitted-before-crash.json')).write_text(json.dumps({'record': planned, 'backend': old_backend, 'pid': crashed, 'crashed_at': time.time()}, indent=2))
        os.kill(crashed, signal.SIGKILL)
        if role == 'coordinator':
            wait(lambda: catalog()['header']['epoch'] > epoch)
        else:
            wait(lambda: catalog()['node:' + target]['session']['id'] != session)
        kept = record(sandbox)
        if kept['assignment'] != planned['assignment'] or not kept.get('resume'):
            raise AssertionError(('wake plan lost after restart', kept))
        unblock(blocked)
        blocked = None
        # The gateway connection can be lost when Coordinator restarts; a fresh
        # real client demand must join the same persisted wake intent.
        try:
            after = future.result(2100)
        except (OSError, HTTPError):
            after = request(sandbox)
        continuity(baseline, after)
        wait(lambda: record(sandbox)['result']['state'] == 'Running')
        backend = physical(target)
        removed = backend != old_backend
        if role == 'adxlet' and not removed:
            raise AssertionError('adxlet restart reused an uncommitted physical backend')
        return_source(source, manager)
        pass_case(index, instance_id=sandbox.id, state_at_crash=planned['result']['state'], old_backend=old_backend,
                  backend_after=backend, old_backend_removed=removed, assignment_preserved=record(sandbox)['assignment'] == planned['assignment'],
                  epoch_before=epoch, epoch_after=catalog()['header']['epoch'], session_before=session,
                  session_after=catalog()['node:' + target]['session']['id'], pid=after['pid'])
    finally:
        if blocked:
            unblock(blocked)
    delete(sandbox)


try:
    # Actual user-port HTTP stays active across several idle detection windows.
    sandbox = create(idle=5)
    baseline = request(sandbox)
    future = executor.submit(request, sandbox, 'held-http', 12)
    time.sleep(2)
    observation = []
    for _ in range(8):
        r = record(sandbox)
        observation.append({'at': time.time(), 'state': r['result']['state'], 'backend': physical('node1')})
        if r['result']['state'] != 'Running':
            raise AssertionError('active HTTP was paused')
        time.sleep(1)
    held = future.result(30)
    continuity(baseline, held)
    pass_case(0, instance_id=sandbox.id, http_duration=held['request_completed_at'] - held['request_started_at'], observations=observation)
    delete(sandbox)

    # Hold the real Execd Prepared response before the backend checkpoint call.
    sandbox = create(idle=5)
    baseline = request(sandbox)
    backend = physical('node1')
    gate = Path(tempfile.mkdtemp(prefix='adx-demand-'))
    gate.chmod(0o777)
    (gate / 'arm').touch()
    log = (evidence / 'prepare-proxy.log').open('w')
    proxy = subprocess.Popen(['nsenter', '--net=/var/run/netns/' + namespaces['node1'], 'setpriv', '--reuid=65534', '--regid=65534', '--clear-groups', 'python3', str(base / 'e2e/firecracker/demand_faults.py'), str(gate), '19590'], stdout=log, stderr=subprocess.STDOUT)
    log.close()
    proxies.append(proxy)
    wait(lambda: (gate / 'ready').exists())
    redirected = block('node1', 50090, 'REDIRECT', 19590)
    # Established HTTP pools predate REDIRECT. Force their real sockets closed
    # once so the next control connection goes through the fault adapter.
    subprocess.run(['nsenter', '--net=/var/run/netns/' + namespaces['node1'], 'ss', '-K', 'dport = :50090'], check=True)
    try:
        prepared = wait(lambda: json.loads((gate / 'prepared.json').read_text()) if (gate / 'prepared.json').exists() else None)
        future = executor.submit(request, sandbox)
        wake = wait(lambda: record(sandbox).get('resume'))
        (gate / 'release').touch()
        after = future.result(2100)
        continuity(baseline, after)
        r = record(sandbox)
        if physical('node1') != backend or r['result'].get('checkpoint'):
            raise AssertionError(('pause captured after earlier traffic demand', r))
        pass_case(1, instance_id=sandbox.id, same_backend=True, checkpoint_created=False, prepared=prepared, wake=wake, http=after)
    finally:
        (gate / 'release').touch()
        for name in ('events.jsonl', 'prepared.json'):
            if (gate / name).exists():
                (evidence / ('prepare-' + name)).write_bytes((gate / name).read_bytes())
        unblock(redirected)
        proxy.terminate()
        proxy.wait(10)
        proxies.remove(proxy)
    delete(sandbox)

    # Block actual OBS upload, observe the VM gone, then send real business HTTP.
    sandbox = create(idle=5)
    baseline = request(sandbox)
    blocked = block('node1', 443)
    try:
        wait(lambda: not inventory('node1'))
        captured_at = time.time()
        future = executor.submit(request, sandbox)
        wait(lambda: record(sandbox).get('resume'))
        time.sleep(2)
        if future.done():
            raise AssertionError('HTTP completed while its checkpoint publication was blocked')
        unblock(blocked)
        blocked = None
        after = future.result(2100)
        continuity(baseline, after)
        pass_case(2, instance_id=sandbox.id, captured_before_request=True, captured_at=captured_at, request_waited=True, http=after)
    finally:
        if blocked:
            unblock(blocked)
    delete(sandbox)

    sandbox = create()
    baseline = request(sandbox)
    sandbox.pause(ttl_seconds=900, timeout_seconds=1800)
    old = paused(sandbox)
    futures = [executor.submit(request, sandbox, 'concurrent-' + str(i)) for i in range(12)]
    responses = [future.result(2100) for future in futures]
    for response in responses:
        continuity(baseline, response)
    new = record(sandbox)
    if new['assignment']['node_id'] != 'node1' or len(inventory('node1')) != 1 or inventory('node2'):
        raise AssertionError('concurrent demand did not produce one original-node execution')
    pass_case(3, instance_id=sandbox.id, request_count=len(responses), assignment=new['assignment'], backend=physical('node1'), pid=baseline['pid'])
    delete(sandbox)

    sandbox = create()
    baseline = request(sandbox)
    sandbox.pause(ttl_seconds=900, timeout_seconds=1800)
    old = paused(sandbox)
    source, manager = stop_source(sandbox)
    time.sleep(4)
    dormant = record(sandbox)
    if dormant['result']['state'] != 'Paused' or inventory('node2') or dormant.get('resume'):
        raise AssertionError(('failed source autonomously woke dormant environment', dormant))
    after = request(sandbox)
    continuity(baseline, after)
    new = record(sandbox)
    if new['assignment']['node_id'] != 'node2' or new['assignment']['generation'] <= old['assignment']['generation']:
        raise AssertionError('traffic did not restore on peer')
    return_source(source, manager)
    pass_case(4, instance_id=sandbox.id, dormant=dormant, restored_assignment=new['assignment'], backend=physical('node2'), http=after)
    delete(sandbox)

    restart_case(5, 'coordinator')
    restart_case(6, 'adxlet')

    sandbox = create()
    request(sandbox)
    pause = sandbox.pause(ttl_seconds=20, timeout_seconds=1800)
    paused(sandbox)
    source, manager = stop_source(sandbox)
    wait(lambda: time.time() >= pause.expires_at + 1)
    try:
        request(sandbox, single_attempt=True)
    except HTTPError as error:
        rejected = error.code
    else:
        raise AssertionError('expired checkpoint accepted traffic')
    if inventory('node1') or inventory('node2'):
        raise AssertionError('expired traffic started a VM')
    pass_case(7, instance_id=sandbox.id, expires_at=pause.expires_at, request_at=time.time(), http_status=rejected, inventories={'node1': inventory('node1'), 'node2': inventory('node2')})
    return_source(source, manager)
    delete(sandbox)

    sandbox = create()
    baseline = request(sandbox)
    pause = sandbox.pause(ttl_seconds=15, timeout_seconds=1800)
    paused(sandbox)
    blocked = block('node1', 50090)
    try:
        future = executor.submit(request, sandbox)
        accepted = wait(lambda: record(sandbox) if record(sandbox).get('resume') else None)
        admitted_at = time.time()
        wait(lambda: any('SANDBOX_STATE_RUNNING' in row for row in inventory('node1')))
        backend_running_at = time.time()
        if backend_running_at >= pause.expires_at:
            raise AssertionError('real restore was not admitted before checkpoint expiry')
        wait(lambda: time.time() > pause.expires_at + 1)
        if future.done():
            raise AssertionError('readiness block did not hold real restore across TTL')
        unblock(blocked)
        blocked = None
        after = future.result(2100)
        continuity(baseline, after)
        pass_case(8, instance_id=sandbox.id, admitted_at=admitted_at, backend_running_at=backend_running_at, accepted=accepted,
                  expires_at=pause.expires_at, completed_at=after['request_completed_at'], backend=physical('node1'), http=after)
    finally:
        if blocked:
            unblock(blocked)
    delete(sandbox)

    sandbox = create()
    baseline = request(sandbox)
    sandbox.pause(ttl_seconds=900, timeout_seconds=1800)
    paused(sandbox)

    def committed():
        r = record(sandbox)
        if r['result']['state'] != 'Running':
            raise AssertionError('resume reply arrived before Running commit')
        return {'assignment': r['assignment'], 'runtime': r['result']['runtime'], 'backend': physical('node1'), 'committed_at': time.time()}

    with ResumeResponseCut('https://127.0.0.1:8443', root / 'secrets', committed) as proxy:
        client = SandboxClient(connection=ConnectionConfig(server_address=f'127.0.0.1:{proxy.port}', token=connection.token, use_tls=True, verify_tls=True))
        original_client = sandbox._client
        sandbox._client = client
        try:
            sandbox.resume()
        finally:
            sandbox._client = original_client
            client.close()
        after_commit = committed()
        if proxy.error or not proxy.commit or len(proxy.attempts) < 2 or len(set(proxy.attempts)) != 1 or not proxy.attempts[0]:
            raise AssertionError(('SDK did not retry the same resume after reply loss', proxy.error, proxy.attempts))
        same = all(proxy.commit[key] == after_commit[key] for key in ('assignment', 'runtime', 'backend'))
        if not same:
            raise AssertionError('resume retry created another execution')
        after = request(sandbox)
        continuity(baseline, after)
        pass_case(9, instance_id=sandbox.id, attempts=len(proxy.attempts), same_execution=same, commit_before_cut=proxy.commit['committed_at'] <= proxy.commit['reply_cut_at'], cut=proxy.commit, after_retry=after_commit)
    delete(sandbox)
    result['status'] = 'passed'
except BaseException as error:
    result['error'] = repr(error)
    print('FAIL', repr(error), flush=True)
    raise
finally:
    for item in list(rules):
        try:
            unblock(item)
        except Exception as error:
            result['cleanup_errors'].append(repr(error))
    for process in proxies:
        process.terminate()
        process.wait(10)
    for manager in stopped:
        try:
            os.kill(manager, signal.SIGCONT)
        except ProcessLookupError:
            pass
    for sandbox in list(sandboxes):
        try:
            delete(sandbox)
        except Exception as error:
            result['cleanup_errors'].append(repr(error))
    executor.shutdown(wait=False, cancel_futures=True)
    if result['cleanup_errors']:
        result['status'] = 'failed'
    (evidence / 'sdk-demand.json').write_text(json.dumps(result, indent=2))
