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
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from demand_contract import CASES, same_wake
from demand_faults import ResumeResponseCut
from s3_client import Client
from storage_fixture import public as public_storage

root = Path(sys.argv[1])
image = sys.argv[2]
base = Path(os.environ['ADX_FC_BASE'])
evidence = root / 'evidence'
os.environ['SSL_CERT_FILE'] = str(root / 'secrets/tls/ca.pem')
from adx_environment import RetentionPolicy, IdleAction, CheckpointStorage, ConnectionConfig, PausePolicy, Environment
from adx_environment._transport import EnvironmentClient

connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(root / 'secrets/api-key').read_text().strip(), use_tls=True, verify_tls=True)
redis_env = {**os.environ, 'REDISCLI_AUTH': (root / 'secrets/redis-key').read_text().strip()}
namespaces = json.loads((evidence / 'topology.json').read_text())['namespaces']
result = {'status': 'failed', 'cases': [], 'storage': public_storage(root), 'cleanup_errors': []}
environments = []
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


def record(environment):
    return catalog().get('environment:' + environment.id)


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


def command(environment, text):
    output = environment.commands.run(text)
    if output.exit_code != 0:
        raise AssertionError(('guest command failed', output.stderr))
    return output.stdout.strip()


def create(idle=0, ttl=900, retention=None, node='node1', cpu=1000):
    environment = Environment(image=image, runtime='firecracker', cpu=cpu, memory=128, storage_mb=32,
                      env={'PYTHONDONTWRITEBYTECODE': '1'}, idle_timeout=idle,
                      idle_action=IdleAction.PAUSE if idle else IdleAction.DELETE, pause_policy=PausePolicy(retention=retention or RetentionPolicy.for_seconds(ttl), timeout_seconds=1800, storage=CheckpointStorage.SHARED),
                      port_forwardings=[8088], connection=connection, create_timeout=180,
                      schedule_affinities=[{'kind': 0, 'affinity': 0, 'weight': 1000,
                                            'labelOps': [{'type': 0, 'labelKey': 'NODE_ID', 'labelValues': [node]}]}])
    environments.append(environment)
    if record(environment)['assignment']['node_id'] != node:
        raise AssertionError(('initial node preference did not select requested node', node))
    environment.files.write('/tmp/demand-server.py', GUEST)
    environment.files.write('/tmp/demand-file', 'durable-' + environment.id)
    command(environment, 'nohup python3 /tmp/demand-server.py >/tmp/demand-server.log 2>&1 </dev/null &')
    command(environment, 'for attempt in $(seq 1 100); do test ! -f /tmp/demand-ready || break; sleep 0.1; done; cat /tmp/demand-ready')
    return environment


def request(environment, nonce=None, hold=0, single_attempt=False):
    nonce = nonce or uuid.uuid4().hex
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context(cafile=str(root / 'secrets/tls/ca.pem'))))
    started = time.time()
    transient = []
    while True:
        try:
            with opener.open(Request(environment.get_port_url(8088) + f'/?nonce={nonce}&hold={hold}', headers=environment.get_port_auth_headers()), timeout=330) as response:
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
    if data['nonce'] != nonce or data['count'] != 1 or data['file'] != 'durable-' + environment.id:
        raise AssertionError(('HTTP executed incorrectly', data))
    return data


def continuity(before, after):
    if before['pid'] != after['pid'] or after['memory'].get(before['nonce']) != 1 or before['file'] != after['file']:
        raise AssertionError(('checkpoint continuity failed', before, after))


def paused(environment):
    value = wait(lambda: (lambda r: r if r and r['result']['state'] == 'Paused' else None)(record(environment)))
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


def kill(environment):
    environment.kill()
    wait(lambda: record(environment) is None)
    environments.remove(environment)


def delete(environment):
    kill(environment)
    wait(lambda: not inventory('node1') and not inventory('node2'))


def capacity_case():
    environment = create()
    baseline = request(environment)
    environment.pause(retention=RetentionPolicy.for_seconds(900), timeout_seconds=1800)
    before = paused(environment)
    initial = catalog()
    source_session = initial['node:node1']['session']['id']
    source_pid = pid(root / 'node1', 'adxlet')
    capacities = {node: initial['node:' + node]['node']['capacity']['cpu_millis'] for node in ('node1', 'node2')}
    if any(cpu != 4000 for cpu in capacities.values()):
        raise AssertionError(('capacity suite requires an actual 4-core cgroup quota', capacities))
    owner_load = create(cpu=capacities['node1'])
    peer_load = create(node='node2', cpu=capacities['node2'])
    owner_backend, peer_backend = physical('node1'), physical('node2')
    saturated = catalog()
    reserved = {node: sum(value['spec']['resources']['cpu_millis'] for key, value in saturated.items()
                          if key.startswith('environment:') and value['assignment']['node_id'] == node
                          and value.get('result', {}).get('resources_held')) for node in capacities}
    if reserved != capacities:
        raise AssertionError(('real running loads did not fill CPU reservations', reserved, capacities))
    (evidence / 'capacity-saturated.json').write_text(json.dumps({'at': time.time(), 'nodes': {
        node: saturated['node:' + node] for node in capacities}, 'reserved': reserved,
        'loads': {node: record(load) for node, load in [('node1', owner_load), ('node2', peer_load)]}}, indent=2))
    barrier = threading.Barrier(13)
    def demand(index):
        barrier.wait(10)
        return request(environment, 'capacity-' + str(index))
    futures = [executor.submit(demand, index) for index in range(12)]
    barrier.wait(10)
    rejected = wait(lambda: (lambda r: r if r and (r.get('resume') or {}).get('local_rejected') else None)(record(environment)))
    rejected_at = time.time()
    if rejected['assignment'] != before['assignment'] or rejected['invalidated'] or rejected['result']['state'] != 'Paused':
        raise AssertionError('ownership moved before observing owner admission rejection')
    if rejected['result']['checkpoint'] != before['result']['checkpoint'] or any(future.done() for future in futures):
        raise AssertionError('capacity rejection changed checkpoint or completed demand early')
    errors = []
    for line in (root/'node1/state/logs/node1.log').read_text().splitlines():
        try: event = json.loads(line)
        except ValueError: continue
        if event.get('environment_id') == environment.id and event.get('operation') == 'ensure_running':
            errors.append(event)
    local = next((event for event in errors if event.get('error') == 'insufficient resources or node admission closed'), None)
    if local is None:
        raise AssertionError(('no actual owner NoCapacity operation result', errors))
    if physical('node1') != owner_backend or physical('node2') != peer_backend:
        raise AssertionError('a target VM appeared before peer capacity was freed')
    healthy = catalog()['node:node1']
    os.kill(source_pid, 0)
    if healthy['session']['id'] != source_session or not healthy['session']['routable'] or not healthy['node']['available']:
        raise AssertionError('source was invalidated or unhealthy during capacity rejection')
    (evidence / 'capacity-owner-rejected.json').write_text(json.dumps({'at': rejected_at, 'record': rejected,
        'node': healthy, 'local_operation_result': local, 'inventories': {node: inventory(node) for node in capacities}}, indent=2))
    peer_release_at = time.time()
    kill(peer_load)
    wait(lambda: not inventory('node2'))
    transferred = wait(lambda: (lambda r: r if r and r['assignment']['node_id'] == 'node2' else None)(record(environment)))
    transferred_at = time.time()
    (evidence / 'capacity-transfer.json').write_text(json.dumps({'at': transferred_at, 'record': transferred}, indent=2))
    responses = [future.result(2100) for future in futures]
    for response in responses: continuity(baseline, response)
    after = record(environment)
    operation = rejected['resume']['operation_id']
    (evidence / 'capacity-completed.json').write_text(json.dumps({'at': time.time(), 'record': after,
        'source_node': catalog()['node:node1'], 'inventories': {node: inventory(node) for node in capacities},
        'responses': responses}, indent=2))
    if not same_wake(rejected, transferred, after):
        raise AssertionError('Coordinator wake identity changed or did not settle')
    if after['assignment']['node_id'] != 'node2' or len(inventory('node2')) != 1:
        raise AssertionError('capacity transfer did not produce one peer execution')
    if physical('node1') != owner_backend or catalog()['node:node1']['session']['id'] != source_session:
        raise AssertionError('capacity transfer changed the healthy source session or execution')
    pass_case(11, instance_id=environment.id, source='node1', target='node2', source_healthy=True,
              source_cpu_capacity=capacities['node1'], source_cpu_reserved=reserved['node1'], local_rejected=True,
              local_error=local['error'], local_attempt_at=local['unix_seconds'], local_rejected_at=rejected_at,
              peer_release_at=peer_release_at, transferred_at=transferred_at,
              generation_before=before['assignment']['generation'], generation_after=after['assignment']['generation'],
              wake_operation_preserved=True, wake_operation_id=operation, request_count=12,
              node_execution_operation_id=after['result']['last_operation']['id'],
              unique_nonces=len({response['nonce'] for response in responses}), responses=responses,
              source_session=source_session, backend=physical('node2'), owner_load_backend=owner_backend)
    kill(environment)
    delete(owner_load)


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


def stop_source(environment):
    source = record(environment)['assignment']['node_id']
    manager = pid(root / source, 'adxlet')
    os.kill(manager, signal.SIGSTOP)
    stopped.add(manager)
    wait(lambda: record(environment).get('invalidated'))
    return source, manager


def return_source(source, manager):
    os.kill(manager, signal.SIGCONT)
    stopped.remove(manager)
    wait(lambda: catalog()['node:' + source]['session']['routable'] and not inventory(source))


def restart_case(index, role):
    environment = create()
    baseline = request(environment)
    environment.pause(retention=RetentionPolicy.for_seconds(900), timeout_seconds=1800)
    paused(environment)
    source, manager = stop_source(environment)
    target = 'node2'
    blocked = block(target, 50090)
    future = executor.submit(request, environment)
    try:
        def uncommitted():
            r = record(environment)
            rows = inventory(target)
            if r['assignment']['node_id'] == target and r.get('recovery', {}).get('pending') and r['result']['state'] == 'Paused' and len(rows) == 1 and 'ENVIRONMENT_STATE_RUNNING' in rows[0]:
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
        kept = record(environment)
        if kept['assignment'] != planned['assignment'] or not kept.get('resume'):
            raise AssertionError(('wake plan lost after restart', kept))
        unblock(blocked)
        blocked = None
        # The gateway connection can be lost when Coordinator restarts; a fresh
        # real client demand must join the same persisted wake intent.
        try:
            after = future.result(2100)
        except (OSError, HTTPError):
            after = request(environment)
        continuity(baseline, after)
        wait(lambda: record(environment)['result']['state'] == 'Running')
        backend = physical(target)
        removed = backend != old_backend
        if role == 'adxlet' and not removed:
            raise AssertionError('adxlet restart reused an uncommitted physical backend')
        return_source(source, manager)
        pass_case(index, instance_id=environment.id, state_at_crash=planned['result']['state'], old_backend=old_backend,
                  backend_after=backend, old_backend_removed=removed, assignment_preserved=record(environment)['assignment'] == planned['assignment'],
                  epoch_before=epoch, epoch_after=catalog()['header']['epoch'], session_before=session,
                  session_after=catalog()['node:' + target]['session']['id'], pid=after['pid'])
    finally:
        if blocked:
            unblock(blocked)
    delete(environment)


try:
    # Actual user-port HTTP stays active across several idle detection windows.
    environment = create(idle=5)
    baseline = request(environment)
    future = executor.submit(request, environment, 'held-http', 12)
    time.sleep(2)
    observation = []
    for _ in range(8):
        r = record(environment)
        observation.append({'at': time.time(), 'state': r['result']['state'], 'backend': physical('node1')})
        if r['result']['state'] != 'Running':
            raise AssertionError('active HTTP was paused')
        time.sleep(1)
    held = future.result(30)
    continuity(baseline, held)
    pass_case(0, instance_id=environment.id, http_duration=held['request_completed_at'] - held['request_started_at'], observations=observation)
    delete(environment)

    # Hold the real Execd Prepared response before the backend checkpoint call.
    environment = create(idle=5)
    baseline = request(environment)
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
        future = executor.submit(request, environment)
        wake = wait(lambda: record(environment).get('resume'))
        (gate / 'release').touch()
        after = future.result(2100)
        continuity(baseline, after)
        r = record(environment)
        if physical('node1') != backend or r['result'].get('checkpoint'):
            raise AssertionError(('pause captured after earlier traffic demand', r))
        pass_case(1, instance_id=environment.id, same_backend=True, checkpoint_created=False, prepared=prepared, wake=wake, http=after)
    finally:
        (gate / 'release').touch()
        for name in ('events.jsonl', 'prepared.json'):
            if (gate / name).exists():
                (evidence / ('prepare-' + name)).write_bytes((gate / name).read_bytes())
        unblock(redirected)
        proxy.terminate()
        proxy.wait(10)
        proxies.remove(proxy)
    delete(environment)

    # Block actual OBS upload, observe the VM gone, then send real business HTTP.
    environment = create(idle=5)
    baseline = request(environment)
    blocked = block('node1', 443)
    try:
        wait(lambda: not inventory('node1'))
        captured_at = time.time()
        future = executor.submit(request, environment)
        wait(lambda: record(environment).get('resume'))
        time.sleep(2)
        if future.done():
            raise AssertionError('HTTP completed while its checkpoint publication was blocked')
        unblock(blocked)
        blocked = None
        after = future.result(2100)
        continuity(baseline, after)
        pass_case(2, instance_id=environment.id, captured_before_request=True, captured_at=captured_at, request_waited=True, http=after)
    finally:
        if blocked:
            unblock(blocked)
    delete(environment)

    environment = create()
    baseline = request(environment)
    environment.pause(retention=RetentionPolicy.for_seconds(900), timeout_seconds=1800)
    old = paused(environment)
    futures = [executor.submit(request, environment, 'concurrent-' + str(i)) for i in range(12)]
    responses = [future.result(2100) for future in futures]
    for response in responses:
        continuity(baseline, response)
    new = record(environment)
    if new['assignment']['node_id'] != 'node1' or len(inventory('node1')) != 1 or inventory('node2'):
        raise AssertionError('concurrent demand did not produce one original-node execution')
    pass_case(3, instance_id=environment.id, request_count=len(responses), assignment=new['assignment'], backend=physical('node1'), pid=baseline['pid'])
    delete(environment)

    environment = create()
    baseline = request(environment)
    environment.pause(retention=RetentionPolicy.for_seconds(900), timeout_seconds=1800)
    old = paused(environment)
    source, manager = stop_source(environment)
    time.sleep(4)
    dormant = record(environment)
    if dormant['result']['state'] != 'Paused' or inventory('node2') or dormant.get('resume'):
        raise AssertionError(('failed source autonomously woke dormant environment', dormant))
    after = request(environment)
    continuity(baseline, after)
    new = record(environment)
    if new['assignment']['node_id'] != 'node2' or new['assignment']['generation'] <= old['assignment']['generation']:
        raise AssertionError('traffic did not restore on peer')
    return_source(source, manager)
    pass_case(4, instance_id=environment.id, dormant=dormant, restored_assignment=new['assignment'], backend=physical('node2'), http=after)
    delete(environment)

    restart_case(5, 'coordinator')
    restart_case(6, 'adxlet')

    environment = create()
    request(environment)
    pause = environment.pause(retention=RetentionPolicy.for_seconds(20), timeout_seconds=1800)
    paused(environment)
    source, manager = stop_source(environment)
    wait(lambda: time.time() >= pause.expires_at + 1)
    try:
        request(environment, single_attempt=True)
    except HTTPError as error:
        rejected = error.code
    else:
        raise AssertionError('expired checkpoint accepted traffic')
    if inventory('node1') or inventory('node2'):
        raise AssertionError('expired traffic started a VM')
    pass_case(7, instance_id=environment.id, expires_at=pause.expires_at, request_at=time.time(), http_status=rejected, inventories={'node1': inventory('node1'), 'node2': inventory('node2')})
    return_source(source, manager)
    delete(environment)

    environment = create()
    baseline = request(environment)
    pause = environment.pause(retention=RetentionPolicy.for_seconds(45), timeout_seconds=1800)
    paused(environment)
    blocked = block('node1', 50090)
    try:
        future = executor.submit(request, environment)
        accepted = wait(lambda: record(environment) if record(environment).get('resume') else None)
        admitted_at = time.time()
        wait(lambda: any('ENVIRONMENT_STATE_RUNNING' in row for row in inventory('node1')))
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
        pass_case(8, instance_id=environment.id, admitted_at=admitted_at, backend_running_at=backend_running_at, accepted=accepted,
                  expires_at=pause.expires_at, completed_at=after['request_completed_at'], backend=physical('node1'), http=after)
    finally:
        if blocked:
            unblock(blocked)
    delete(environment)

    environment = create()
    baseline = request(environment)
    environment.pause(retention=RetentionPolicy.for_seconds(900), timeout_seconds=1800)
    paused(environment)

    def committed():
        r = record(environment)
        if r['result']['state'] != 'Running':
            raise AssertionError('resume reply arrived before Running commit')
        return {'assignment': r['assignment'], 'runtime': r['result']['runtime'], 'backend': physical('node1'), 'committed_at': time.time()}

    with ResumeResponseCut('https://127.0.0.1:8443', root / 'secrets', committed) as proxy:
        client = EnvironmentClient(connection=ConnectionConfig(server_address=f'127.0.0.1:{proxy.port}', token=connection.token, use_tls=True, verify_tls=True))
        original_client = environment._client
        environment._client = client
        try:
            environment.resume()
        finally:
            environment._client = original_client
            client.close()
        after_commit = committed()
        if proxy.error or not proxy.commit or len(proxy.attempts) < 2 or len(set(proxy.attempts)) != 1 or not proxy.attempts[0]:
            raise AssertionError(('SDK did not retry the same resume after reply loss', proxy.error, proxy.attempts))
        same = all(proxy.commit[key] == after_commit[key] for key in ('assignment', 'runtime', 'backend'))
        if not same:
            raise AssertionError('resume retry created another execution')
        after = request(environment)
        continuity(baseline, after)
        pass_case(9, instance_id=environment.id, attempts=len(proxy.attempts), same_execution=same, commit_before_cut=proxy.commit['committed_at'] <= proxy.commit['reply_cut_at'], cut=proxy.commit, after_retry=after_commit)
    delete(environment)
    environment = create(idle=5, retention=RetentionPolicy.forever())
    baseline = request(environment)
    permanent = paused(environment)
    if permanent['result']['checkpoint']['expires_at_unix_seconds'] is not None:
        raise AssertionError('automatic permanent pause has an expiry')
    source, manager = stop_source(environment)
    epoch = catalog()['header']['epoch']
    os.kill(pid(root, 'coordinator'), signal.SIGKILL)
    wait(lambda: catalog()['header']['epoch'] > epoch)
    dormant = record(environment)
    if dormant['result']['state'] != 'Paused' or dormant.get('resume') or inventory('node2'):
        raise AssertionError('permanent pause woke during Coordinator restart')
    after = request(environment)
    continuity(baseline, after)
    return_source(source, manager)
    target = record(environment)['assignment']['node_id']
    if target != 'node2':
        raise AssertionError('permanent snapshot did not restore across nodes')
    manual = environment.pause(retention=RetentionPolicy.forever(), timeout_seconds=1800)
    if manual.expires_at is not None:
        raise AssertionError('manual permanent pause has an expiry')
    kept = paused(environment)
    session = catalog()['node:' + target]['session']['id']
    os.kill(pid(root / target, 'adxlet'), signal.SIGKILL)
    wait(lambda: catalog()['node:' + target]['session']['id'] != session)
    wait(lambda: catalog()['node:' + target]['session']['routable'])
    current = record(environment)
    if current['result']['state'] != 'Paused' or current['result']['checkpoint'] != kept['result']['checkpoint'] or inventory(target):
        raise AssertionError('permanent pause changed during adxlet restart')
    after_restart = request(environment)
    continuity(baseline, after_restart)
    pass_case(10, instance_id=environment.id, automatic_expiry=None, manual_expiry=manual.expires_at,
              dormant_after_coordinator_restart=True, epoch_before=epoch, epoch_after=catalog()['header']['epoch'],
              session_before=session, session_after=catalog()['node:' + target]['session']['id'],
              target=target, traffic_after_manual_pause=True, continuity=True)
    delete(environment)
    capacity_case()
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
    for environment in list(environments):
        try:
            kill(environment)
        except Exception as error:
            result['cleanup_errors'].append(repr(error))
    try:
        wait(lambda: not inventory('node1') and not inventory('node2'))
    except Exception as error:
        result['cleanup_errors'].append(repr(error))
    executor.shutdown(wait=False, cancel_futures=True)
    if result['cleanup_errors']:
        result['status'] = 'failed'
    (evidence / 'sdk-demand.json').write_text(json.dumps(result, indent=2))
