#!/usr/bin/env python3
"""Installed SDK, direct Coordinator HTTP and two real Firecracker backends."""
import gzip
import json
import os
from pathlib import Path
import signal
import ssl
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from autoscaling_contract import CASES

root = Path(sys.argv[1])
image = sys.argv[2]
base = Path(os.environ['ADX_FC_BASE'])
evidence = root / 'evidence'
os.environ['SSL_CERT_FILE'] = str(root / 'secrets/tls/ca.pem')
from adx_environment import CheckpointStorage, ConnectionConfig, PausePolicy, RetentionPolicy, Environment

secrets = root / 'secrets'
connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(secrets / 'api-key').read_text().strip(), use_tls=True, verify_tls=True)
other_connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(secrets / 'other-key').read_text().strip(), use_tls=True, verify_tls=True)
http = build_opener(ProxyHandler({}))
https = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context(cafile=str(secrets / 'tls/ca.pem'))))
result = {'status': 'failed', 'cases': [], 'cleanup_errors': [], 'suite': 'autoscaling', 'profile': 'targeted'}
handles = []
futures = []
owned = {}
executor = ThreadPoolExecutor(max_workers=4)


def passed(index, **details):
    item = {'name': CASES[index], 'passed': True, **details}
    result['cases'].append(item)
    (evidence / 'sdk-autoscaling.json').write_text(json.dumps(result, indent=2))
    print('PASS', json.dumps(item), flush=True)


def wait(predicate, seconds=120):
    end = time.monotonic() + seconds
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= end:
            raise TimeoutError('autoscaling E2E condition timed out')
        time.sleep(0.2)


def management(path, method='GET', **query):
    url = 'http://127.0.0.1:19192' + path
    if query:
        url += '?' + urlencode(query)
    # No API Key, caller context or administrative identity is sent here.
    with http.open(Request(url, method=method), timeout=15) as response:
        return json.load(response)


def query(**filters):
    return management('/instance-manager/query-tenant-instances', **filters)


def pause_node(node, paused):
    response = management('/global-scheduler/node/localschedulingstatus', 'POST' if paused else 'DELETE', node_id=node)
    assert response['status'] == ('evicting' if paused else 'normal'), response
    wait(lambda: query(node_id=node)['node']['scheduling_paused'] is paused, 15)


def record(environment):
    value = query(instance_id=environment.id)
    assert value['count'] == 1, value
    return value['instanceInfos'][0]


def catalog_record(environment):
    env = {**os.environ, 'REDISCLI_AUTH': (secrets / 'redis-key').read_text().strip()}
    raw = subprocess.check_output([str(base / 'tools/redis-cli'), '--json', 'HGET', 'adx:{acceptance}:control:v1', 'environment:' + environment.id], env=env, text=True, timeout=10)
    value = json.loads(raw)
    return json.loads(value) if value else None


def inventory(node):
    raw = subprocess.check_output(['/opt/adx-fc/bin/sbox', '-a', str(root / node / 'sandboxd/sandboxd.sock'), 'list'], text=True, timeout=10)
    return raw.strip().splitlines()[1:]


def command(environment, text):
    value = environment.commands.run(text, timeout=30)
    assert value.exit_code == 0, (value.exit_code, value.stderr)
    return value.stdout.strip()


def create(node=None, using=connection, name=None):
    name = name or 'autoscale-' + uuid.uuid4().hex[:12]
    owned['default-' + name] = using
    environment = Environment(name=name, image=image, runtime='firecracker', cpu=1000, memory=128,
                      storage_mb=32, idle_timeout=0, node_id=node,
                      pause_policy=PausePolicy(storage=CheckpointStorage.SHARED, retention=RetentionPolicy.for_seconds(900)),
                      port_forwardings=[8088], connection=using, schedule_timeout=60, create_timeout=150)
    handles.append(environment)
    return environment


def delete(environment):
    Environment.delete(environment.id, connection=owned[environment.id])
    environment.close()
    handles.remove(environment)
    owned.pop(environment.id)
    wait(lambda: query(instance_id=environment.id)['count'] == 0, 60)


def set_mode(mode):
    def service():
        status = json.loads(subprocess.check_output([str(base / 'package/bin/adxctl'), 'status', '--config', str(root / 'deployment.yaml')], text=True, timeout=10))
        return next(item for item in status['services'] if item['role'] == 'apiserver')
    pid = service()['pid']
    args = Path(f'/proc/{pid}/cmdline').read_bytes().decode().split('\0')
    path = Path(args[args.index('--config') + 1])
    assert path.resolve().is_relative_to((root / 'state').resolve()), path
    config = json.loads(path.read_text())
    config['create_mode'] = mode
    temporary = path.with_suffix('.new')
    temporary.write_text(json.dumps(config))
    temporary.chmod(0o600)
    temporary.replace(path)
    os.kill(pid, signal.SIGTERM)
    request = Request('https://127.0.0.1:8443/api/environment/v1/resources', headers={'Authorization': 'Bearer ' + connection.token})
    def ready():
        if service()['pid'] in (None, pid):
            return False
        try:
            with https.open(request, timeout=2) as response:
                return len(json.load(response)['items']) == 2
        except OSError:
            return False
    wait(ready, 60)
    time.sleep(1.2)


def local_claim_seen(instance_id):
    for path in (root / 'state/logs').glob('coordinator*.log*'):
        if path.name.endswith('.tmp'):
            continue
        try:
            data = gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()
        except (OSError, EOFError):
            continue
        if any('local_environment_claim' in line and instance_id in line for line in data.decode(errors='replace').splitlines()):
            return True
    return False


def placement(mode, index):
    set_mode(mode)
    live = create('node2')
    before = catalog_record(live)['result']['runtime']['id']
    pause_node('node2', True)
    time.sleep(2)  # At least one normal registration must preserve the pause.
    assert query(node_id='node2')['node']['accepting_allocations'] is False
    assert command(live, 'printf existing-still-running') == 'existing-still-running'
    assert catalog_record(live)['result']['runtime']['id'] == before
    name = 'autoscale-queued-' + uuid.uuid4().hex[:10]
    future = executor.submit(create, 'node2', connection, name)
    futures.append(future)
    instance_id = 'default-' + name
    wait(lambda: any(item['instanceID'] == instance_id for item in management('/global-scheduler/scheduling_queue')['instanceInfos']), 20)
    assert not future.done(), 'paused node accepted a new pinned create'
    assert query(instance_id=instance_id)['count'] == 0, 'queued demand already owns resources'
    alternate = create()
    assert record(alternate)['node_id'] == 'node1'
    assert command(alternate, 'printf alternate-ready') == 'alternate-ready'
    local_claim = wait(lambda: local_claim_seen(alternate.id), 10) if mode == 'local_first' else None
    pause_node('node2', False)
    queued = future.result(timeout=180)
    assert record(queued)['node_id'] == 'node2'
    assert command(queued, 'printf queue-resumed') == 'queue-resumed'
    wait(lambda: not management('/global-scheduler/scheduling_queue')['instanceInfos'], 15)
    passed(index, mode=mode, queue_observed=True, new_owner='node1', running_preserved=True,
           local_claim_observed=local_claim, paused_node='node2', resumed_instance=queued.id)
    for environment in (live, alternate, queued):
        delete(environment)
    wait(lambda: not inventory('node1') and not inventory('node2'))


def wake_case(environment, traffic, index):
    before = catalog_record(environment)['result']['runtime']['id']
    environment.pause(timeout_seconds=180)
    wait(lambda: record(environment)['status'] == 'paused' and not record(environment)['resources_held'])
    wait(lambda: not inventory('node1'))
    pause_node('node1', True)
    if traffic:
        future = executor.submit(lambda: https.open(Request(environment.get_port_url(8088) + '/marker', headers=environment.get_port_auth_headers()), timeout=180).read().decode())
    else:
        future = executor.submit(environment.resume)
    futures.append(future)
    started = time.monotonic()
    while time.monotonic() - started < 3:
        assert not future.done(), 'wake completed while node scheduling was paused'
        current = record(environment)
        assert current['status'] == 'paused' and not current['resources_held'], current
        assert not inventory('node1'), 'a physical VM started while scheduling was paused'
        time.sleep(0.2)
    blocked = time.monotonic() - started
    pause_node('node1', False)
    value = future.result(timeout=180)
    if traffic:
        assert value == 'autoscaling-durable-marker', value
    after = wait(lambda: (lambda r: r if r['status'] == 'running' else None)(record(environment)))
    assert after['node_id'] == 'node1'
    runtime_after = catalog_record(environment)['result']['runtime']['id']
    guest_pid = command(environment, 'cat /tmp/autoscale-http.pid; kill -0 $(cat /tmp/autoscale-http.pid)')
    assert guest_pid == result['guest_pid'], (guest_pid, result['guest_pid'])
    assert command(environment, 'cat /tmp/autoscale-http/marker') == 'autoscaling-durable-marker'
    passed(index, blocked_seconds=round(blocked, 3), resources_held_while_paused=False,
           physical_while_paused=0, guest_pid_preserved=True, runtime_before=before, runtime_after=runtime_after)


try:
    samples = [create('node1'), create('node2'), create('node2', other_connection)]
    rows = query(page_size=1)
    assert rows['count'] == 3
    paged = [query(page=page, page_size=1)['instanceInfos'][0] for page in (1, 2, 3)]
    assert {row['instanceID'] for row in paged} == {environment.id for environment in samples}
    assert {row['tenant_id'] for row in paged} == {'e2e', 'e2e-other'}
    assert query(node_id='node2')['count'] == 2
    assert query(node_id='node2', tenant_id='e2e-other')['count'] == 1
    assert query(node_id='node1', instance_id=samples[1].id)['count'] == 0
    for filters, status in [({'node_id': 'absent'}, 404), ({'node_id': ''}, 400), ({'page_size': 1001}, 400)]:
        try:
            query(**filters)
        except HTTPError as error:
            assert error.code == status
        else:
            raise AssertionError(('invalid query succeeded', filters))
    passed(0, cross_tenant=True, paged_count=3, nodes=sorted({row['node_id'] for row in paged}))
    for environment in samples:
        delete(environment)
    placement('central', 1)
    placement('local_first', 2)
    set_mode('central')
    sleeper = create('node1')
    command(sleeper, 'mkdir -p /tmp/autoscale-http; printf autoscaling-durable-marker >/tmp/autoscale-http/marker; nohup /__adx/bin/busybox httpd -f -p 8088 -h /tmp/autoscale-http >/tmp/autoscale-http.log 2>&1 </dev/null & echo $! >/tmp/autoscale-http.pid')
    result['guest_pid'] = command(sleeper, 'cat /tmp/autoscale-http.pid; kill -0 $(cat /tmp/autoscale-http.pid)')
    wait(lambda: https.open(Request(sleeper.get_port_url(8088) + '/marker', headers=sleeper.get_port_auth_headers()), timeout=5).read().decode() == 'autoscaling-durable-marker', 15)
    wake_case(sleeper, False, 3)
    wake_case(sleeper, True, 4)
    delete(sleeper)
    wait(lambda: query()['count'] == 0 and not inventory('node1') and not inventory('node2'))
    passed(5, remaining_count=0)
    result['status'] = 'passed'
except BaseException as error:
    result['error'] = repr(error)
    raise
finally:
    for node in ('node1', 'node2'):
        try:
            pause_node(node, False)
        except Exception as error:
            result['cleanup_errors'].append('unpause ' + node + ': ' + repr(error))
    for future in futures:
        try:
            future.result(timeout=180)
        except Exception as error:
            result['cleanup_errors'].append('in-flight operation: ' + repr(error))
    executor.shutdown(wait=True)
    for instance_id, using in list(owned.items()):
        try:
            Environment.delete(instance_id, connection=using)
        except Exception as error:
            result['cleanup_errors'].append('delete ' + instance_id + ': ' + repr(error))
    for handle in handles:
        handle.close()
    if result['cleanup_errors']:
        result['status'] = 'failed'
    (evidence / 'sdk-autoscaling.json').write_text(json.dumps(result, indent=2) + '\n')
