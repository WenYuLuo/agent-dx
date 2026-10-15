#!/usr/bin/env python3
"""Public installed-SDK cache acceptance on two real Firecracker nodes."""
import contextlib
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import quote

from cache_contract import CASES
from s3_client import Client

root = Path(sys.argv[1])
image = sys.argv[2]
base = Path(os.environ['ADX_FC_BASE'])
evidence = root / 'evidence'
output = evidence / 'sdk-cache.json'
os.environ['SSL_CERT_FILE'] = str(root / 'secrets/tls/ca.pem')
from adx_environment import ConnectionConfig, Environment, RetentionPolicy

connection = ConnectionConfig(
    server_address='127.0.0.1:8443',
    token=(root / 'secrets/api-key').read_text().strip(),
    use_tls=True,
    verify_tls=True,
)
s3 = Client(root)
authenv = {**os.environ, 'REDISCLI_AUTH': (root / 'secrets/redis-key').read_text().strip()}
result = {'status': 'failed', 'cases': [], 'cleanup_errors': [], 'suite': 'cache'}
handles = []


def call(args, **kwargs):
    return subprocess.check_output(list(map(str, args)), text=True, timeout=30, **kwargs).strip()


def wait(predicate, seconds=180):
    deadline = time.monotonic() + seconds
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise TimeoutError('checkpoint-cache E2E condition timed out')
        time.sleep(0.2)


def catalog():
    raw = call([base / 'tools/redis-cli', '--json', 'HGETALL', 'adx:{acceptance}:control:v1'], env=authenv)
    return {key: json.loads(value) for key, value in json.loads(raw).items()}


def record(environment):
    return catalog()['environment:' + environment.id]


def passed(index, **details):
    item = {'name': CASES[index], 'passed': True, **details}
    result['cases'].append(item)
    output.write_text(json.dumps(result, indent=2) + '\n')
    logging.info('PASS %s', json.dumps(item))


def command(environment, script):
    value = environment.commands.run(script, timeout=30)
    if value.exit_code != 0:
        raise AssertionError((value.exit_code, value.stderr))
    return value.stdout.strip()


def create():
    value = Environment(name='cache-' + uuid.uuid4().hex[:10], image=image, runtime='firecracker',
                        cpu=500, memory=128, storage_mb=32, idle_timeout=0,
                        node_id='node1', connection=connection, schedule_timeout=60, create_timeout=180)
    handles.append(value)
    command(value, (
        "sh -c 'echo $$ >/tmp/cache.pid; n=0; while :; do n=$((n+1)); "
        'echo $n >/tmp/cache.next; mv /tmp/cache.next /tmp/cache.counter; '
        "sleep 0.1; done' >/tmp/cache.log 2>&1 </dev/null &"
    ))
    pid = wait(lambda: command(value, 'cat /tmp/cache.pid'))
    counter = wait(lambda: int(command(value, 'cat /tmp/cache.counter')) >= 2)
    payload = (value.id + '-checkpoint-cache').encode() * 128
    value.files.write('/tmp/cache.bin', payload)
    return {'environment': value, 'pid': pid, 'payload': payload, 'counter_started': counter}


def pause(item):
    environment = item['environment']
    item['before'] = int(command(environment, 'cat /tmp/cache.counter'))
    started = time.monotonic()
    saved = environment.pause(retention=RetentionPolicy.for_seconds(1800), timeout_seconds=120)
    artifact = record(environment)['result']['checkpoint']['artifact']
    item.update(artifact=artifact, size=saved.size, pause_seconds=time.monotonic() - started)
    if not (artifact['storage'] == 'shared' and saved.size > 0):
        raise AssertionError(artifact)
    return artifact


def cache_paths(item):
    identity = item['artifact']['location']
    folder = root / 'node1/checkpoints'
    return [folder / 'staging' / identity, folder / 'cache' / identity]


def cached_path(item):
    return next((path for path in cache_paths(item) if path.is_dir()), None)


def remote_manifest(item):
    artifact = item['artifact']
    key = f"/{s3.config['bucket']}/{s3.config['prefix']}/{artifact['location']}/manifest.json"
    value = json.loads(s3.request('GET', key))
    if value['size'] != artifact['size_bytes']:
        raise AssertionError(value)
    return value


def resume(item):
    started = time.monotonic()
    value = item['environment'].resume()
    if value.node_id != 'node1':
        raise AssertionError(value)
    item['resume_seconds'] = time.monotonic() - started
    if command(item['environment'], 'cat /tmp/cache.pid') != item['pid']:
        raise AssertionError('restored process PID changed')
    after = wait(lambda: (lambda n: n if n > item['before'] else None)(
        int(command(item['environment'], 'cat /tmp/cache.counter'))
    ))
    if item['environment'].files.read('/tmp/cache.bin', format='bytes') != item['payload']:
        raise AssertionError('restored file content changed')
    return {
        'pid_preserved': True,
        'before': item['before'],
        'after': after,
        'resume_seconds': item['resume_seconds'],
        'checkpoint_bytes': item['size'],
    }


def delete(item):
    environment = item['environment']
    environment.kill()
    environment.close()
    handles.remove(environment)
    wait(lambda: 'environment:' + environment.id not in catalog())
    wait(lambda: not any(path.exists() for path in cache_paths(item)))


def node_process():
    status = json.loads(call([base / 'package/bin/adxctl', 'status', '--config', root / 'node1/deployment.yaml']))
    return next(service['pid'] for service in status['services'] if service['role'] == 'adxlet')


def restart(entries=None, budget=None):
    old_pid = node_process()
    before = catalog()['node:node1']['session']['id']
    if entries is not None or budget is not None:
        argv = Path(f'/proc/{old_pid}/cmdline').read_bytes().decode().split('\0')
        path = Path(argv[argv.index('--config') + 1])
        if not (path.resolve().is_relative_to((root / 'node1/state').resolve())):
            raise AssertionError(path)
        config = json.loads(path.read_text())
        if entries is not None:
            config['checkpoint_storage']['cache_max_entries'] = entries
        if budget is not None:
            config['checkpoint_storage']['cache_budget_bytes'] = budget
        temporary = path.with_suffix('.cache-new')
        temporary.write_text(json.dumps(config))
        temporary.chmod(0o600)
        temporary.replace(path)
    os.kill(old_pid, signal.SIGKILL)

    def ready():
        node = catalog()['node:node1']
        return node['session']['id'] != before and node['session']['routable'] and node['node']['available']

    wait(ready)
    if node_process() == old_pid:
        raise AssertionError('adxlet process was not replaced')
    return {'session_changed': True, 'previous_session': before, 'session': catalog()['node:node1']['session']['id']}


@contextlib.contextmanager
def storage_blocked():
    topology = json.loads((evidence / 'topology.json').read_text())
    namespace = topology['namespaces']['node1']
    cmd = ['nsenter', '--net=/var/run/netns/' + namespace, 'iptables']
    rule = [
        'OUTPUT', '-d', '10.240.0.1', '-p', 'tcp', '--dport', '19090',
        '-m', 'comment', '--comment', root.name + '-cache', '-j', 'REJECT',
    ]
    subprocess.run([*cmd, '-I', *rule], check=True)
    try:
        subprocess.run([*cmd, '-C', *rule], check=True)
        yield
    finally:
        subprocess.run([*cmd, '-D', *rule], check=True)


def empty_objects():
    query = 'list-type=2&prefix=' + quote(s3.config['prefix'] + '/', safe='')
    listing = s3.request('GET', '/' + s3.config['bucket'], query=query)
    return len(ET.fromstring(listing).findall('{*}Contents')) == 0


def main():
    try:
        # Defaults are exercised first, without explicit quota overrides.
        item = create()
        pause(item)
        path = cached_path(item)
        if path is None:
            raise AssertionError('Pause did not seed its local cache')
        index = root / 'node1/checkpoints/cache-index' / (item['artifact']['location'] + '.json')
        if not (json.loads(index.read_text())['committed'] is True):
            raise AssertionError('Pause cache index was not committed')
        remote_manifest(item)
        with storage_blocked():
            details = resume(item)
        passed(0, storage_blocked=True, defaults={'entries': 500, 'bytes': 50 * 1024**3}, **details)
        delete(item)

        restart(entries=2, budget=50 * 1024**3)
        items = []
        for _ in range(3):
            item = create()
            pause(item)
            items.append(item)
        a, b, c = items
        if cached_path(a) is not None:
            raise AssertionError('oldest reusable checkpoint was not evicted')
        if not (cached_path(b) and cached_path(c)):
            raise AssertionError('recent reusable checkpoints were evicted')
        remote_manifest(a)
        passed(1, max_entries=2, oldest_evicted=True, remote_preserved=True,
               artifacts=[item['artifact']['location'] for item in items])
        details = resume(a)
        if not (cached_path(a) and cached_path(b) and cached_path(c)):
            raise AssertionError('cold restore lost its pinned or reusable checkpoints')
        passed(2, cache_full=True, **details)
        for item in items:
            delete(item)

        restart(entries=1, budget=1)
        pinned = []
        for _ in range(2):
            item = create()
            pause(item)
            if cached_path(item) is not None:
                raise AssertionError('oversized reusable copy was not evicted')
            resume(item)
            pinned.append(item)
            if not (all(cached_path(value) for value in pinned)):
                raise AssertionError('active execution checkpoint was evicted')
        passed(3, budget_bytes=1, smallest_checkpoint=min(item['size'] for item in pinned),
               simultaneous_execution_pins=len(pinned), sizes=[item['size'] for item in pinned])
        sessions = restart()
        for item in pinned:
            if cached_path(item) is None:
                raise AssertionError('node restart lost an active execution checkpoint')
            if command(item['environment'], 'cat /tmp/cache.pid') != item['pid']:
                raise AssertionError('node restart changed the active process PID')
        passed(4, files_preserved=True, pid_preserved=True, **sessions)
        for item in pinned:
            delete(item)

        restart(entries=3, budget=50 * 1024**3)
        older = create()
        pause(older)
        newer = create()
        pause(newer)
        if not (cached_path(older) and cached_path(newer)):
            raise AssertionError('Pause did not cache both checkpoints')
        sessions = restart(entries=1)
        if cached_path(older) is not None:
            raise AssertionError('node restart did not evict the oldest checkpoint')
        if cached_path(newer) is None:
            raise AssertionError('node restart evicted the newest checkpoint')
        with storage_blocked():
            resume(newer)
        resume(older)
        passed(5, oldest_evicted=True, newest_preserved=True, **sessions)
        delete(older)
        delete(newer)

        wait(lambda: not any(key.startswith('environment:') for key in catalog()))
        wait(lambda: not any(
            path.is_file() for node in ('node1', 'node2') for path in (root / node / 'checkpoints').rglob('*')
        ))
        wait(empty_objects)
        inventories = {}
        for node in ('node1', 'node2'):
            raw = call(['/opt/adx-fc/bin/sbox', '-a', root / node / 'sandboxd/sandboxd.sock', 'list'])
            inventories[node] = len(raw.strip().splitlines()[1:])
        if not (inventories == {'node1': 0, 'node2': 0}):
            raise AssertionError(inventories)
        passed(6, remaining_environments=0, local_files=0, s3_objects=0, inventories=inventories)
        result['status'] = 'passed'
    except BaseException as error:
        result['error'] = repr(error)
        result['traceback'] = traceback.format_exc()
        raise
    finally:
        for environment in reversed(handles):
            try:
                environment.kill()
                environment.close()
            except Exception as error:
                result['cleanup_errors'].append(repr(error))
        output.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stdout)
    main()
