#!/usr/bin/env python3
"""Linux-only 64KiB control/workload smoke on one ordinary filesystem.

This is neither FUSE validation nor a performance sample: it deliberately omits
product lifecycle and the formal 30-second quiet window.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time

import remote_workload as worker


def wait_for(configs, field):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        states = [worker.action('status', config) for config in configs]
        if any(state.get('result', {}).get('completed') is False for state in states):
            raise RuntimeError('worker failed before GO: ' + json.dumps(states))
        if all(field in state for state in states): return
        time.sleep(.01)
    raise TimeoutError('smoke did not reach ' + field)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, help='fresh absolute evidence directory on Linux ext4')
    args = parser.parse_args()
    if os.uname().sysname != 'Linux': parser.error('run only on Linux')
    root = worker.safe_path(args.root)
    worker.require_empty_directory(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    summary = {'scope': 'ordinary Linux filesystem protocol smoke, not FUSE or performance acceptance',
               'size_bytes': 65536, 'formal_quiet_window': False, 'runs': []}
    source = Path(worker.__file__).read_text()
    for series in ['contract', 'product']:
        directory = root / series
        directory.mkdir(mode=0o700)
        with socket.socket() as candidate:
            candidate.bind(('127.0.0.1', 0))
            port = candidate.getsockname()[1]
        token = secrets.token_hex(32)
        configs = {}
        waiters = {}
        try:
            for role in ['reader', 'writer']:
                config = {'role': role, 'run_root': str(directory / role),
                          'run_id': 'smoke-' + series, 'series': series, 'size_bytes': 65536,
                          'salt': 1, 'path': str(directory / 'test.bin'), 'metrics': [],
                          'timeout_seconds': 30, 'control': {'listen_ip': '127.0.0.1',
                              'peer_ip': '127.0.0.1', 'expected_peer': '127.0.0.1', 'port': port}}
                configs[role] = config
                worker.action('install', {'run_root': config['run_root'], 'role_config': config,
                                          'source': source, 'token': token})
                worker.action('start', config)
                if role == 'reader': wait_for([config], 'listening')
            wait_for(list(configs.values()), 'armed')
            for role, config in configs.items():
                waiter = subprocess.Popen([sys.executable, worker.__file__, 'wait'], stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                waiter.stdin.write(json.dumps(config))
                waiter.stdin.close()
                waiter.stdin = None
                waiters[role] = waiter
            wait_for(list(configs.values()), 'watching')
            worker.action('go', configs['writer'])
            states = {}
            for role, waiter in waiters.items():
                stdout, stderr = waiter.communicate(timeout=30)
                if waiter.returncode: raise RuntimeError('pidfd wait failed: ' + stderr)
                states[role] = json.loads(stdout)
            if any(state['wait']['exit_status'] != 0 for state in states.values()):
                raise RuntimeError('workload failed: ' + json.dumps(states))
            summary['runs'].append({'series': series, 'completed': True, 'states': states})
        finally:
            for config in configs.values(): worker.action('stop', config)
            for waiter in waiters.values():
                if waiter.poll() is None: waiter.communicate(timeout=15)
    summary['completed'] = True
    worker.atomic_json(root / 'smoke-result.json', summary)
    print(json.dumps({'completed': True, 'result': str(root / 'smoke-result.json')}))


if __name__ == '__main__': main()
