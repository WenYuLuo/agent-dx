#!/usr/bin/env python3
"""Parameterized, sequential DFS/JuiceFS comparison; product deployment is external.

All timed work runs in prearmed Linux workers. SSH/Lima, readiness, identity,
metrics collection and lifecycle hooks are not timed as application operations.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import secrets
import shlex
import statistics
import string
import subprocess
import sys
import time
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('dfs_compare_remote', HERE / 'remote_workload.py')
worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(worker)
require_empty_directory = worker.require_empty_directory
require_free_port = worker.require_free_port
HOOKS = ('preflight', 'start', 'ready', 'stop', 'cleanup', 'verify_clean')
PLACEHOLDERS = {'run_id', 'label', 'series', 'round', 'run_root', 'mount', 'size_bytes', 'successful', 'diagnostic'}
IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}')
SHA256 = re.compile(r'[a-f0-9]{64}')


def read_flags(series):
    if series not in ('contract', 'product'):
        raise ValueError('series must be contract or product')
    return ['O_RDONLY', 'O_DIRECT'] if series == 'contract' else ['O_RDONLY']


def positive_int(value, name, maximum=2**63 - 1):
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= maximum:
        raise ValueError(name + ' must be a bounded positive integer')


def validate_template(value):
    if not isinstance(value, str) or '\0' in value or '\n' in value or '\r' in value:
        raise ValueError('command/path must be one string without control characters')
    for _, field, spec, conversion in string.Formatter().parse(value):
        if field is not None and (field not in PLACEHOLDERS or spec or conversion):
            raise ValueError('unknown or compound placeholder: ' + str(field))


def expand_argv(argv, context):
    return [argument.format_map(context) for argument in argv]


def safe_guest_path(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts or len(path.parts) < 3:
        raise ValueError('expected isolated absolute guest path')
    return path


def validate_config(config):
    if not IDENTIFIER.fullmatch(config.get('run_id', '')):
        raise ValueError('invalid run_id')
    worker.safe_path(config.get('output', ''))
    positive_int(config.get('size_bytes'), 'size_bytes', 1024**3)
    if config['size_bytes'] % 4096:
        raise ValueError('O_DIRECT datasets must be multiples of 4096 bytes')
    positive_int(config.get('rounds', 3), 'rounds', 3)
    if config.get('ready_wait_seconds', 30) != 30 or config.get('repeat_gap_seconds', 1) != 1:
        raise ValueError('ready wait and repeat gap must be 30s and 1s')
    if not isinstance(config.get('diagnostic', False), bool):
        raise ValueError('diagnostic must be boolean')
    if config.get('diagnostic') and config.get('rounds', 3) != 1:
        raise ValueError('diagnostics require a separate one-round run')
    if not config.get('diagnostic') and config.get('rounds', 3) != 3:
        raise ValueError('formal series requires exactly three rounds; no appended samples')
    positive_int(config.get('timeout_seconds', 180), 'timeout_seconds', 3600)
    positive_int(config.get('hook_timeout_seconds', 180), 'hook_timeout_seconds', 3600)
    if set(config.get('nodes', {})) != {'a', 'b', 'c'}:
        raise ValueError('nodes must contain a, b and c')
    for node in config['nodes'].values():
        safe_guest_path(node.get('root', ''))
        positive_int(node.get('min_free_bytes'), 'min_free_bytes')
        argv = node.get('transport')
        if (not isinstance(argv, list) or not argv or
                Path(argv[0]).name not in ('ssh', 'limactl') or
                any(not isinstance(arg, str) or '\0' in arg or '\n' in arg for arg in argv)):
            raise ValueError('transport must be an ssh or limactl argv array')
        if Path(argv[0]).name == 'limactl' and (len(argv) < 4 or argv[1] != 'shell' or argv[-1] != '--'):
            raise ValueError('Lima transport must end its shell options with --')
        if node.get('command_prefix', []) not in [[], ['sudo', '-n']]:
            raise ValueError('command_prefix must be empty or explicitly sudo -n')
        for port in node.get('ports', []): positive_int(port, 'port', 65535)
        for binary in node.get('binaries', []):
            safe_guest_path(binary.get('path', ''))
            if not SHA256.fullmatch(binary.get('sha256', '')): raise ValueError('invalid binary SHA256')
        for name in node.get('dependencies', ['python3']) + node.get('process_names', []):
            if not re.fullmatch(r'[A-Za-z0-9_.+-]+', name): raise ValueError('invalid executable name')
    control = config.get('control', {})
    for key in ['listen_ip', 'peer_ip', 'expected_peer']:
        if ipaddress.ip_address(control.get(key, '')).version != 4:
            raise ValueError('control channel requires explicit IPv4 addresses')
    if control['listen_ip'] == '0.0.0.0' or control['expected_peer'] == '0.0.0.0':
        raise ValueError('control channel requires specific endpoint identities')
    positive_int(control.get('port'), 'control port', 65535)
    if not isinstance(config.get('labels'), dict) or not config['labels']:
        raise ValueError('at least one label required')
    if 'orders' in config:
        orders = config['orders']
        if not isinstance(orders, list) or len(orders) != config.get('rounds', 3):
            raise ValueError('orders must freeze one label permutation per round')
        for order in orders:
            if not isinstance(order, list) or len(order) != len(config['labels']) or set(order) != set(config['labels']):
                raise ValueError('every order must contain all configured labels exactly once')
    for name, label in config['labels'].items():
        if not IDENTIFIER.fullmatch(name): raise ValueError('invalid label')
        for node in ['a', 'b']:
            value = label.get('mounts', {}).get(node, '')
            validate_template(value)
            safe_guest_path(value.format_map({key: 'validation' for key in PLACEHOLDERS}))
        for series in ['contract', 'product']:
            settings = label.get('settings', {}).get(series)
            if not isinstance(settings, dict) or not {'read_ahead_kb', 'prefetch'} <= settings.keys():
                raise ValueError('both series must record effective read_ahead_kb and prefetch')
            if series == 'contract' and (settings['read_ahead_kb'] != 0 or settings['prefetch'] != 0):
                raise ValueError('contract requires both readahead and prefetch disabled')
        for name in HOOKS:
            commands = label.get('hooks', {}).get(name)
            if not isinstance(commands, list) or not commands:
                raise ValueError('missing lifecycle hook: ' + name)
            for command in commands:
                if command.get('node') not in ['a', 'b', 'c', 'controller']:
                    raise ValueError('unknown lifecycle node')
                argv = command.get('argv')
                if not isinstance(argv, list) or not argv: raise ValueError('hook argv required')
                for arg in argv: validate_template(arg)
                if Path(argv[0]).name in ['sh', 'bash', 'zsh', 'eval']:
                    raise ValueError('hook must invoke an explicit executable, not a shell')
        metric_names = set()
        for item in label.get('metrics', []):
            parsed = urlparse(item.get('url', ''))
            if (item.get('node') not in ['a', 'b', 'c'] or parsed.scheme != 'http'
                    or parsed.hostname not in ('127.0.0.1', 'localhost', '::1')
                    or parsed.username or parsed.password or parsed.fragment):
                raise ValueError('metrics must use a node-local unauthenticated HTTP endpoint')
            positive_int(parsed.port, 'metrics port', 65535)
            if not IDENTIFIER.fullmatch(item.get('name', '')) or item['name'] in metric_names:
                raise ValueError('invalid or duplicate metric name')
            metric_names.add(item['name'])


def round_orders(config, labels):
    configured = config.get('orders')
    if configured is not None:
        return [[label for label in order if label in labels] for order in configured]
    return [list(labels) if number % 2 == 0 else list(reversed(labels))
            for number in range(config.get('rounds', 3))]


def summarize(values):
    if not values or any(isinstance(value, bool) or not isinstance(value, (float, int))
                         or not math.isfinite(value) or value <= 0 for value in values):
        raise ValueError('summary needs positive finite timings')
    ratio = max(values) / min(values)
    return {'samples_ns': values, 'median_ns': statistics.median(values), 'max_min': ratio,
            'stable': ratio <= 1.10, 'stability_limit': 1.10}


def parse_metrics(text):
    values = {}
    for line in text.splitlines():
        if not line or line.startswith('#'): continue
        match = re.fullmatch(r'([^\s]+(?:\{.*\})?)\s+([^\s]+)(?:\s+\d+)?', line)
        if not match: continue
        try: value = float(match.group(2))
        except ValueError: continue
        if math.isfinite(value): values[match.group(1)] = value
    return values


def metric_delta(before, after):
    first, second = parse_metrics(before), parse_metrics(after)
    changes = {}
    for name in first.keys() & second.keys():
        # Only monotonic counters/histogram fields are meaningful differences;
        # callers retain raw gauges and timestamps instead of treating them as work.
        difference = second[name] - first[name]
        if difference < 0:
            raise ValueError('metric reset or gauge cannot be differenced: ' + name)
        changes[name] = difference
    return changes


def metric_window_boundaries(items):
    windows = {}
    for item in items:
        writer = item['node'] == 'a'
        windows[item['name']] = {
            'node': item['node'], 'clock': 'this node CLOCK_MONOTONIC',
            'first': {
                'name': 'publish_and_first' if writer else 'begin_to_first_complete',
                'start': ('A snapshot before BEGIN and create' if writer else
                          'B snapshot after BEGIN, before BEGIN_READY and publication'),
                'end': ('A snapshot after first SHA/EOF ACK verification' if writer else
                        'B snapshot after first SHA/EOF ACK send'),
                'includes_publish': writer,
                'includes_publication_wait': not writer,
                'isolated_read': False,
                'legacy_key': 'metric_deltas.<name>.first'},
            'repeat': {
                'name': 'first_snapshot_to_repeat_complete',
                'start': 'after-first snapshot',
                'end': ('after repeat ACK verification' if writer else 'after repeat ACK send'),
                'includes_fixed_repeat_gap': True, 'isolated_read': False}}
    return windows


def atomic_json(path, value):
    worker.atomic_json(path, value)


class Transport:
    def __init__(self, argv, command_prefix=None):
        self.argv = argv
        self.command_prefix = command_prefix or []

    def command(self, argv):
        remote_argv = self.command_prefix + argv
        return self.argv + ([shlex.join(remote_argv)] if Path(self.argv[0]).name == 'ssh' else remote_argv)

    def spawn_wait(self, config):
        command = self.command(['python3', '-c', (HERE / 'remote_workload.py').read_text(), 'wait'])
        child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        child.stdin.write(json.dumps(config))
        child.stdin.close()
        child.stdin = None
        return child

    def run(self, argv, payload=None, timeout=60):
        # SSH has a remote shell by protocol. Quote every literal argument once;
        # do not interpolate a command template or pass user values to eval.
        command = self.command(argv)
        result = subprocess.run(command, input=None if payload is None else json.dumps(payload),
                                text=True, capture_output=True, timeout=timeout, check=False)
        if result.returncode:
            raise RuntimeError(json.dumps({'argv': argv[:2], 'exit_code': result.returncode,
                                           'stdout': result.stdout[-16384:], 'stderr': result.stderr[-16384:]}))
        return result.stdout

    def remote(self, operation, config):
        return json.loads(self.run(['python3', '-c', (HERE / 'remote_workload.py').read_text(), operation], config))


def hook(config, label, name, context, transports, evidence):
    results = []
    for index, command in enumerate(label['hooks'][name]):
        target = command['node']
        local = dict(context)
        local['run_root'] = (str(evidence) if target == 'controller' else
                             config['nodes'][target]['root'] + '/' + context['run_id'])
        local['mount'] = label['mounts'].get(target, '').format_map(context)
        argv = expand_argv(command['argv'], local)
        before = time.monotonic_ns()
        if target == 'controller':
            response = subprocess.run(argv, text=True, capture_output=True, check=False,
                                      timeout=config.get('hook_timeout_seconds', 180))
            stdout, stderr, status = response.stdout, response.stderr, response.returncode
        else:
            try:
                stdout = transports[target].run(argv, timeout=config.get('hook_timeout_seconds', 180))
                stderr, status = '', 0
            except Exception as error:
                stdout, stderr, status = '', str(error), 1
        record = {'node': target, 'stdout': stdout, 'stderr': stderr, 'exit_code': status,
                  'elapsed_ns': time.monotonic_ns() - before}
        atomic_json(evidence / f'hook-{name}-{index}.json', record)
        if status: raise RuntimeError('lifecycle hook failed: ' + name + ': ' + stderr[-4096:])
        if name in ['ready', 'verify_clean']:
            value = json.loads(stdout)
            if value.get('ready' if name == 'ready' else 'clean') is not True:
                raise RuntimeError('lifecycle hook did not confirm ' + name)
            if name == 'ready':
                expected = label['settings'][context['series']]
                actual = value.get('effective_settings', {})
                if any(actual.get(key) != expected_value for key, expected_value in expected.items()):
                    raise RuntimeError('effective settings differ from frozen comparison settings')
        results.append(record)
    return results


def wait_status(transports, configs, wanted, timeout):
    deadline = time.monotonic() + timeout
    latest = {}
    while time.monotonic() < deadline:
        for node, config in configs.items():
            state = transports[node].remote('status', config)
            latest[node] = state
            if state.get('result', {}).get('completed') is False:
                raise RuntimeError('worker failed: ' + node + ': ' + json.dumps(state['result']))
        if all(wanted in state for state in latest.values()): return latest
        time.sleep(.1)
    raise TimeoutError('workload did not reach ' + wanted + ': ' + json.dumps(latest))


def run_round(config, series, label_name, number, output):
    label = config['labels'][label_name]
    run_id = f"{config['run_id']}-{series}-{label_name}-{number:02d}"
    evidence = output / run_id
    evidence.mkdir(mode=0o700)
    context = {'run_id': run_id, 'series': series, 'label': label_name, 'round': str(number),
               'size_bytes': str(config['size_bytes']), 'successful': 'false',
               'diagnostic': str(config.get('diagnostic', False)).lower()}
    transports = {name: Transport(node['transport'], node.get('command_prefix', []))
                  for name, node in config['nodes'].items()}
    worker_configs = {}
    installed = []
    waiters = {}
    lifecycle_started = False
    successful = False
    result = {'run_id': run_id, 'series': series, 'label': label_name, 'round': number,
              'completed': False, 'diagnostic': config.get('diagnostic', False),
              'open_flags': read_flags(series), 'settings': label['settings'][series],
              'budgets': label.get('budgets', {}),
              'page_cache_initial_state': label.get('page_cache_initial_state', 'recorded by fixture preflight'),
              'size_bytes': config['size_bytes'], 'admission': {},
              'identity': label.get('identity', {}),
              'external_backend': label.get('external_backend', {})}
    token = secrets.token_hex(32)
    try:
        # All independent nodes are checked before any lifecycle mutation.
        for name, node in config['nodes'].items():
            admission = {**node, 'run_root': node['root'] + '/' + run_id}
            if name in ['a', 'b']:
                mount_path = Path(label['mounts'][name].format_map(context))
                control_path = Path(admission['run_root'])
                if (mount_path == control_path or mount_path in control_path.parents
                        or control_path in mount_path.parents):
                    raise ValueError('workload mount and control root must be disjoint')
            admission['ports'] = list(node.get('ports', []))
            if name == 'b' and config['control']['port'] not in admission['ports']:
                admission['ports'].append(config['control']['port'])
            result['admission'][name] = transports[name].remote('preflight', admission)
        hook(config, label, 'preflight', context, transports, evidence)
        lifecycle_started = True
        hook(config, label, 'start', context, transports, evidence)
        result['ready'] = hook(config, label, 'ready', context, transports, evidence)
        source = (HERE / 'remote_workload.py').read_text()
        result['workload_sha256'] = hashlib.sha256(source.encode()).hexdigest()
        for name, role in [('b', 'reader'), ('a', 'writer')]:
            run_root = config['nodes'][name]['root'] + '/' + run_id
            mount = label['mounts'][name].format_map(context)
            if Path(mount) == Path(run_root) or Path(run_root) in Path(mount).parents or Path(mount) in Path(run_root).parents:
                raise ValueError('workload mount and control root must be disjoint')
            worker_config = {'run_root': run_root, 'run_id': run_id, 'role': role,
                             'series': series, 'size_bytes': config['size_bytes'],
                             'salt': number, 'path': mount + '/test.bin', 'control': config['control'],
                             'timeout_seconds': config.get('timeout_seconds', 180),
                             'metrics': [item for item in label.get('metrics', []) if item['node'] == name]}
            worker_configs[name] = worker_config
            transports[name].remote('install', {'run_root': run_root, 'source': source, 'token': token,
                                                'role_config': worker_config})
            installed.append(name)
            transports[name].remote('start', worker_config)
            if name == 'b': wait_status(transports, {'b': worker_config}, 'listening', 30)
        wait_status(transports, worker_configs, 'armed', config.get('timeout_seconds', 180))
        for name, worker_config in worker_configs.items():
            waiters[name] = transports[name].spawn_wait(worker_config)
        wait_status(transports, worker_configs, 'watching', 30)
        # One unconditional ready+prearmed quiet window, equal for both products.
        quiet_start = time.monotonic_ns()
        time.sleep(30)
        result['ready_quiet_actual_ns'] = time.monotonic_ns() - quiet_start
        transports['a'].remote('go', worker_configs['a'])
        states = {}
        # Both remote wait processes were already blocked on pidfd before GO.
        # No SSH/Lima process launch, status poll or host scrape is performed here.
        for name, waiter in waiters.items():
            stdout, stderr = waiter.communicate(timeout=config.get('timeout_seconds', 180))
            if waiter.returncode:
                raise RuntimeError('prearmed wait failed: ' + name + ': ' + stderr[-8192:])
            states[name] = json.loads(stdout)
            if states[name].get('result', {}).get('completed') is not True:
                raise RuntimeError('worker failed: ' + name + ': ' + json.dumps(states[name]))
        if any(state.get('result', {}).get('completed') is not True for state in states.values()):
            raise RuntimeError('worker failed: ' + json.dumps(states))
        writer_result = states['a']['result']
        reader_result = states['b']['result']
        result.update(writer=writer_result, reader=reader_result)
        if any(state['wait']['exit_status'] != 0 for state in states.values()):
            raise RuntimeError('workload returned nonzero exit status')
        result['timings'] = {'publish_ns': writer_result['publish']['publish_ns'],
                             'first_ns': writer_result['first']['full_read_ns'],
                             'repeat_ns': writer_result['repeat']['full_read_ns'],
                             'e2e_ns': writer_result['e2e_ns'],
                             'total_with_repeat_ns': writer_result['total_with_repeat_ns']}
        result['metric_deltas'] = {}
        result['metric_window_boundaries'] = metric_window_boundaries(label.get('metrics', []))
        snapshots = {'before': {}, 'after_first': {}, 'after_repeat': {}}
        for role_result in [writer_result, reader_result]:
            for phase in snapshots:
                snapshots[phase].update(role_result['metrics'][phase])
        for metric_name, before in snapshots['before'].items():
            result['metric_deltas'][metric_name] = {}
            for phase, left, right in [('first', 'before', 'after_first'), ('repeat', 'after_first', 'after_repeat')]:
                # Prometheus gauges may decrease. Retain them raw; difference only
                # monotonic counters selected explicitly by the fixture.
                patterns = label.get('metric_counter_patterns', ['_total', '_bucket', '_sum', '_count'])
                def counters(text):
                    return '\n'.join(line for line in text.splitlines() if not line.startswith('#')
                        and any(pattern in line.split('{', 1)[0].split(' ', 1)[0] for pattern in patterns))
                result['metric_deltas'][metric_name][phase] = metric_delta(
                    counters(snapshots[left][metric_name]['text']), counters(snapshots[right][metric_name]['text']))
        successful = True
        context['successful'] = 'true'
    except Exception as error:
        result['failure'] = {'type': type(error).__name__, 'message': str(error)}
    finally:
        result['cleanup_errors'] = []
        # Stop only processes identified by this run's PID/start/boot/command.
        for name in reversed(installed):
            try:
                transports[name].remote('stop', worker_configs[name])
            except Exception as error:
                result['cleanup_errors'].append({'step': 'worker-stop-' + name, 'error': str(error)})
            try:
                files = transports[name].remote('collect', worker_configs[name])
                node_dir = evidence / ('worker-' + name)
                node_dir.mkdir(mode=0o700)
                for filename, text in files.items():
                    if Path(filename).name != filename: raise ValueError('unsafe collected filename')
                    (node_dir / filename).write_text(text)
            except Exception as error:
                result['cleanup_errors'].append({'step': 'worker-collect-' + name, 'error': str(error)})
        for name, waiter in waiters.items():
            if waiter.poll() is None:
                try:
                    waiter.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    try:
                        waiter.terminate()
                        waiter.communicate(timeout=5)
                    except Exception as error:
                        result['cleanup_errors'].append({'step': 'wait-channel-terminate-' + name,
                                                         'error': str(error)})
                    result['cleanup_errors'].append({'step': 'wait-channel-' + name,
                                                     'error': 'prearmed wait did not exit after worker stop'})
        if lifecycle_started:
            stop_ok = False
            try:
                hook(config, label, 'stop', context, transports, evidence)
                stop_ok = True
            except Exception as error:
                result['cleanup_errors'].append({'step': 'stop', 'error': str(error)})
            if successful and stop_ok and not result['cleanup_errors']:
                try: hook(config, label, 'cleanup', context, transports, evidence)
                except Exception as error:
                    result['cleanup_errors'].append({'step': 'cleanup', 'error': str(error)})
            try: hook(config, label, 'verify_clean', context, transports, evidence)
            except Exception as error:
                result['cleanup_errors'].append({'step': 'verify_clean', 'error': str(error)})
        result['completed'] = successful and not result['cleanup_errors']
        result['failed_payload_preserved'] = not successful
        atomic_json(evidence / 'result.json', result)
    return result


def run(config, series_list, labels):
    validate_config(config)
    output = Path(config['output'])
    require_empty_directory(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    result = {'schema_version': 1, 'run_id': config['run_id'], 'runs': [], 'summary': {},
              'completed': False, 'diagnostic': config.get('diagnostic', False),
              'config_sha256': hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
              'primary_e2e': 'A create to verified B first SHA/EOF ACK; no phase sum or S3 subtraction',
              'repeat_gap_seconds': 1, 'ready_wait_seconds': 30,
              'series_separate': True, 'no_automatic_retry': True,
              'round_orders': round_orders(config, labels)}
    for series in series_list:
        read_flags(series)
        for number in range(1, config.get('rounds', 3) + 1):
            for label in result['round_orders'][number - 1]:
                sample = run_round(config, series, label, number, output)
                result['runs'].append(sample)
                atomic_json(output / 'result.json', result)
                if not sample['completed']:
                    # A failed sample is retained and the affected campaign stops.
                    # No invisible retry or replacement round is permitted.
                    return result
        for label in labels:
            selected = [item for item in result['runs'] if item['series'] == series and item['label'] == label]
            result['summary'][series + '/' + label] = {
                name: summarize([item['timings'][name] for item in selected]) for name in
                ['publish_ns', 'first_ns', 'repeat_ns', 'e2e_ns', 'total_with_repeat_ns']}
    result['completed'] = True
    result['stable'] = all(metric['stable'] for group in result['summary'].values()
                           for name, metric in group.items() if name != 'total_with_repeat_ns')
    atomic_json(output / 'result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--series', choices=['contract', 'product', 'both'], default='both')
    parser.add_argument('--rounds', type=int)
    parser.add_argument('--labels', help='comma-separated frozen labels, in execution order')
    parser.add_argument('--validate-only', action='store_true')
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    if args.rounds is not None: config['rounds'] = args.rounds
    validate_config(config)
    labels = args.labels.split(',') if args.labels else list(config['labels'])
    if not labels or len(set(labels)) != len(labels) or any(label not in config['labels'] for label in labels):
        parser.error('labels must select distinct configured labels')
    if args.validate_only:
        print(json.dumps({'valid': True, 'labels': labels, 'series': args.series}))
        return 0
    result = run(config, ['contract', 'product'] if args.series == 'both' else [args.series], labels)
    print(json.dumps({'completed': result['completed'], 'stable': result.get('stable'),
                      'result': str(Path(config['output']) / 'result.json')}))
    return 0 if result['completed'] else 1


if __name__ == '__main__': sys.exit(main())
