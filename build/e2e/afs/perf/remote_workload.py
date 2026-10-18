#!/usr/bin/env python3
"""Linux workload and authenticated, prearmed A-clock control channel.

This module owns only its control processes and one test file. Product lifecycle
and external storage deployment belong to the configured fixture.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import mmap
import os
from pathlib import Path
import shutil
import signal
import select
import socket
import struct
import subprocess
import sys
import time
import urllib.request

BLOCK = 4 * 1024 * 1024
MAX_FRAME = 1024 * 1024


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_suffix('.writing')
    with open(temporary, 'x', encoding='utf-8') as target:
        os.chmod(temporary, 0o600)
        json.dump(value, target, sort_keys=True)
    os.replace(temporary, path)


def safe_path(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts or len(path.parts) < 3:
        raise ValueError('expected an isolated absolute path below a parent directory')
    for candidate in [path, *path.parents]:
        if candidate.is_symlink():
            raise ValueError('symlink in controlled path')
    return path


def require_empty_directory(path):
    path = safe_path(str(path))
    if path.exists() and (not path.is_dir() or next(path.iterdir(), None) is not None):
        raise ValueError('controlled directory is not empty: ' + str(path))


def require_free_port(address, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        candidate.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        candidate.bind((address, port))


def process_identity(pid):
    text = Path('/proc', str(pid), 'stat').read_text()
    fields = text[text.rindex(')') + 2:].split()
    return {'pid': pid, 'start_ticks': fields[19], 'state': fields[0],
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def identity_matches(saved):
    try:
        current = process_identity(saved['pid'])
    except FileNotFoundError:
        return False
    if any(current[key] != saved[key] for key in ['start_ticks', 'boot_id']):
        raise RuntimeError('owned PID identity changed; refusing to signal')
    return current['state'] != 'Z'


def memory_admission(config):
    required = config.get('min_available_memory_bytes')
    if required is None:
        return None
    if isinstance(required, bool) or not isinstance(required, int) or required <= 0:
        raise ValueError('minimum available memory must be a positive byte count')
    for line in Path('/proc/meminfo').read_text().splitlines():
        fields = line.split()
        if len(fields) == 3 and fields[0] == 'MemAvailable:' and fields[2] == 'kB':
            available = int(fields[1]) * 1024
            if available < required:
                raise RuntimeError('insufficient MemAvailable before starting fixture: '
                                   + json.dumps({'available_bytes': available, 'required_bytes': required}))
            return {'available_bytes': available, 'required_bytes': required,
                    'source': '/proc/meminfo MemAvailable; not a product memory limit'}
    raise RuntimeError('MemAvailable unavailable; memory admission not proven')


def preflight(config):
    root = safe_path(config['run_root'])
    require_empty_directory(root)
    ancestor = root.parent
    while not ancestor.exists():
        ancestor = ancestor.parent
    free = shutil.disk_usage(ancestor).free
    if free < config['min_free_bytes']:
        raise RuntimeError('insufficient free space before starting fixture')
    memory = memory_admission(config)
    missing = [name for name in config.get('dependencies', ['python3']) if shutil.which(name) is None]
    if missing:
        raise RuntimeError('missing dependencies: ' + ', '.join(missing))
    for port in config.get('ports', []):
        require_free_port('0.0.0.0', port)
    forbidden = set(config.get('process_names', []))
    collisions = []
    for proc in Path('/proc').iterdir():
        if proc.name.isdigit():
            try:
                name = (proc / 'comm').read_text().strip()
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
            if name in forbidden:
                collisions.append({'pid': int(proc.name), 'name': name})
    if collisions:
        raise RuntimeError('process name collision: ' + json.dumps(collisions))
    binaries = []
    for binary in config.get('binaries', []):
        path = Path(binary['path'])
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != binary['sha256']:
            raise RuntimeError('binary SHA256 differs: ' + str(path))
        binaries.append({'path': str(path), 'sha256': actual})
    return {'admitted': True, 'free_bytes': free, 'binaries': binaries,
            'memory': memory,
            'kernel': os.uname().release,
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def metrics(config):
    results = {}
    for item in config.get('metrics', []):
        before = time.monotonic_ns()
        with urllib.request.urlopen(item['url'], timeout=5) as response:
            text = response.read(16 * 1024 * 1024 + 1)
        if len(text) > 16 * 1024 * 1024:
            raise RuntimeError('metrics response exceeds bounded snapshot size')
        results[item['name']] = {'text': text.decode('utf-8'), 'start_monotonic_ns': before,
                                  'end_monotonic_ns': time.monotonic_ns()}
    return results


class MetricsBudgetExceeded(RuntimeError):
    def __init__(self, timing):
        self.timing = timing
        super().__init__('between-read metrics exceeded the fixed gap budget: ' + json.dumps(timing))


def between_metrics(config, gap_start_ns):
    start = time.monotonic_ns()
    snapshot = metrics(config)
    end = time.monotonic_ns()
    timing = {'role': config['role'], 'start_monotonic_ns': start, 'end_monotonic_ns': end,
              'elapsed_ns': end - start, 'gap_elapsed_ns': end - gap_start_ns, 'limit_ns': 500000000}
    # Include work between the ACK boundary and the snapshot, so a slow ACK or
    # scheduling pause cannot consume the entire fixed repeat gap unnoticed.
    if timing['elapsed_ns'] > timing['limit_ns'] or timing['gap_elapsed_ns'] > timing['limit_ns']:
        raise MetricsBudgetExceeded(timing)
    return snapshot, timing


def prepare_payload(size, salt):
    data = bytearray(size)
    for offset in range(0, size, 65536):
        length = min(65536, size - offset)
        data[offset:offset + length] = ((offset // 65536 + salt).to_bytes(8, 'little') * 8192)[:length]
    return data, hashlib.sha256(data).hexdigest()


def expected_digest(size, salt):
    digest = hashlib.sha256()
    for offset in range(0, size, 65536):
        length = min(65536, size - offset)
        digest.update(((offset // 65536 + salt).to_bytes(8, 'little') * 8192)[:length])
    return digest.hexdigest()


def write_prepared(path, data, digest, write_block_bytes=BLOCK):
    if (isinstance(write_block_bytes, bool) or not isinstance(write_block_bytes, int)
            or not 0 < write_block_bytes <= BLOCK or write_block_bytes % 4096):
        raise ValueError('write block must be a positive 4096-aligned size at most 4MiB')
    path = Path(path)
    temporary = path.with_name(path.name + '.tmp')
    if path.exists() or temporary.exists():
        raise FileExistsError('refusing to overwrite an existing workload file')
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    start = time.monotonic_ns()
    calls = []
    syscalls = 0
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            open_end = time.monotonic_ns()
            write_start = time.monotonic_ns()
            for offset in range(0, len(data), write_block_bytes):
                pending = memoryview(data)[offset:offset + write_block_bytes]
                before = time.monotonic_ns()
                while pending:
                    count = os.write(fd, pending)
                    syscalls += 1
                    if count <= 0:
                        raise OSError('write made no progress')
                    pending = pending[count:]
                calls.append(time.monotonic_ns() - before)
            fsync_start = time.monotonic_ns()
            os.fsync(fd)
            fsync_end = time.monotonic_ns()
            fsync_ns = fsync_end - fsync_start
        finally:
            os.close(fd)
        os.rename(temporary, path)
        os.fsync(parent)
        end = time.monotonic_ns()
    finally:
        os.close(parent)
    return {'publish_ns': end - start, 'start_monotonic_ns': start, 'end_monotonic_ns': end,
            'open_end_monotonic_ns': open_end, 'write_start_monotonic_ns': write_start,
            'file_fsync_end_monotonic_ns': fsync_end,
            'write_and_fsync_ns': fsync_end - write_start,
            'write_and_fsync_boundary': 'before first write loop to successful file fsync return; excludes open, close, rename and directory fsync',
            'write_block_bytes': write_block_bytes, 'write_syscalls': syscalls,
            'file_fsync_ns': fsync_ns, 'write_call_ns': calls, 'bytes': len(data),
            'sha256': digest, 'file_fsync': True, 'parent_fsync': True,
            'publication': 'create/write/file fsync/close/rename/parent fsync'}


def read_file(path, direct, diagnostic=False):
    start = time.monotonic_ns()
    fd = os.open(path, os.O_RDONLY | (os.O_DIRECT if direct else 0))
    opened = time.monotonic_ns()
    digest = hashlib.sha256()
    count = 0
    calls = []
    intervals = [] if diagnostic else None
    interval_overflow = 0
    try:
        size = os.fstat(fd).st_size
        with mmap.mmap(-1, BLOCK) as buffer:
            while True:
                before = time.monotonic_ns()
                length = os.readv(fd, [buffer])
                completed = time.monotonic_ns()
                elapsed = completed - before
                if intervals is not None:
                    if len(intervals) < 128:
                        intervals.append({'start_ns': before, 'end_ns': completed, 'bytes': length})
                    else:
                        interval_overflow += 1
                if length == 0:
                    eof_ns = elapsed
                    break
                calls.append(elapsed)
                count += length
                digest.update(memoryview(buffer)[:length])
    finally:
        os.close(fd)
    end = time.monotonic_ns()
    result = {'full_read_ns': end - start, 'open_ns': opened - start,
            'read_io_ns': sum(calls), 'read_call_ns': calls, 'eof_ns': eof_ns,
            'bytes': count, 'stat_bytes': size, 'sha256': digest.hexdigest(), 'eof': True,
            'o_direct': direct, 'full_read_includes_application_sha256': True}
    if intervals is not None:
        result.update(start_monotonic_ns=start, end_monotonic_ns=end,
                      read_intervals=intervals, read_intervals_overflow=interval_overflow)
    return result


def verify_read(value, size, digest):
    if (value.get('bytes') != size or value.get('stat_bytes') != size
            or value.get('sha256') != digest or value.get('eof') is not True):
        raise RuntimeError('reader length, EOF or SHA256 verification failed')
    return value


def send_frame(connection, token, frame):
    payload = json.dumps(frame, separators=(',', ':'), sort_keys=True).encode()
    if len(payload) > MAX_FRAME:
        raise ValueError('control frame exceeds size bound')
    connection.sendall(struct.pack('!I', len(payload)) + hmac.digest(token, payload, 'sha256') + payload)


def receive_exact(connection, size):
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise EOFError('control channel closed before complete frame')
        result.extend(chunk)
    return bytes(result)


def receive_frame(connection, token, run_id, kind):
    size = struct.unpack('!I', receive_exact(connection, 4))[0]
    if size > MAX_FRAME:
        raise ValueError('received control frame exceeds size bound')
    signature = receive_exact(connection, 32)
    payload = receive_exact(connection, size)
    if not hmac.compare_digest(signature, hmac.digest(token, payload, 'sha256')):
        raise ValueError('control authentication failed')
    frame = json.loads(payload)
    if frame.get('run_id') != run_id or frame.get('type') != kind:
        raise ValueError('unexpected control run or phase')
    return frame


def wait_marker(path, seconds):
    deadline = time.monotonic() + seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('prearmed control marker timed out: ' + path.name)
        time.sleep(.01)


def role(config):
    root = Path(config['run_root'])
    token_path = root / 'token'
    if token_path.is_symlink() or token_path.stat().st_mode & 0o777 != 0o600:
        raise ValueError('control token must be private regular mode0600 file')
    token = bytes.fromhex(token_path.read_text())
    if len(token) != 32:
        raise ValueError('invalid control token length')
    run_id = config['run_id']
    common = {'run_id': run_id}
    if config['role'] == 'reader':
        with socket.socket() as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((config['control']['listen_ip'], config['control']['port']))
            server.listen(1)
            server.settimeout(config['timeout_seconds'])
            atomic_json(root / 'listening.json', {'listening': True})
            connection, address = server.accept()
            if address[0] != config['control']['expected_peer']:
                connection.close()
                raise ValueError('unexpected control peer address')
            with connection:
                connection.settimeout(config['timeout_seconds'])
                connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                hello = receive_frame(connection, token, run_id, 'HELLO')
                # Both sides independently prepare/verify dataset identity outside timing.
                expected = expected_digest(config['size_bytes'], config['salt'])
                if hello.get('sha256') != expected or hello.get('bytes') != config['size_bytes']:
                    raise ValueError('writer dataset differs')
                send_frame(connection, token, {**common, 'type': 'READY'})
                atomic_json(root / 'armed.json', {'armed': True})
                receive_frame(connection, token, run_id, 'BEGIN')
                before = metrics(config)
                send_frame(connection, token, {**common, 'type': 'BEGIN_READY'})
                receive_frame(connection, token, run_id, 'PUBLISHED')
                first = verify_read(read_file(config['path'], config['series'] == 'contract', config.get('diagnostic', False)),
                                    config['size_bytes'], expected)
                cold_end = time.monotonic_ns()
                send_frame(connection, token, {**common, 'type': 'FIRST_COMPLETE', 'read': first})
                # Snapshot is after the first ACK, outside the A first E2E boundary.
                between, between_timing = between_metrics(config, cold_end)
                deadline = cold_end + 1_000_000_000
                while time.monotonic_ns() < deadline:
                    time.sleep(max(0, (deadline - time.monotonic_ns()) / 1e9))
                repeat_start = time.monotonic_ns()
                repeat = verify_read(read_file(config['path'], config['series'] == 'contract', config.get('diagnostic', False)),
                                     config['size_bytes'], expected)
                gap_ns = repeat_start - cold_end
                send_frame(connection, token, {**common, 'type': 'COMPLETE', 'read': repeat,
                                               'repeat_gap_ns': gap_ns})
                after = metrics(config)
                return {'completed': True, 'first': first, 'repeat': repeat,
                        'repeat_gap_ns': gap_ns,
                        'metrics_sampling': {'after_first': between_timing},
                        'metrics': {'before': before, 'after_first': between, 'after_repeat': after},
                        'metrics_outside_application_read_timing': True,
                        'after_first_metrics_overlap_fixed_repeat_gap': True}
    data, digest = prepare_payload(config['size_bytes'], config['salt'])
    with socket.create_connection((config['control']['peer_ip'], config['control']['port']),
                                  timeout=config['timeout_seconds']) as connection:
        connection.settimeout(config['timeout_seconds'])
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        send_frame(connection, token, {**common, 'type': 'HELLO', 'sha256': digest, 'bytes': len(data)})
        receive_frame(connection, token, run_id, 'READY')
        atomic_json(root / 'armed.json', {'armed': True})
        wait_marker(root / 'GO', config['timeout_seconds'])
        before = metrics(config)
        send_frame(connection, token, {**common, 'type': 'BEGIN'})
        receive_frame(connection, token, run_id, 'BEGIN_READY')
        publish = write_prepared(config['path'], data, digest, config.get('write_block_bytes', BLOCK))
        start = publish['start_monotonic_ns']
        send_frame(connection, token, {**common, 'type': 'PUBLISHED'})
        first = receive_frame(connection, token, run_id, 'FIRST_COMPLETE')
        verify_read(first['read'], len(data), digest)
        first_end = time.monotonic_ns()
        # A-side counters are sampled after the primary E2E boundary and during
        # the fixed reader gap. Abort if sampling risks delaying final ACK receipt.
        between, between_timing = between_metrics(config, first_end)
        final = receive_frame(connection, token, run_id, 'COMPLETE')
        verify_read(final['read'], len(data), digest)
        if final.get('repeat_gap_ns', 0) < 1_000_000_000:
            raise ValueError('repeat gap below one second')
        end = time.monotonic_ns()
        after = metrics(config)
        return {'completed': True, 'publish': publish, 'first': first['read'], 'repeat': final['read'],
                'e2e_ns': first_end - start, 'total_with_repeat_ns': end - start,
                'repeat_gap_ns': final['repeat_gap_ns'],
                'metrics_sampling': {'after_first': between_timing},
                'metrics': {'before': before, 'after_first': between, 'after_repeat': after},
                'clock': 'A CLOCK_MONOTONIC',
                'start_monotonic_ns': start, 'first_end_monotonic_ns': first_end,
                'end_monotonic_ns': end, 'phase_sum_or_backend_subtraction': False,
                'timing_boundary': 'A create begin to verified B first complete SHA/EOF ACK',
                'total_boundary': 'A create begin to verified B repeat complete SHA/EOF ACK; includes 1s gap'}


def run_role(config):
    root = Path(config['run_root'])
    identity = process_identity(os.getpid())
    atomic_json(root / 'pid.json', identity)
    def interrupted(_signum, _frame):
        raise InterruptedError('owned workload stopped')
    signal.signal(signal.SIGTERM, interrupted)
    status = 1
    try:
        result = role(config)
        status = 0
    except Exception as error:
        result = {'completed': False, 'error_type': type(error).__name__, 'error': str(error)}
        if isinstance(error, MetricsBudgetExceeded):
            result['metrics_sampling'] = {'after_first': error.timing}
    atomic_json(root / 'result.json', result)
    atomic_json(root / 'wait.json', {**identity, 'exit_status': status})
    return status


def action(name, config):
    root = safe_path(config['run_root'])
    if name == 'preflight':
        return preflight(config)
    if name == 'install':
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        digest = hashlib.sha256(config['source'].encode()).hexdigest()
        # Deploy immutable helper source once per identity, not a new tool snapshot
        # for every measurement round. The per-run directory contains state only.
        source_dir = safe_path(str(root.parent / 'helpers' / digest))
        source_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        source = source_dir / 'remote_workload.py'
        if source.exists():
            if source.is_symlink() or hashlib.sha256(source.read_bytes()).hexdigest() != digest:
                raise RuntimeError('existing workload helper identity differs')
        else:
            with open(source, 'x', encoding='utf-8') as target:
                os.chmod(source, 0o600)
                target.write(config['source'])
        role_config = dict(config['role_config'], program=str(source))
        atomic_json(root / 'config.json', role_config)
        token = root / 'token'
        token.write_text(config['token'])
        os.chmod(token, 0o600)
        return {'installed': True, 'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
    if name == 'start':
        with open(root / 'worker.log', 'xb') as log:
            program = json.loads((root / 'config.json').read_text())['program']
            child = subprocess.Popen([sys.executable, program, 'role',
                                      str(root / 'config.json')], stdout=log, stderr=log,
                                     stdin=subprocess.DEVNULL, start_new_session=True)
        # The PID returned by Popen is retained immediately, including early startup failures.
        try:
            atomic_json(root / 'launcher.json', process_identity(child.pid))
        except FileNotFoundError:
            raise RuntimeError('owned workload exited before PID identity capture')
        return {'launched': True, 'pid': child.pid}
    if name == 'status':
        return {key: json.loads((root / (key + '.json')).read_text())
                for key in ['listening', 'armed', 'watching', 'result', 'wait'] if (root / (key + '.json')).exists()}
    if name == 'wait':
        # This action is started and acknowledged before the quiet window/GO.
        # It holds one existing control connection without creating Python
        # processes, HTTP scrapes or SSH sessions during measurement.
        if (root / 'wait.json').exists():
            atomic_json(root / 'watching.json', {'watching': True, 'already_finished': True})
            return action('status', config)
        saved = json.loads((root / 'pid.json').read_text())
        try:
            if not identity_matches(saved):
                if not (root / 'wait.json').exists():
                    raise RuntimeError('worker exited without final result')
                atomic_json(root / 'watching.json', {'watching': True, 'already_finished': True})
                return action('status', config)
            descriptor = os.pidfd_open(saved['pid'], 0)
        except ProcessLookupError:
            if not (root / 'wait.json').exists():
                raise RuntimeError('worker disappeared without final result')
            atomic_json(root / 'watching.json', {'watching': True, 'already_finished': True})
            return action('status', config)
        except (AttributeError, OSError) as error:
            raise RuntimeError('Linux pidfd wait unavailable; affected measurement cannot start') from error
        try:
            if not identity_matches(saved) and not (root / 'wait.json').exists():
                raise RuntimeError('worker identity changed while establishing pidfd wait')
            poller = select.poll()
            poller.register(descriptor, select.POLLIN)
            atomic_json(root / 'watching.json', {'watching': True, 'pidfd': True})
            if not poller.poll(config.get('timeout_seconds', 180) * 1000):
                raise TimeoutError('owned workload pidfd wait timed out')
        finally:
            os.close(descriptor)
        if not (root / 'wait.json').exists():
            raise RuntimeError('owned workload exited without final wait evidence')
        return action('status', config)
    if name == 'go':
        (root / 'GO').touch(exist_ok=False)
        return {'go': True, 'monotonic_ns': time.monotonic_ns()}
    if name == 'stop':
        identity_file = root / 'pid.json'
        if not identity_file.exists(): identity_file = root / 'launcher.json'
        saved = json.loads(identity_file.read_text()) if identity_file.exists() else None
        if saved and identity_matches(saved):
            # Verify executable/config identity as well as PID/start/boot identity.
            cmd = Path('/proc', str(saved['pid']), 'cmdline').read_bytes().split(b'\0')
            program = json.loads((root / 'config.json').read_text())['program']
            if os.fsencode(program) not in cmd or os.fsencode(root / 'config.json') not in cmd:
                raise RuntimeError('owned workload command differs; refusing to signal')
            os.kill(saved['pid'], signal.SIGTERM)
            deadline = time.monotonic() + 10
            while identity_matches(saved) and time.monotonic() < deadline: time.sleep(.05)
            if identity_matches(saved): raise RuntimeError('owned workload did not stop normally')
        (root / 'token').unlink(missing_ok=True)
        return {'stopped': True, 'token_removed': True}
    if name == 'collect':
        return {path.name: path.read_text() for path in root.iterdir()
                if path.name not in ['token', 'config.json', 'remote_workload.py']
                and path.is_file() and path.stat().st_size <= 32 * 1024 * 1024}
    raise ValueError('unknown worker operation')


if __name__ == '__main__':
    if sys.argv[1] == 'role':
        sys.exit(run_role(json.loads(Path(sys.argv[2]).read_text())))
    print(json.dumps(action(sys.argv[1], json.load(sys.stdin))))
