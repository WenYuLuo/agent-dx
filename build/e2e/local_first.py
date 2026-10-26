"""Real SDK -> Ingress -> local-first API -> Node -> sandboxd/EXECD acceptance."""

import gzip
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from adx_environment import Environment
from node import catalog


def run(connection, image, output):
    instances = []
    prefix = 'lf-' + uuid.uuid4().hex[:10]

    def create(name, **extra):
        return Environment(
            name=name,
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            connection=connection,
            create_timeout=150,
            **extra,
        )

    def record(instance):
        return json.loads(catalog()['environment:' + instance.id])

    try:
        first = create(prefix + '-a')
        instances.append(first)
        second = create(prefix + '-b')
        instances.append(second)
        owners = [record(s)['assignment']['node_id'] for s in instances]
        if not (owners == ['node1', 'node2']):
            raise AssertionError(owners)
        # Duplicate SDK objects use independent HTTP requests but the same name.
        with ThreadPoolExecutor(max_workers=2) as pool:
            copies = list(pool.map(lambda _: create(prefix + '-race'), range(2)))
        instances.extend(copies)
        if not (copies[0].id == copies[1].id):
            raise AssertionError()
        for instance in (first, second, copies[0]):
            result = instance.commands.run("printf 'local-first-ready'")
            if not (result.exit_code == 0 and result.stdout == 'local-first-ready'):
                raise AssertionError()
        before = record(copies[0])
        changed = Environment(
            name=prefix + '-race',
            image=image,
            runtime='runc',
            cpu=600,
            memory=512,
            idle_timeout=0,
            connection=connection,
            create_timeout=150,
        )
        instances.append(changed)
        after = record(changed)
        if changed.id != copies[0].id or after != before:
            raise AssertionError('same Environment ID did not reuse the confirmed result and original spec')
        records = [record(s) for s in (first, second, copies[0])]
        if not (len({r['spec']['id'] for r in records}) == 3):
            raise AssertionError()
        if not (all(r['result']['resources_held'] for r in records)):
            raise AssertionError()
        # Require evidence of the actual local reservation path, not just placement
        # that could also have resulted from the default center Spread policy.
        expected = {r['spec']['id'] for r in records}
        end = time.monotonic() + 10
        while True:
            claimed = set()
            for path in Path('/tmp/adx-e2e/state/logs').glob('coordinator*.log*'):
                if path.name.endswith('.tmp'):
                    continue
                try:
                    data = gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()
                    for line in data.decode(errors='replace').splitlines():
                        if 'local_environment_reservation' in line:
                            claimed.update(identity for identity in expected if identity in line)
                except (EOFError, OSError):
                    continue
            if claimed == expected:
                break
            if time.monotonic() > end:
                raise AssertionError(('missing local reservation evidence', expected - claimed))
            time.sleep(0.1)
        output.write_text(
            json.dumps(
                {
                    'status': 'passed',
                    'local_reservations': sorted(claimed),
                    'entry_owners': owners,
                    'unique_instances': 3,
                    'duplicate_converged': True,
                    'changed_spec_reused': True,
                    'commands_passed': 3,
                    'assignments': [r['assignment'] for r in records],
                },
                indent=2,
            )
        )
        print(
            (
                'PASS local-first: two-node rotation, same-ID convergence, '
                'original-spec reuse and three real EXECD commands'
            ),
            flush=True,
        )
    finally:
        removed = set()
        for instance in instances:
            try:
                if instance.id not in removed:
                    instance.kill()
                    removed.add(instance.id)
            finally:
                instance.close()
