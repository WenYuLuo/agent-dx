"""Public SDK checks for resource-observation expiry without node-session loss."""

import json
import time
import uuid

from adx_environment import Environment
from functional_lifecycle import _wait_deleted
from node import backend, labeled_backend, live_catalog as catalog, persisted_runtime_id


def create(connection, image, output):
    environment = Environment(
        image=image,
        runtime='runc',
        cpu=250,
        memory=256,
        idle_timeout=0,
        detached=True,
        node_id='node1',
        connection=connection,
        create_timeout=150,
    )
    try:
        result = environment.commands.run('printf resource-observer-live')
        if not (result.exit_code == 0 and result.stdout == 'resource-observer-live'):
            raise AssertionError()
        records = catalog()
        record = json.loads(records['environment:' + environment.id])
        node = json.loads(records['node:node1'])
        backends = labeled_backend(environment.id)
        if not (record['assignment']['node_id'] == 'node1' and len(backends) == 1):
            raise AssertionError()
        if not (record['result']['state'] == 'Running' and node['node']['available']):
            raise AssertionError()
        output.write_text(
            json.dumps(
                {
                    'instance_id': environment.id,
                    'session_id': node['session']['id'],
                    'runtime_id': persisted_runtime_id(record['result']),
                    'backend': backends[0],
                }
            )
            + '\n'
        )
    except Exception:
        Environment.delete(environment.id, connection=connection)
        raise
    finally:
        environment.close()


def accepted(connection, image, evidence):
    live = json.loads((evidence / 'resource-live.json').read_text())
    before = catalog()
    held = {
        key
        for key, value in before.items()
        if key.startswith('environment:') and (json.loads(value).get('result') or {}).get('resources_held')
    }
    backends = backend()
    started = time.monotonic()
    created = Environment(
        name='stale-accepted-' + uuid.uuid4().hex,
        image=image,
        runtime='runc',
        node_id='node1',
        cpu=250,
        memory=256,
        idle_timeout=0,
        create_timeout=40,
        schedule_timeout=10,
        connection=connection,
    )
    try:
        command = created.commands.run('printf stale-capacity-usable')
        if command.exit_code != 0 or command.stdout != 'stale-capacity-usable':
            raise AssertionError('new runtime was not executable while the observer was stopped')
        after = catalog()
        held_after = {
            key
            for key, value in after.items()
            if key.startswith('environment:') and (json.loads(value).get('result') or {}).get('resources_held')
        }
        if held_after != held | {'environment:' + created.id}:
            raise AssertionError('existing allocations changed during stale-capacity creation')
        if labeled_backend(live['instance_id']) != [live['backend']] or len(backend()) != len(backends) + 1:
            raise AssertionError('existing backend changed or duplicate runtime was created')
        result = {
            'instance_id': created.id,
            'node_id': 'node1',
            'held_before': len(held),
            'held_during': len(held_after),
            'backend': live['backend'],
            'seconds': round(time.monotonic() - started, 3),
        }
    finally:
        try:
            Environment.delete(created.id, connection=connection)
            _wait_deleted(created.id, connection, timeout=60)
            if backend() != backends:
                raise AssertionError('stale-capacity test allocation was not physically released')
        finally:
            created.close()
    (evidence / 'resource-accepted.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def verify(connection, image, evidence, output):
    live = json.loads((evidence / 'resource-live.json').read_text())
    stale = json.loads((evidence / 'resource-stale.json').read_text())
    fresh = json.loads((evidence / 'resource-fresh.json').read_text())
    accepted_result = json.loads((evidence / 'resource-accepted.json').read_text())
    created = None
    deleted = set()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    try:
        retained = Environment.from_id(live['instance_id'], connection=connection)
        try:
            command = retained.commands.run('printf resource-observer-retained')
            if not (command.exit_code == 0 and command.stdout == 'resource-observer-retained'):
                raise AssertionError()
        finally:
            retained.close()

        created = Environment(
            image=image,
            runtime='runc',
            cpu=250,
            memory=256,
            node_id='node1',
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        record = json.loads(catalog()['environment:' + created.id])
        if not (record['assignment']['node_id'] == 'node1'):
            raise AssertionError()
        command = created.commands.run('printf resource-observer-reopened')
        if not (command.exit_code == 0 and command.stdout == 'resource-observer-reopened'):
            raise AssertionError()

        for sid in (created.id, live['instance_id']):
            Environment.delete(sid, connection=connection)
            deleted.add(sid)
            _wait_deleted(sid, connection, timeout=60)
            if not ('environment:' + sid not in catalog()):
                raise AssertionError()
            if not (not labeled_backend(sid)):
                raise AssertionError()
        report['status'] = 'passed'
        report['cases'] = [
            {
                'id': 'reliability.resource-stale-admission',
                'status': 'passed',
                'seconds': stale['seconds'] + accepted_result['seconds'],
            },
            {'id': 'reliability.resource-observation-recovery', 'status': 'passed', 'seconds': fresh['seconds']},
        ]
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if created is not None:
            created.close()
        for sid in (live['instance_id'], created.id if created else None):
            if sid and sid not in deleted:
                try:
                    Environment.delete(sid, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
