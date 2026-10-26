"""Installed SDK placement acceptance; Redis is read only for observed assignments."""

import json
import uuid

from adx_environment import Environment, EnvironmentError
from node import catalog, live_catalog
from runtime_inventory import require_runc_only_inventory, require_unassigned_create


def run(connection, image, output):
    report = {'status': 'failed', 'cases': [], 'instances': [], 'cleanup_errors': []}
    handles = []

    def condition(kind, mode, key, value, **flags):
        return {
            'kind': kind,
            'affinity': mode,
            'labelOps': [{'type': 0, 'labelKey': key, 'labelValues': [value]}],
            **flags,
        }

    def create(**options):
        environment = Environment(
            image=image,
            runtime='runc',
            cpu=250,
            memory=256,
            idle_timeout=0,
            connection=connection,
            create_timeout=120,
            **options,
        )
        handles.append(environment)
        report['instances'].append(environment.id)
        return environment

    def assigned(environment):
        record = json.loads(catalog()['environment:' + environment.id])
        if not (record['result']['state'] == 'Running'):
            raise AssertionError()
        return record['assignment']['node_id']

    def verify(name, expected, **options):
        print('[PLACEMENT RUN]', name, flush=True)
        environment = create(**options)
        actual = assigned(environment)
        if not (actual == expected):
            raise AssertionError((name, expected, actual))
        command = environment.commands.run('printf placement-ready')
        if not (command.exit_code == 0 and command.stdout == 'placement-ready'):
            raise AssertionError()
        environment.kill()
        report['cases'].append(
            {
                'name': name,
                'expected_node': expected,
                'actual_node': actual,
                'instance_id': environment.id,
                'passed': True,
            }
        )
        print('[PLACEMENT PASS]', name, 'node=' + actual, flush=True)

    try:
        inventories = require_runc_only_inventory(live_catalog())
        report['cases'].append({'name': 'sandboxd runtime inventory', 'runtime_classes': inventories, 'passed': True})
        print('[PLACEMENT PASS] sandboxd runtime inventory ' + json.dumps(inventories, sort_keys=True), flush=True)
        left = create(node_id='node1', labels={'peer': 'left'})
        right = create(node_id='node2', labels={'peer': 'right'})
        if not ((assigned(left), assigned(right)) == ('node1', 'node2')):
            raise AssertionError()
        verify(
            'environment affinity OR',
            'node1',
            schedule_affinities=[condition(1, 2, 'peer', 'missing'), condition(1, 2, 'peer', 'left')],
        )
        verify('instance anti-affinity', 'node2', schedule_affinities=[condition(1, 3, 'peer', 'left')])
        # Equal anchor reservations keep the resource score equal across the two nodes.
        verify(
            'weighted node preference',
            'node2',
            schedule_affinities=[
                condition(0, 0, 'NODE_ID', 'node1', weight=1),
                condition(0, 0, 'NODE_ID', 'node2', weight=9),
            ],
        )
        verify(
            'ordered node preference',
            'node2',
            schedule_affinities=[
                condition(0, 0, 'NODE_ID', 'node2', preferredPriority=True),
                condition(0, 0, 'NODE_ID', 'node1', preferredPriority=True),
            ],
        )
        verify(
            'node_id constrains every OR branch',
            'node2',
            node_id='node2',
            schedule_affinities=[condition(0, 2, 'NODE_ID', 'node1'), condition(0, 2, 'NODE_ID', 'node2')],
        )
        guard = create(node_id='node1', schedule_affinities=[condition(1, 3, 'future', 'yes')])
        if not (assigned(guard) == 'node1'):
            raise AssertionError()
        # Prefer node1 to prove that the existing peer's required anti-affinity wins.
        verify(
            'reverse instance anti-affinity',
            'node2',
            labels={'future': 'yes'},
            schedule_affinities=[condition(0, 0, 'NODE_ID', 'node1', weight=1000)],
        )
        rejected_name = 'runtime-rejected-' + uuid.uuid4().hex[:12]
        rejected_id = 'default-' + rejected_name
        print('[PLACEMENT RUN] unavailable runtime stays unassigned', flush=True)
        try:
            unexpected = Environment(
                name=rejected_name,
                image=image,
                runtime='runsc',
                node_id='node1',
                cpu=250,
                memory=256,
                idle_timeout=0,
                connection=connection,
                schedule_timeout=3,
                create_timeout=40,
            )
        except EnvironmentError as error:
            if 'central scheduling queue deadline exceeded' not in str(error):
                raise AssertionError(str(error)) from error
        else:
            handles.append(unexpected)
            report['instances'].append(unexpected.id)
            raise AssertionError('runsc was assigned to a runc-only node')
        require_unassigned_create(catalog(), rejected_id)
        report['cases'].append(
            {'name': 'unavailable runtime stays unassigned', 'runtime': 'runsc', 'node_id': 'node1', 'passed': True}
        )
        print('[PLACEMENT PASS] unavailable runtime stays unassigned', flush=True)
        report['status'] = 'passed'
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        for environment in reversed(handles):
            try:
                environment.kill()
            except Exception as error:
                report['cleanup_errors'].append(str(error))
            finally:
                environment.close()
        try:
            records = catalog()
            for sid in report['instances']:
                if not ('environment:' + sid not in records):
                    raise AssertionError(sid)
            report['terminal_resources_released'] = True
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
        if report['cleanup_errors'] and 'error' not in report:
            raise RuntimeError('placement cleanup failed')
