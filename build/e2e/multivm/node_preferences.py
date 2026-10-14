#!/usr/bin/env python3
"""Verify node preferences and affinity on two actual worker VMs."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path

if __package__:
    from .contract import verify_inventory
    from .placement_policy import comparable_nodes, configured_policy
    from .sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
else:
    from contract import verify_inventory
    from placement_policy import comparable_nodes, configured_policy
    from sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines


def condition(kind, affinity, key, value, **flags):
    return {
        'kind': kind,
        'affinity': affinity,
        'labelOps': [{'type': 0, 'labelKey': key, 'labelValues': [value]}],
        **flags,
    }


def run_preferences(
    inventory, connection, image, socket, output, environment_factory=None, resource_reader=None, remote=ssh
):
    verify_inventory(inventory)
    if environment_factory is None or resource_reader is None:
        from adx_environment import Environment, resources

        environment_factory = environment_factory or Environment
        resource_reader = resource_reader or resources
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    workers = (machines['worker-1'], machines['worker-2'])
    node_ids = tuple(worker['node_id'] for worker in workers)
    report = {
        'status': 'failed',
        'profile': 'multi-vm-node-preferences',
        'cases': [],
        'instances': [],
        'cleanup_errors': [],
    }
    handles = []
    output.mkdir(parents=True, exist_ok=True)
    prefix = 'mv-pref-' + uuid.uuid4().hex[:12]

    def create(name, **options):
        environment = environment_factory(
            name=prefix + '-' + name,
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

    def verify(name, expected, **options):
        environment = create(name, **options)
        assignment = persisted_assignment(control, environment.id, remote)
        actual = assignment.get('node_id')
        if actual != expected or assignment.get('state') != 'Running' or not assignment.get('resources_held'):
            raise AssertionError(f'{name}: expected {expected}, got {assignment}')
        physical = {worker['node_id']: backend_ids(worker, socket, environment.id, remote) for worker in workers}
        if len(physical[expected]) != 1 or any(ids for node_id, ids in physical.items() if node_id != expected):
            raise AssertionError(f'{name}: physical backend mismatch: {physical}')
        result = environment.commands.run('printf preference-ok')
        if (result.exit_code, result.stdout) != (0, 'preference-ok'):
            raise AssertionError(f'{name}: command failed')
        report['cases'].append(
            {
                'name': name,
                'id': environment.id,
                'expected_node': expected,
                'actual_node': actual,
                'backend_id': physical[expected][0],
            }
        )
        environment.kill()

    try:
        report['machines'] = verify_machines(inventory, remote)
        report['placement'] = configured_policy(control, remote)
        nodes = {node.id: node for node in resource_reader(connection=connection)}
        report['preflight'] = comparable_nodes(nodes, node_ids)
        left = create('anchor-left', node_id=node_ids[0], labels={'peer': 'left'})
        right = create('anchor-right', node_id=node_ids[1], labels={'peer': 'right'})
        for environment, expected in ((left, node_ids[0]), (right, node_ids[1])):
            actual = persisted_assignment(control, environment.id, remote)
            if actual.get('node_id') != expected or actual.get('state') != 'Running':
                raise AssertionError(f'anchor not assigned to {expected}: {actual}')
            physical = {worker['node_id']: backend_ids(worker, socket, environment.id, remote) for worker in workers}
            if len(physical[expected]) != 1 or any(ids for node_id, ids in physical.items() if node_id != expected):
                raise AssertionError(f'anchor backend not unique on {expected}: {physical}')

        verify(
            'weighted node preference',
            node_ids[1],
            schedule_affinities=[
                condition(0, 0, 'NODE_ID', node_ids[0], weight=1),
                condition(0, 0, 'NODE_ID', node_ids[1], weight=9),
            ],
        )
        verify(
            'ordered node preference',
            node_ids[1],
            schedule_affinities=[
                condition(0, 0, 'NODE_ID', node_ids[1], preferredPriority=True),
                condition(0, 0, 'NODE_ID', node_ids[0], preferredPriority=True),
            ],
        )
        verify(
            'environment affinity OR',
            node_ids[0],
            schedule_affinities=[
                condition(1, 2, 'peer', 'missing'),
                condition(1, 2, 'peer', 'left'),
            ],
        )
        verify(
            'environment anti-affinity',
            node_ids[1],
            schedule_affinities=[
                condition(1, 3, 'peer', 'left'),
            ],
        )
        verify(
            'explicit node and OR constraints',
            node_ids[1],
            node_id=node_ids[1],
            schedule_affinities=[condition(0, 2, 'NODE_ID', node_ids[0]), condition(0, 2, 'NODE_ID', node_ids[1])],
        )
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        for environment in reversed(handles):
            try:
                environment.kill()
            except Exception as error:
                report['cleanup_errors'].append(f'{environment.id} delete: {error}')
            finally:
                try:
                    environment.close()
                except Exception as error:
                    report['cleanup_errors'].append(f'{environment.id} close: {error}')
        try:
            deadline = time.monotonic() + 15
            while handles:
                residual = {
                    environment.id: {
                        'assignment': persisted_assignment(control, environment.id, remote),
                        'backends': {
                            worker['node_id']: backend_ids(worker, socket, environment.id, remote) for worker in workers
                        },
                    }
                    for environment in handles
                }
                if all(
                    item['assignment']['state'] == 'Deleted'
                    and not item['assignment']['resources_held']
                    and not any(item['backends'].values())
                    for item in residual.values()
                ):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(f'node preference cleanup incomplete: {residual}')
                time.sleep(0.2)
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'node-preferences-result.json').write_text(json.dumps(report, indent=2) + '\n')
        if report['cleanup_errors'] and not report.get('error'):
            raise RuntimeError('; '.join(report['cleanup_errors']))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--token-file', required=True, type=Path)
    parser.add_argument('--ca', required=True, type=Path)
    parser.add_argument('--image', required=True)
    parser.add_argument('--socket', default='/run/sandboxd/sandboxd.sock')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    from adx_environment import ConnectionConfig

    connection = ConnectionConfig(
        server_address=args.endpoint, token=args.token_file.read_text().strip(), use_tls=True, verify_tls=True
    )
    report = run_preferences(json.loads(args.inventory.read_text()), connection, args.image, args.socket, args.output)
    print(json.dumps({'status': report['status'], 'cases': len(report['cases'])}), flush=True)


if __name__ == '__main__':
    main()
