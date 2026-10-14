"""Reject a missing recovery capability before any real command is started."""

import json
import time
import uuid
from pathlib import PurePosixPath

from command_response_cut import CommandResponseCutProxy


def run(connection, image, output, secrets):
    from adx_environment import ConnectionConfig, Environment, UnsupportedFeature
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'unsupported-' + uuid.uuid4().hex[:12]
    command_id = 'unsupported-' + uuid.uuid4().hex[:12]
    marker = str(PurePosixPath('/tmp') / f'{command_id}.marker')
    environment = None
    attached = None
    recovered = None
    deleted = False
    proxy = CommandResponseCutProxy(
        'https://127.0.0.1:8443',
        certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key',
        ca=secrets / 'tls/ca.pem',
        cut_start=False,
        strip_watch_capability=True,
    )
    try:
        environment = Environment(
            name=name,
            image=image,
            runtime='runc',
            node_id='node1',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        initial = json.loads(catalog()['environment:' + environment.id])
        backend = labeled_backend(environment.id)
        if not (len(backend) == 1):
            raise AssertionError(backend)

        with proxy:
            proxied = ConnectionConfig(
                server_address=f'127.0.0.1:{proxy.port}',
                token=connection.token,
                use_tls=True,
                verify_tls=True,
            )
            attached = Environment.from_id(environment.id, connection=proxied)
            try:
                attached.commands.run(
                    f'printf x > {marker}',
                    background=True,
                    command_id=command_id,
                )
            except UnsupportedFeature as error:
                if not (error.environment_id == environment.id):
                    raise AssertionError(error) from error
                if not (error.command_id == command_id):
                    raise AssertionError(error) from error
            else:
                raise AssertionError('SDK started a command without the required capability')
            if not (len(proxy.capability_attempts) == 1):
                raise AssertionError(proxy.capability_attempts)
            if 'multiplexed-command-watch' not in proxy.capability_attempts[0]['capabilities']:
                raise AssertionError()
            if not (proxy.start_attempts == []):
                raise AssertionError(proxy.start_attempts)
            attached.close()
            attached = None

        recovered = Environment.from_id(environment.id, connection=connection)
        handle = recovered.commands.run(
            f'printf x > {marker}; printf capability-ok',
            background=True,
            command_id=command_id,
        )
        result = handle.wait(timeout=20)
        if not (result.exit_code == 0 and result.stdout == 'capability-ok'):
            raise AssertionError(result)
        marker_result = recovered.commands.run(f'cat {marker}')
        if not (marker_result.exit_code == 0 and marker_result.stdout == 'x'):
            raise AssertionError(marker_result)
        recovered.commands.run(f'rm {marker}')
        recovered.close()
        recovered = None

        final = json.loads(catalog()['environment:' + environment.id])
        if not (final['assignment']['generation'] == initial['assignment']['generation']):
            raise AssertionError()
        if not (labeled_backend(environment.id) == backend):
            raise AssertionError()
        Environment.delete(environment.id, connection=connection)
        deleted = True
        _wait_deleted(environment.id, connection, timeout=60)
        if not ('environment:' + environment.id not in catalog()):
            raise AssertionError()
        if not (not labeled_backend(environment.id)):
            raise AssertionError()

        report['status'] = 'passed'
        report['cases'].append(
            {
                'id': 'command.unsupported-feature',
                'status': 'passed',
                'seconds': round(time.monotonic() - started, 3),
                'instance_id': environment.id,
                'command_id': command_id,
                'rejected_start_attempts': len(proxy.start_attempts),
                'backend': backend[0],
            }
        )
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if attached is not None:
            attached.close()
        if recovered is not None:
            recovered.close()
        if environment is not None:
            if not deleted:
                try:
                    Environment.delete(environment.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
            environment.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        report['capability_attempts'] = proxy.capability_attempts
        report['start_attempts'] = proxy.start_attempts
        output.write_text(json.dumps(report, indent=2) + '\n')
