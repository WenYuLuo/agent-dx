"""Distinguish an expired Execd command from an ID that never existed."""

import json
import time
import uuid


def run(connection, image, output):
    from adx_environment import CommandExpired, CommandNotFound, Environment
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'command-expiry-' + uuid.uuid4().hex[:12]
    command_id = 'expired-' + uuid.uuid4().hex[:12]
    missing_id = 'never-seen-' + uuid.uuid4().hex[:12]
    environment = None
    deleted = False
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

        try:
            environment.commands.get(missing_id)
        except CommandNotFound as error:
            if not (not isinstance(error, CommandExpired)):
                raise AssertionError(error) from error
            if not (error.environment_id == environment.id and error.command_id == missing_id):
                raise AssertionError() from error
        else:
            raise AssertionError('unknown command did not return CommandNotFound')

        finished = environment.commands.run(
            'printf expired-command',
            background=True,
            command_id=command_id,
        ).wait(timeout=20)
        if not (finished.exit_code == 0 and finished.stdout == 'expired-command'):
            raise AssertionError(finished)
        # This case injects a one-second terminal-record TTL on node1 only.
        time.sleep(2)
        try:
            environment.commands.get(command_id)
        except CommandExpired as error:
            if not (error.environment_id == environment.id and error.command_id == command_id):
                raise AssertionError() from error
        else:
            raise AssertionError('expired command did not return CommandExpired')

        alive = environment.commands.run('printf runtime-alive')
        if not (alive.exit_code == 0 and alive.stdout == 'runtime-alive'):
            raise AssertionError(alive)
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
                'id': 'command.expired-result',
                'status': 'passed',
                'seconds': round(time.monotonic() - started, 3),
                'instance_id': environment.id,
                'expired_command_id': command_id,
                'missing_command_id': missing_id,
                'backend': backend[0],
            }
        )
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if environment is not None:
            if not deleted:
                try:
                    Environment.delete(environment.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
            environment.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
