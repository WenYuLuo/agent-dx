"""Installed SDK probes through real API, Adxlet, Execd and sandboxd."""

import json
import os
import time
from pathlib import Path

from adx_environment import HealthcheckProbe, ReadinessProbe, RestartPolicy, Environment, EnvironmentError
from node import catalog, labeled_backend, persisted_runtime_id
from route_ready import wait_for_route


def record(instance_id):
    value = catalog().get('environment:' + instance_id)
    return json.loads(value) if value else None


def wait(predicate, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise TimeoutError('workload probe did not converge')


def terminal(instance_id):
    observed = record(instance_id)
    if not observed:
        return None
    result = observed.get('result')
    return result if result and result['state'] == 'Failed' and not result['resources_held'] else None


def run(connection, image, output):
    instances = []
    failed_instances = []
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    stream_enabled = os.environ.get('ADX_E2E_RUNTIME_CONTROL') == '1'
    report['transport'] = 'control-stream' if stream_enabled else 'http'
    report['stream_registrations'] = []

    def registered(instance_id):
        marker = '[execd-control] registered environment=' + instance_id + ' '
        for path in Path('/opt/adx/logs/runtime').glob(instance_id + '-*.out'):
            with path.open('rb') as stream:
                stream.seek(max(0, path.stat().st_size - 65536))
                for line in stream.read().decode(errors='replace').splitlines():
                    if marker in line:
                        return {'runtime_id': path.stem, 'registration': line}
        return None

    def create(**options):
        defaults = dict(
            image=image, inherit_entrypoint=True, runtime='runc', node_id='node1',
            cpu=500, memory=512, idle_timeout=0, init_timeout=20,
            create_timeout=150, connection=connection,
        )
        instance = Environment(**{**defaults, **options})
        instances.append(instance)
        wait_for_route(instance)
        if stream_enabled:
            report['stream_registrations'].append(wait(lambda: registered(instance.id), timeout=10))
        return instance

    def passed(case, started, **evidence):
        report['cases'].append(dict(
            id='probes.' + case, status='passed',
            seconds=round(time.monotonic() - started, 3), **evidence,
        ))

    def delete(instance):
        instance.kill()
        wait(lambda: record(instance.id) is None and not labeled_backend(instance.id))
        instance.close()
        instances.remove(instance)

    try:
        started = time.monotonic()
        ready = create(
            env={'PROBE_READY_DELAY': '4'},
            readiness_probe=ReadinessProbe.http_get(port=8080, path='/ready', period_seconds=1),
        )
        elapsed = time.monotonic() - started
        command = ready.commands.run('test -f /tmp/adx-probe-ready && printf ready')
        if elapsed < 4 or command.exit_code != 0 or command.stdout != 'ready':
            raise AssertionError(('creation returned before readiness', elapsed, command))
        if record(ready.id)['result']['state'] != 'Running':
            raise AssertionError('readiness did not publish Running')
        passed('http-readiness-gates-create', started, instance_id=ready.id, create_seconds=elapsed)
        delete(ready)

        started = time.monotonic()
        tcp = create(readiness_probe=ReadinessProbe.tcp_socket(port=8080, period_seconds=1))
        if tcp.commands.run('printf tcp-ready').stdout != 'tcp-ready':
            raise AssertionError('TCP-ready environment is not executable')
        passed('tcp-readiness', started, instance_id=tcp.id)
        delete(tcp)

        started = time.monotonic()
        execute = create(
            env={'PROBE_VALUE': 'inherited'},
            readiness_probe=ReadinessProbe.exec(command=[
                '/bin/sh', '-c', 'test "$PROBE_VALUE" = inherited && test "$(pwd)" = /workload',
            ]),
        )
        passed('exec-inherits-workload-context', started, instance_id=execute.id)
        delete(execute)

        started = time.monotonic()
        try:
            unexpected = create(
                init_timeout=8,
                readiness_probe=ReadinessProbe.exec(
                    command=['/bin/sh', '-c', 'printf not-ready >&2; exit 7'], period_seconds=1,
                ),
            )
        except EnvironmentError as error:
            failure = error.probe_failure
            if not failure or failure['kind'] != 'readiness_timeout':
                raise AssertionError(('missing readiness failure', vars(error))) from error
            if error.retry != 'never' or error.outcome != 'terminal' or not error.environment_id:
                raise AssertionError(('nonterminal readiness result', vars(error))) from error
            failed_id = error.environment_id
            failed_instances.append(failed_id)
            failed = wait(lambda: terminal(failed_id))
            wait(lambda: not labeled_backend(failed_id))
            if failed['failure']['probe_failure']['kind'] != 'readiness_timeout':
                raise AssertionError('persisted readiness failure missing')
            passed('readiness-timeout-cleanup', started, instance_id=failed_id, failure=failure)
            Environment.delete(failed_id, connection=connection)
            wait(lambda: record(failed_id) is None and not labeled_backend(failed_id))
            failed_instances.remove(failed_id)
        else:
            raise AssertionError('failing readiness returned a environment: ' + unexpected.id)

        started = time.monotonic()
        health = create(
            readiness_probe=ReadinessProbe.http_get(port=8080, path='/ready', period_seconds=1),
            healthcheck_probe=HealthcheckProbe.http_get(
                port=8080, path='/health', period_seconds=1, failure_threshold=3,
            ),
        )
        if health.commands.run('touch /tmp/adx-probe-unhealthy').exit_code != 0:
            raise AssertionError('health fault injection failed')
        failed = wait(lambda: terminal(health.id))
        failure = failed['failure']['probe_failure']
        if failure['kind'] != 'healthcheck_failed' or failure['consecutive_failures'] != 3 or failure['http_status'] != 503:
            raise AssertionError(failure)
        if failed['restart_pending']:
            raise AssertionError('health failure restarted without a policy')
        wait(lambda: not labeled_backend(health.id))
        if health.get_info().failure.probe_failure != failure:
            raise AssertionError('SDK health failure differs from persisted result')
        passed('health-threshold-cleanup', started, instance_id=health.id, failure=failure)
        delete(health)

        started = time.monotonic()
        restart = create(
            readiness_probe=ReadinessProbe.http_get(port=8080, path='/ready', period_seconds=1),
            healthcheck_probe=HealthcheckProbe.http_get(
                port=8080, path='/health', period_seconds=1, failure_threshold=2,
            ),
            restart_policy=RestartPolicy(max_attempts=1, initial_backoff_seconds=2, max_backoff_seconds=2),
        )
        old = persisted_runtime_id(record(restart.id)['result'])
        old_backend = labeled_backend(restart.id)
        if restart.commands.run('touch /tmp/adx-probe-unhealthy').exit_code != 0:
            raise AssertionError('restart health fault injection failed')

        def restarted():
            observed = record(restart.id)['result']
            if observed['state'] == 'Running' and observed['restart_attempts'] == 1 and persisted_runtime_id(observed) != old:
                return observed
            return None

        new = wait(restarted)
        if new.get('failure') is not None:
            raise AssertionError('restart retained the previous probe failure')
        backends = labeled_backend(restart.id)
        if len(backends) != 1 or backends == old_backend:
            raise AssertionError(('backend not replaced', old_backend, backends))
        wait_for_route(restart)
        command = restart.commands.run('test ! -e /tmp/adx-probe-unhealthy && printf restarted')
        if command.exit_code != 0 or command.stdout != 'restarted':
            raise AssertionError('restarted workload is not fresh and executable')
        time.sleep(3)
        if record(restart.id)['result']['state'] != 'Running':
            raise AssertionError('healthy restart failed again')
        passed('health-bounded-cold-restart', started, instance_id=restart.id, old_runtime=old, new_runtime=persisted_runtime_id(new))
        delete(restart)

        started = time.monotonic()
        idle = create(
            idle_timeout=6,
            healthcheck_probe=HealthcheckProbe.http_get(port=8080, path='/health', period_seconds=1),
        )
        wait(lambda: record(idle.id) is None and not labeled_backend(idle.id), timeout=45)
        passed('internal-health-does-not-extend-idle', started, instance_id=idle.id)
        idle.close()
        instances.remove(idle)
        report['status'] = 'passed'
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        for instance_id in failed_instances:
            try:
                Environment.delete(instance_id, connection=connection)
            except Exception as error:
                report['cleanup_errors'].append(f'{instance_id}: {error}')
        for instance in instances:
            try:
                instance.kill()
            except Exception as error:
                report['cleanup_errors'].append(f'{instance.id}: {error}')
            finally:
                instance.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
        if report['cleanup_errors'] and 'error' not in report:
            raise RuntimeError('probe acceptance cleanup failed')
