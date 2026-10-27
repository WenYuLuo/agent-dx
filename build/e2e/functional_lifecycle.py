#!/usr/bin/env python3
"""Installed SDK acceptance for ordinary instance lifecycle behavior."""

import json
import logging
import subprocess
import sys
import time
import uuid
from pathlib import Path

from adx_environment import DirectoryVolumeSource, Environment, EnvironmentNotFound, VolumeSpec


def _wait_deleted(instance_id, connection, timeout=90):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            handle = Environment.from_id(instance_id, connection=connection)
        except EnvironmentNotFound:
            return
        except RuntimeError as error:
            if 'is not running' in str(error):
                return
            last_error = error
        except Exception as error:
            last_error = error
        else:
            handle.close()
        time.sleep(1)
    raise TimeoutError(f"instance {instance_id} was not reclaimed: {last_error}")


def _volume_catalog(connection):
    """Round-trip managed definitions; no filesystem mount is implied."""
    volume_id = 'e2e-volume-' + uuid.uuid4().hex
    spec = VolumeSpec(
        id=volume_id,
        source=DirectoryVolumeSource(storage_domain='e2e-volumes', path='workspaces/e2e/' + volume_id),
    )
    created = False
    try:
        volume = Environment.ensure_volume(spec, connection=connection)
        created = True
        replay = Environment.ensure_volume(spec, connection=connection)
        fetched = Environment.get_volume(volume_id, connection=connection)
        if not (volume.uid == replay.uid == fetched.uid and fetched.spec == spec):
            raise AssertionError('volume ensure/get did not retain identity and definition')
        def listed():
            token = None
            for _ in range(1000):
                page, token = Environment.list_volumes(page_size=1, page_token=token, connection=connection)
                if any(item.uid == volume.uid for item in page):
                    return True
                if not token:
                    return False
            raise AssertionError('volume catalog pagination did not converge')

        if not listed():
            raise AssertionError('new volume missing from paginated catalog')
        Environment.delete_volume(volume_id, connection=connection)
        created = False
        if listed():
            raise AssertionError('deleted volume remains in catalog')
        return volume.uid
    finally:
        if created:
            Environment.delete_volume(volume_id, connection=connection)


def run(connection, image, output):
    checks = {}
    cases = []
    remaining = set()

    def begin(case_id):
        print(f'[SDK CASE RUN] {case_id}', flush=True)
        return time.monotonic()

    def passed(case_id, started):
        seconds = round(time.monotonic() - started, 3)
        cases.append({'id': case_id, 'status': 'passed', 'seconds': seconds})
        print(f'[SDK CASE PASS] {case_id} ({seconds:.3f}s)', flush=True)

    try:
        started = begin('volume.catalog-crud-and-replay')
        checks['managed_volume_uid'] = _volume_catalog(connection)
        passed('volume.catalog-crud-and-replay', started)

        started = begin('lifecycle.detached-close-reattach-delete')
        detached = Environment(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        )
        detached_id = detached.id
        remaining.add(detached_id)
        detached.kill()
        attached = Environment.from_id(detached_id, connection=connection)
        try:
            result = attached.commands.run("printf 'reattached'")
            if not (result.exit_code == 0 and result.stdout == 'reattached'):
                raise AssertionError()
        finally:
            attached.close()
        Environment.delete(detached_id, connection=connection)
        remaining.remove(detached_id)
        _wait_deleted(detached_id, connection)
        checks['detached_reattach_and_delete'] = detached_id
        passed('lifecycle.detached-close-reattach-delete', started)

        started = begin('lifecycle.named-create-reuses-running-owner')
        name = f'e2e-named-{uuid.uuid4().hex[:12]}'
        first = Environment(
            name=name,
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        named_id = first.id
        remaining.add(named_id)
        first.close()
        reopened = Environment(
            name=name,
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        try:
            if not (reopened.id == named_id):
                raise AssertionError()
            if not (reopened.commands.run("printf 'same-owner'").stdout == 'same-owner'):
                raise AssertionError()
        finally:
            reopened.close()
        Environment.delete(named_id, connection=connection)
        remaining.remove(named_id)
        _wait_deleted(named_id, connection)
        passed('lifecycle.named-create-reuses-running-owner', started)

        started = begin('lifecycle.attached-close-preserves-instance')
        attached = Environment(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        )
        attached_id = attached.id
        remaining.add(attached_id)
        attached.close()
        reopened = Environment.from_id(attached_id, connection=connection)
        try:
            result = reopened.commands.run("printf 'close-preserved'")
            if not (result.exit_code == 0 and result.stdout == 'close-preserved'):
                raise AssertionError()
        finally:
            reopened.close()
        # A handle obtained through from_id is detached from ownership by
        # design. Delete explicitly after proving close preserved the remote.
        Environment.delete(attached_id, connection=connection)
        remaining.remove(attached_id)
        _wait_deleted(attached_id, connection)
        passed('lifecycle.attached-close-preserves-instance', started)

        started = begin('lifecycle.context-manager-deletes')
        context_id = ''
        with Environment(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        ) as context_environment:
            context_id = context_environment.id
            remaining.add(context_id)
            if not (context_environment.is_running()):
                raise AssertionError()
        remaining.remove(context_id)
        _wait_deleted(context_id, connection)
        passed('lifecycle.context-manager-deletes', started)

        started = begin('lifecycle.idle-timeout-reclaims')
        idle = Environment(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=6,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        )
        idle_id = idle.id
        remaining.add(idle_id)
        if not (idle.commands.run("printf 'idle-start'").stdout == 'idle-start'):
            raise AssertionError()
        idle.close()
        _wait_deleted(idle_id, connection)
        remaining.remove(idle_id)
        checks['idle_timeout_reclamation'] = idle_id
        passed('lifecycle.idle-timeout-reclaims', started)

        started = begin('lifecycle.idle-with-background-command-after-client-exit')
        child_evidence = Path(output).parent / 'idle-background-client.json'
        subprocess.run(
            [sys.executable, '/opt/adx/e2e/idle_background_client.py', image, str(child_evidence)],
            check=True,
            timeout=180,
        )
        child = json.loads(child_evidence.read_text())
        background_id = child['instance_id']
        remaining.add(background_id)
        if not (child['command_running_before_exit']):
            raise AssertionError()
        # The separate SDK process has exited, but its 120-second command has
        # not. The 90-second wait proves idle cleanup does not wait for it.
        _wait_deleted(background_id, connection, timeout=90)
        remaining.remove(background_id)
        from node import catalog

        if not ('environment:' + background_id not in catalog()):
            raise AssertionError()
        checks['idle_background_client_exit'] = background_id
        passed('lifecycle.idle-with-background-command-after-client-exit', started)

        result = {'status': 'passed', 'checks': checks, 'cases': cases}
        Path(output).write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)
    finally:
        for instance_id in remaining:
            try:
                Environment.delete(instance_id, connection=connection)
            except Exception:
                logging.getLogger(__name__).debug("Best-effort operation failed", exc_info=True)
