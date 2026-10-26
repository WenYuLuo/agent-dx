"""Concurrent public identities and durable replay across process restart."""

import base64
import json
import logging
import ssl
import time
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

from adx_environment import Environment
from node import catalog


def create(endpoint, token, ca, request_id, name, image, cpu=250):
    request = urllib.request.Request(
        urllib.parse.urlunsplit(('https', endpoint, '/api/environment/v1/environments', '', '')),
        data=json.dumps(
            {
                'name': name,
                'image': image,
                'runtime': 'runc',
                'cpu': cpu,
                'memory': 256,
                'idleTimeoutSeconds': 0,
                'createTimeoutSeconds': 120,
            }
        ).encode(),
        headers={
            'Authorization': 'Bearer ' + token,
            'Content-Type': 'application/json',
            'X-Request-Id': request_id,
        },
        method='POST',
    )
    with urllib.request.urlopen(request, context=ssl.create_default_context(cafile=str(ca)), timeout=150) as response:
        body = json.load(response)
    result = json.loads(base64.b64decode(body['data'], validate=True))
    if result['status'] != 'running':
        raise AssertionError(body)
    return result['environmentId']


def run(connection, image, evidence, secrets, phase):
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    saved = evidence / 'create-identities.json'
    ca = secrets / 'tls/ca.pem'
    endpoint = 'coordinator:8443'
    if phase == 'seed':
        prefix = 'identities-' + uuid.uuid4().hex[:12]
        request_id = str(uuid.uuid4())
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = [
                pool.submit(
                    create,
                    endpoint,
                    connection.token,
                    ca,
                    request_id,
                    prefix + '-' + str(i),
                    image,
                    250 + i,
                )
                for i in range(12)
            ]
            ids = [future.result() for future in futures]
        if len(set(ids)) != 1:
            raise AssertionError(('same request produced multiple environments', ids))
        identity = ids[0]
        records = catalog()
        environments = {key for key in records if key.startswith('environment:')}
        if environments != {'environment:' + identity}:
            raise AssertionError(('unconfirmed candidates persisted', environments))
        winner = json.loads(records['environment:' + identity])
        # A new request with the winning name must reuse its original specification.
        name = identity.removeprefix('default-')
        alias = str(uuid.uuid4())
        if create(endpoint, connection.token, ca, alias, name, image, 999) != identity:
            raise AssertionError('same environment did not reuse the winner')
        if json.loads(catalog()['environment:' + identity]) != winner:
            raise AssertionError('successful replay overwrote the original result')
        saved.write_text(
            json.dumps(
                {
                    'instance_id': identity,
                    'name': name,
                    'request_id': request_id,
                    'alias': alias,
                    'winner': winner,
                    'concurrent_requests': 12,
                    'seconds': time.monotonic() - started,
                },
                indent=2,
            )
            + '\n'
        )
        (evidence / 'live-instances.json').write_text(json.dumps([identity]))
    else:
        original = json.loads(saved.read_text())
        identity = original['instance_id']
        try:
            for request_id in (original['request_id'], original['alias']):
                if (
                    create(
                        endpoint,
                        connection.token,
                        ca,
                        request_id,
                        'restart-candidate-' + uuid.uuid4().hex,
                        image,
                        888,
                    )
                    != identity
                ):
                    raise AssertionError('durable request receipt was lost on restart')
            after = json.loads(catalog()['environment:' + identity])
            for key in ('spec', 'assignment'):
                if after[key] != original['winner'][key]:
                    raise AssertionError(('winner changed on restart', key))
            if after['result']['runtime'] != original['winner']['result']['runtime']:
                raise AssertionError('runtime changed on replay after restart')
            environment = Environment.from_id(identity, connection=connection)
            try:
                result = environment.commands.run('printf durable-identities')
                if result.exit_code != 0 or result.stdout != 'durable-identities':
                    raise AssertionError('retained runtime did not execute')
            finally:
                environment.close()
            original['status'] = 'passed'
            original['restart_replay'] = True
            saved.write_text(json.dumps(original, indent=2) + '\n')
        finally:
            Environment.delete(identity, connection=connection)
    logging.info('PASS create identities %s', phase)
