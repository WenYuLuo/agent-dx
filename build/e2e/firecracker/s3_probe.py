#!/usr/bin/env python3
import json
import pathlib
import sys
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import quote

from s3_client import Client

run = pathlib.Path(sys.argv[1])
artifact = sys.argv[sys.argv.index('--artifact') + 1] if '--artifact' in sys.argv else None
if artifact:
    uuid.UUID(artifact)
creating = '--create' in sys.argv
client = Client(run)
config = client.config
path = '/' + config['bucket'] + ('/' + config['prefix'] + '/' + artifact + '/manifest.json' if artifact else '')
query = 'list-type=2&prefix=' + quote(config['prefix'] + '/', safe='')


def multipart_inventory():
    return client.request('GET', '/' + config['bucket'], query='prefix=' + quote(config['prefix'] + '/', safe='') + '&uploads=')


if '--cleanup' in sys.argv:
    if config['kind'] != 'real_obs' or not config['prefix'].endswith('/' + run.name):
        raise ValueError('cleanup requires a real OBS run-scoped prefix')
    deleted, aborted = [], []
    multipart = ET.fromstring(multipart_inventory())
    for upload in multipart.findall('{*}Upload'):
        key, identity = upload.findtext('{*}Key'), upload.findtext('{*}UploadId')
        if not key.startswith(config['prefix'] + '/'):
            raise ValueError('multipart upload outside run prefix')
        client.request('DELETE', '/' + config['bucket'] + '/' + quote(key, safe='/'), query='uploadId=' + quote(identity, safe=''))
        aborted.append(key)
    while True:
        listing = ET.fromstring(client.request('GET', '/' + config['bucket'], query=query))
        objects = listing.findall('{*}Contents')
        if not objects:
            break
        for item in objects:
            key = item.findtext('{*}Key')
            if not key.startswith(config['prefix'] + '/'):
                raise ValueError('object outside run prefix')
            client.request('DELETE', '/' + config['bucket'] + '/' + quote(key, safe='/'))
            deleted.append(key)
    if ET.fromstring(multipart_inventory()).findall('{*}Upload'):
        raise ValueError('OBS multipart uploads remain after cleanup')
    (run / 'evidence/obs-fixture-cleanup.json').write_text(json.dumps({'deleted': deleted, 'aborted': aborted, 'empty': True}, indent=2))
    print('OBS FIXTURE CLEANUP', len(deleted), 'objects', len(aborted), 'multipart uploads', flush=True)
    sys.exit(0)
if creating and config['kind'] == 'real_obs':
    existing = client.request('GET', path, query=query)
    if ET.fromstring(existing).findall('{*}Contents'):
        raise ValueError('OBS run prefix must be empty before deployment')
    if ET.fromstring(multipart_inventory()).findall('{*}Upload'):
        raise ValueError('OBS run prefix has unfinished multipart uploads')
    probe = path + '/' + config['prefix'] + '/access-probe'
    client.request('PUT', probe, b'ADX OBS acceptance probe')
    if client.request('GET', probe) != b'ADX OBS acceptance probe':
        raise ValueError('OBS preflight readback mismatch')
    client.request('DELETE', probe)
    (run / 'evidence/obs-preflight.json').write_text(json.dumps({'passed': True, 'storage': config, 'request_id': client.last_request_id}))
    print('REAL OBS WRITE/READ/DELETE PREFLIGHT PASSED', flush=True)
    sys.exit(0)
raw = client.request('PUT' if creating else 'GET', path, query='' if creating or artifact else query)
if creating:
    print('S3 BUCKET CREATED', flush=True)
    sys.exit(0)
if artifact:
    manifest = json.loads(raw)
    if not (manifest['version'] == 1 and manifest['size'] > 0 and manifest['files']):
        raise AssertionError()
    (pathlib.Path(sys.argv[1]) / 'evidence/s3-paused-manifest.json').write_bytes(raw)
    print('S3 PAUSED MANIFEST VERIFIED', artifact, flush=True)
    sys.exit(0)
root = ET.fromstring(raw)
objects = root.findall('{*}Contents')
if not (not objects):
    raise AssertionError(raw)
(pathlib.Path(sys.argv[1]) / 'evidence/s3-final.xml').write_bytes(raw)
if config['kind'] == 'real_obs':
    multipart = multipart_inventory()
    if ET.fromstring(multipart).findall('{*}Upload'):
        raise AssertionError('OBS multipart upload inventory is not empty')
    (run / 'evidence/s3-final-multipart.xml').write_bytes(multipart)
print('S3 OBJECT INVENTORY EMPTY', flush=True)
