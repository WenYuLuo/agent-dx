"""Private external OBS configuration and public, run-scoped storage evidence."""

import json
import re
from pathlib import Path
from urllib.parse import urlsplit


def external(root, config_path):
    path = Path(config_path)
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError('OBS configuration must be a private regular file')
    value = json.loads(path.read_text())
    endpoint = urlsplit(value['endpoint'])
    if endpoint.scheme != 'https' or endpoint.username or endpoint.password or endpoint.path not in ('', '/'):
        raise ValueError('OBS requires an HTTPS endpoint without credentials or a path')
    if not endpoint.hostname or not endpoint.hostname.endswith('.myhuaweicloud.com'):
        raise ValueError('real OBS endpoint required')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', value['bucket']):
        raise ValueError('invalid OBS bucket')
    prefix = value['prefix'].strip('/')
    if not prefix or any(part in ('', '.', '..') for part in prefix.split('/')):
        raise ValueError('dedicated OBS prefix required')
    root = Path(root)
    for name, field in [('s3-user', 'access_key'), ('s3-key', 'secret_key')]:
        if not value[field]:
            raise ValueError('OBS credential missing')
        destination = root / 'secrets' / name
        destination.write_text(value[field])
        destination.chmod(0o600)
    public = {
        'kind': 'real_obs', 'alias': 'obs', 'bucket': value['bucket'],
        'region': value['region'],
        'endpoint': 'https://' + value['bucket'] + '.' + endpoint.netloc,
        'virtual_hosted': True,
        'prefix': prefix + '/' + root.name,
    }
    (root / 'evidence/storage.json').write_text(json.dumps(public, indent=2))
    return public


def public(root):
    path = Path(root) / 'evidence/storage.json'
    if path.exists():
        return json.loads(path.read_text())
    return {
        'kind': 'minio', 'alias': 'shared', 'bucket': 'checkpoints',
        'region': 'us-east-1', 'endpoint': 'http://127.0.0.1:19090', 'prefix': 'adx',
    }
