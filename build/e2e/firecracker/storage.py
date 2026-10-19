#!/usr/bin/env python3
"""Run one real storage backend on two isolated FC executors, preserving evidence."""

import json
import os
import shutil
from pathlib import Path
import subprocess
import sys

from filesystem_fixture import FilesystemFixture


def main():
    root = Path(sys.argv[1]).resolve()
    base = Path(os.environ.get('ADX_FC_BASE', '/opt/adx'))
    env = {**os.environ, 'ADX_FC_SUITE': 'storage'}
    fixture = None
    errors = []
    status = 1
    try:
        if env.get('ADX_FC_AFS_ROOT'):
            fixture = FilesystemFixture(Path(env['ADX_FC_SOURCE']), base / 'package/bin',
                                        Path(env['ADX_FC_AFS_ROOT']), root.parent / (root.name + '-afs-evidence'),
                                        trace_cow=env.get('ADX_STORAGE_TRACE_RESTORE') == '1')
            fixture.start()
            fixture.metrics('start')
            if env.get('ADX_STORAGE_GC') == '1':
                fixture.verify_gc()
            env['ADX_FC_AFS_METRICS_PORT'] = str(fixture.metrics_port)
            env['ADX_FC_AFS_META_PORT'] = str(fixture.meta_port)
        status = subprocess.run([sys.executable, str(base / 'e2e/firecracker/transfer.py'), str(root)], env=env).returncode
    finally:
        if fixture:
            try:
                fixture.metrics('final')
            except Exception as error:
                errors.append('final metrics: ' + repr(error))
            finally:
                errors.extend(fixture.stop())
        if root.exists():
            cleanup = json.dumps({'exit_code': status, 'errors': errors}, indent=2)
            (root / 'storage-cleanup.json').write_text(cleanup)
            if (root / 'evidence').exists():
                (root / 'evidence/storage-cleanup.json').write_text(cleanup)
                if fixture:
                    shutil.copytree(fixture.evidence, root / 'evidence/afs', dirs_exist_ok=True)
                subprocess.run([sys.executable, str(base / 'e2e/firecracker/collect.py'), str(root)], check=True)
    return status if not errors else 1


if __name__ == '__main__':
    sys.exit(main())
