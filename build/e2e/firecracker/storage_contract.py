"""Validate physical evidence and summarize matched storage benchmark samples."""

import math
import statistics

CASES = ('durable capture and direct filesystem path',
         'concurrent traffic wake preserves guest state',
         'node failure retains execution inputs across Coordinator restart',
         'Coordinator reclaims after source reconciliation',
         'anonymous checkpoint follows Environment',
         'Coordinator reclaims stopped incomplete capture')


def source_invalidated(record, source):
    # Recovery can reserve the replacement and clear invalidated between polls.
    return record['invalidated'] or record['assignment']['node_id'] != source


def summarize(samples, repetitions):
    groups = {}
    for row in samples:
        if (row['request_count'] != 12 or row['restore_count'] != 1
                or row['continuity'] is not True or row['artifact_bytes'] <= 0):
            raise ValueError('missing successful physical traffic evidence')
        groups.setdefault(row['mode'] + '/' + row['cache'], []).append(row)
    if not groups or repetitions < 3:
        raise ValueError('at least three repetitions are required')
    modes = {row['mode'] for row in samples}
    if len(modes) != 1 or set(groups) != {next(iter(modes)) + '/' + cache for cache in ('cold', 'warm')}:
        raise ValueError('one backend with complete cold and warm groups is required')
    result = {}
    for key, rows in groups.items():
        if len(rows) != repetitions:
            raise ValueError('incomplete benchmark group: ' + key)
        result[key] = {'samples': len(rows)}
        for field in ('pause_seconds', 'first_http_seconds', 'touch_seconds', 'artifact_bytes'):
            values = [r[field] for r in rows]
            if not all(math.isfinite(v) and v >= 0 for v in values):
                raise ValueError('invalid timing or byte measurement')
            result[key][field] = {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
    return result


def verify(result):
    if (result.get('status') != 'passed' or result.get('cleanup_errors') != []
            or result.get('inventories') != {'node1': 0, 'node2': 0}
            or [c.get('name') for c in result.get('cases', [])] != list(CASES)
            or any(c.get('passed') is not True for c in result['cases'])):
        raise ValueError('incomplete storage Firecracker lifecycle evidence')
    if result.get('diagnostic_only'):
        trace = result.get('restore_trace', {})
        if (result.get('storage', {}).get('kind') != 'afs' or result['repetitions'] != 1
                or len(result['samples']) != 2 or trace.get('metadata_cow_confirmed') is not True
                or trace.get('full_source_read_before_first_touch') is not False
                or trace.get('late_touch_remote_read_bytes', 0) <= 0
                or any(s['request_count'] != 12 or s['restore_count'] != 1 or not s['continuity'] for s in result['samples'])):
            raise ValueError('incomplete restore diagnostic evidence')
    else:
        summarize(result['samples'], result['repetitions'])
