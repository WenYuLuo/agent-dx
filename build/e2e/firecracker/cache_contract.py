"""Required public-SDK, real-Firecracker checkpoint-cache evidence."""

CASES = (
    'local Pause cache resumes while object storage is unreachable',
    'entry LRU evicts oldest local copy and preserves remote artifact',
    'evicted checkpoint restores while reusable cache is full',
    'oversized checkpoints restore while older execution files stay pinned',
    'node restart reconstructs live execution pins above cache quota',
    'node restart restores LRU order and applies a lower entry limit',
    'authoritative and physical cleanup',
)


def verify(result):
    cases = result.get('cases', [])
    if (result.get('status') != 'passed' or result.get('cleanup_errors') != []
            or result.get('inventories') != {'node1': 0, 'node2': 0}):
        raise ValueError('incomplete checkpoint-cache E2E evidence')
    if ([case.get('name') for case in cases] != list(CASES)
            or not all(case.get('passed') is True for case in cases)):
        raise ValueError('incomplete checkpoint-cache E2E evidence')
    if not cases[0].get('storage_blocked') or not cases[0].get('pid_preserved'):
        raise ValueError('warm resume must preserve the process with S3 blocked')
    if cases[1].get('max_entries') != 2 or not cases[1].get('oldest_evicted') or not cases[1].get('remote_preserved'):
        raise ValueError('missing independent entry-limit eviction evidence')
    if not cases[2].get('cache_full') or not cases[2].get('pid_preserved'):
        raise ValueError('cache quota must not block cold restore')
    if (cases[3].get('budget_bytes') != 1 or cases[3].get('smallest_checkpoint', 0) <= 1
            or cases[3].get('simultaneous_execution_pins', 0) < 2):
        raise ValueError('missing oversized/all-pinned restoration evidence')
    if (not cases[4].get('session_changed') or not cases[4].get('pid_preserved')
            or not cases[4].get('files_preserved')):
        raise ValueError('live restore files must survive node reconciliation')
    if (not cases[5].get('session_changed') or not cases[5].get('oldest_evicted')
            or not cases[5].get('newest_preserved')):
        raise ValueError('missing restart LRU reconstruction evidence')
    if (cases[6].get('remaining_environments') != 0 or cases[6].get('local_files') != 0
            or cases[6].get('s3_objects') != 0):
        raise ValueError('checkpoint cache or object inventory remains after cleanup')
