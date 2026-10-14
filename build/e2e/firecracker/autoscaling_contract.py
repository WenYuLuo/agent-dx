"""Required real-runtime evidence for Coordinator autoscaling operations."""

CASES = (
    'Coordinator node query filters and pagination',
    'central pause queues pinned create and preserves running execution',
    'local-first pause excludes node and resumes queued create',
    'manual resume waits for administrative unpause',
    'traffic wake waits for administrative unpause',
    'authoritative and physical cleanup',
)


def verify(result):
    cases = result.get('cases', [])
    if (result.get('status') != 'passed' or result.get('cleanup_errors') != []
            or result.get('inventories') != {'node1': 0, 'node2': 0}
            or [case.get('name') for case in cases] != list(CASES)
            or not all(case.get('passed') is True for case in cases)):
        raise ValueError('incomplete autoscaling E2E evidence')
    if not cases[0].get('cross_tenant') or cases[0].get('paged_count') != 3:
        raise ValueError('missing authoritative filtered pagination evidence')
    for case in cases[1:3]:
        if not case.get('queue_observed') or case.get('new_owner') != 'node1' or not case.get('running_preserved'):
            raise ValueError('missing paused-node placement evidence')
    if cases[2].get('local_claim_observed') is not True:
        raise ValueError('local-first path was not exercised')
    for case in cases[3:5]:
        if (case.get('blocked_seconds', 0) < 2 or case.get('resources_held_while_paused') is not False
                or case.get('physical_while_paused') != 0 or not case.get('guest_pid_preserved')
                or not case.get('runtime_before') or not case.get('runtime_after')
                or case['runtime_before'] == case['runtime_after']):
            raise ValueError('missing real paused-runtime wake admission evidence')
    if cases[5].get('remaining_count') != 0:
        raise ValueError('Environment ownership remains after cleanup')
