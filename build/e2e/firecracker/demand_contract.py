"""Fail closed on missing real OBS, real traffic, or fault boundary evidence."""

CASES = (
    'in-flight HTTP prevents idle pause',
    'traffic before capture aborts pause',
    'traffic after capture waits for OBS publication and resumes',
    'concurrent traffic wakes one execution on original node',
    'node failure preserves dormancy and traffic restores on peer',
    'Coordinator restart during restore preserves wake intent',
    'adxlet restart removes uncommitted restore',
    'expired checkpoint rejects traffic without execution',
    'restore admitted before TTL completes after TTL',
    'committed resume response lost and retry reuses execution',
    'permanent pause survives restarts and traffic restores across nodes',
    'healthy owner rejects capacity before concurrent traffic restores on peer',
)


def verify(result):
    cases = result.get('cases', [])
    if (result.get('status') != 'passed' or result.get('cleanup_errors') != []
            or result.get('inventories') != {'node1': 0, 'node2': 0}
            or result.get('storage', {}).get('kind') != 'real_obs'
            or result.get('obs_empty') is not True
            or [c.get('name') for c in cases] != list(CASES)
            or any(c.get('passed') is not True for c in cases)):
        raise ValueError('incomplete real OBS Firecracker demand evidence')
    by_name = {c['name']: c for c in cases}
    before = by_name[CASES[1]]
    if before.get('same_backend') is not True or before.get('checkpoint_created') is not False:
        raise ValueError('missing before-capture abort evidence')
    after = by_name[CASES[2]]
    if after.get('captured_before_request') is not True or after.get('request_waited') is not True:
        raise ValueError('missing after-capture traffic evidence')
    restart = by_name[CASES[6]]
    if restart.get('old_backend_removed') is not True or restart.get('state_at_crash') != 'Paused':
        raise ValueError('missing uncommitted backend cleanup evidence')
    ttl = by_name[CASES[8]]
    if not (ttl['admitted_at'] < ttl['expires_at'] < ttl['completed_at']):
        raise ValueError('restore did not cross the real TTL boundary')
    cut = by_name[CASES[9]]
    if cut.get('same_execution') is not True or cut.get('attempts', 0) < 2 or cut.get('commit_before_cut') is not True:
        raise ValueError('missing committed response-loss retry evidence')
    permanent = by_name[CASES[10]]
    if (permanent.get('automatic_expiry', 0) is not None or permanent.get('manual_expiry', 0) is not None
            or permanent.get('dormant_after_coordinator_restart') is not True
            or permanent.get('traffic_after_manual_pause') is not True or permanent.get('continuity') is not True
            or permanent.get('target') != 'node2' or permanent.get('epoch_after', 0) <= permanent.get('epoch_before', 0)
            or not permanent.get('session_after') or permanent.get('session_after') == permanent.get('session_before')):
        raise ValueError('missing permanent retention restart and traffic evidence')
    capacity = by_name[CASES[11]]
    if (capacity.get('source_healthy') is not True or capacity.get('local_rejected') is not True
            or capacity.get('local_error') != 'insufficient resources or node admission closed'
            or capacity.get('source_cpu_capacity', 0) <= 0
            or capacity.get('source_cpu_reserved') != capacity.get('source_cpu_capacity')
            or capacity.get('source') != 'node1' or capacity.get('target') != 'node2'
            or capacity.get('generation_after', 0) <= capacity.get('generation_before', 0)
            or capacity.get('wake_operation_preserved') is not True
            or capacity.get('request_count') != 12 or capacity.get('unique_nonces') != 12
            or capacity.get('source_restore_count') != 0 or capacity.get('peer_restore_count') != 1):
        raise ValueError('missing healthy-owner capacity transfer or single restore evidence')
    if not (capacity['local_attempt_at'] <= capacity['local_rejected_at']
            < capacity['peer_release_at'] < capacity['transferred_at']):
        raise ValueError('transfer preceded definitive owner rejection')
