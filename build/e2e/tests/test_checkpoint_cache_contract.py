import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'firecracker'))
from cache_contract import CASES, verify


def complete():
    cases = [{'name': name, 'passed': True} for name in CASES]
    cases[0].update(storage_blocked=True, pid_preserved=True)
    cases[1].update(max_entries=2, oldest_evicted=True, remote_preserved=True)
    cases[2].update(cache_full=True, pid_preserved=True)
    cases[3].update(budget_bytes=1, smallest_checkpoint=128 * 1024**2, simultaneous_execution_pins=2)
    cases[4].update(session_changed=True, pid_preserved=True, files_preserved=True)
    cases[5].update(session_changed=True, oldest_evicted=True, newest_preserved=True)
    cases[6].update(remaining_environments=0, local_files=0, s3_objects=0)
    return {'status': 'passed', 'cases': cases, 'cleanup_errors': [], 'inventories': {'node1': 0, 'node2': 0}}


class CheckpointCacheContractTests(unittest.TestCase):
    def test_complete_evidence_is_accepted(self):
        verify(complete())

    def test_missing_scenario_or_dirty_backend_is_rejected(self):
        for change in ('missing-case', 'runtime-leak', 'cleanup-error'):
            result = complete()
            if change == 'missing-case':
                result['cases'].pop()
            elif change == 'runtime-leak':
                result['inventories']['node1'] = 1
            else:
                result['cleanup_errors'] = ['stop failed']
            with self.subTest(change=change), self.assertRaises(ValueError):
                verify(result)

    def test_quota_and_restart_claims_require_physical_evidence(self):
        fields = [(0, 'storage_blocked'), (1, 'remote_preserved'), (2, 'cache_full'),
                  (3, 'simultaneous_execution_pins'), (4, 'files_preserved'), (5, 'newest_preserved')]
        for index, field in fields:
            result = copy.deepcopy(complete())
            result['cases'][index][field] = False
            with self.subTest(field=field), self.assertRaises(ValueError):
                verify(result)

    def test_local_or_object_storage_leaks_are_rejected(self):
        for field in ('remaining_environments', 'local_files', 's3_objects'):
            result = complete()
            result['cases'][6][field] = 1
            with self.subTest(field=field), self.assertRaises(ValueError):
                verify(result)


if __name__ == '__main__':
    unittest.main()
