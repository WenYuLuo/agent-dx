import ast
import copy
import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace

DRIVER_ROOT = Path(__file__).resolve().parents[1] / 'firecracker'
SPEC = importlib.util.spec_from_file_location('checkpoint_cache_contract', DRIVER_ROOT / 'cache_contract.py')
CONTRACT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTRACT)
CASES, verify = CONTRACT.CASES, CONTRACT.verify


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

    def test_invalid_overall_evidence_is_rejected(self):
        for change in ('failed-status', 'case-order', 'failed-case'):
            result = complete()
            if change == 'failed-status':
                result['status'] = 'failed'
            elif change == 'case-order':
                result['cases'].reverse()
            else:
                result['cases'][0]['passed'] = False
            with self.subTest(change=change), self.assertRaises(ValueError):
                verify(result)

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


class OptimizedCacheDriverTests(unittest.TestCase):
    def command(self, response):
        source = DRIVER_ROOT / 'sdk_cache.py'
        tree = ast.parse(source.read_text())
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'command')
        module = ast.Module(body=[function], type_ignores=[])
        namespace = {}
        exec(compile(module, str(source), 'exec', optimize=2), namespace)
        environment = SimpleNamespace(commands=SimpleNamespace(run=lambda *args, **kwargs: response))
        return namespace['command'](environment, 'test command')

    def test_failed_sdk_command_is_rejected_with_optimization(self):
        response = SimpleNamespace(exit_code=7, stdout='', stderr='execution failed')
        with self.assertRaises(AssertionError):
            self.command(response)

    def test_successful_sdk_command_returns_output_with_optimization(self):
        response = SimpleNamespace(exit_code=0, stdout='  preserved value\n', stderr='')
        self.assertEqual(self.command(response), 'preserved value')


if __name__ == '__main__':
    unittest.main()
