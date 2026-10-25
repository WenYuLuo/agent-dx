"""A recovered backend inventory alone does not establish node admission readiness."""

import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SPEC = importlib.util.spec_from_file_location('restart_node', Path(__file__).resolve().parents[1] / 'node.py')
node = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(node)


class SandboxdRestartReadinessTests(unittest.TestCase):
    def run_restart(self, health, reports, *, expires=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid = root / 'sandboxd.pid'
            pid.write_text('1')
            ticks = iter([0, 66] if expires else range(20))
            with (
                mock.patch.object(node, 'P', root),
                mock.patch.object(node, 'E', root),
                mock.patch.object(node.sys, 'argv', ['node.py', 'restart-sandboxd', 'node1']),
                mock.patch.object(node.os, 'kill', side_effect=lambda *_: pid.write_text('2')),
                mock.patch.object(node, 'backend', return_value=['backend-1']),
                mock.patch.object(node, 'command', side_effect=health) as check,
                mock.patch.object(node, 'nodes', side_effect=reports),
                mock.patch.object(node.time, 'monotonic', side_effect=lambda: next(ticks)),
                mock.patch.object(node.time, 'sleep') as sleep,
            ):
                if expires:
                    with self.assertRaises(TimeoutError):
                        node.main()
                    self.assertFalse((root / 'sandboxd-restart-node1.json').exists())
                else:
                    node.main()
                    result = json.loads((root / 'sandboxd-restart-node1.json').read_text())
                    self.assertEqual(result['backend_ids_after'], ['backend-1'])
                    self.assertTrue(result['backend_serving'])
                    self.assertTrue(result['node_admission_ready'])
                return check.call_count, sleep.call_count

    @staticmethod
    def report(available):
        return [{'node': {'id': 'node1', 'available': available, 'runtime_classes': ['runc']}}]

    def test_waits_for_serving_and_coordinator_admission(self):
        calls, sleeps = self.run_restart(
            ['Healthz status: NOT_SERVING', 'Healthz status: SERVING', 'Healthz status: SERVING'],
            [self.report(False), self.report(True)],
        )
        self.assertEqual((calls, sleeps), (3, 2))

    def test_unhealthy_backend_cannot_pass_with_preserved_ids(self):
        self.run_restart(['Healthz status: NOT_SERVING'], [], expires=True)

    def test_health_transport_failure_is_retried(self):
        calls, sleeps = self.run_restart(
            [subprocess.CalledProcessError(1, 'sbox'), 'Healthz status: SERVING'],
            [self.report(True)],
        )
        self.assertEqual((calls, sleeps), (2, 1))


if __name__ == '__main__':
    unittest.main()
