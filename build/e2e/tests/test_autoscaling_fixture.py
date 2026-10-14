import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'firecracker'))
from autoscaling_contract import CASES, verify


class AutoscalingEvidenceTests(unittest.TestCase):
    def evidence(self):
        cases = [{'name': name, 'passed': True} for name in CASES]
        cases[0].update(cross_tenant=True, paged_count=3)
        for case in cases[1:3]:
            case.update(queue_observed=True, new_owner='node1', running_preserved=True, local_claim_observed=True)
        for case in cases[3:5]:
            case.update(blocked_seconds=3, resources_held_while_paused=False, physical_while_paused=0,
                        guest_pid_preserved=True, runtime_before='old', runtime_after='restored')
        cases[5]['remaining_count'] = 0
        return {'status': 'passed', 'cases': cases, 'cleanup_errors': [], 'inventories': {'node1': 0, 'node2': 0}}

    def test_complete_real_runtime_evidence_is_required(self):
        verify(self.evidence())
        for bad in ({'cases': self.evidence()['cases'][:-1]}, {'cleanup_errors': ['runtime leak']},
                    {'inventories': {'node1': 1, 'node2': 0}}):
            with self.assertRaises(ValueError):
                verify({**self.evidence(), **bad})

    def test_restoring_without_a_pause_barrier_cannot_pass(self):
        for index, field, value in [(2, 'local_claim_observed', False), (3, 'blocked_seconds', 0),
                                    (3, 'resources_held_while_paused', True), (4, 'physical_while_paused', 1),
                                    (4, 'runtime_after', 'old'), (4, 'guest_pid_preserved', False)]:
            bad = copy.deepcopy(self.evidence())
            bad['cases'][index][field] = value
            with self.assertRaises(ValueError):
                verify(bad)
