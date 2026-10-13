"""Correlate actual Firecracker restores with ADX-labelled backend identities."""

import re

from demand_contract import CASES


def audit(evidence, result):
    cases = result.get('cases', [])
    if len(cases) != len(CASES):
        raise ValueError('physical audit requires the complete demand suite')
    identities, restores = {}, []
    for node in ('node1', 'node2'):
        for line in (evidence / f'{node}-sandboxd-service.log').read_text().splitlines():
            created = re.search(r'Type:create SandboxID:(sbox-[\w-]+)', line)
            if created:
                for case in cases:
                    if re.search(r'(?<![\w-])' + re.escape(case['instance_id']) + r'(?![\w-])', line):
                        identities[(node, created[1])] = case['instance_id']
            restored = re.search(r'firecracker: restored sandbox (sbox-[\w-]+) pid=(\d+)', line)
            if restored:
                restores.append({'node': node, 'backend': restored[1], 'vmm_pid': int(restored[2])})
    if any((r['node'], r['backend']) not in identities for r in restores):
        raise ValueError('restore has no matching demand Environment identity')
    proof = []
    for index, case in enumerate(cases):
        observed = [r for r in restores if identities.get((r['node'], r['backend'])) == case['instance_id']]
        expected = 0 if index in (0, 1, 7) else 1
        if index == 10:
            expected = 2
        if index in (5, 6):
            expected = 2 if case['old_backend_removed'] else 1
        if len(observed) != expected:
            raise ValueError(f'{case["name"]}: expected {expected} physical restores, observed {observed}')
        if index == 11:
            source = sum(r['node'] == 'node1' for r in observed)
            peer = sum(r['node'] == 'node2' for r in observed)
            if source != 0 or peer != 1:
                raise ValueError('capacity transfer must restore once on peer and never on owner')
            case.update(source_restore_count=source, peer_restore_count=peer)
        proof.append({'case': case['name'], 'instance_id': case['instance_id'],
                      'physical_restore_count': len(observed), 'restores': observed})
    return proof
