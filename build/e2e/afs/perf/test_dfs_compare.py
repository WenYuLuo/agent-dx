"""Driver contracts; these tests do not start services or VMs."""
import importlib.util
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('dfs_compare', HERE / 'dfs_compare.py')
driver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(driver)


def fixture():
    hooks = {name: [{'node': 'a', 'argv': ['true']}] for name in driver.HOOKS}
    return {'run_id': 'trial', 'output': '/private/tmp/dfs-results', 'size_bytes': 64 * 1024**2,
            'rounds': 3, 'ready_wait_seconds': 30, 'repeat_gap_seconds': 1,
            'diagnostic': False, 'control': {'listen_ip': '192.0.2.2', 'peer_ip': '192.0.2.2',
                'expected_peer': '192.0.2.1', 'port': 19123},
            'nodes': {n: {'transport': ['limactl', 'shell', 'test-' + n, '--'],
                          'root': '/tmp/dfs-controlled-' + n, 'min_free_bytes': 1024}
                      for n in ['a', 'b', 'c']},
            'labels': {'afs-baseline': {'mounts': {'a': '/mnt/a/{run_id}', 'b': '/mnt/b/{run_id}'},
                'hooks': hooks, 'settings': {'contract': {'read_ahead_kb': 0, 'prefetch': 0},
                    'product': {'read_ahead_kb': 128, 'prefetch': 1}}, 'metrics': []}}}


class Configuration(unittest.TestCase):
    def test_valid_dual_modes(self):
        driver.validate_config(fixture())
        self.assertEqual(driver.read_flags('contract'), ['O_RDONLY', 'O_DIRECT'])
        self.assertEqual(driver.read_flags('product'), ['O_RDONLY'])

    def test_reject_shell_transport(self):
        cfg = fixture(); cfg['nodes']['a']['transport'] = ['sh', '-c', 'anything']
        with self.assertRaises(ValueError): driver.validate_config(cfg)

    def test_reject_unsafe_root_and_run_id(self):
        for root in ['/', '/tmp', '/tmp/../etc', 'relative']:
            cfg = fixture(); cfg['nodes']['a']['root'] = root
            with self.subTest(root=root), self.assertRaises(ValueError): driver.validate_config(cfg)
        cfg = fixture(); cfg['run_id'] = '../escape'
        with self.assertRaises(ValueError): driver.validate_config(cfg)

    def test_reject_unknown_template_and_incorrect_contract(self):
        cfg = fixture(); cfg['labels']['afs-baseline']['hooks']['start'][0]['argv'] = ['echo', '{bad}']
        with self.assertRaises(ValueError): driver.validate_config(cfg)
        cfg = fixture(); cfg['labels']['afs-baseline']['settings']['contract']['prefetch'] = 1
        with self.assertRaises(ValueError): driver.validate_config(cfg)

    def test_round_order_is_frozen_and_reversed(self):
        cfg = fixture()
        self.assertEqual(driver.round_orders(cfg, ['afs', 'jfs']),
                         [['afs', 'jfs'], ['jfs', 'afs'], ['afs', 'jfs']])
        cfg['orders'] = [['afs-baseline'], ['afs-baseline'], ['afs-baseline']]
        driver.validate_config(cfg)
        cfg['orders'][0] = ['afs-baseline', 'afs-baseline']
        with self.assertRaises(ValueError): driver.validate_config(cfg)

    def test_explicit_noninteractive_sudo_prefix_is_fixed(self):
        cfg = fixture()
        cfg['nodes']['a']['command_prefix'] = ['sudo', '-n']
        driver.validate_config(cfg)
        lima = driver.Transport(['limactl', 'shell', 'test-a', '--'], ['sudo', '-n'])
        self.assertEqual(lima.command(['python3', '-c', 'print(1)'])[-5:],
                         ['sudo', '-n', 'python3', '-c', 'print(1)'])
        ssh = driver.Transport(['ssh', 'host'], ['sudo', '-n'])
        self.assertEqual(driver.shlex.split(ssh.command(['echo', '$(literal)'])[-1]),
                         ['sudo', '-n', 'echo', '$(literal)'])
        cfg['nodes']['a']['command_prefix'] = ['sudo', 'sh', '-c']
        with self.assertRaises(ValueError): driver.validate_config(cfg)

    def test_shell_characters_remain_literal_arguments(self):
        self.assertEqual(driver.expand_argv(['echo', '$(touch /tmp/nope)', '{run_id}'], {'run_id': 'run'}),
                         ['echo', '$(touch /tmp/nope)', 'run'])

    def test_formal_diagnostic_separation(self):
        cfg = fixture(); cfg['diagnostic'] = True
        with self.assertRaises(ValueError): driver.validate_config(cfg)
        cfg['rounds'] = 1
        driver.validate_config(cfg)


class Admission(unittest.TestCase):
    def test_reject_nonempty_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / 'keep').write_text('asset')
            with self.assertRaises(ValueError): driver.require_empty_directory(Path(tmp).resolve())
            self.assertEqual((Path(tmp) / 'keep').read_text(), 'asset')

    def test_empty_directory_allowed(self):
        with tempfile.TemporaryDirectory() as tmp: driver.require_empty_directory(Path(tmp).resolve())

    def test_occupied_port_rejected(self):
        with socket.socket() as server:
            server.bind(('127.0.0.1', 0)); server.listen(1)
            with self.assertRaises(OSError): driver.require_free_port('127.0.0.1', server.getsockname()[1])


class Accounting(unittest.TestCase):
    def test_median_stability_no_cherry_pick(self):
        result = driver.summarize([100, 110, 105])
        self.assertEqual(result['median_ns'], 105)
        self.assertEqual(result['max_min'], 1.1)
        self.assertTrue(result['stable'])
        self.assertFalse(driver.summarize([100, 111, 105])['stable'])
        with self.assertRaises(ValueError): driver.summarize([])
        with self.assertRaises(ValueError): driver.summarize([0, 1])

    def test_metric_differences_detect_counter_reset(self):
        before = 'read_count{kind="x"} 10\n# ignored\nsize_bucket{le="4096"} 3\n'
        after = 'read_count{kind="x"} 14\nsize_bucket{le="4096"} 7\n'
        self.assertEqual(driver.metric_delta(before, after)['read_count{kind="x"}'], 4)
        with self.assertRaises(ValueError): driver.metric_delta(before, 'read_count{kind="x"} 2\n')

    def test_metric_windows_do_not_mislabel_publication_as_read(self):
        windows = driver.metric_window_boundaries([
            {'name': 'node-a', 'node': 'a'}, {'name': 'node-b', 'node': 'b'}])
        self.assertEqual(windows['node-a']['first']['name'], 'publish_and_first')
        self.assertTrue(windows['node-a']['first']['includes_publish'])
        self.assertEqual(windows['node-b']['first']['name'], 'begin_to_first_complete')
        self.assertFalse(windows['node-b']['first']['includes_publish'])
        self.assertTrue(windows['node-b']['repeat']['includes_fixed_repeat_gap'])
        self.assertFalse(windows['node-a']['first']['isolated_read'])


class MetricsSampling(unittest.TestCase):
    def test_between_snapshot_records_raw_duration_for_both_roles(self):
        for role in ['reader', 'writer']:
            with self.subTest(role=role), \
                    mock.patch.object(driver.worker, 'metrics', return_value={'sample': 'raw'}), \
                    mock.patch.object(driver.worker.time, 'monotonic_ns', side_effect=[100, 500000100]):
                snapshot, timing = driver.worker.between_metrics({'role': role}, 100)
            self.assertEqual(snapshot, {'sample': 'raw'})
            self.assertEqual(timing['elapsed_ns'], 500000000)
            self.assertEqual(timing['role'], role)

    def test_over_budget_snapshot_fails_instead_of_extending_repeat_gap(self):
        with mock.patch.object(driver.worker, 'metrics', return_value={}), \
                mock.patch.object(driver.worker.time, 'monotonic_ns', side_effect=[100, 500000101]):
            with self.assertRaises(driver.worker.MetricsBudgetExceeded) as caught:
                driver.worker.between_metrics({'role': 'reader'}, 100)
        self.assertEqual(caught.exception.timing['elapsed_ns'], 500000001)
        self.assertEqual(caught.exception.timing['limit_ns'], 500000000)

    def test_overrun_failure_retains_structured_raw_timing(self):
        timing = {'role': 'reader', 'elapsed_ns': 500000001, 'limit_ns': 500000000}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            config = {'run_root': str(root), 'role': 'reader'}
            with mock.patch.object(driver.worker, 'process_identity', return_value={'pid': 1}), \
                    mock.patch.object(driver.worker.signal, 'signal'), \
                    mock.patch.object(driver.worker, 'role', side_effect=driver.worker.MetricsBudgetExceeded(timing)):
                self.assertEqual(driver.worker.run_role(config), 1)
            result = json.loads((root / 'result.json').read_text())
            self.assertFalse(result['completed'])
            self.assertEqual(result['metrics_sampling']['after_first'], timing)

    def test_gap_work_before_snapshot_is_also_bounded(self):
        with mock.patch.object(driver.worker, 'metrics', return_value={}), \
                mock.patch.object(driver.worker.time, 'monotonic_ns', side_effect=[600000000, 600000001]):
            with self.assertRaises(driver.worker.MetricsBudgetExceeded) as caught:
                driver.worker.between_metrics({'role': 'reader'}, 0)
        self.assertEqual(caught.exception.timing['elapsed_ns'], 1)
        self.assertEqual(caught.exception.timing['gap_elapsed_ns'], 600000001)


class ControlProtocol(unittest.TestCase):
    def test_authenticated_frames_reject_wrong_phase_and_key(self):
        for expected, key in [('WRONG', b'a' * 32), ('READY', b'b' * 32)]:
            sender, receiver = socket.socketpair()
            with sender, receiver:
                driver.worker.send_frame(sender, b'a' * 32, {'run_id': 'run', 'type': 'READY'})
                with self.assertRaises(ValueError):
                    driver.worker.receive_frame(receiver, key, 'run', expected)

    def test_authenticated_frame_roundtrip(self):
        sender, receiver = socket.socketpair()
        with sender, receiver:
            value = {'run_id': 'run', 'type': 'READY', 'bytes': 4096}
            driver.worker.send_frame(sender, b'a' * 32, value)
            self.assertEqual(driver.worker.receive_frame(receiver, b'a' * 32, 'run', 'READY'), value)

    def test_expected_digest_matches_prepared_content(self):
        for size in [4096, 65536, 69632]:
            data, digest = driver.worker.prepare_payload(size, 9)
            self.assertEqual(len(data), size)
            self.assertEqual(driver.worker.expected_digest(size, 9), digest)

    def test_helper_is_deployed_once_per_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            for run in ['first', 'second']:
                target = root / run
                driver.worker.action('install', {'run_root': str(target), 'source': '# helper\n',
                    'token': '11' * 32, 'role_config': {'run_root': str(target)}})
                self.assertEqual((target / 'token').stat().st_mode & 0o777, 0o600)
                self.assertFalse((target / 'remote_workload.py').exists())
                driver.worker.action('stop', {'run_root': str(target)})
                self.assertFalse((target / 'token').exists())
            self.assertEqual(len(list((root / 'helpers').glob('*/remote_workload.py'))), 1)


class FailureLifecycle(unittest.TestCase):
    def exercise(self, fail_workload=False, fail_stop=False):
        cfg = fixture()
        calls = []
        transport = mock.Mock()
        transport.remote.return_value = {}
        metrics = {'before': {}, 'after_first': {}, 'after_repeat': {}}
        writer = {'completed': True, 'publish': {'publish_ns': 100},
                  'first': {'full_read_ns': 200}, 'repeat': {'full_read_ns': 100},
                  'e2e_ns': 400, 'total_with_repeat_ns': 1000000500, 'metrics': metrics}
        reader = {'completed': True, 'metrics': metrics}
        states = {'a': {'result': writer, 'wait': {'exit_status': 0}},
                  'b': {'result': reader, 'wait': {'exit_status': 0}}}
        waiters = []
        for node in ['b', 'a']:
            waiter = mock.Mock()
            waiter.returncode = 0
            waiter.poll.return_value = 0
            waiter.communicate.return_value = (json.dumps(states[node]), '')
            waiters.append(waiter)
        if fail_workload:
            waiters[0].communicate.side_effect = RuntimeError('read SHA failed')
        transport.spawn_wait.side_effect = waiters
        def invoke(_config, _label, name, context, _transports, _evidence):
            calls.append((name, context['successful']))
            if name == 'stop' and fail_stop:
                raise RuntimeError('normal shutdown failed')
            return []
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp).resolve()
            with mock.patch.object(driver, 'Transport', return_value=transport), \
                    mock.patch.object(driver, 'hook', side_effect=invoke), \
                    mock.patch.object(driver, 'wait_status', side_effect=[{}, {}, {}]), \
                    mock.patch.object(driver.time, 'sleep'):
                result = driver.run_round(cfg, 'contract', 'afs-baseline', 1, output)
            saved = output / result['run_id'] / 'result.json'
            self.assertTrue(saved.exists())
        return result, calls

    def test_success_retires_data_only_after_stop(self):
        result, calls = self.exercise()
        self.assertTrue(result['completed'])
        self.assertEqual(calls[-3:], [('stop', 'true'), ('cleanup', 'true'), ('verify_clean', 'true')])

    def test_failed_workload_preserves_payload_and_diagnostics(self):
        result, calls = self.exercise(fail_workload=True)
        self.assertFalse(result['completed'])
        self.assertTrue(result['failed_payload_preserved'])
        self.assertEqual(calls[-2:], [('stop', 'false'), ('verify_clean', 'false')])
        self.assertNotIn('cleanup', [name for name, _ in calls])
        self.assertEqual(result['failure']['message'], 'read SHA failed')

    def test_failed_shutdown_prevents_data_cleanup(self):
        result, calls = self.exercise(fail_stop=True)
        self.assertFalse(result['completed'])
        self.assertNotIn('cleanup', [name for name, _ in calls])
        self.assertEqual(result['cleanup_errors'][0]['step'], 'stop')


if __name__ == '__main__': unittest.main()
