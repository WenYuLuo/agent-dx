import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

MODULE = Path(__file__).with_name("checkpoint.py")
spec = importlib.util.spec_from_file_location("accept", MODULE)
accept = importlib.util.module_from_spec(spec)
spec.loader.exec_module(accept)


class CheckpointAcceptanceUnitTest(unittest.TestCase):
    def test_observation_requires_memory_counter_schema(self):
        self.assertEqual(
            accept.observation("42 nonce 7 123.5\n"),
            {"pid": "42", "nonce": "nonce", "count": 7, "epoch": 123.5},
        )
        with self.assertRaises(accept.AcceptanceError):
            accept.observation("42 nonce nope 1")

    def test_state_round_trip_rejects_secret_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            state = {"run_id": "run", "boxes": {"source": "sbx"}, "snapshots": ["snap"]}
            accept.save_state(path, state)
            self.assertNotIn("token", path.read_text().lower())
            self.assertEqual(accept.load_state(path), state)
            path.write_text(json.dumps({**state, "token": "secret"}))
            with self.assertRaises(accept.AcceptanceError):
                accept.load_state(path)

    def test_snapshot_timeout_is_single_attempt_without_post_retry(self):
        calls = []

        def send():
            calls.append(1)
            raise TimeoutError()

        self.assertEqual(accept.single_snapshot_attempt(send, TimeoutError, True), (None, True))
        self.assertEqual(calls, [1])
        with self.assertRaises(accept.AcceptanceError):
            accept.single_snapshot_attempt(send, TimeoutError, False)

    def test_redaction_and_expected_rejection_statuses(self):
        self.assertNotIn("secret", accept.redact("X-API-Key=secret"))

        response = SimpleNamespace(status_code=403, text="denied")
        self.assertIs(accept.require_status(response, (401, 403), "isolation"), response)
        with self.assertRaises(accept.AcceptanceError):
            accept.require_status(response, (200,), "isolation")


if __name__ == "__main__":
    unittest.main()
