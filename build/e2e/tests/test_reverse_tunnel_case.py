import http.client
import shlex
import shutil
import subprocess
import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = ROOT.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(REPOSITORY / 'platform/sdk/environment/python'))

from functional_data_plane import (  # noqa: E402
    TUNNEL_BODY,
    make_tunnel_upstream,
    tunnel_fetch_command,
)


class ReverseTunnelCaseTests(unittest.TestCase):
    def test_environment_probe_flushes_request_and_reads_complete_response(self):
        upstream = make_tunnel_upstream()
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        try:
            command = tunnel_fetch_command(f'http://127.0.0.1:{upstream.server_address[1]}')
            arguments = shlex.split(command)
            executable = shutil.which(arguments[0])
            if executable is None:
                raise FileNotFoundError(arguments[0])
            arguments[0] = executable
            result = subprocess.run(
                arguments,
                shell=False,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f'{TUNNEL_BODY}:/functional', result.stdout)
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=5)

    def test_upstream_preserves_raw_path_query_and_response_length(self):
        upstream = make_tunnel_upstream()
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        try:
            for path in ('/functional', '/encoded%2Fpart?x=1&y=two%20words', '/'):
                with self.subTest(path=path):
                    connection = http.client.HTTPConnection(*upstream.server_address, timeout=5)
                    try:
                        connection.request('GET', path)
                        response = connection.getresponse()
                        body = response.read()
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.getheader('Content-Type'), 'text/plain')
                        self.assertEqual(int(response.getheader('Content-Length')), len(body))
                        self.assertEqual(body.decode(), f'{TUNNEL_BODY}:{path}')
                    finally:
                        connection.close()
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=5)


if __name__ == '__main__':
    unittest.main()
