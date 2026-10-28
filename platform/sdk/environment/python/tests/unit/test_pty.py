import ssl
import asyncio
import json
from contextlib import asynccontextmanager
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from adx_environment import ConnectionConfig
from adx_environment._pty_transport import _build_pty_uri
from adx_environment.pty import Pty, _pty_server, _use_tls


class PtyTests(unittest.TestCase):
    def test_explicit_tls_verification_applies_to_pty_transport(self):
        for use_tls, verify_tls, expected in (
            (True, True, ssl.CERT_REQUIRED),
            (True, False, ssl.CERT_NONE),
            (False, True, None),
        ):
            with self.subTest(use_tls=use_tls, verify_tls=verify_tls):
                config = ConnectionConfig(
                    server_address="frontend.example:443",
                    token="test",
                    use_tls=use_tls,
                    verify_tls=verify_tls,
                )
                with patch("adx_environment.pty._PtyConnection") as connection:
                    factory = Pty("environment-1", connection=config)
                    session = factory.create("echo ok")
                    context = connection.call_args.kwargs["ssl_context"]
                    if expected is None:
                        self.assertIsNone(context)
                    else:
                        self.assertEqual(context.verify_mode, expected)
                        self.assertEqual(context.check_hostname, verify_tls)
                    session.close()

    def test_uri_preserves_command_protocol(self):
        uri = _build_pty_uri(
            server="frontend.example:443",
            use_tls=True,
            environment_id="environment/1",
            command=["/bin/bash", "-lc", "echo hello"],
            rows=24,
            cols=80,
        )
        parsed = urlparse(uri)
        query = parse_qs(parsed.query)
        self.assertEqual(parsed.scheme, "wss")
        self.assertEqual(parsed.path, "/direct/environment%2F1/pty")
        self.assertEqual(query["protocol"], ["environment.pty.v1"])
        self.assertEqual(query["command"], ["/bin/bash", "-lc", "echo hello"])
        self.assertNotIn("token", query)

    def test_pty_uses_adx_process_configuration(self):
        pty = Pty("environment-1")
        self.assertEqual(pty._environment_id, "environment-1")

    def test_explicit_gateway_defaults_to_plain_websocket(self):
        with patch.dict(
            "os.environ",
            {"ADX_GATEWAY_ADDRESS": "gateway:8080"},
            clear=True,
        ):
            self.assertFalse(_use_tls())

    def test_pty_uses_explicit_connection_without_environment_state(self):
        seen = {}

        class Connection:
            def __init__(self, uri, **kwargs):
                seen["uri"] = uri
                seen["token"] = kwargs["token"]

            def start(self, _timeout):
                pass

            def close(self):
                pass

        config = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
            gateway_address="gateway.example:8443",
            gateway_use_tls=True,
        )
        with (
            patch("adx_environment.pty._PtyConnection", Connection),
            patch.dict("os.environ", {}, clear=True),
        ):
            Pty("environment-1", connection=config).create("echo ok")

        parsed = urlparse(seen["uri"])
        self.assertEqual(parsed.scheme, "wss")
        self.assertEqual(parsed.netloc, "gateway.example:8443")
        self.assertEqual(parsed.path, "/direct/environment-1/pty")
        self.assertEqual(seen["token"], "secret")
        self.assertNotIn("token", parse_qs(parsed.query))

    def test_gateway_address_is_preferred_for_data_plane_pty(self):
        with patch.dict(
            "os.environ",
            {
                "ADX_SERVER_ADDRESS": "frontend:8888",
                "ADX_GATEWAY_ADDRESS": "ingress:8080",
                "ADX_TLS": "0",
                "ADX_GATEWAY_TLS": "1",
            },
            clear=True,
        ):
            self.assertEqual(_pty_server(), "ingress:8080")
            self.assertTrue(_use_tls())


class PtyEarlyCloseTests(unittest.IsolatedAsyncioTestCase):
    async def exercise_peer_closed_before_resize(self, terminal):
        import websockets.asyncio.client as ws_client
        from websockets.asyncio.server import serve
        from adx_environment._pty_transport import _PtyConnection

        async def handler(socket):
            await socket.send(json.dumps({
                "version": 1, "type": "started", "session_id": "pty-fast"
            }))
            if terminal:
                await socket.send(json.dumps({
                    "version": 1, "type": "exited", "session_id": "pty-fast",
                    "exit_code": 7,
                }))
            await socket.close()

        connect = ws_client.connect

        @asynccontextmanager
        async def after_peer_close(*args, **kwargs):
            async with connect(*args, **kwargs) as socket:
                # A real close handshake completes before the SDK gets its
                # connection. Terminal frames remain queued for recv().
                await asyncio.wait_for(socket.wait_closed(), 2)
                yield socket

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            transport = _PtyConnection(
                f"ws://127.0.0.1:{port}/pty", token="test-key",
                ssl_context=None, rows=24, cols=80, on_data=None,
                on_done=lambda: None,
            )
            with patch.object(ws_client, "connect", after_peer_close):
                try:
                    await asyncio.to_thread(transport.start, 5)
                    return await asyncio.to_thread(transport.wait, 5)
                finally:
                    await asyncio.to_thread(transport.close)

    async def test_buffered_terminal_survives_initial_resize_after_close(self):
        exit_code = await self.exercise_peer_closed_before_resize(True)
        self.assertEqual(exit_code, 7)

    async def test_close_without_terminal_does_not_fabricate_success(self):
        from adx_environment._pty_transport import _PtyTransportError
        with self.assertRaises(_PtyTransportError):
            await self.exercise_peer_closed_before_resize(False)


if __name__ == "__main__":
    unittest.main()
