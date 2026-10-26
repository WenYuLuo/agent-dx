"""Public create replay fixture uses the API's encoded response contract."""

import base64
import importlib.util
import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


class CreateIdentityFixtureTests(unittest.TestCase):
    def test_create_decodes_envelope_and_sends_public_request_identity(self):
        path = Path(__file__).resolve().parents[1] / 'create_identities.py'
        spec = importlib.util.spec_from_file_location('identity_fixture', path)
        fixture = importlib.util.module_from_spec(spec)
        with mock.patch.dict(
            sys.modules,
            {
                'adx_environment': types.SimpleNamespace(Environment=object),
                'node': types.SimpleNamespace(catalog=lambda: {}),
            },
        ):
            spec.loader.exec_module(fixture)
        envelope = json.dumps(
            {
                'code': 200,
                'data': base64.b64encode(
                    json.dumps(
                        {
                            'environmentId': 'default-winner',
                            'status': 'running',
                        }
                    ).encode()
                ).decode(),
            }
        ).encode()
        with (
            mock.patch.object(fixture.ssl, 'create_default_context'),
            mock.patch.object(fixture.urllib.request, 'urlopen', return_value=io.BytesIO(envelope)) as request,
        ):
            identity = fixture.create('coordinator:8443', 'fixture-token', '/ca.pem', 'request-1', 'candidate', 'image')
        self.assertEqual(identity, 'default-winner')
        sent = request.call_args.args[0]
        self.assertEqual(sent.get_header('X-request-id'), 'request-1')
        self.assertEqual(json.loads(sent.data)['name'], 'candidate')
