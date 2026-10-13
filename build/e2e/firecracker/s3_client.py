"""Small SigV4 client for isolated MinIO and real OBS acceptance fixtures."""

import datetime
import hashlib
import hmac
import urllib.request
from urllib.parse import urlsplit
from storage_fixture import public as public_storage


class Client:
    def __init__(self, run):
        self.access = (run / 'secrets/s3-user').read_text().strip()
        self.secret = (run / 'secrets/s3-key').read_text().strip()
        self.config = public_storage(run)
        self.last_request_id = None

    def request(self, method, path, data=b'', query=''):
        if self.config.get('virtual_hosted'):
            bucket_path = '/' + self.config['bucket']
            if path != bucket_path and not path.startswith(bucket_path + '/'):
                raise ValueError('request outside fixture bucket')
            path = path[len(bucket_path):] or '/'
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp, date = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
        host = urlsplit(self.config['endpoint']).netloc
        region = self.config['region']
        digest = hashlib.sha256(data).hexdigest()
        headers = f'host:{host}\nx-amz-content-sha256:{digest}\nx-amz-date:{stamp}\n'
        signed = 'host;x-amz-content-sha256;x-amz-date'
        canonical = '\n'.join([method, path, query, headers, signed, digest])
        scope = f'{date}/{region}/s3/aws4_request'
        message = '\n'.join(['AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])

        def sign(key, value):
            return hmac.new(key, value.encode(), hashlib.sha256).digest()

        key = sign(sign(sign(sign(('AWS4' + self.secret).encode(), date), region), 's3'), 'aws4_request')
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        request = urllib.request.Request(
            self.config['endpoint'] + path + ('?' + query if query else ''),
            method=method,
            data=data if method == 'PUT' else None,
            headers={
                'Host': host,
                'x-amz-date': stamp,
                'x-amz-content-sha256': digest,
                'Authorization': (
                    f'AWS4-HMAC-SHA256 Credential={self.access}/{scope}, SignedHeaders={signed}, Signature={signature}'
                ),
            },
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            self.last_request_id = response.headers.get('x-obs-request-id') or response.headers.get('x-amz-request-id')
            return response.read()
