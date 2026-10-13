import json
import os
import subprocess
import unittest
import uuid
from pathlib import Path


class AppRuntimeTests(unittest.TestCase):
    def test_app_profile_private_database_and_localhost_ports(self):
        env=os.environ|{'MINIO_ROOT_PASSWORD':uuid.uuid4().hex,'LAKEHOUSE_POSTGRES_PASSWORD':uuid.uuid4().hex}
        result=subprocess.run(['docker','compose','--env-file','/dev/null','--profile','app','--profile','serving','--profile','streaming','config','--format','json'],
                              env=env,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        services=json.loads(result.stdout)['services']
        for name in ('app-postgres','api','campaign-worker','web'):
            self.assertIn(name,services)
            self.assertIn('app',services[name]['profiles'])
        self.assertNotIn('ports',services['app-postgres'])
        self.assertEqual(services['api']['ports'][0]['host_ip'],'127.0.0.1')
        self.assertEqual(services['web']['ports'][0]['host_ip'],'127.0.0.1')
        self.assertEqual(services['web']['environment']['API_INTERNAL_URL'],'http://api:8000')
        self.assertNotIn('CLICKHOUSE_PASSWORD',services['web'].get('environment',{}))

    def test_env_initializer_preserves_existing_credentials(self):
        import tempfile
        from marketpulse_app.local_env import initialize
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'.env'
            path.write_text('APP_POSTGRES_PASSWORD=keep-local-test-value\nMINIO_ROOT_PASSWORD=keep-source-value\n')
            initialize(path)
            self.assertEqual(path.read_text(),'APP_POSTGRES_PASSWORD=keep-local-test-value\nMINIO_ROOT_PASSWORD=keep-source-value\n')
