import unittest,tempfile
from pathlib import Path
from unittest.mock import patch,Mock
from marketpulse_app.proof_http import ProofHTTP
class ProofHTTPTests(unittest.TestCase):
    def test_origin_cookies_and_unknown_destination_denied(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'password';p.write_text('synthetic password')
            with patch('urllib.request.build_opener') as factory:
                factory.return_value.open.return_value.__enter__.return_value.read.return_value=b'{}'
                http=ProofHTTP('http://localhost:48000','http://localhost:43100','tester',p)
                req=factory.return_value.open.call_args.args[0]
                self.assertEqual(req.get_header('Origin'),'http://localhost:43100');self.assertNotIn('password',req.full_url)
                with self.assertRaises(ValueError):http.open('http://evil.test')
