import tempfile
import unittest
from pathlib import Path
from marketpulse_app.ops_setup import setup
class SetupTests(unittest.TestCase):
    def test_permissions_preservation_and_symlink_refusal(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'.secrets';setup(p);token=(p/'metrics_token').read_bytes();setup(p)
            self.assertEqual(token,(p/'metrics_token').read_bytes());self.assertEqual(p.stat().st_mode&0o777,0o700)
            self.assertEqual((p/'metrics_token').stat().st_mode&0o777,0o444)
            self.assertEqual((p/'admin_password').stat().st_mode&0o777,0o600)
            (p/'metrics_token').unlink();(p/'metrics_token').symlink_to(p/'admin_password')
            with self.assertRaises(ValueError):setup(p)
