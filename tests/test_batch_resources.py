import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from marketpulse_batch.resources import run_spark_operation


class SparkRunnerResourceTests(unittest.TestCase):
    def run_with(self, environment: dict[str, str]):
        with tempfile.TemporaryDirectory() as root:
            settings = SimpleNamespace(state_path=Path(root), minio_endpoint='http://minio:9000', minio_user='u', minio_password='p',
                                       nessie_uri='http://nessie', clickhouse_password='c', ledger_password='l')
            def complete(command, **kwargs):
                output = Path(command[command.index('--output') + 1]); output.write_text(json.dumps({'ok': True}))
                return SimpleNamespace(returncode=0, stdout='', stderr='', args=command, kwargs=kwargs)
            with patch.dict('os.environ', environment, clear=False), patch('marketpulse_batch.resources.subprocess.run', side_effect=complete) as run:
                run_spark_operation(settings, 'pin_bronze', {})
            return run.call_args

    def test_driver_memory_and_timeout_default_to_the_fixture_budget(self):
        with patch.dict('os.environ', {}, clear=False):
            import os; os.environ.pop('BATCH_SPARK_DRIVER_MEMORY', None); os.environ.pop('BATCH_SPARK_TIMEOUT_SECONDS', None)
            call = self.run_with({})
        command = call.args[0]
        self.assertEqual('2g', command[command.index('--driver-memory') + 1]); self.assertEqual(1800, call.kwargs['timeout'])

    def test_long_history_can_raise_driver_memory_and_timeout(self):
        call = self.run_with({'BATCH_SPARK_DRIVER_MEMORY': '5g', 'BATCH_SPARK_TIMEOUT_SECONDS': '7200'})
        command = call.args[0]
        self.assertEqual('5g', command[command.index('--driver-memory') + 1]); self.assertEqual(7200, call.kwargs['timeout'])

    def test_invalid_resource_overrides_fail_before_spark_starts(self):
        for environment in ({'BATCH_SPARK_DRIVER_MEMORY': '5g; rm'}, {'BATCH_SPARK_TIMEOUT_SECONDS': '0'}):
            with self.assertRaises(ValueError):
                self.run_with(environment)


if __name__ == '__main__':
    unittest.main()
