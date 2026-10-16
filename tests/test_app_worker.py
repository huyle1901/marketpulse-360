import unittest
from marketpulse_app.worker import consumer_config
from marketpulse_app.settings import load_settings


class WorkerBoundaryTests(unittest.TestCase):
    def test_consumer_reads_only_committed_and_requires_ack_before_offsets(self):
        config=consumer_config('kafka:29092')
        self.assertEqual(config['isolation.level'],'read_committed')
        self.assertFalse(config['enable.auto.commit'])
        self.assertFalse(config['enable.auto.offset.store'])
        self.assertEqual(config['auto.offset.reset'],'earliest')

    def test_missing_app_credentials_and_repr_redaction(self):
        with self.assertRaises(ValueError):
            load_settings({'CLICKHOUSE_PASSWORD':'secret-value'})
        settings=load_settings({'CLICKHOUSE_PASSWORD':'secret-value','APP_POSTGRES_PASSWORD':'private-value'})
        self.assertNotIn('secret-value',repr(settings))
        self.assertNotIn('private-value',repr(settings))
