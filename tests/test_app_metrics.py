import unittest
from marketpulse_app.metrics import Metrics,Sample,render
from marketpulse_app.collector import Collector
from marketpulse_app.platform_metrics import committed_lag,flink_samples
class MetricsTests(unittest.TestCase):
    def test_histogram_and_fixed_route(self):
        m=Metrics();m.observe('/api/v1/dashboard','GET',200,.1)
        text=m.render();self.assertIn('mp_api_request_seconds_bucket',text);self.assertIn('le="0.1"} 1',text)
        m.observe('/private/id?token=x','HACK',500,1)
        self.assertNotIn('private',m.render());self.assertNotIn('token',m.render());self.assertIn('unknown',m.render())
    def test_collector_clears_failed_source_without_erasing_other(self):
        c=Collector({'good':lambda:[Sample('mp_test',1)],'bad':lambda:[Sample('mp_old',3)]})
        c.poll('good');c.poll('bad');self.assertIn('mp_old',c.render())
        c.sources['bad']=lambda:(_ for _ in ()).throw(OSError('secret'))
        c.poll('bad');text=c.render();self.assertNotIn('mp_old',text);self.assertIn('mp_test',text);self.assertNotIn('secret',text)
    def test_missing_offsets_and_flink_job_are_unknown(self):
        self.assertIsNone(committed_lag(10,-1001));self.assertEqual(committed_lag(10,8),2)
        self.assertEqual(flink_samples({'jobs':[]}),[Sample('mp_flink_job_running',0)])
    def test_sample_labels_escaped_and_nonfinite_rejected(self):
        self.assertIn('kind="a\\"b"',render([Sample('mp_test',1,(('kind','a"b'),))]))
        with self.assertRaises(ValueError):render([Sample('mp_test',float('nan'))])
