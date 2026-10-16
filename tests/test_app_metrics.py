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
    def test_invalid_source_samples_are_isolated_before_cache_acceptance(self):
        for invalid in ([Sample('mp_bad',float('nan'))],[Sample('mp_bad',float('inf'))],[None],[Sample('bad name',1)],[Sample('mp_bad',1),Sample('mp_bad',2)]):
            c=Collector({'kafka':lambda:[Sample('mp_good',7)],'flink':lambda:invalid})
            c.poll('kafka');c.poll('flink');text=c.render()
            self.assertIn('mp_good 7',text);self.assertIn('mp_source_up{source="flink"} 0',text);self.assertNotIn('mp_bad',text)
    def test_two_flink_managers_have_unique_documented_aggregates(self):
        from unittest.mock import patch
        from marketpulse_app.platform_metrics import poll_flink
        def get(url):
            if url.endswith('/jobs/overview'):return {'jobs':[{'name':'marketpulse-stream-v1','state':'RUNNING','jid':'job'}]}
            if url.endswith('/checkpoints'):return {'counts':{}}
            if url.endswith('/jobs/job'):return {'vertices':[]}
            if url.endswith('/taskmanagers'):return {'taskmanagers':[{'id':'one'},{'id':'two'}]}
            one='/one/' in url
            return [{'id':'Status.JVM.Memory.Heap.Used','value':'100' if one else '200'},{'id':'Status.JVM.CPU.Load','value':'0.2' if one else '0.8'}]
        with patch('marketpulse_app.platform_metrics.get_json',side_effect=get):samples=poll_flink()
        identities=[(s.name,s.labels) for s in samples];self.assertEqual(len(identities),len(set(identities)))
        values={dict(s.labels)['kind']:s.value for s in samples if s.name=='mp_flink_jvm'}
        self.assertEqual(values,{'heap_bytes_sum':300,'cpu_load_mean':.5})
