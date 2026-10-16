"""Run-owned Kafka workloads and observed recovery; independent oracle gates."""
from __future__ import annotations
import json
import hashlib
import platform
import re
import subprocess
import threading
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from .contracts import percentiles, verify_outputs, verify_recovery
from .resources import ResourceSampler
from .workload import STREAM_SCALES, events

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'compose.evaluation.yaml'), '-p', 'marketpulse-evaluation', '--profile', 'evaluation']


def dc(*args: str) -> str:
    return subprocess.run([*COMPOSE, *args], check=True, capture_output=True, text=True, timeout=180).stdout.strip()


def metadata() -> dict[str, Any]:
    return {'git_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'python': platform.python_version(), 'platform': platform.platform(),
            'docker': json.loads(subprocess.check_output(['docker', 'info', '--format', '{{json .}}'], text=True)),
            'scope': 'isolated bounded finance benchmark; not full MP03 parity or dashboard latency'}


def safe_metadata() -> dict[str, Any]:
    raw = metadata(); info = raw.pop('docker')
    raw['docker'] = {key: info.get(key) for key in ('ServerVersion', 'NCPU', 'MemTotal', 'Architecture', 'OperatingSystem')}
    # Bind uncommitted implementation too; Git revision alone would identify its parent.
    paths = [ROOT / 'compose.evaluation.yaml', ROOT / 'evaluation-job/pom.xml']
    for folder in ('src/marketpulse_eval', 'evaluation-job/src', 'docker/evaluation'):
        paths.extend(p for p in (ROOT / folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts)
    hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
    raw['source_files'] = hashes
    raw['source_digest'] = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    raw['working_tree_dirty'] = bool(subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=ROOT))
    return raw


def comparable(left: dict, right: dict) -> None:
    if not left.get('passed') or not right.get('passed') or any(left[k] != right[k] for k in ('count', 'input_hash', 'output_hash')):
        raise ValueError('engine comparisons require identical inputs and oracle outputs')


def require_recovery(proof: dict | None, engine: str, requested: bool) -> None:
    if requested:
        if proof is None: raise ValueError('requested recovery was not exercised')
        verify_recovery(proof, engine)


def rest(path: str) -> dict:
    with urllib.request.urlopen('http://localhost:49381' + path, timeout=10) as response:
        return json.load(response)


def run_stream(engine: str, scale: str, *, recovery: bool = False, eps: int = 0) -> dict[str, Any]:
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    if engine not in ('flink', 'streams') or scale not in STREAM_SCALES or eps < 0:
        raise ValueError('invalid engine/scale/rate')
    if recovery and eps == 0: eps = 300
    rows = events(STREAM_SCALES[scale]); run = 'eval-' + uuid.uuid4().hex[:16]
    source, sink = run + '-in', run + '-out'
    admin = AdminClient({'bootstrap.servers': 'localhost:49392'})
    futures = admin.create_topics([NewTopic(t, 1, 1) for t in (source, sink)])
    for future in futures.values(): future.result(timeout=30)
    job, stream_name = None, None
    consumer = Consumer({'bootstrap.servers': 'localhost:49392', 'group.id': run + '-observer',
        'isolation.level': 'read_committed', 'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    consumer.assign([TopicPartition(sink, 0, 0)])
    try:
        if engine == 'flink':
            dc('up', '-d', 'jobmanager', 'taskmanager')
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    if rest('/overview').get('slots-total', 0) >= 1: break
                except (OSError, ValueError): pass
                time.sleep(.5)
            result = dc('exec', '-T', 'jobmanager', 'flink', 'run', '-d', '-c', 'io.marketpulse.eval.FlinkJob',
                        '/opt/flink/usrlib/evaluation-job.jar', 'kafka:29092', source, sink, run)
            match = re.search(r'JobID ([a-f0-9]{32})', result)
            if not match: raise ValueError('missing submitted Flink job ID')
            job = match.group(1)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                info = rest('/jobs/' + job)
                if info['state'] == 'RUNNING' and all(v['status'] == 'RUNNING' for v in info['vertices']): break
                if info['state'] in ('FAILED', 'CANCELED'): raise ValueError('Flink job failed before input')
                time.sleep(.5)
            else: raise TimeoutError('Flink operators did not start')
            containers = ['marketpulse-evaluation-jobmanager-1', 'marketpulse-evaluation-taskmanager-1']
        else:
            # A separate named one-off container preserves its own application ID across restart.
            stream_name = 'mp10-' + run
            dc('run', '-d', '--no-deps', '--name', stream_name, 'streams', 'kafka:29092', source, sink, run)
            containers = [stream_name]
            time.sleep(3)
        producer = Producer({'bootstrap.servers': 'localhost:49392', 'enable.idempotence': True, 'linger.ms': 5})
        state: dict[str, Any] = {'sent': 0, 'acked': 0, 'error': None, 'done_at': None}
        def acknowledged(error: Any, message: Any) -> None:
            if error: state['error'] = str(error)
            else: state['acked'] += 1
        def publish() -> None:
            try:
                start = time.monotonic()
                for row in rows:
                    if eps:
                        delay = row['seq'] / eps - (time.monotonic() - start)
                        if delay > 0: time.sleep(delay)
                    row['sent_ms'] = time.time_ns() // 1_000_000
                    while True:
                        try:
                            producer.produce(source, key=str(row['campaign']), value=json.dumps(row), on_delivery=acknowledged)
                            break
                        except BufferError: producer.poll(.01)
                    state['sent'] += 1; producer.poll(0)
                if producer.flush(60): raise TimeoutError('unacknowledged benchmark inputs')
            except Exception as error: state['error'] = str(error)
            finally: state['done_at'] = time.monotonic()
        outputs, latencies = [], []
        recovery_proof = None; recovery_thread = None; recovered_at = None
        with ResourceSampler([*containers, 'marketpulse-evaluation-kafka-1']) as resources:
            started = time.monotonic(); thread = threading.Thread(target=publish, daemon=True); thread.start()
            deadline = started + max(180, len(rows) / max(eps, 1) + 120) if eps else started + 300
            while len(outputs) < len(rows) and time.monotonic() < deadline:
                if state['error']: raise ValueError(state['error'])
                for message in consumer.consume(1000, timeout=.1):
                    if message.error(): raise ValueError(str(message.error()))
                    row = json.loads(message.value()); outputs.append(row)
                    latencies.append(time.time_ns() / 1_000_000 - row['sent_ms'])
                    if (recovery_proof and recovery_thread and not recovery_thread.is_alive()
                            and recovered_at is None and row['seq'] >= recovery_proof['input_before']):
                        recovered_at = time.monotonic()
                        recovery_proof['seconds'] = recovered_at - recovery_proof['restart_monotonic']
                        recovery_proof['after_outputs'] = len(outputs)
                        recovery_proof['input_after'] = state['sent']
                if recovery and recovery_proof is None and len(outputs) >= 100:
                    checkpoint = rest('/jobs/' + job + '/checkpoints').get('latest', {}).get('completed') if job else None
                    if engine == 'flink' and not checkpoint: continue
                    recovery_proof = {'before_outputs': len(outputs), 'input_before': state['sent'],
                        'producer_active': thread.is_alive(), 'restart_monotonic': time.monotonic(),
                        'completed_checkpoint': checkpoint['id'] if checkpoint else None, 'same_application_id': engine == 'streams'}
                    def restart() -> None:
                        try:
                            if engine == 'flink': dc('restart', '-t', '0', 'taskmanager')
                            else: subprocess.run(['docker', 'restart', '-t', '0', stream_name],check=True,capture_output=True,timeout=90)
                        except Exception as error: state['error'] = str(error)
                    recovery_thread = threading.Thread(target=restart, daemon=True); recovery_thread.start()
            finished = time.monotonic()
            thread.join(timeout=65)
            if thread.is_alive() or state['error'] or state['acked'] != len(rows): raise ValueError('producer did not finish cleanly')
            if recovery_thread: recovery_thread.join(timeout=90)
            if recovery_proof and job:
                checkpoint_info = rest('/jobs/' + job + '/checkpoints')
                restored = checkpoint_info.get('latest', {}).get('restored')
                recovery_proof['restored_checkpoint'] = restored['id'] if restored else None
            # Catch extra committed outputs after the expected count too.
            extras = consumer.consume(1000, timeout=1)
            if extras: raise ValueError('unexpected additional committed outputs')
        checked = verify_outputs(rows, outputs)
        require_recovery(recovery_proof, engine, recovery)
        return {'version': 1, 'kind': 'stream', 'engine': engine, 'scale': scale, 'seed': 360, 'run_id': run,
            'versions': {'flink': '2.2.1', 'kafka_streams': '4.3.1', 'kafka': '4.3.1', 'java': '17'},
            'correctness': checked, 'recovery': recovery_proof, 'resources': resources.result(),
            'latency': percentiles(latencies), 'timing_scope': 'host_send_to_read_committed_observation',
            'throughput_inputs_per_second': len(rows) / (finished - started), 'elapsed_seconds': finished - started,
            'producer_ack_seconds': state['done_at'] - started, 'rate_limit_eps': eps,
            'percentile_method': 'nearest_rank', 'metadata': safe_metadata()}
    finally:
        consumer.close()
        if job:
            try: dc('exec', '-T', 'jobmanager', 'flink', 'cancel', job)
            except subprocess.SubprocessError: pass
        if stream_name:
            subprocess.run(['docker', 'rm', '-f', stream_name],capture_output=True)
