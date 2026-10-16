"""Counter-based container resource samples; no host Docker socket mount."""
from __future__ import annotations
import subprocess
import threading
import time
from typing import Any


def parse_cgroup(text: str) -> dict[str, float | int]:
    cpu, rest = text.split('MEMORY\n')
    memory, stat = rest.split('STAT\n')
    counters = dict(line.split() for line in cpu.splitlines() if line.strip())
    gauges = dict(line.split() for line in stat.splitlines() if line.strip())
    return {'cpu_seconds': int(counters['usage_usec']) / 1_000_000,
            'working_set_bytes': max(0, int(memory.strip()) - int(gauges.get('inactive_file', 0)))}


class ResourceSampler:
    def __init__(self, containers: list[str], interval: float = 1):
        self.containers, self.interval = containers, interval
        self.samples: dict[str, list[dict[str, Any]]] = {c: [] for c in containers}
        self.errors: dict[str, int] = {c: 0 for c in containers}
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def sample(self) -> None:
        for container in self.containers:
            try:
                output = subprocess.run(['docker', 'exec', container, 'sh', '-c',
                    'cat /sys/fs/cgroup/cpu.stat; echo MEMORY; cat /sys/fs/cgroup/memory.current; echo STAT; cat /sys/fs/cgroup/memory.stat'],
                    capture_output=True, text=True, check=True, timeout=5).stdout
                self.samples[container].append({'monotonic': time.monotonic(), **parse_cgroup(output)})
            except (subprocess.SubprocessError, ValueError, KeyError):
                self.errors[container] += 1

    def _run(self) -> None:
        while not self.stop.wait(self.interval):
            self.sample()

    def __enter__(self) -> ResourceSampler:
        self.sample(); self.thread.start(); return self

    def __exit__(self, *args: object) -> None:
        self.stop.set(); self.thread.join(timeout=20); self.sample()

    def result(self) -> dict[str, Any]:
        result = {}
        for name, rows in self.samples.items():
            if len(rows) < 2:
                raise ValueError('insufficient resource sample coverage: ' + name)
            # A restarted container's cumulative CPU counter resets. Sum positive deltas.
            cpu = sum(max(0, b['cpu_seconds'] - a['cpu_seconds']) for a, b in zip(rows, rows[1:]))
            result[name] = {'cpu_seconds_observed': cpu, 'peak_sampled_working_set_bytes': max(r['working_set_bytes'] for r in rows),
                'samples': len(rows), 'failed_samples': self.errors[name], 'interval_seconds': self.interval,
                'coverage_seconds': rows[-1]['monotonic'] - rows[0]['monotonic'],
                'cpu_reset_may_undercount_restart': any(b['cpu_seconds'] < a['cpu_seconds'] for a, b in zip(rows, rows[1:]))}
        return result
