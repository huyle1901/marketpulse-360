from __future__ import annotations

import tempfile
import unittest
import io
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import BinaryIO

from marketpulse_mock.replay_reader import iter_replay_records
from marketpulse_mock.smoke_verify import main


FIXTURE = Path("data/mock/fixtures/seed-360")


class SmokeVerifyTests(unittest.TestCase):
    def test_consumed_ids_and_dlq_reasons_match_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            consumed = Path(temporary_directory)
            streams: dict[str, BinaryIO] = {}
            try:
                for record in iter_replay_records(FIXTURE, include_dlq=True):
                    stream = streams.get(record.topic)
                    if stream is None:
                        stream = (consumed / f"{record.topic}.jsonl").open("wb")
                        streams[record.topic] = stream
                    stream.write(record.value + b"\n")
            finally:
                for stream in streams.values():
                    stream.close()

            with redirect_stdout(io.StringIO()):
                self.assertEqual(0, main([str(FIXTURE), str(consumed)]))

    def test_duplicate_consumed_identity_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            consumed = Path(temporary_directory)
            grouped: dict[str, list[bytes]] = {}
            for record in iter_replay_records(FIXTURE, include_dlq=True):
                grouped.setdefault(record.topic, []).append(record.value)
            for topic, values in grouped.items():
                if topic == "marketing.raw.web-events.v1":
                    values.append(values[0])
                (consumed / f"{topic}.jsonl").write_bytes(b"\n".join(values) + b"\n")

            with redirect_stderr(io.StringIO()):
                self.assertEqual(1, main([str(FIXTURE), str(consumed)]))


if __name__ == "__main__":
    unittest.main()
