from __future__ import annotations

import contextlib
import io
import unittest

from marketpulse_batch.cli import main


class BatchCliTests(unittest.TestCase):
    def test_invalid_workload_fails_before_configuration(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as result:
            main(["query", "arbitrary_sql", "--from-date", "2026-01-01", "--to-date", "2026-01-03"])
        self.assertEqual(result.exception.code, 2)

    def test_invalid_interval_returns_failure(self) -> None:
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            status = main(["query", "campaign_overview", "--from-date", "2026-01-03", "--to-date", "2026-01-01"])
        self.assertEqual(status, 2)
        self.assertIn("interval", error.getvalue())
