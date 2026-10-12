from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError

from marketpulse_mock.registry import register_contracts


class Response:
    def __init__(self, body: bytes = b"{}") -> None:
        self.body = body

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.body


class RegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.contract_dir = Path(self.temporary_directory.name)
        schemas = {
            "web-events-v1.json": {"title": "Web"},
            "order-events-v1.json": {"title": "Order"},
            "email-events-v1.json": {"title": "Email"},
            "validation-dlq-v1.json": {"title": "DLQ"},
            "customer-events-v1.json": {"title": "Clean"},
            "sessions-v1.json": {"title": "Sessions"},
            "funnel-metrics-v1.json": {"title": "Funnel"},
            "revenue-metrics-v1.json": {"title": "Revenue"},
            "campaign-metrics-v1.json": {"title": "Campaign"},
            "flink-validation-dlq-v1.json": {"title": "Flink DLQ"},
        }
        for name, schema in schemas.items():
            (self.contract_dir / name).write_text(json.dumps(schema))

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_first_registration_creates_artifacts_with_v3_envelope(self) -> None:
        requests: list[object] = []

        def opener(request: object, timeout: float) -> Response:
            requests.append(request)
            if request.method == "GET":
                raise HTTPError(request.full_url, 404, "missing", {}, None)
            return Response()

        summary = register_contracts(
            "http://registry:8080", self.contract_dir, opener=opener
        )

        self.assertEqual(10, summary.created)
        posts = [request for request in requests if request.method == "POST"]
        self.assertEqual(10, len(posts))
        payload = json.loads(posts[0].data)
        self.assertEqual("JSON", payload["artifactType"])
        self.assertEqual("1", payload["firstVersion"]["version"])
        self.assertEqual("application/json", payload["firstVersion"]["content"]["contentType"])
        self.assertIn("/apis/registry/v3/groups/marketpulse/artifacts", posts[0].full_url)

    def test_second_identical_registration_is_a_no_op(self) -> None:
        stored = {
            "web-events-v1": {"title": "Web"},
            "order-events-v1": {"title": "Order"},
            "email-events-v1": {"title": "Email"},
            "validation-dlq-v1": {"title": "DLQ"},
            "customer-events-v1": {"title": "Clean"},
            "sessions-v1": {"title": "Sessions"},
            "funnel-metrics-v1": {"title": "Funnel"},
            "revenue-metrics-v1": {"title": "Revenue"},
            "campaign-metrics-v1": {"title": "Campaign"},
            "flink-validation-dlq-v1": {"title": "Flink DLQ"},
        }

        def opener(request: object, timeout: float) -> Response:
            artifact_id = request.full_url.split("/artifacts/")[1].split("/")[0]
            return Response(json.dumps(stored[artifact_id]).encode())

        summary = register_contracts(
            "http://registry:8080", self.contract_dir, opener=opener
        )

        self.assertEqual(10, summary.unchanged)
        self.assertEqual(0, summary.created)
        self.assertTrue(summary.success)

    def test_changed_contract_creates_a_new_version(self) -> None:
        methods: list[str] = []

        def opener(request: object, timeout: float) -> Response:
            methods.append(request.method)
            if request.method == "GET":
                return Response(b'{"title":"Old"}')
            self.assertIn("/versions", request.full_url)
            payload = json.loads(request.data)
            self.assertNotIn("artifactType", payload)
            self.assertIn("content", payload)
            return Response()

        summary = register_contracts(
            "http://registry:8080", self.contract_dir, opener=opener
        )

        self.assertEqual(10, summary.versioned)
        self.assertEqual(10, methods.count("POST"))

    def test_registry_transport_failure_is_reported(self) -> None:
        def opener(request: object, timeout: float) -> Response:
            raise HTTPError(request.full_url, 503, "unavailable", {}, None)

        summary = register_contracts(
            "http://registry:8080", self.contract_dir, opener=opener
        )

        self.assertEqual(10, summary.failed)
        self.assertFalse(summary.success)


if __name__ == "__main__":
    unittest.main()
