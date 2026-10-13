from __future__ import annotations

import hashlib
import io
import json
import os
import unittest
from datetime import date
from decimal import Decimal

from marketpulse_batch.canonical import rowset_checksum
from marketpulse_batch.export import ExportManifest, ExportObject, ExportRelation
from marketpulse_batch.copying import load_candidate
from marketpulse_lakehouse.contracts import TABLE_CONTRACTS


class MemoryObjects:
    def __init__(self):
        self.values = {}

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.values[(Bucket, Key)])}


@unittest.skipUnless(os.environ.get("BATCH_TEST_CLICKHOUSE"), "opt-in real ClickHouse integration")
class BatchCopyingTests(unittest.TestCase):
    def setUp(self) -> None:
        import clickhouse_connect
        from marketpulse_batch.settings import load_settings
        settings = load_settings(os.environ)
        self.client = clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
                                                   username=settings.clickhouse_user, password=settings.clickhouse_password)
        # Unique per test prevents collisions and never drops existing databases.
        import uuid
        self.identity = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        self.objects = MemoryObjects()
        relations = []
        for name, contract in TABLE_CONTRACTS.items():
            if not name.startswith("gold.") and name not in ("silver.campaigns", "silver.customers"):
                continue
            columns = tuple((c.name, c.iceberg_type, c.nullable) for c in contract.columns if not c.operational)
            row = {}
            for field, kind, nullable in columns:
                if nullable:
                    value = None
                elif kind == "string":
                    value = "001"
                elif kind in ("long", "int"):
                    value = 9007199254740993 if kind == "long" else 1
                elif kind == "boolean":
                    value = True
                elif kind == "date":
                    value = "2026-01-01"
                elif kind == "timestamp":
                    value = "2026-01-01T00:00:00.000000Z"
                else:
                    value = "1.230000"
                row[field] = value
            content = (json.dumps(row, separators=(",", ":")) + "\n").encode()
            key = f"batch-exports/{self.identity}/test/{name}/part-00000.json"
            self.objects.values[("marketpulse-lakehouse", key)] = content
            count, checksum = rowset_checksum(columns, [row])
            obj = ExportObject(f"s3://marketpulse-lakehouse/{key}", hashlib.sha256(content).hexdigest(), len(content), 1)
            relations.append(ExportRelation(name, 1, columns, (obj,), count, checksum))
        self.manifest = ExportManifest(self.identity, tuple(relations))

    def test_exact_typed_roundtrip_all_seven_relations(self) -> None:
        database = load_candidate(self.client, self.manifest, attempt=1, s3=self.objects)
        self.assertEqual(database, f"mp_release_{self.identity}_1")
        for relation in self.manifest.relations:
            result = self.client.query(f"SELECT * FROM {database}.raw_{relation.name.split('.')[1]}", query_tz="UTC", tz_mode="aware")
            rows = [dict(zip(result.column_names, values)) for values in result.result_rows]
            self.assertEqual(rowset_checksum(relation.columns, rows), (1, relation.business_checksum))

    def test_partial_copy_new_attempt(self) -> None:
        first_object = self.manifest.relations[0].objects[0]
        key = first_object.uri.split("s3://marketpulse-lakehouse/", 1)[1]
        original = self.objects.values[("marketpulse-lakehouse", key)]
        self.objects.values[("marketpulse-lakehouse", key)] = b"corrupt\n"
        with self.assertRaises(ValueError):
            load_candidate(self.client, self.manifest, attempt=1, s3=self.objects)
        self.objects.values[("marketpulse-lakehouse", key)] = original
        with self.assertRaisesRegex(ValueError, "exists"):
            load_candidate(self.client, self.manifest, attempt=1, s3=self.objects)
        database = load_candidate(self.client, self.manifest, attempt=2, s3=self.objects)
        self.assertEqual(self.client.query(f"SELECT count() FROM {database}.raw_campaigns").first_row[0], 1)

    def test_bad_object_hash_rejected(self) -> None:
        key = next(iter(self.objects.values))
        self.objects.values[key] += b"\n"
        with self.assertRaises(ValueError):
            load_candidate(self.client, self.manifest, attempt=1, s3=self.objects)
