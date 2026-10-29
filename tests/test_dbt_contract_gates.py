"""dbt required-field gates must list exactly the contract's non-null business columns (certification compares them)."""
import re
import unittest
from pathlib import Path

from marketpulse_batch.export import EXPORTED_RELATIONS
from marketpulse_lakehouse.contracts import TABLE_CONTRACTS

SCHEMA = (Path(__file__).resolve().parents[1] / "dbt/marketpulse_serving/models/schema.yml").read_text()


class DbtContractGateTests(unittest.TestCase):
    def test_required_fields_match_contracts_for_every_exported_relation(self):
        for logical in EXPORTED_RELATIONS:
            model = logical.split(".")[1]
            block = re.search(rf"  - name: {model}\n(.*?)(?=\n  - name: |\Z)", SCHEMA, re.S)
            self.assertIsNotNone(block, model)
            required = re.search(r"required_fields:\s*\n\s*arguments:\s*\n\s*columns: \[([^\]]*)\]", block.group(1))
            self.assertIsNotNone(required, model)
            declared = [c.strip() for c in required.group(1).split(",")]
            expected = [c.name for c in TABLE_CONTRACTS[logical].columns if not c.nullable and not c.operational]
            self.assertEqual(declared, expected, model)


if __name__ == "__main__":
    unittest.main()
