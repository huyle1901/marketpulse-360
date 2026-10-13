#!/usr/bin/env python3
"""Validate one benchmark report, or reconcile pre/post-restart reports."""

import argparse
import json
from pathlib import Path

from marketpulse_batch.evidence import validate_evidence

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("before", type=Path)
parser.add_argument("after", type=Path, nargs="?")
args = parser.parse_args()
validate_evidence(json.loads(args.before.read_text()), json.loads(args.after.read_text()) if args.after else None)
print("Serving evidence accepted" + ("; restart preserved release, row counts and exact financial totals" if args.after else ""))
