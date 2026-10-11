"""Command-line interface for mock-data generation and validation."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Sequence

from .config import PROFILES, resolve_config
from .generator import generate_dataset
from .validator import validate_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketpulse-mock")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("profiles", help="list named scale profiles")

    generate = subparsers.add_parser("generate", help="generate a mock dataset")
    generate.add_argument("--profile", default="fixture")
    generate.add_argument("--seed", type=int, default=360)
    generate.add_argument("--output", type=Path, required=True)
    generate.add_argument("--rows-per-file", type=int)
    generate.add_argument("--start-date", type=date.fromisoformat)
    generate.add_argument("--overwrite", action="store_true")

    validate = subparsers.add_parser("validate", help="validate a generated dataset")
    validate.add_argument("--input", type=Path, required=True)
    validate.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "profiles":
            for name, profile in PROFILES.items():
                print(
                    f"{name:10} days={profile.days:<2} customers={profile.customers:<7} "
                    f"campaigns={profile.campaigns:<3} impressions/day={profile.impressions_per_day}"
                )
            return 0
        if args.command == "generate":
            config = resolve_config(
                args.profile,
                seed=args.seed,
                output_dir=args.output,
                rows_per_file=args.rows_per_file,
                start_date=args.start_date,
            )
            manifest = generate_dataset(config, overwrite=args.overwrite)
            print(
                f"Generated {sum(manifest.datasets.values())} rows in "
                f"{len(manifest.files)} files at {config.output_dir}"
            )
            return 0
        if args.command == "validate":
            report = validate_dataset(args.input)
            if args.as_json:
                print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
            elif report.valid:
                print(
                    f"Valid dataset: {report.files_checked} files, "
                    f"{report.rows_checked} rows"
                )
            else:
                for error in report.errors:
                    print(f"error: {error}", file=sys.stderr)
            return 0 if report.valid else 2
    except (ValueError, FileExistsError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    parser.error(f"unsupported command: {args.command}")
    return 2
