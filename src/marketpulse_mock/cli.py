"""Command-line interface for mock-data generation and validation."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from datetime import date
from pathlib import Path
from typing import Sequence

from .config import PROFILES, resolve_config
from .generator import generate_dataset
from .kafka_client import ConfluentKafkaPublisher, create_topics
from .registry import register_contracts
from .replay import replay
from .replay_cursor import replay_window
from .replay_pacing import PacingConfig, ReplayController, ReplayPacer
from .replay_reader import iter_replay_records
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

    kafka = subparsers.add_parser("kafka", help="operate the local Kafka replay layer")
    kafka_commands = kafka.add_subparsers(dest="kafka_command", required=True)

    topics = kafka_commands.add_parser("topics", help="create replay topics")
    topics.add_argument("--bootstrap-servers", default="localhost:9092")

    registry = kafka_commands.add_parser(
        "register-contracts", help="register JSON Schemas in Apicurio"
    )
    registry.add_argument("--registry-url", default="http://localhost:8081")
    registry.add_argument("--contract-dir", type=Path, default=Path("contracts/kafka"))

    kafka_replay = kafka_commands.add_parser(
        "replay", help="replay generated events into Kafka"
    )
    kafka_replay.add_argument("--input", type=Path, required=True)
    kafka_replay.add_argument("--bootstrap-servers", default="localhost:9092")
    kafka_replay.add_argument("--include-dlq", action="store_true")
    kafka_replay.add_argument("--from-date", type=date.fromisoformat, help="inclusive UTC event date")
    kafka_replay.add_argument("--to-date", type=date.fromisoformat, help="exclusive UTC event date")
    kafka_replay.add_argument("--resume-state", type=Path, help="durable acknowledged replay cursor")
    kafka_replay.add_argument("--init-state", action="store_true", help="initialize a new cursor after history preparation")
    kafka_replay.add_argument("--batch-size", type=int, default=500, help="records per acknowledged cursor batch")
    pacing = kafka_replay.add_mutually_exclusive_group()
    pacing.add_argument("--speed", type=float)
    pacing.add_argument("--events-per-second", type=float)
    pacing.add_argument("--no-sleep", action="store_true")
    return parser


def install_signal_handlers(controller: ReplayController) -> None:
    """SIGUSR1 toggles pause; SIGINT requests a graceful flushed stop."""

    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, lambda _signum, _frame: controller.toggle())
    signal.signal(signal.SIGINT, lambda _signum, _frame: controller.stop())


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
        if args.command == "kafka" and args.kafka_command == "topics":
            summary = create_topics(args.bootstrap_servers)
            print(json.dumps({
                "created": summary.created,
                "existing": summary.existing,
                "failed": summary.failed,
                "errors": list(summary.errors),
            }, sort_keys=True))
            for error in summary.errors:
                print(f"error: {error}", file=sys.stderr)
            return 0 if summary.success else 2
        if args.command == "kafka" and args.kafka_command == "register-contracts":
            summary = register_contracts(args.registry_url, args.contract_dir)
            print(json.dumps(summary.to_dict(), sort_keys=True))
            for error in summary.errors:
                print(f"error: {error}", file=sys.stderr)
            return 0 if summary.success else 2
        if args.command == "kafka" and args.kafka_command == "replay":
            if args.init_state and args.resume_state is None:
                raise ValueError("--init-state requires --resume-state")
            if args.resume_state is not None and (args.from_date is None or args.to_date is None):
                raise ValueError("resumable replay requires --from-date and --to-date")
            config = PacingConfig(
                speed=(
                    args.speed
                    if args.speed is not None
                    else 1000.0
                    if args.events_per_second is None and not args.no_sleep
                    else None
                ),
                events_per_second=args.events_per_second,
                no_sleep=args.no_sleep,
            )
            controller = ReplayController()
            install_signal_handlers(controller)
            pacing_mode = (
                "no-sleep"
                if config.no_sleep
                else f"events-per-second={config.events_per_second:g}"
                if config.events_per_second is not None
                else f"speed={config.speed:g}x"
            )
            print(
                f"replay pid={os.getpid()} pacing={pacing_mode}",
                file=sys.stderr,
            )
            publisher = ConfluentKafkaPublisher(args.bootstrap_servers)
            if args.resume_state is not None:
                summary = replay_window(
                    args.input, args.resume_state, args.from_date, args.to_date,
                    publisher, ReplayPacer(config, controller), controller,
                    target_id=publisher.cluster_id(), initialize=args.init_state,
                    include_dlq=args.include_dlq, batch_size=args.batch_size,
                )
                print(json.dumps(summary.to_dict(), sort_keys=True))
                return 0
            summary = replay(
                iter_replay_records(args.input, include_dlq=args.include_dlq, from_date=args.from_date, to_date=args.to_date),
                publisher,
                ReplayPacer(config, controller),
                controller,
            )
            print(json.dumps(summary.to_dict(), sort_keys=True))
            return 0 if summary.success else 2
    except (ValueError, RuntimeError, FileExistsError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    parser.error(f"unsupported command: {args.command}")
    return 2
