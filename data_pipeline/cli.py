from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from dataclasses import asdict
from pathlib import Path

from data_pipeline.datasets import DATASETS
from data_pipeline.generate import generate_datasets
from data_pipeline.models import PipelineError
from data_pipeline.observability import configure_logging, write_report
from data_pipeline.pipeline import latest_run, reconciliation, run_pipeline

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the reusable local data pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="Run one dataset pipeline")
    run.add_argument("--dataset", choices=sorted(DATASETS), required=True)
    run.add_argument("--input", type=Path, required=True)
    run.add_argument("--db", type=Path, default=Path("outputs/pipeline.db"))
    run.add_argument("--report", type=Path)
    run.add_argument("--log-level", default="INFO")

    demo = subparsers.add_parser("demo", help="Run the sample customer and order batches")
    demo.add_argument("--data-dir", type=Path, default=Path("data/sample"))
    demo.add_argument("--db", type=Path, default=Path("outputs/pipeline.db"))
    demo.add_argument("--report", type=Path, default=Path("outputs/reconciliation.json"))
    demo.add_argument("--log-level", default="INFO")

    generate = subparsers.add_parser("generate", help="Generate deterministic larger CSV inputs")
    generate.add_argument("--rows", type=int, default=100_000)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument("--output-dir", type=Path, default=Path("data/generated"))

    return parser


def _run_one(dataset: str, input_path: Path, database_path: Path, report_path: Path | None):
    definition = DATASETS[dataset]
    summary = run_pipeline(definition, input_path, database_path)
    report = {
        **asdict(summary),
        "reconciliation": {
            **latest_run(database_path, summary.run_id),
            **reconciliation(database_path, dataset),
        },
    }
    if report_path is not None:
        write_report(
            summary,
            report_path,
            {
                **latest_run(database_path, summary.run_id),
                **reconciliation(database_path, dataset),
            },
        )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "generate":
        try:
            generate_datasets(args.output_dir, args.rows, args.seed)
        except ValueError as exc:
            parser.error(str(exc))
        print(f"Generated {args.rows} customers and orders in {args.output_dir}")
        return 0

    configure_logging(args.log_level)
    try:
        if args.command == "run":
            report = _run_one(args.dataset, args.input, args.db, args.report)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0

        results = [
            _run_one(
                dataset,
                args.data_dir / f"{dataset}.csv",
                args.db,
                None,
            )
            for dataset in ("customers", "orders")
        ]
        combined = {"runs": results}
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(combined, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(combined, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError, sqlite3.Error, PipelineError) as exc:
        LOGGER.error("Pipeline command failed: %s", exc)
        print(f"Pipeline command failed: {exc}", file=sys.stderr)
        return 1
