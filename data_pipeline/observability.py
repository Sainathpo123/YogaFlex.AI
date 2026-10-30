from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path

from data_pipeline.models import RunSummary


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def write_report(summary: RunSummary, path: Path, reconciliation: dict[str, object]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    report = asdict(summary)
    report["reconciliation"] = reconciliation
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
