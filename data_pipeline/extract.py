from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path

from data_pipeline.models import ExtractedRecord, SchemaError


def extract_csv(path: Path, expected_headers: tuple[str, ...]) -> Iterator[ExtractedRecord]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise SchemaError("CSV is empty or missing its header row")

        headers = reader.fieldnames
        missing = sorted(set(expected_headers) - set(headers))
        unexpected = sorted(set(headers) - set(expected_headers))
        if missing or unexpected:
            details = []
            if missing:
                details.append(f"missing columns: {', '.join(missing)}")
            if unexpected:
                details.append(f"unexpected columns: {', '.join(unexpected)}")
            raise SchemaError("; ".join(details))
        if len(headers) != len(set(headers)):
            raise SchemaError("CSV contains duplicate column names")

        for row in reader:
            yield ExtractedRecord(line_number=reader.line_num, values=row)
