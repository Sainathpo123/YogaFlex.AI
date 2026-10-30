from __future__ import annotations

import csv
import hashlib
import json
import logging
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from data_pipeline.extract import extract_csv
from data_pipeline.models import (
    DatasetDefinition,
    PipelineError,
    RecordValidationError,
    RunSummary,
    SchemaError,
)
from data_pipeline.storage import connect, load_records

LOGGER = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject(
    connection: sqlite3.Connection,
    run_id: str,
    dataset: str,
    source_file: str,
    line_number: int,
    raw_record: object,
    reason: str,
) -> None:
    record_for_json = {
        "_extra_values" if key is None else key: value
        for key, value in raw_record.items()
    } if isinstance(raw_record, dict) else raw_record
    connection.execute(
        """
        INSERT INTO rejects (run_id, dataset, source_file, line_number, raw_record, reason)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            dataset,
            source_file,
            line_number,
            json.dumps(record_for_json, sort_keys=True, default=str),
            reason,
        ),
    )


def run_pipeline(
    definition: DatasetDefinition, input_path: Path, database_path: Path
) -> RunSummary:
    run_id = uuid.uuid4().hex
    source_file = input_path.name
    started_at = utc_now()
    started_clock = time.perf_counter()
    input_rows = accepted_rows = rejected_rows = 0
    source_sha256: str | None = None
    connection = connect(database_path)
    connection.execute(
        """
        INSERT INTO pipeline_runs (run_id, dataset, source_file, started_at, status)
        VALUES (?, ?, ?, ?, 'running')
        """,
        (run_id, definition.name, source_file, started_at),
    )
    connection.commit()

    try:
        source_sha256 = sha256_file(input_path)
        connection.execute(
            "UPDATE pipeline_runs SET source_sha256 = ? WHERE run_id = ?",
            (source_sha256, run_id),
        )
        connection.commit()

        accepted: list[dict[str, str | int]] = []
        seen_keys: set[str] = set()
        rejected_records: list[tuple[int, object, str]] = []
        reference_keys = (
            definition.reference_keys(connection) if definition.reference_keys else set()
        )
        lookup = reference_keys.__contains__

        try:
            for extracted in extract_csv(input_path, definition.expected_headers):
                input_rows += 1
                raw = extracted.values
                structural_errors = []
                if None in raw:
                    structural_errors.append("record has more values than the header")
                missing_values = [
                    header
                    for header in definition.expected_headers
                    if raw.get(header) is None
                ]
                if missing_values:
                    structural_errors.append(
                        f"record is missing values for: {', '.join(missing_values)}"
                    )

                if structural_errors:
                    reason = "; ".join(structural_errors)
                    rejected_records.append((extracted.line_number, raw, reason))
                    rejected_rows += 1
                    continue

                string_record = {
                    key: value if isinstance(value, str) else None
                    for key, value in raw.items()
                    if isinstance(key, str)
                }
                try:
                    validated = definition.validate(string_record, lookup)
                    transformed = definition.transform(validated)
                except RecordValidationError as exc:
                    rejected_records.append((extracted.line_number, raw, str(exc)))
                    rejected_rows += 1
                    continue

                key_value = str(transformed[definition.key_column])
                if key_value in seen_keys:
                    rejected_records.append(
                        (extracted.line_number, raw, f"duplicate key in input batch: {key_value}")
                    )
                    rejected_rows += 1
                    continue
                seen_keys.add(key_value)
                accepted.append(transformed)
        except csv.Error as exc:
            raise PipelineError(f"CSV parsing failed: {exc}") from exc

        accepted_rows = len(accepted)
        for line_number, raw, reason in rejected_records:
            _reject(
                connection,
                run_id,
                definition.name,
                source_file,
                line_number,
                raw,
                reason,
            )
        output_rows = load_records(
            connection, definition, accepted, source_file=source_file, run_id=run_id
        )
        ended_at = utc_now()
        duration_ms = int((time.perf_counter() - started_clock) * 1000)
        connection.execute(
            """
            UPDATE pipeline_runs
            SET source_sha256 = ?, ended_at = ?, status = 'completed',
                input_rows = ?, accepted_rows = ?, output_rows = ?,
                rejected_rows = ?, duration_ms = ?
            WHERE run_id = ?
            """,
            (
                source_sha256,
                ended_at,
                input_rows,
                accepted_rows,
                output_rows,
                rejected_rows,
                duration_ms,
                run_id,
            ),
        )
        connection.commit()
        LOGGER.info(
            "run completed run_id=%s dataset=%s input=%d accepted=%d output=%d rejected=%d",
            run_id,
            definition.name,
            input_rows,
            accepted_rows,
            output_rows,
            rejected_rows,
        )
        return RunSummary(
            run_id=run_id,
            dataset=definition.name,
            source_file=source_file,
            source_sha256=source_sha256,
            started_at=started_at,
            ended_at=ended_at,
            status="completed",
            input_rows=input_rows,
            accepted_rows=accepted_rows,
            output_rows=output_rows,
            rejected_rows=rejected_rows,
            duration_ms=duration_ms,
            failure_reason=None,
        )
    except Exception as exc:
        connection.rollback()
        ended_at = utc_now()
        duration_ms = int((time.perf_counter() - started_clock) * 1000)
        failure_reason = f"{type(exc).__name__}: {exc}"
        connection.execute(
            """
            UPDATE pipeline_runs
            SET source_sha256 = ?, ended_at = ?, status = 'failed',
                input_rows = ?, accepted_rows = ?, output_rows = 0,
                rejected_rows = ?, duration_ms = ?, failure_reason = ?
            WHERE run_id = ?
            """,
            (
                source_sha256,
                ended_at,
                input_rows,
                accepted_rows,
                rejected_rows,
                duration_ms,
                failure_reason,
                run_id,
            ),
        )
        connection.commit()
        LOGGER.exception(
            "run failed run_id=%s dataset=%s reason=%s",
            run_id,
            definition.name,
            failure_reason,
        )
        raise
    finally:
        connection.close()


def reconciliation(database_path: Path, dataset: str) -> dict[str, object]:
    connection = connect(database_path)
    try:
        if dataset == "customers":
            target_rows = connection.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
            return {"target_rows": target_rows}
        if dataset == "orders":
            row = connection.execute(
                """
                SELECT COUNT(*) AS target_rows,
                       COALESCE(SUM(total_amount_cents), 0) AS total_amount_cents,
                       COUNT(DISTINCT customer_id) AS referenced_customers
                FROM orders
                """
            ).fetchone()
            return {
                "target_rows": row["target_rows"],
                "total_amount_cents": row["total_amount_cents"],
                "referenced_customers": row["referenced_customers"],
            }
        raise ValueError(f"Unknown dataset: {dataset}")
    finally:
        connection.close()


def latest_run(database_path: Path, run_id: str) -> dict[str, object]:
    connection = connect(database_path)
    try:
        row = connection.execute(
            "SELECT input_rows, accepted_rows, rejected_rows FROM pipeline_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown run_id: {run_id}")
        return {
            "input_rows": row["input_rows"],
            "accepted_rows": row["accepted_rows"],
            "rejected_rows": row["rejected_rows"],
            "counts_reconcile": row["input_rows"] == row["accepted_rows"] + row["rejected_rows"],
        }
    finally:
        connection.close()
