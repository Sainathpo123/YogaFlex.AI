from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, TypeAlias


RawRecord: TypeAlias = dict[str | None, str | list[str] | None]
TargetRecord: TypeAlias = dict[str, str | int]
Validator: TypeAlias = Callable[[dict[str, str | None], Callable[[str], bool]], object]
Transformer: TypeAlias = Callable[[object], TargetRecord]
ReferenceKeysLoader: TypeAlias = Callable[[sqlite3.Connection], set[str]]


@dataclass(frozen=True)
class DatasetDefinition:
    name: str
    expected_headers: tuple[str, ...]
    key_column: str
    target_table: str
    validate: Validator
    transform: Transformer
    reference_keys: ReferenceKeysLoader | None = None


@dataclass(frozen=True)
class ExtractedRecord:
    line_number: int
    values: dict[str | None, str | list[str] | None]


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    dataset: str
    source_file: str
    source_sha256: str | None
    started_at: str
    ended_at: str
    status: str
    input_rows: int
    accepted_rows: int
    output_rows: int
    rejected_rows: int
    duration_ms: int
    failure_reason: str | None


class PipelineError(Exception):
    """Raised when a batch cannot be processed safely."""


class SchemaError(PipelineError):
    """Raised when the input file's schema is not the expected schema."""


class RecordValidationError(ValueError):
    """Raised when an individual source record fails validation."""
