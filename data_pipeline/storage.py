from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from data_pipeline.models import DatasetDefinition, TargetRecord


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_sha256 TEXT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    input_rows INTEGER NOT NULL DEFAULT 0,
    accepted_rows INTEGER NOT NULL DEFAULT 0,
    output_rows INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT
);
CREATE TABLE IF NOT EXISTS rejects (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    raw_record TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    created_at TEXT NOT NULL,
    source_file TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id)
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(customer_id),
    order_ts TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    total_amount_cents INTEGER NOT NULL CHECK (total_amount_cents >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'cancelled')),
    source_file TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_rejects_run_id ON rejects(run_id);
"""

UPSERT_SQL = {
    "customers": """
        INSERT INTO customers (customer_id, email, created_at, source_file, run_id)
        VALUES (:customer_id, :email, :created_at, :source_file, :run_id)
        ON CONFLICT(customer_id) DO UPDATE SET
            email = excluded.email,
            created_at = excluded.created_at,
            source_file = excluded.source_file,
            run_id = excluded.run_id
    """,
    "orders": """
        INSERT INTO orders (
            order_id, customer_id, order_ts, quantity, unit_price_cents,
            total_amount_cents, status, source_file, run_id
        )
        VALUES (
            :order_id, :customer_id, :order_ts, :quantity, :unit_price_cents,
            :total_amount_cents, :status, :source_file, :run_id
        )
        ON CONFLICT(order_id) DO UPDATE SET
            customer_id = excluded.customer_id,
            order_ts = excluded.order_ts,
            quantity = excluded.quantity,
            unit_price_cents = excluded.unit_price_cents,
            total_amount_cents = excluded.total_amount_cents,
            status = excluded.status,
            source_file = excluded.source_file,
            run_id = excluded.run_id
    """,
}


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(SCHEMA)
    connection.commit()
    return connection


def load_records(
    connection: sqlite3.Connection,
    definition: DatasetDefinition,
    records: Iterable[TargetRecord],
    source_file: str,
    run_id: str,
) -> int:
    sql = UPSERT_SQL.get(definition.target_table)
    if sql is None:
        raise ValueError(f"No load statement is configured for {definition.target_table}")
    rows = [
        {**record, "source_file": source_file, "run_id": run_id}
        for record in records
    ]
    connection.executemany(sql, rows)
    return len(rows)


def customer_keys(connection: sqlite3.Connection) -> set[str]:
    return {
        row["customer_id"]
        for row in connection.execute("SELECT customer_id FROM customers")
    }
