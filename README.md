# Local Data Engineering Pipeline

A small, reusable CSV ingestion framework demonstrated with **customers** and
**orders**. It uses only Python's standard library and SQLite: no cloud account,
credentials, or external data are required.

## Architecture

```text
CSV source
  -> extract/header checks
  -> structural + domain validation and reference checks
  -> deterministic normalization and derived fields
  -> SQLite upsert + rejected-record quarantine
  -> run ledger, structured logs, and JSON reconciliation report
```

Reusable pipeline components live in `data_pipeline/`: extraction, validation,
transformation, loading/storage, orchestration, metrics/logging, and a seeded
synthetic data generator. Dataset-specific rules are registered in
`data_pipeline/datasets.py`.

## Setup and run

Requires Python 3.10 or newer; there are no third-party dependencies.

```powershell
python --version
python -m unittest discover -v
python -m data_pipeline demo
```

The demo loads `data/sample/customers.csv` before `data/sample/orders.csv` into
`outputs/pipeline.db` and writes `outputs/reconciliation.json`. The command
prints each run's metrics and output reconciliation. Re-running it is safe:
target rows are upserted by their stable IDs, and a batch's later repeated key
is quarantined rather than loaded twice.

Run one dataset explicitly:

```powershell
python -m data_pipeline run --dataset customers --input data/sample/customers.csv --db outputs/pipeline.db
python -m data_pipeline run --dataset orders --input data/sample/orders.csv --db outputs/pipeline.db
```

Generate a reproducible larger pair of input files (the `rows` value applies
to each dataset):

```powershell
python -m data_pipeline generate --rows 100000 --seed 42 --output-dir data/generated
python -m data_pipeline run --dataset customers --input data/generated/customers.csv --db outputs/large.db
python -m data_pipeline run --dataset orders --input data/generated/orders.csv --db outputs/large.db
```

Customers must be loaded before orders so the foreign-key relationship can be
checked. Use a fresh database for a clean backfill, or rerun a source file to
retry it; failed runs are recorded and do not publish partial target changes.
Each invocation receives a new `run_id`; the input SHA-256 and source filename
are recorded for traceability. CSV columns must match the declared schema;
unexpected or missing columns fail the batch explicitly. Row-level issues are
stored in `rejects` with their run, source file, line number, raw values, and
reason.

## Data dictionary and rules

See [docs/data_dictionary.md](docs/data_dictionary.md) for table grains,
fields, keys, nullability, and business rules. In brief:

* `customers`: one current customer profile per `customer_id`.
* `orders`: one current order per `order_id`, with a foreign key to customers.
* Timestamps require ISO-8601 timezone offsets and are normalized to UTC.
* Money is stored as integer cents to avoid floating-point rounding.
* IDs are producer-supplied stable keys. `source_file` and `run_id` are added
  to each target row.
* Customer and order tables are current-state snapshots; changed records
  overwrite the prior state. This demo does not retain historical versions.

## Tests and evidence

`python -m unittest discover -v` covers valid and empty batches, schema drift,
malformed/short rows, required nulls, invalid numeric values, duplicate keys,
foreign-key failures, email normalization, zero-price orders, out-of-order
timestamps, and reruns. The sample inputs deliberately include valid and
invalid rows; the generated outputs include per-run input, accepted, written,
and rejected counts and a reconciliation assertion (`input = accepted +
rejected`). The order report also includes total target rows, distinct
referenced customers, and total amount in cents.

On the current Windows environment (Python 3.13.5), a measured run generated
100,000 rows per dataset in 3.18 seconds, then loaded customers in 3.54 seconds
and orders in 9.34 seconds into a fresh SQLite database. These are directional
measurements, not a hardware-independent performance guarantee. Order
validation preloads customer IDs once into a set instead of issuing one SQL
lookup per order; SQLite foreign-key checks remain the final integrity guard.
Writes are also batched with `executemany` inside the publish transaction.

## Known limitations and trade-offs

This is a local, single-process starter, not a distributed scheduler or
warehouse. It assumes UTF-8 CSV files and two fixed schemas. A dataset's
validated rows are kept in memory until its transaction is written, which is
appropriate for the supplied 100,000-row exercise but not unbounded files.
The SHA-256 pass reads files in chunks. Snapshot upserts intentionally replace
current state; use append-only event tables or versioned records if business
history must be preserved. The sample and generator use synthetic data only.
