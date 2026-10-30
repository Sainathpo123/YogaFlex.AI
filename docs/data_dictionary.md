# Data Dictionary

All SQLite tables use `snake_case`; timestamps are ISO-8601 UTC strings ending
in `Z`. Source IDs are deterministic producer keys. Every published entity
records the input `source_file` and publishing `run_id`.

## `customers`

**Grain:** one current customer profile per `customer_id`.

| Column | Type | Nullable | Key / rule |
|---|---|---:|---|
| `customer_id` | TEXT | No | Primary key; non-empty source ID |
| `email` | TEXT | No | Lowercased; basic address/domain validation |
| `created_at` | TEXT | No | Timezone-aware ISO-8601, normalized to UTC |
| `source_file` | TEXT | No | Input basename |
| `run_id` | TEXT | No | Foreign key to `pipeline_runs.run_id` |

Customer records are upserted by `customer_id`. Email is not assumed to be a
unique business key.

## `orders`

**Grain:** one current order per `order_id`.

| Column | Type | Nullable | Key / rule |
|---|---|---:|---|
| `order_id` | TEXT | No | Primary key; non-empty source ID |
| `customer_id` | TEXT | No | Foreign key to `customers.customer_id` |
| `order_ts` | TEXT | No | Timezone-aware ISO-8601, normalized to UTC |
| `quantity` | INTEGER | No | Positive integer |
| `unit_price_cents` | INTEGER | No | Non-negative; input has at most two decimals |
| `total_amount_cents` | INTEGER | No | Derived as quantity × unit price cents |
| `status` | TEXT | No | `pending`, `completed`, or `cancelled` |
| `source_file` | TEXT | No | Input basename |
| `run_id` | TEXT | No | Foreign key to `pipeline_runs.run_id` |

Orders may arrive in any timestamp order; no watermark or late-record drop
policy is applied. A changed order with an existing ID updates its current
state. The sample accepts zero-price orders and stores their total as zero.

## Operational tables

* `pipeline_runs`: one row per invocation; records IDs, source hash, UTC start
  and end time, status, duration, input/accepted/output/rejected counts, and
  failure reason.
* `rejects`: one row per invalid or duplicate input record; includes the run,
  dataset, filename, CSV line number, JSON-encoded raw values, and reason.

Input rows reconcile as `accepted_rows + rejected_rows`. For customers and
orders, accepted records are deterministic upserts. SQLite transactions make
the target writes, quarantine rows, and successful run status atomic. A failed
batch leaves a failed ledger row and no partial target publish.

## CSV source contracts

* `customers.csv`: `customer_id,email,created_at`
* `orders.csv`: `order_id,customer_id,order_ts,quantity,unit_price,status`

Unexpected or missing header columns fail the complete batch, while malformed
individual records are quarantined. `DictReader` line numbers refer to the
physical CSV line at the end of each record.
