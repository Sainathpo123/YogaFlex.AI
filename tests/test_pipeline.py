from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from data_pipeline.datasets import DATASETS
from data_pipeline.models import SchemaError
from data_pipeline.pipeline import latest_run, run_pipeline
from data_pipeline.storage import connect


CUSTOMER_HEADER = "customer_id,email,created_at\n"
ORDER_HEADER = "order_id,customer_id,order_ts,quantity,unit_price,status\n"
CUSTOMER = "cust_1,user@example.test,2024-01-01T00:00:00Z\n"


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.database = self.root / "pipeline.db"
        self.customers = self.root / "customers.csv"
        self.customers.write_text(CUSTOMER_HEADER + CUSTOMER, encoding="utf-8")
        self.customer_run = run_pipeline(DATASETS["customers"], self.customers, self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _orders(self, body: str, header: str = ORDER_HEADER) -> Path:
        path = self.root / "orders.csv"
        path.write_text(header + body, encoding="utf-8")
        return path

    def _rows(self, query: str, parameters: tuple = ()):
        connection = connect(self.database)
        try:
            return connection.execute(query, parameters).fetchall()
        finally:
            connection.close()

    def test_happy_path_and_reconciliation(self) -> None:
        path = self._orders("ord_1,cust_1,2024-01-02T00:00:00Z,2,3.25,completed\n")
        summary = run_pipeline(DATASETS["orders"], path, self.database)
        self.assertEqual((summary.input_rows, summary.accepted_rows, summary.output_rows), (1, 1, 1))
        self.assertEqual(latest_run(self.database, summary.run_id)["counts_reconcile"], True)
        order = self._rows("SELECT total_amount_cents FROM orders WHERE order_id = 'ord_1'")[0]
        self.assertEqual(order["total_amount_cents"], 650)

    def test_empty_batch_is_successful(self) -> None:
        path = self._orders("")
        summary = run_pipeline(DATASETS["orders"], path, self.database)
        self.assertEqual(summary.input_rows, 0)
        self.assertEqual(summary.accepted_rows, 0)
        self.assertEqual(summary.rejected_rows, 0)

    def test_duplicate_key_and_malformed_extra_field_are_quarantined(self) -> None:
        body = (
            "ord_1,cust_1,2024-01-02T00:00:00Z,1,3.25,completed\n"
            "ord_1,cust_1,2024-01-03T00:00:00Z,1,3.25,completed\n"
            "ord_2,cust_1,2024-01-03T00:00:00Z,1,3.25,completed,unexpected\n"
        )
        summary = run_pipeline(DATASETS["orders"], self._orders(body), self.database)
        self.assertEqual((summary.accepted_rows, summary.rejected_rows), (1, 2))
        rejects = self._rows(
            "SELECT reason FROM rejects WHERE run_id = ? ORDER BY line_number",
            (summary.run_id,),
        )
        self.assertIn("duplicate key", rejects[0]["reason"])
        self.assertIn("more values", rejects[1]["reason"])

    def test_missing_values_and_invalid_numeric_values_are_rejected(self) -> None:
        body = (
            "ord_1,cust_1,2024-01-02T00:00:00Z,1,3.25,completed\n"
            "ord_2,cust_1,2024-01-02T00:00:00Z,,3.25,completed\n"
            "ord_3,cust_1,2024-01-02T00:00:00Z,1,not-a-price,completed\n"
        )
        summary = run_pipeline(DATASETS["orders"], self._orders(body), self.database)
        self.assertEqual((summary.accepted_rows, summary.rejected_rows), (1, 2))

    def test_unknown_customer_is_rejected(self) -> None:
        path = self._orders("ord_1,cust_missing,2024-01-02T00:00:00Z,1,3.25,completed\n")
        summary = run_pipeline(DATASETS["orders"], path, self.database)
        self.assertEqual(summary.rejected_rows, 1)
        reason = self._rows("SELECT reason FROM rejects WHERE run_id = ?", (summary.run_id,))[0][
            "reason"
        ]
        self.assertIn("unknown customer_id", reason)

    def test_zero_price_and_out_of_order_timestamp_are_valid(self) -> None:
        body = (
            "ord_later,cust_1,2024-03-01T00:00:00Z,1,0.00,pending\n"
            "ord_earlier,cust_1,2024-01-01T00:00:00Z,1,1.00,completed\n"
        )
        summary = run_pipeline(DATASETS["orders"], self._orders(body), self.database)
        self.assertEqual(summary.accepted_rows, 2)
        rows = self._rows(
            "SELECT order_id, total_amount_cents FROM orders ORDER BY order_id"
        )
        self.assertEqual([(row["order_id"], row["total_amount_cents"]) for row in rows], [
            ("ord_earlier", 100),
            ("ord_later", 0),
        ])

    def test_rerunning_same_batch_does_not_duplicate_target_rows(self) -> None:
        path = self._orders("ord_1,cust_1,2024-01-02T00:00:00Z,1,3.25,completed\n")
        first = run_pipeline(DATASETS["orders"], path, self.database)
        second = run_pipeline(DATASETS["orders"], path, self.database)
        self.assertEqual(first.output_rows, second.output_rows)
        self.assertEqual(self._rows("SELECT COUNT(*) AS n FROM orders")[0]["n"], 1)

    def test_schema_change_fails_explicitly_and_retry_succeeds(self) -> None:
        path = self._orders(
            "ord_1,cust_1,2024-01-02T00:00:00Z,1,3.25,completed\n",
            ORDER_HEADER.rstrip() + ",new_column\n",
        )
        with self.assertRaisesRegex(SchemaError, "unexpected columns"):
            run_pipeline(DATASETS["orders"], path, self.database)
        run = self._rows(
            "SELECT status, failure_reason FROM pipeline_runs WHERE dataset = 'orders'"
        )[0]
        self.assertEqual(run["status"], "failed")
        self.assertIn("unexpected columns", run["failure_reason"])

        path.write_text(ORDER_HEADER + "ord_1,cust_1,2024-01-02T00:00:00Z,1,3.25,completed\n")
        retry = run_pipeline(DATASETS["orders"], path, self.database)
        self.assertEqual(retry.status, "completed")

    def test_partial_load_failure_rolls_back_and_can_be_retried(self) -> None:
        path = self._orders(
            "ord_1,cust_1,2024-01-02T00:00:00Z,1,3.25,completed\n"
            "ord_2,cust_1,2024-01-03T00:00:00Z,1,4.25,completed\n"
        )
        connection = connect(self.database)
        try:
            connection.execute(
                """
                CREATE TRIGGER reject_second_order BEFORE INSERT ON orders
                WHEN NEW.order_id = 'ord_2'
                BEGIN
                    SELECT RAISE(ABORT, 'simulated load failure');
                END
                """
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(sqlite3.IntegrityError, "simulated load failure"):
            run_pipeline(DATASETS["orders"], path, self.database)
        self.assertEqual(self._rows("SELECT COUNT(*) AS n FROM orders")[0]["n"], 0)
        failed = self._rows(
            "SELECT status, accepted_rows FROM pipeline_runs "
            "WHERE dataset = 'orders' ORDER BY started_at DESC LIMIT 1"
        )[0]
        self.assertEqual((failed["status"], failed["accepted_rows"]), ("failed", 2))

        connection = connect(self.database)
        try:
            connection.execute("DROP TRIGGER reject_second_order")
            connection.commit()
        finally:
            connection.close()
        retried = run_pipeline(DATASETS["orders"], path, self.database)
        self.assertEqual(retried.status, "completed")
        self.assertEqual(self._rows("SELECT COUNT(*) AS n FROM orders")[0]["n"], 2)

    def test_customer_email_is_normalized_and_bad_customer_is_quarantined(self) -> None:
        path = self.root / "more_customers.csv"
        path.write_text(
            CUSTOMER_HEADER
            + "cust_2,UPPER@example.test,2024-01-02T00:00:00+02:00\n"
            + "cust_3,invalid,2024-01-02T00:00:00Z\n",
            encoding="utf-8",
        )
        summary = run_pipeline(DATASETS["customers"], path, self.database)
        self.assertEqual((summary.accepted_rows, summary.rejected_rows), (1, 1))
        customer = self._rows("SELECT email, created_at FROM customers WHERE customer_id='cust_2'")[0]
        self.assertEqual(customer["email"], "upper@example.test")
        self.assertEqual(customer["created_at"], "2024-01-01T22:00:00Z")

    def test_duplicate_customer_is_quarantined_and_upsert_is_idempotent(self) -> None:
        path = self.root / "duplicate_customers.csv"
        path.write_text(
            CUSTOMER_HEADER
            + "cust_2,first@example.test,2024-01-02T00:00:00Z\n"
            + "cust_2,second@example.test,2024-01-03T00:00:00Z\n",
            encoding="utf-8",
        )
        first = run_pipeline(DATASETS["customers"], path, self.database)
        second = run_pipeline(DATASETS["customers"], path, self.database)
        self.assertEqual((first.accepted_rows, first.rejected_rows), (1, 1))
        self.assertEqual((second.accepted_rows, second.rejected_rows), (1, 1))
        self.assertEqual(self._rows("SELECT COUNT(*) AS n FROM customers")[0]["n"], 2)


if __name__ == "__main__":
    unittest.main()
