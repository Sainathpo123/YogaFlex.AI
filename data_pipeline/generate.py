from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_datasets(output_dir: Path, rows: int = 100_000, seed: int = 42) -> None:
    if rows <= 0:
        raise ValueError("rows must be greater than zero")
    output_dir.mkdir(parents=True, exist_ok=True)
    randomizer = random.Random(seed)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with (output_dir / "customers.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(("customer_id", "email", "created_at"))
        for index in range(rows):
            customer_id = f"customer_{index + 1:08d}"
            writer.writerow(
                (
                    customer_id,
                    f"user{index + 1}@example.test",
                    (start + timedelta(minutes=index)).isoformat().replace("+00:00", "Z"),
                )
            )

    with (output_dir / "orders.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            ("order_id", "customer_id", "order_ts", "quantity", "unit_price", "status")
        )
        for index in range(rows):
            order_time = start + timedelta(minutes=randomizer.randrange(rows * 60))
            cents = randomizer.randrange(0, 100_001)
            writer.writerow(
                (
                    f"order_{index + 1:08d}",
                    f"customer_{randomizer.randrange(rows) + 1:08d}",
                    order_time.isoformat().replace("+00:00", "Z"),
                    randomizer.randrange(1, 6),
                    f"{cents // 100}.{cents % 100:02d}",
                    randomizer.choice(("pending", "completed", "cancelled")),
                )
            )
