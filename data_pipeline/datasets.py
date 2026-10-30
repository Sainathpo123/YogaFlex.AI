from __future__ import annotations

from data_pipeline.models import DatasetDefinition
from data_pipeline.storage import customer_keys
from data_pipeline.transform import transform_customer, transform_order
from data_pipeline.validation import validate_customer, validate_order


DATASETS = {
    "customers": DatasetDefinition(
        name="customers",
        expected_headers=("customer_id", "email", "created_at"),
        key_column="customer_id",
        target_table="customers",
        validate=validate_customer,
        transform=transform_customer,
    ),
    "orders": DatasetDefinition(
        name="orders",
        expected_headers=(
            "order_id",
            "customer_id",
            "order_ts",
            "quantity",
            "unit_price",
            "status",
        ),
        key_column="order_id",
        target_table="orders",
        validate=validate_order,
        transform=transform_order,
        reference_keys=customer_keys,
    ),
}
