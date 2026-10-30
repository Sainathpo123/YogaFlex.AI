from __future__ import annotations

from data_pipeline.models import TargetRecord
from data_pipeline.validation import Customer, Order


def _iso_utc(value) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def transform_customer(customer: object) -> TargetRecord:
    if not isinstance(customer, Customer):
        raise TypeError("transform_customer requires a validated Customer")
    return {
        "customer_id": customer.customer_id,
        "email": customer.email,
        "created_at": _iso_utc(customer.created_at),
    }


def transform_order(order: object) -> TargetRecord:
    if not isinstance(order, Order):
        raise TypeError("transform_order requires a validated Order")
    return {
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "order_ts": _iso_utc(order.order_ts),
        "quantity": order.quantity,
        "unit_price_cents": order.unit_price_cents,
        "total_amount_cents": order.quantity * order.unit_price_cents,
        "status": order.status,
    }
