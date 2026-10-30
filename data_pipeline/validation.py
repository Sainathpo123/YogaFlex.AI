from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable

from data_pipeline.models import RecordValidationError

MAX_SQLITE_INTEGER = 2**63 - 1


@dataclass(frozen=True)
class Customer:
    customer_id: str
    email: str
    created_at: datetime


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_id: str
    order_ts: datetime
    quantity: int
    unit_price_cents: int
    status: str


def _required(row: dict[str, str | None], field: str) -> str:
    value = row.get(field)
    if value is None or not value.strip():
        raise RecordValidationError(f"{field} is required")
    return value.strip()


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecordValidationError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecordValidationError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def validate_customer(
    row: dict[str, str | None], _customer_exists: Callable[[str], bool]
) -> Customer:
    customer_id = _required(row, "customer_id")
    email = _required(row, "email").lower()
    if email.count("@") != 1 or "." not in email.rsplit("@", 1)[1]:
        raise RecordValidationError("email must contain a valid address and domain")
    created_at = _timestamp(_required(row, "created_at"), "created_at")
    return Customer(customer_id=customer_id, email=email, created_at=created_at)


def validate_order(
    row: dict[str, str | None], customer_exists: Callable[[str], bool]
) -> Order:
    order_id = _required(row, "order_id")
    customer_id = _required(row, "customer_id")
    if not customer_exists(customer_id):
        raise RecordValidationError(f"unknown customer_id: {customer_id}")

    quantity_text = _required(row, "quantity")
    try:
        quantity = int(quantity_text)
    except ValueError as exc:
        raise RecordValidationError("quantity must be an integer") from exc
    if quantity <= 0:
        raise RecordValidationError("quantity must be greater than zero")
    if quantity > MAX_SQLITE_INTEGER:
        raise RecordValidationError("quantity exceeds the supported integer range")

    price_text = _required(row, "unit_price")
    try:
        price = Decimal(price_text)
    except InvalidOperation as exc:
        raise RecordValidationError("unit_price must be a decimal amount") from exc
    if not price.is_finite() or price < 0:
        raise RecordValidationError("unit_price must be a finite, non-negative amount")
    if price > Decimal(MAX_SQLITE_INTEGER) / 100:
        raise RecordValidationError("unit_price exceeds the supported cents range")
    try:
        rounded_price = price.quantize(Decimal("0.01"))
    except InvalidOperation as exc:
        raise RecordValidationError("unit_price exceeds the supported precision") from exc
    if price != rounded_price:
        raise RecordValidationError("unit_price must have no more than two decimal places")
    unit_price_cents = int(price * 100)
    if quantity > MAX_SQLITE_INTEGER // max(unit_price_cents, 1):
        raise RecordValidationError("order total exceeds the supported integer range")

    status = _required(row, "status").lower()
    if status not in {"pending", "completed", "cancelled"}:
        raise RecordValidationError("status must be pending, completed, or cancelled")

    return Order(
        order_id=order_id,
        customer_id=customer_id,
        order_ts=_timestamp(_required(row, "order_ts"), "order_ts"),
        quantity=quantity,
        unit_price_cents=unit_price_cents,
        status=status,
    )
