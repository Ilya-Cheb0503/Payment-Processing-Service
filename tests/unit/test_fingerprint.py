from decimal import Decimal

from payment_service.domain.fingerprint import request_fingerprint


def test_fingerprint_normalizes_decimal_and_metadata_order() -> None:
    first = request_fingerprint(
        amount=Decimal("1500.5"),
        currency="RUB",
        description="Order payment",
        metadata={"b": 2, "a": 1},
        webhook_url="https://example.com/webhook/",
    )
    second = request_fingerprint(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Order payment",
        metadata={"a": 1, "b": 2},
        webhook_url="https://example.com/webhook",
    )
    assert first == second


def test_fingerprint_changes_when_payload_changes() -> None:
    base = dict(
        amount=Decimal("10.00"),
        currency="USD",
        description="x",
        metadata={"order_id": "1"},
        webhook_url="https://example.com/hook",
    )
    original = request_fingerprint(**base)
    changed = request_fingerprint(**{**base, "amount": Decimal("10.01")})
    assert original != changed
