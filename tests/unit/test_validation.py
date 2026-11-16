from decimal import Decimal

import pytest
from pydantic import ValidationError

from payment_service.api.schemas import PaymentCreateRequest


def test_valid_request() -> None:
    payload = PaymentCreateRequest(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Order payment",
        metadata={"order_id": "123"},
        webhook_url="https://example.com/webhook",
    )
    assert payload.amount == Decimal("1500.50")


@pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-1"), Decimal("1.001")])
def test_invalid_amount(amount: Decimal) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest(
            amount=amount,
            currency="RUB",
            description="Order payment",
            webhook_url="https://example.com/webhook",
        )


def test_invalid_currency() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest(
            amount=Decimal("10.00"),
            currency="GBP",
            description="Order payment",
            webhook_url="https://example.com/webhook",
        )


def test_invalid_webhook_url() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest(
            amount=Decimal("10.00"),
            currency="USD",
            description="Order payment",
            webhook_url="not-a-url",
        )
