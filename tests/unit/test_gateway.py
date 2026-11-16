import random
from decimal import Decimal
from uuid import uuid4

import pytest

from payment_service.config import Settings
from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus
from payment_service.services.gateway import PaymentGateway


def _payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.00"),
        currency="RUB",
        description="test",
        payment_metadata={},
        idempotency_key="k",
        request_fingerprint="f",
        webhook_url="https://example.com/hook",
    )


@pytest.mark.asyncio
async def test_gateway_success_is_deterministic_with_seed() -> None:
    settings = Settings(
        _env_file=None,
        gateway_min_delay_seconds=0,
        gateway_max_delay_seconds=0,
        gateway_success_rate=1.0,
    )
    gateway = PaymentGateway(settings, rng=random.Random(0))
    result = await gateway.charge(_payment())
    assert result.status is PaymentStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_gateway_can_fail() -> None:
    settings = Settings(
        _env_file=None,
        gateway_min_delay_seconds=0,
        gateway_max_delay_seconds=0,
        gateway_success_rate=0.0,
    )
    gateway = PaymentGateway(settings, rng=random.Random(0))
    result = await gateway.charge(_payment())
    assert result.status is PaymentStatus.FAILED
