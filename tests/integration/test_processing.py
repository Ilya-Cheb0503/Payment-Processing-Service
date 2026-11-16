from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus, WebhookStatus
from payment_service.exceptions import ProcessingExhaustedError, RetryableProcessingError
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.services.gateway import GatewayResult, PaymentGateway
from payment_service.services.processing import PaymentProcessor
from payment_service.services.webhook import WebhookSender
from payment_service.time import utcnow


class FixedGateway(PaymentGateway):
    def __init__(self, status: PaymentStatus, calls: list[str]) -> None:
        self._status = status
        self.calls = calls

    async def charge(self, payment: Payment) -> GatewayResult:
        self.calls.append(str(payment.id))
        return GatewayResult(self._status)


async def _insert_payment(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    status: str = PaymentStatus.PENDING.value,
    webhook_status: str = WebhookStatus.PENDING.value,
    webhook_attempts: int = 0,
) -> Payment:
    payment = Payment(
        id=uuid4(),
        amount=Decimal("25.00"),
        currency="RUB",
        description="processing",
        payment_metadata={"order_id": "p1"},
        status=status,
        idempotency_key=str(uuid4()),
        request_fingerprint="abc",
        webhook_url="https://merchant.test/webhook",
        webhook_status=webhook_status,
        webhook_attempts=webhook_attempts,
        processed_at=None if status == PaymentStatus.PENDING.value else utcnow(),
    )
    async with session_factory() as session:
        session.add(payment)
        await session.commit()
        await session.refresh(payment)
        return payment


def _processor(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    gateway: PaymentGateway,
    handler,
) -> PaymentProcessor:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    return PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway,
        webhook_sender=WebhookSender(client, timeout_seconds=1),
        settings=settings,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_successful_processing_and_webhook(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    payment = await _insert_payment(session_factory)
    received: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200, json={"ok": True})

    calls: list[str] = []
    processor = _processor(session_factory, settings, FixedGateway(PaymentStatus.SUCCEEDED, calls), handler)
    await processor.process(PaymentCreatedEvent(payment_id=payment.id, created_at=payment.created_at))

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    assert stored is not None
    assert stored.status == PaymentStatus.SUCCEEDED.value
    assert stored.processed_at is not None
    assert stored.webhook_status == WebhookStatus.DELIVERED.value
    assert len(received) == 1
    assert calls == [str(payment.id)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_business_failure_is_not_an_infra_error(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    payment = await _insert_payment(session_factory)
    processor = _processor(
        session_factory,
        settings,
        FixedGateway(PaymentStatus.FAILED, []),
        lambda request: httpx.Response(200, json={"ok": True}),
    )
    await processor.process(PaymentCreatedEvent(payment_id=payment.id, created_at=payment.created_at))
    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    assert stored is not None
    assert stored.status == PaymentStatus.FAILED.value
    assert stored.webhook_status == WebhookStatus.DELIVERED.value


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redelivery_does_not_change_final_status(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    payment = await _insert_payment(session_factory, status=PaymentStatus.SUCCEEDED.value)
    calls: list[str] = []
    processor = _processor(
        session_factory,
        settings,
        FixedGateway(PaymentStatus.FAILED, calls),
        lambda request: httpx.Response(200, json={"ok": True}),
    )
    await processor.process(PaymentCreatedEvent(payment_id=payment.id, created_at=payment.created_at))
    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    assert stored is not None
    assert stored.status == PaymentStatus.SUCCEEDED.value
    assert calls == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_crash_after_payment_commit_retries_webhook_only(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    payment = await _insert_payment(
        session_factory,
        status=PaymentStatus.SUCCEEDED.value,
        webhook_status=WebhookStatus.PENDING.value,
    )
    calls: list[str] = []
    processor = _processor(
        session_factory,
        settings,
        FixedGateway(PaymentStatus.FAILED, calls),
        lambda request: httpx.Response(200, json={"ok": True}),
    )
    await processor.process(PaymentCreatedEvent(payment_id=payment.id, created_at=payment.created_at))
    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    assert stored is not None
    assert stored.status == PaymentStatus.SUCCEEDED.value
    assert stored.webhook_status == WebhookStatus.DELIVERED.value
    assert calls == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_webhook_http_500_is_retryable_and_does_not_fail_payment(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    payment = await _insert_payment(session_factory)
    processor = _processor(
        session_factory,
        settings,
        FixedGateway(PaymentStatus.SUCCEEDED, []),
        lambda request: httpx.Response(500, json={"error": True}),
    )
    with pytest.raises(RetryableProcessingError):
        await processor.process(PaymentCreatedEvent(payment_id=payment.id, created_at=payment.created_at))
    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    assert stored is not None
    assert stored.status == PaymentStatus.SUCCEEDED.value
    assert stored.webhook_attempts == 1
    assert stored.webhook_status == WebhookStatus.PENDING.value


@pytest.mark.integration
@pytest.mark.asyncio
async def test_webhook_retry_exhaustion_goes_to_failed_webhook_status(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    payment = await _insert_payment(
        session_factory,
        status=PaymentStatus.SUCCEEDED.value,
        webhook_attempts=2,
    )
    processor = _processor(
        session_factory,
        settings,
        FixedGateway(PaymentStatus.SUCCEEDED, []),
        lambda request: httpx.Response(500, json={"error": True}),
    )
    with pytest.raises(ProcessingExhaustedError):
        await processor.process(PaymentCreatedEvent(payment_id=payment.id, created_at=payment.created_at))
    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    assert stored is not None
    assert stored.status == PaymentStatus.SUCCEEDED.value
    assert stored.webhook_status == WebhookStatus.FAILED.value
    assert stored.webhook_attempts == 3
