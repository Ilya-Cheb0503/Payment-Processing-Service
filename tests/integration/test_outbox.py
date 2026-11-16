from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain.enums import PaymentStatus
from payment_service.messaging.events import EVENT_PAYMENT_CREATED
from payment_service.messaging.publisher import OutboxPublisher, publish_backoff
from payment_service.messaging.topology import build_topology
from payment_service.services.payments import PaymentService
from payment_service.time import utcnow


class RecordingBroker:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.published: list[dict] = []

    async def publish(self, body: object, **kwargs: object) -> None:
        if self.error:
            raise self.error
        self.published.append({"body": body, **kwargs})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_payment_and_outbox_are_written_atomically(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        service = PaymentService(session)
        payment = await service.create_payment(
            amount=Decimal("10.00"),
            currency="RUB",
            description="atomic",
            metadata={"k": "v"},
            webhook_url="https://example.com/hook",
            idempotency_key="atomic-key",
        )
        await session.commit()
        payment_id = payment.id

    async with session_factory() as session:
        stored = await session.get(Payment, payment_id)
        event = await session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment_id))
        assert stored is not None
        assert stored.status == PaymentStatus.PENDING.value
        assert event is not None
        assert event.event_type == EVENT_PAYMENT_CREATED
        assert event.published_at is None
        assert event.payload["payment_id"] == str(payment_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failed_publish_keeps_outbox_event(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    async with session_factory() as session:
        service = PaymentService(session)
        payment = await service.create_payment(
            amount=Decimal("11.00"),
            currency="USD",
            description="broker down",
            metadata={},
            webhook_url="https://example.com/hook",
            idempotency_key="broker-down",
        )
        await session.commit()
        payment_id = payment.id

    broker = RecordingBroker(error=ConnectionError("broker unavailable"))
    publisher = OutboxPublisher(
        session_factory=session_factory,
        broker=broker,  # type: ignore[arg-type]
        topology=build_topology(settings),
        settings=settings,
    )
    published = await publisher.publish_available()
    assert published == 0

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment_id))
        assert event is not None
        assert event.published_at is None
        assert event.publish_attempts == 1
        assert event.last_error is not None
        assert event.available_at > utcnow() - timedelta(seconds=1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_after_broker_recovers(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    async with session_factory() as session:
        service = PaymentService(session)
        payment = await service.create_payment(
            amount=Decimal("12.00"),
            currency="EUR",
            description="recover",
            metadata={},
            webhook_url="https://example.com/hook",
            idempotency_key="recover-key",
        )
        await session.commit()
        payment_id = payment.id

    failing = RecordingBroker(error=ConnectionError("down"))
    publisher = OutboxPublisher(
        session_factory=session_factory,
        broker=failing,  # type: ignore[arg-type]
        topology=build_topology(settings),
        settings=settings,
    )
    await publisher.publish_available()

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment_id))
        assert event is not None
        event.available_at = utcnow()
        await session.commit()

    recovered = RecordingBroker()
    publisher = OutboxPublisher(
        session_factory=session_factory,
        broker=recovered,  # type: ignore[arg-type]
        topology=build_topology(settings),
        settings=settings,
    )
    assert await publisher.publish_available() == 1
    assert recovered.published[0]["routing_key"] == build_topology(settings).new_queue.name
    assert recovered.published[0]["persist"] is True

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment_id))
        assert event is not None
        assert event.published_at is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_already_published_event_is_not_republished(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    async with session_factory() as session:
        payment_id = uuid4()
        session.add(
            OutboxEvent(
                id=uuid4(),
                event_type=EVENT_PAYMENT_CREATED,
                aggregate_id=payment_id,
                payload={"event_type": "payment.created", "payment_id": str(payment_id), "created_at": utcnow().isoformat()},
                published_at=utcnow(),
                available_at=utcnow(),
            )
        )
        await session.commit()

    broker = RecordingBroker()
    publisher = OutboxPublisher(
        session_factory=session_factory,
        broker=broker,  # type: ignore[arg-type]
        topology=build_topology(settings),
        settings=settings,
    )
    assert await publisher.publish_available() == 0
    assert broker.published == []


def test_publish_backoff_grows() -> None:
    assert publish_backoff(1).total_seconds() == 2
    assert publish_backoff(3).total_seconds() == 8
    assert publish_backoff(20).total_seconds() == 60
