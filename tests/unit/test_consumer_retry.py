from uuid import uuid4

import pytest

from payment_service.config import Settings
from payment_service.exceptions import ProcessingExhaustedError, RetryableProcessingError
from payment_service.messaging.consumer import PaymentConsumer
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.messaging.topology import HEADER_ATTEMPT, ROUTING_DLQ, ROUTING_RETRY_1, ROUTING_RETRY_2, build_topology
from payment_service.time import utcnow


class FakeMessage:
    def __init__(self, headers: dict[str, object]) -> None:
        self.headers = headers
        self.acked = False

    async def ack(self) -> None:
        self.acked = True


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[tuple[object, dict]] = []

    async def publish(self, body: object, **kwargs: object) -> None:
        self.published.append((body, kwargs))


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def process(self, event: PaymentCreatedEvent) -> None:
        self.calls += 1
        if self.error:
            raise self.error


def _event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(payment_id=uuid4(), created_at=utcnow())


def _consumer(processor: FakeProcessor, broker: FakeBroker) -> PaymentConsumer:
    settings = Settings(
        _env_file=None,
        max_processing_attempts=3,
        retry_ttl_ms_1=1000,
        retry_ttl_ms_2=4000,
    )
    return PaymentConsumer(
        broker=broker,  # type: ignore[arg-type]
        topology=build_topology(settings),
        processor=processor,  # type: ignore[arg-type]
        settings=settings,
    )


@pytest.mark.asyncio
async def test_success_acks_without_retry() -> None:
    broker = FakeBroker()
    consumer = _consumer(FakeProcessor(), broker)
    msg = FakeMessage({HEADER_ATTEMPT: 1})
    await consumer.handle(_event(), msg)  # type: ignore[arg-type]
    assert msg.acked
    assert broker.published == []


@pytest.mark.asyncio
async def test_retryable_error_goes_to_retry_queue() -> None:
    broker = FakeBroker()
    consumer = _consumer(FakeProcessor(RetryableProcessingError("boom")), broker)
    msg = FakeMessage({HEADER_ATTEMPT: 1})
    await consumer.handle(_event(), msg)  # type: ignore[arg-type]
    assert msg.acked
    assert len(broker.published) == 1
    _, kwargs = broker.published[0]
    assert kwargs["routing_key"] == ROUTING_RETRY_1
    assert kwargs["persist"] is True
    assert kwargs["headers"][HEADER_ATTEMPT] == 2


@pytest.mark.asyncio
async def test_second_failure_uses_second_retry_queue() -> None:
    broker = FakeBroker()
    consumer = _consumer(FakeProcessor(RetryableProcessingError("boom")), broker)
    msg = FakeMessage({HEADER_ATTEMPT: 2})
    await consumer.handle(_event(), msg)  # type: ignore[arg-type]
    _, kwargs = broker.published[0]
    assert kwargs["routing_key"] == ROUTING_RETRY_2
    assert kwargs["headers"][HEADER_ATTEMPT] == 3


@pytest.mark.asyncio
async def test_third_failure_goes_to_dlq() -> None:
    broker = FakeBroker()
    consumer = _consumer(FakeProcessor(RetryableProcessingError("boom")), broker)
    event = _event()
    msg = FakeMessage({HEADER_ATTEMPT: 3})
    await consumer.handle(event, msg)  # type: ignore[arg-type]
    body, kwargs = broker.published[0]
    assert kwargs["routing_key"] == ROUTING_DLQ
    assert body["payment_id"] == str(event.payment_id)
    assert body["attempts"] == 3
    assert "boom" in body["last_error"]


@pytest.mark.asyncio
async def test_exhausted_error_goes_to_dlq_immediately() -> None:
    broker = FakeBroker()
    consumer = _consumer(FakeProcessor(ProcessingExhaustedError("webhook failed")), broker)
    msg = FakeMessage({HEADER_ATTEMPT: 1})
    await consumer.handle(_event(), msg)  # type: ignore[arg-type]
    _, kwargs = broker.published[0]
    assert kwargs["routing_key"] == ROUTING_DLQ


@pytest.mark.asyncio
async def test_no_infinite_retry_after_limit() -> None:
    broker = FakeBroker()
    consumer = _consumer(FakeProcessor(RetryableProcessingError("boom")), broker)
    msg = FakeMessage({HEADER_ATTEMPT: 3})
    await consumer.handle(_event(), msg)  # type: ignore[arg-type]
    assert all(kwargs["routing_key"] != ROUTING_RETRY_1 for _, kwargs in broker.published)
    assert all(kwargs["routing_key"] != ROUTING_RETRY_2 for _, kwargs in broker.published)
