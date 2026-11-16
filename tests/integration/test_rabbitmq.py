from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from faststream.rabbit import RabbitMessage

from payment_service.exceptions import RetryableProcessingError
from payment_service.messaging.broker import create_broker
from payment_service.messaging.consumer import PaymentConsumer
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.messaging.topology import (
    HEADER_ATTEMPT,
    PAYMENTS_DLQ,
    PAYMENTS_NEW_QUEUE,
    build_topology,
    declare_topology,
)
from payment_service.time import utcnow
from tests.conftest import make_settings


class CountingProcessor:
    def __init__(self) -> None:
        self.calls = 0

    async def process(self, event: PaymentCreatedEvent) -> None:
        self.calls += 1
        raise RetryableProcessingError("forced retry")


class RecordingDlq:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.event = asyncio.Event()

    async def handle(self, body: dict, msg: RabbitMessage) -> None:
        self.messages.append(body)
        await msg.ack()
        self.event.set()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_message_is_retried_three_times_then_dead_lettered(rabbitmq_url: str) -> None:
    settings = make_settings(
        rabbitmq_url=rabbitmq_url,
        retry_ttl_ms_1=150,
        retry_ttl_ms_2=150,
        max_processing_attempts=3,
        rabbit_name_prefix=f"t{uuid4().hex[:8]}.",
    )
    broker = create_broker(settings)
    topology = build_topology(settings)
    processor = CountingProcessor()
    consumer = PaymentConsumer(
        broker=broker,
        topology=topology,
        processor=processor,  # type: ignore[arg-type]
        settings=settings,
    )
    consumer.register()
    dlq = RecordingDlq()

    @broker.subscriber(topology.dlq, topology.exchange)
    async def on_dlq(body: dict, msg: RabbitMessage) -> None:
        await dlq.handle(body, msg)

    event = PaymentCreatedEvent(payment_id=uuid4(), created_at=utcnow())
    try:
        await broker.connect()
        await declare_topology(broker, topology)
        await broker.start()
        await broker.publish(
            event.model_dump(mode="json"),
            exchange=topology.exchange,
            routing_key=topology.new_queue.name,
            persist=True,
            headers={HEADER_ATTEMPT: 1},
            correlation_id=str(event.payment_id),
        )
        await asyncio.wait_for(dlq.event.wait(), timeout=15)
    finally:
        await broker.stop()

    assert processor.calls == 3
    assert len(dlq.messages) == 1
    assert dlq.messages[0]["payment_id"] == str(event.payment_id)
    assert dlq.messages[0]["attempts"] == 3
    assert dlq.messages[0]["event_type"] == "payment.dead_letter"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_new_events_are_routed_to_payments_new(rabbitmq_url: str) -> None:
    settings = make_settings(rabbitmq_url=rabbitmq_url, rabbit_name_prefix=f"r{uuid4().hex[:8]}.")
    broker = create_broker(settings)
    topology = build_topology(settings)
    received = asyncio.Event()
    seen: list[str] = []

    @broker.subscriber(topology.new_queue, topology.exchange)
    async def on_new(body: PaymentCreatedEvent, msg: RabbitMessage) -> None:
        seen.append(str(body.payment_id))
        await msg.ack()
        received.set()

    event = PaymentCreatedEvent(payment_id=uuid4(), created_at=utcnow())
    try:
        await broker.connect()
        await declare_topology(broker, topology)
        await broker.start()
        await broker.publish(
            event.model_dump(mode="json"),
            exchange=topology.exchange,
            routing_key=topology.new_queue.name,
            persist=True,
        )
        await asyncio.wait_for(received.wait(), timeout=5)
    finally:
        await broker.stop()

    assert seen == [str(event.payment_id)]
    assert PAYMENTS_NEW_QUEUE == "payments.new"
    assert PAYMENTS_DLQ == "payments.dlq"
