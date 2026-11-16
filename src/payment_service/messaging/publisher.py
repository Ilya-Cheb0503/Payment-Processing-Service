from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.db.models import OutboxEvent
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.messaging.topology import (
    HEADER_ATTEMPT,
    HEADER_PAYMENT_ID,
    BrokerTopology,
)
from payment_service.repositories.outbox import OutboxRepository
from payment_service.time import utcnow

logger = logging.getLogger(__name__)


def publish_backoff(attempts: int) -> timedelta:
    seconds = min(2 ** min(attempts, 6), 60)
    return timedelta(seconds=seconds)


class OutboxPublisher:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        topology: BrokerTopology,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._topology = topology
        self._settings = settings
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="outbox-publisher")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def publish_available(self) -> int:
        async with self._session_factory() as session:
            async with session.begin():
                repo = OutboxRepository(session)
                events = await repo.lock_unpublished_batch(
                    now=utcnow(),
                    limit=self._settings.outbox_batch_size,
                )
                published = 0
                for event in events:
                    try:
                        await self._publish_event(event)
                        event.published_at = utcnow()
                        event.last_error = None
                        published += 1
                    except Exception as exc:  # noqa: BLE001 - broker errors must stay in outbox
                        event.publish_attempts += 1
                        event.last_error = str(exc)[:2000]
                        event.available_at = utcnow() + publish_backoff(event.publish_attempts)
                        logger.warning(
                            "outbox publish failed",
                            extra={
                                "outbox_id": str(event.id),
                                "payment_id": str(event.aggregate_id),
                                "attempts": event.publish_attempts,
                                "error": str(exc),
                            },
                        )
                return published

    async def _publish_event(self, event: OutboxEvent) -> None:
        payload = PaymentCreatedEvent.model_validate(event.payload)
        await self._broker.publish(
            payload.model_dump(mode="json"),
            exchange=self._topology.exchange,
            routing_key=self._topology.new_queue.name,
            persist=True,
            correlation_id=str(payload.payment_id),
            message_id=str(event.id),
            headers={
                HEADER_ATTEMPT: 1,
                HEADER_PAYMENT_ID: str(payload.payment_id),
            },
        )
        logger.info(
            "outbox event published",
            extra={"outbox_id": str(event.id), "payment_id": str(payload.payment_id)},
        )

    async def _run(self) -> None:
        logger.info("outbox publisher started")
        while not self._stop.is_set():
            try:
                published = await self.publish_available()
            except Exception:
                logger.exception("outbox publisher loop failed")
                published = 0
            if published == 0:
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self._settings.outbox_poll_interval_seconds,
                    )
                except TimeoutError:
                    continue
        logger.info("outbox publisher stopped")
