from __future__ import annotations

import logging
from typing import Any

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage

from payment_service.config import Settings
from payment_service.exceptions import ProcessingExhaustedError, RetryableProcessingError
from payment_service.messaging.events import DeadLetterPayload, PaymentCreatedEvent
from payment_service.messaging.topology import (
    HEADER_ATTEMPT,
    HEADER_ERROR,
    HEADER_PAYMENT_ID,
    BrokerTopology,
)
from payment_service.services.processing import PaymentProcessor
from payment_service.time import utcnow

logger = logging.getLogger(__name__)


def _attempt(headers: dict[str, Any] | None) -> int:
    raw = (headers or {}).get(HEADER_ATTEMPT, 1)
    try:
        return max(int(raw), 1)
    except (TypeError, ValueError):
        return 1


class PaymentConsumer:
    def __init__(
        self,
        *,
        broker: RabbitBroker,
        topology: BrokerTopology,
        processor: PaymentProcessor,
        settings: Settings,
    ) -> None:
        self._broker = broker
        self._topology = topology
        self._processor = processor
        self._settings = settings

    def register(self) -> None:
        @self._broker.subscriber(
            self._topology.new_queue,
            self._topology.exchange,
            ack_policy=AckPolicy.MANUAL,
        )
        async def handle_payment_created(
            body: PaymentCreatedEvent,
            msg: RabbitMessage,
        ) -> None:
            await self.handle(body, msg)

    async def handle(self, event: PaymentCreatedEvent, msg: RabbitMessage) -> None:
        headers = dict(msg.headers or {})
        attempt = _attempt(headers)
        logger.info(
            "received payment.created",
            extra={"payment_id": str(event.payment_id), "attempt": attempt},
        )
        try:
            await self._processor.process(event)
        except ProcessingExhaustedError as exc:
            await self._publish_dlq(event, attempt=attempt, error=str(exc), headers=headers)
            await msg.ack()
            return
        except RetryableProcessingError as exc:
            await self._retry_or_dlq(event, attempt=attempt, error=str(exc), headers=headers)
            await msg.ack()
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors follow the same retry budget
            logger.exception(
                "unexpected consumer error",
                extra={"payment_id": str(event.payment_id), "attempt": attempt},
            )
            await self._retry_or_dlq(event, attempt=attempt, error=str(exc), headers=headers)
            await msg.ack()
            return
        await msg.ack()

    async def _retry_or_dlq(
        self,
        event: PaymentCreatedEvent,
        *,
        attempt: int,
        error: str,
        headers: dict[str, Any],
    ) -> None:
        if attempt >= self._settings.max_processing_attempts:
            await self._publish_dlq(event, attempt=attempt, error=error, headers=headers)
            return
        routing_key = self._topology.retry_routing_keys[min(attempt, len(self._topology.retry_routing_keys)) - 1]
        next_headers = {
            **headers,
            HEADER_ATTEMPT: attempt + 1,
            HEADER_PAYMENT_ID: str(event.payment_id),
            HEADER_ERROR: error[:500],
        }
        await self._broker.publish(
            event.model_dump(mode="json"),
            exchange=self._topology.exchange,
            routing_key=routing_key,
            persist=True,
            correlation_id=str(event.payment_id),
            headers=next_headers,
        )
        logger.warning(
            "payment processing scheduled for retry",
            extra={
                "payment_id": str(event.payment_id),
                "attempt": attempt,
                "next_attempt": attempt + 1,
                "routing_key": routing_key,
                "error": error,
            },
        )

    async def _publish_dlq(
        self,
        event: PaymentCreatedEvent,
        *,
        attempt: int,
        error: str,
        headers: dict[str, Any],
    ) -> None:
        payload = DeadLetterPayload(
            payment_id=event.payment_id,
            original_event=event.model_dump(mode="json"),
            attempts=attempt,
            last_error=error,
            failed_at=utcnow(),
        )
        await self._broker.publish(
            payload.model_dump(mode="json"),
            exchange=self._topology.exchange,
            routing_key=self._topology.dlq.name,
            persist=True,
            correlation_id=str(event.payment_id),
            headers={
                **headers,
                HEADER_ATTEMPT: attempt,
                HEADER_PAYMENT_ID: str(event.payment_id),
                HEADER_ERROR: error[:500],
            },
        )
        logger.error(
            "payment processing moved to DLQ",
            extra={"payment_id": str(event.payment_id), "attempt": attempt, "error": error},
        )
