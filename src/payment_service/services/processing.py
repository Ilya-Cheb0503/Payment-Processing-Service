from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus, WebhookStatus
from payment_service.exceptions import ProcessingExhaustedError, RetryableProcessingError
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.repositories.payments import PaymentRepository
from payment_service.services.gateway import PaymentGateway
from payment_service.services.webhook import WebhookDeliveryError, WebhookSender
from payment_service.time import utcnow

logger = logging.getLogger(__name__)


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._settings = settings

    async def process(self, event: PaymentCreatedEvent) -> None:
        payment = await self._finalize_payment(event.payment_id)
        await self._deliver_webhook(payment.id)

    async def _finalize_payment(self, payment_id: UUID) -> Payment:
        async with self._session_factory() as session:
            async with session.begin():
                repo = PaymentRepository(session)
                payment = await repo.get_by_id(payment_id, for_update=True)
                if payment is None:
                    raise RetryableProcessingError(f"payment {payment_id} is not visible yet")
                if payment.status != PaymentStatus.PENDING.value:
                    logger.info(
                        "payment already finalized",
                        extra={"payment_id": str(payment.id), "status": payment.status},
                    )
                    return payment
                result = await self._gateway.charge(payment)
                payment.status = result.status.value
                payment.processed_at = utcnow()
                logger.info(
                    "payment processed",
                    extra={"payment_id": str(payment.id), "status": payment.status},
                )
                return payment

    async def _deliver_webhook(self, payment_id: UUID) -> None:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get_by_id(payment_id)
            if payment is None:
                raise RetryableProcessingError(f"payment {payment_id} is not visible yet")
            if payment.webhook_status == WebhookStatus.DELIVERED.value:
                logger.info("webhook already delivered", extra={"payment_id": str(payment.id)})
                return
            if payment.status == PaymentStatus.PENDING.value:
                raise RetryableProcessingError("payment is still pending")

        try:
            await self._webhook_sender.send(payment)
        except WebhookDeliveryError as exc:
            await self._register_webhook_failure(payment_id, str(exc))
            return

        async with self._session_factory() as session:
            async with session.begin():
                stored = await PaymentRepository(session).get_by_id(payment_id, for_update=True)
                if stored is None:
                    raise RetryableProcessingError(f"payment {payment_id} is not visible yet")
                if stored.webhook_status == WebhookStatus.DELIVERED.value:
                    return
                stored.webhook_status = WebhookStatus.DELIVERED.value
                stored.webhook_delivered_at = utcnow()
                stored.webhook_last_error = None
        logger.info("webhook delivered", extra={"payment_id": str(payment_id)})

    async def _register_webhook_failure(self, payment_id: UUID, error: str) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                payment = await PaymentRepository(session).get_by_id(payment_id, for_update=True)
                if payment is None:
                    raise RetryableProcessingError(f"payment {payment_id} is not visible yet")
                if payment.webhook_status == WebhookStatus.DELIVERED.value:
                    return
                payment.webhook_attempts += 1
                payment.webhook_last_error = error[:2000]
                attempts = payment.webhook_attempts
                if attempts >= self._settings.max_webhook_attempts:
                    payment.webhook_status = WebhookStatus.FAILED.value
        if attempts >= self._settings.max_webhook_attempts:
            logger.error(
                "webhook delivery exhausted",
                extra={"payment_id": str(payment_id), "attempts": attempts, "error": error},
            )
            raise ProcessingExhaustedError(error)
        logger.warning(
            "webhook delivery failed",
            extra={"payment_id": str(payment_id), "attempts": attempts, "error": error},
        )
        raise RetryableProcessingError(error)
