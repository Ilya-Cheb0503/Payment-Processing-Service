from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain.enums import PaymentStatus, WebhookStatus
from payment_service.domain.fingerprint import request_fingerprint
from payment_service.exceptions import IdempotencyConflict, PaymentNotFound
from payment_service.messaging.events import EVENT_PAYMENT_CREATED, PaymentCreatedEvent
from payment_service.repositories.outbox import OutboxRepository
from payment_service.repositories.payments import PaymentRepository
from payment_service.time import utcnow

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create_payment(
        self,
        *,
        amount: Decimal,
        currency: str,
        description: str,
        metadata: dict[str, Any] | None,
        webhook_url: str,
        idempotency_key: str,
    ) -> Payment:
        fingerprint = request_fingerprint(
            amount=amount,
            currency=currency,
            description=description,
            metadata=metadata,
            webhook_url=webhook_url,
        )
        payment = Payment(
            id=uuid4(),
            amount=amount,
            currency=currency,
            description=description,
            payment_metadata=metadata or {},
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            webhook_url=str(webhook_url),
            webhook_status=WebhookStatus.PENDING.value,
            created_at=utcnow(),
        )
        event = OutboxEvent(
            id=uuid4(),
            event_type=EVENT_PAYMENT_CREATED,
            aggregate_id=payment.id,
            payload=PaymentCreatedEvent(
                payment_id=payment.id,
                created_at=payment.created_at,
            ).model_dump(mode="json"),
            created_at=payment.created_at,
            available_at=payment.created_at,
        )
        self._payments.add(payment)
        self._outbox.add(event)
        try:
            await self._session.flush()
        except IntegrityError:
            await self._session.rollback()
            return await self._resolve_idempotent_conflict(
                idempotency_key=idempotency_key,
                fingerprint=fingerprint,
            )
        logger.info(
            "payment created",
            extra={"payment_id": str(payment.id), "idempotency_key": idempotency_key},
        )
        return payment

    async def ping(self) -> None:
        await self._session.connection()

    async def get_payment(self, payment_id: UUID) -> Payment:
        payment = await self._payments.get_by_id(payment_id)
        if payment is None:
            raise PaymentNotFound(f"Payment {payment_id} was not found")
        return payment

    async def _resolve_idempotent_conflict(self, *, idempotency_key: str, fingerprint: str) -> Payment:
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is None:
            raise IdempotencyConflict("Idempotency key conflict, but original payment was not found")
        if existing.request_fingerprint != fingerprint:
            logger.info(
                "idempotency conflict",
                extra={"payment_id": str(existing.id), "idempotency_key": idempotency_key},
            )
            raise IdempotencyConflict(
                "Idempotency-Key was reused with a different request payload"
            )
        logger.info(
            "idempotent replay",
            extra={"payment_id": str(existing.id), "idempotency_key": idempotency_key},
        )
        return existing
