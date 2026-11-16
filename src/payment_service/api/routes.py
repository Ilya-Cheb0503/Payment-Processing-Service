from uuid import UUID

from fastapi import APIRouter, Depends, status

from payment_service.api.deps import get_payment_service, require_api_key, require_idempotency_key
from payment_service.api.schemas import (
    ErrorResponse,
    HealthResponse,
    PaymentCreatedResponse,
    PaymentCreateRequest,
    PaymentDetailsResponse,
)
from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus, WebhookStatus
from payment_service.services.payments import PaymentService

payments_router = APIRouter(
    prefix="/api/v1",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)
health_router = APIRouter(tags=["health"])


def _details(payment: Payment) -> PaymentDetailsResponse:
    return PaymentDetailsResponse(
        id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        status=PaymentStatus(payment.status),
        webhook_url=payment.webhook_url,
        webhook_status=WebhookStatus(payment.webhook_status),
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


@payments_router.post(
    "/payments",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreatedResponse,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
    },
)
async def create_payment(
    payload: PaymentCreateRequest,
    idempotency_key: str = Depends(require_idempotency_key),
    service: PaymentService = Depends(get_payment_service),
) -> PaymentCreatedResponse:
    payment = await service.create_payment(
        amount=payload.amount,
        currency=payload.currency.value,
        description=payload.description,
        metadata=payload.metadata,
        webhook_url=str(payload.webhook_url),
        idempotency_key=idempotency_key,
    )
    return PaymentCreatedResponse(
        payment_id=payment.id,
        status=PaymentStatus(payment.status),
        created_at=payment.created_at,
    )


@payments_router.get(
    "/payments/{payment_id}",
    response_model=PaymentDetailsResponse,
    responses={
        401: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
async def get_payment(
    payment_id: UUID,
    service: PaymentService = Depends(get_payment_service),
) -> PaymentDetailsResponse:
    payment = await service.get_payment(payment_id)
    return _details(payment)


@health_router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


@health_router.get("/ready", response_model=HealthResponse)
async def ready(service: PaymentService = Depends(get_payment_service)) -> HealthResponse:
    await service.ping()
    return HealthResponse(status="ok")
