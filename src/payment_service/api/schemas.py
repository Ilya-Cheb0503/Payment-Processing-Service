from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_serializer, field_validator

from payment_service.domain.enums import Currency, PaymentStatus, WebhookStatus

MAX_AMOUNT = Decimal("9999999999.99")


class PaymentCreateRequest(BaseModel):
    amount: Decimal
    currency: Currency
    description: str = Field(min_length=1, max_length=512)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, value: Decimal) -> Decimal:
        try:
            quantized = value.quantize(Decimal("0.01"))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("amount must be a valid decimal with at most 2 fraction digits") from exc
        if quantized != value:
            raise ValueError("amount must have at most 2 decimal places")
        if quantized <= 0:
            raise ValueError("amount must be greater than 0")
        if quantized > MAX_AMOUNT:
            raise ValueError("amount is too large")
        return quantized


class PaymentCreatedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")


class PaymentDetailsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    webhook_url: str
    webhook_status: WebhookStatus
    created_at: datetime
    processed_at: datetime | None

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), "f")

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")

    @field_serializer("processed_at")
    def serialize_processed_at(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat().replace("+00:00", "Z")


class ErrorResponse(BaseModel):
    code: str
    message: str


class HealthResponse(BaseModel):
    status: str
