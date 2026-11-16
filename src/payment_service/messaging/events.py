from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from payment_service.time import utcnow

EVENT_PAYMENT_CREATED = "payment.created"


class PaymentCreatedEvent(BaseModel):
    event_type: Literal["payment.created"] = EVENT_PAYMENT_CREATED
    payment_id: UUID
    created_at: datetime


class DeadLetterPayload(BaseModel):
    event_type: Literal["payment.dead_letter"] = "payment.dead_letter"
    payment_id: UUID
    original_event: dict[str, Any]
    attempts: int
    last_error: str
    failed_at: datetime = Field(default_factory=utcnow)
