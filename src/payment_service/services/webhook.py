from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

import httpx

from payment_service.db.models import Payment
from payment_service.time import utcnow


class WebhookDeliveryError(Exception):
    pass


def build_webhook_payload(payment: Payment) -> dict[str, Any]:
    amount = format(Decimal(payment.amount).quantize(Decimal("0.01")), "f")
    processed_at = payment.processed_at.isoformat().replace("+00:00", "Z") if payment.processed_at else None
    return {
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": amount,
        "currency": payment.currency,
        "description": payment.description,
        "metadata": payment.payment_metadata,
        "processed_at": processed_at,
    }


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, timeout_seconds: float) -> None:
        self._client = client
        self._timeout_seconds = timeout_seconds

    async def send(self, payment: Payment) -> None:
        payload = build_webhook_payload(payment)
        try:
            response = await self._client.post(
                payment.webhook_url,
                json=payload,
                timeout=self._timeout_seconds,
                headers={"Content-Type": "application/json"},
            )
        except httpx.TimeoutException as exc:
            raise WebhookDeliveryError(f"webhook timeout: {exc}") from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"webhook connection error: {exc}") from exc
        if response.status_code >= 300:
            raise WebhookDeliveryError(f"webhook HTTP {response.status_code}")

    @staticmethod
    def empty_payload(payment_id: UUID) -> dict[str, Any]:
        return {
            "payment_id": str(payment_id),
            "status": "unknown",
            "processed_at": utcnow().isoformat().replace("+00:00", "Z"),
        }
