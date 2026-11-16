from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus
from payment_service.services.webhook import WebhookDeliveryError, WebhookSender, build_webhook_payload
from payment_service.time import utcnow


def _payment(url: str = "https://merchant.test/webhook") -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Order payment",
        payment_metadata={"order_id": "123"},
        status=PaymentStatus.SUCCEEDED.value,
        idempotency_key="k",
        request_fingerprint="f",
        webhook_url=url,
        processed_at=utcnow(),
    )


@pytest.mark.asyncio
async def test_webhook_success() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/webhook"
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = WebhookSender(client, timeout_seconds=1)
        await sender.send(_payment())


@pytest.mark.asyncio
async def test_webhook_http_500() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(500, json={"error": True}))
    async with httpx.AsyncClient(transport=transport) as client:
        sender = WebhookSender(client, timeout_seconds=1)
        with pytest.raises(WebhookDeliveryError, match="HTTP 500"):
            await sender.send(_payment())


@pytest.mark.asyncio
async def test_webhook_timeout() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timeout")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = WebhookSender(client, timeout_seconds=0.1)
        with pytest.raises(WebhookDeliveryError, match="timeout"):
            await sender.send(_payment())


def test_webhook_payload_shape() -> None:
    payment = _payment()
    payload = build_webhook_payload(payment)
    assert payload["payment_id"] == str(payment.id)
    assert payload["status"] == "succeeded"
    assert payload["amount"] == "1500.50"
    assert payload["metadata"] == {"order_id": "123"}
    assert payload["processed_at"].endswith("Z")
