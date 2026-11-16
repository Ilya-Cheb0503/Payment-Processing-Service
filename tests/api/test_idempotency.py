import asyncio

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.models import OutboxEvent, Payment
from tests.conftest import auth_headers, payment_payload


@pytest.mark.integration
@pytest.mark.asyncio
async def test_replay_same_key_same_body(api_client: AsyncClient) -> None:
    first = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers=auth_headers(idempotency_key="same-key"),
    )
    second = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers=auth_headers(idempotency_key="same-key"),
    )
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_same_key_different_body_conflicts(api_client: AsyncClient) -> None:
    first = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers=auth_headers(idempotency_key="conflict-key"),
    )
    second = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(amount="10.00"),
        headers=auth_headers(idempotency_key="conflict-key"),
    )
    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json()["code"] == "idempotency_conflict"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_same_key_creates_one_payment_and_one_outbox(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async def create() -> int:
        response = await api_client.post(
            "/api/v1/payments",
            json=payment_payload(metadata={"order_id": "concurrent"}),
            headers=auth_headers(idempotency_key="concurrent-key"),
        )
        return response.status_code, response.json()

    results = await asyncio.gather(*[create() for _ in range(8)])
    statuses = {status for status, _ in results}
    assert statuses <= {202, 409}
    payment_ids = {body["payment_id"] for status, body in results if status == 202}
    assert len(payment_ids) == 1

    async with session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(Payment))
        events = await session.scalar(select(func.count()).select_from(OutboxEvent))
    assert payments == 1
    assert events == 1
