from uuid import uuid4

import pytest
from httpx import AsyncClient

from tests.conftest import auth_headers, payment_payload


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_and_get_payment(api_client: AsyncClient) -> None:
    created = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers=auth_headers(idempotency_key="create-1"),
    )
    assert created.status_code == 202, created.text
    body = created.json()
    assert body["status"] == "pending"
    assert "payment_id" in body
    assert body["created_at"].endswith("Z")

    fetched = await api_client.get(
        f"/api/v1/payments/{body['payment_id']}",
        headers=auth_headers(idempotency_key=None),
    )
    assert fetched.status_code == 200
    details = fetched.json()
    assert details["amount"] == "1500.50"
    assert details["currency"] == "RUB"
    assert details["status"] == "pending"
    assert details["metadata"] == {"order_id": "123"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_api_key(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers={"Idempotency-Key": "no-api-key"},
    )
    assert response.status_code == 401
    assert response.json()["code"] == "unauthorized"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_api_key(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers={"X-API-Key": "wrong", "Idempotency-Key": "bad-key"},
    )
    assert response.status_code == 401


@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_idempotency_key(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers=auth_headers(idempotency_key=None),
    )
    assert response.status_code == 400
    assert response.json()["code"] == "validation_error"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_amount(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(amount="0"),
        headers=auth_headers(idempotency_key="bad-amount"),
    )
    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_currency(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=payment_payload(currency="GBP"),
        headers=auth_headers(idempotency_key="bad-currency"),
    )
    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_payment(api_client: AsyncClient) -> None:
    response = await api_client.get(
        f"/api/v1/payments/{uuid4()}",
        headers=auth_headers(idempotency_key=None),
    )
    assert response.status_code == 404
    assert response.json()["code"] == "payment_not_found"
