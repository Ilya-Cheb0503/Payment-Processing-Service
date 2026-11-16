from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from payment_service.api.app import create_app
from payment_service.config import Settings
from payment_service.db.base import Base
from payment_service.db.models import OutboxEvent, Payment


def postgres_url_from_container(raw_url: str) -> str:
    return (
        raw_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")
        .replace("postgresql+psycopg://", "postgresql+asyncpg://")
        .replace("postgresql://", "postgresql+asyncpg://")
    )


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "api_key": "test-api-key",
        "database_url": "postgresql+asyncpg://payments:payments@localhost:5432/payments",
        "rabbitmq_url": "amqp://payments:payments@localhost:5672/",
        "gateway_min_delay_seconds": 0,
        "gateway_max_delay_seconds": 0,
        "gateway_success_rate": 1.0,
        "webhook_timeout_seconds": 1.0,
        "outbox_poll_interval_seconds": 0.05,
        "retry_ttl_ms_1": 200,
        "retry_ttl_ms_2": 400,
        "max_processing_attempts": 3,
        "max_webhook_attempts": 3,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[arg-type]


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    env_url = os.getenv("TEST_DATABASE_URL")
    if env_url:
        yield env_url
        return
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError as exc:
        pytest.skip(f"PostgreSQL is not configured: {exc}")
    try:
        container = PostgresContainer("postgres:16-alpine")
        container.start()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker/PostgreSQL is unavailable: {exc}")
    try:
        yield postgres_url_from_container(container.get_connection_url())
    finally:
        container.stop()


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    env_url = os.getenv("TEST_RABBITMQ_URL")
    if env_url:
        yield env_url
        return
    try:
        from testcontainers.rabbitmq import RabbitMqContainer
    except ImportError as exc:
        pytest.skip(f"RabbitMQ is not configured: {exc}")
    try:
        container = RabbitMqContainer("rabbitmq:3.13-management-alpine")
        container.start()
        params = container.get_connection_params()
        import time

        import pika

        last_error: Exception | None = None
        for _ in range(30):
            try:
                connection = pika.BlockingConnection(params)
                connection.close()
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time.sleep(1)
        if last_error is not None:
            raise last_error
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker/RabbitMQ is unavailable: {exc}")
    try:
        params = container.get_connection_params()
        vhost = "" if container.vhost == "/" else container.vhost
        yield f"amqp://{container.username}:{container.password}@{params.host}:{params.port}/{vhost}"
    finally:
        container.stop()


@pytest.fixture
def settings(postgres_dsn: str) -> Settings:
    return make_settings(database_url=postgres_dsn)


@pytest.fixture
async def engine(postgres_dsn: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_dsn, pool_pre_ping=True, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    async with engine.begin() as conn:
        await conn.execute(OutboxEvent.__table__.delete())
        await conn.execute(Payment.__table__.delete())
    yield async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@pytest.fixture
async def api_client(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=settings, session_factory=session_factory, engine=engine)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def auth_headers(api_key: str = "test-api-key", idempotency_key: str | None = "key-1") -> dict[str, str]:
    headers = {"X-API-Key": api_key}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    return headers


def payment_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order payment",
        "metadata": {"order_id": "123"},
        "webhook_url": "https://example.com/webhook",
    }
    payload.update(overrides)
    return payload


def sample_amount() -> Decimal:
    return Decimal("1500.50")
