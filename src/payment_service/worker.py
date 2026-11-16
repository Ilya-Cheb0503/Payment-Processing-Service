from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from faststream import ContextRepo, FastStream

from payment_service.config import format_settings, get_settings
from payment_service.db.session import create_engine, create_session_factory
from payment_service.logging import setup_logging
from payment_service.messaging.broker import create_broker
from payment_service.messaging.consumer import PaymentConsumer
from payment_service.messaging.publisher import OutboxPublisher
from payment_service.messaging.topology import build_topology, declare_topology
from payment_service.services.gateway import PaymentGateway
from payment_service.services.processing import PaymentProcessor
from payment_service.services.webhook import WebhookSender

logger = logging.getLogger(__name__)


def create_worker() -> FastStream:
    settings = get_settings()
    setup_logging(settings.log_level)
    broker = create_broker(settings)
    topology = build_topology(settings)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    http_client = httpx.AsyncClient()
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=PaymentGateway(settings),
        webhook_sender=WebhookSender(http_client, settings.webhook_timeout_seconds),
        settings=settings,
    )
    consumer = PaymentConsumer(
        broker=broker,
        topology=topology,
        processor=processor,
        settings=settings,
    )
    consumer.register()
    publisher = OutboxPublisher(
        session_factory=session_factory,
        broker=broker,
        topology=topology,
        settings=settings,
    )

    @asynccontextmanager
    async def lifespan(_context: ContextRepo) -> AsyncIterator[None]:
        print(format_settings(settings), flush=True)
        try:
            yield
        finally:
            await http_client.aclose()
            await engine.dispose()

    app = FastStream(broker, lifespan=lifespan)

    @app.after_startup
    async def on_started() -> None:
        await declare_topology(broker, topology)
        publisher.start()
        logger.info("payment consumer started")

    @app.on_shutdown
    async def on_stopping() -> None:
        await publisher.stop()
        logger.info("payment consumer stopping")

    return app


app = create_worker()


def main() -> None:
    import anyio

    anyio.run(app.run)


if __name__ == "__main__":
    main()
