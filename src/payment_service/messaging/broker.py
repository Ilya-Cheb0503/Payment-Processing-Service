from faststream.rabbit import Channel, RabbitBroker

from payment_service.config import Settings


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            publisher_confirms=True,
            prefetch_count=settings.broker_prefetch_count,
        ),
        graceful_timeout=settings.graceful_timeout_seconds,
        app_id="payment-processing-service",
    )
