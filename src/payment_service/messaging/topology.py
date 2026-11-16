from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

from payment_service.config import Settings

PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_NEW_QUEUE = "payments.new"
PAYMENTS_DLQ = "payments.dlq"
PAYMENTS_RETRY_1 = "payments.retry.1"
PAYMENTS_RETRY_2 = "payments.retry.2"

ROUTING_NEW = PAYMENTS_NEW_QUEUE
ROUTING_RETRY_1 = PAYMENTS_RETRY_1
ROUTING_RETRY_2 = PAYMENTS_RETRY_2
ROUTING_DLQ = PAYMENTS_DLQ

HEADER_ATTEMPT = "x-attempt"
HEADER_PAYMENT_ID = "x-payment-id"
HEADER_ERROR = "x-last-error"


@dataclass(frozen=True, slots=True)
class BrokerTopology:
    exchange: RabbitExchange
    new_queue: RabbitQueue
    retry_queues: tuple[RabbitQueue, RabbitQueue]
    dlq: RabbitQueue
    retry_routing_keys: tuple[str, str]


def _name(prefix: str, value: str) -> str:
    return f"{prefix}{value}"


def build_topology(settings: Settings) -> BrokerTopology:
    prefix = settings.rabbit_name_prefix
    exchange_name = _name(prefix, PAYMENTS_EXCHANGE_NAME)
    new_name = _name(prefix, PAYMENTS_NEW_QUEUE)
    retry_1 = _name(prefix, PAYMENTS_RETRY_1)
    retry_2 = _name(prefix, PAYMENTS_RETRY_2)
    dlq_name = _name(prefix, PAYMENTS_DLQ)
    exchange = RabbitExchange(
        exchange_name,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    new_queue = RabbitQueue(
        new_name,
        durable=True,
        routing_key=new_name,
    )
    retry_queues = (
        RabbitQueue(
            retry_1,
            durable=True,
            routing_key=retry_1,
            arguments={
                "x-message-ttl": settings.retry_ttl_ms_1,
                "x-dead-letter-exchange": exchange_name,
                "x-dead-letter-routing-key": new_name,
            },
        ),
        RabbitQueue(
            retry_2,
            durable=True,
            routing_key=retry_2,
            arguments={
                "x-message-ttl": settings.retry_ttl_ms_2,
                "x-dead-letter-exchange": exchange_name,
                "x-dead-letter-routing-key": new_name,
            },
        ),
    )
    dlq = RabbitQueue(
        dlq_name,
        durable=True,
        routing_key=dlq_name,
    )
    return BrokerTopology(
        exchange=exchange,
        new_queue=new_queue,
        retry_queues=retry_queues,
        dlq=dlq,
        retry_routing_keys=(retry_1, retry_2),
    )


async def declare_topology(broker, topology: BrokerTopology) -> None:
    exchange = await broker.declare_exchange(topology.exchange)
    for queue in (topology.new_queue, *topology.retry_queues, topology.dlq):
        declared = await broker.declare_queue(queue)
        routing_key = queue.name
        bind = getattr(declared, "bind", None)
        if bind is not None:
            await bind(exchange, routing_key=routing_key)
