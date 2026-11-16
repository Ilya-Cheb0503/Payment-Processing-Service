# Payment Processing Service

Асинхронный микросервис приёма и обработки платежей. API принимает платёж, атомарно сохраняет его вместе с outbox-событием и сразу отвечает `202 Accepted`. Фоновый publisher публикует событие в RabbitMQ только после confirm брокера. Consumer эмулирует платёжный шлюз, фиксирует финальный статус и доставляет webhook.

Система даёт **at-least-once** доставку событий и уведомлений, не exactly-once.

## Стек

- Python 3.12+, FastAPI, Pydantic v2
- SQLAlchemy 2.0 Async + PostgreSQL
- Alembic
- RabbitMQ + FastStream 0.6
- httpx
- Docker Compose
- pytest

## Архитектура

```
src/payment_service/
  api/            HTTP-слой, auth, схемы, обработчики ошибок
  domain/         валюты, статусы, fingerprint идемпотентности
  db/             SQLAlchemy-модели payments и outbox
  repositories/   узкий доступ к данным
  services/       создание платежа, шлюз, webhook, обработка
  messaging/      topology, consumer, transactional outbox publisher
  worker.py       FastStream consumer + outbox publisher
```

API не публикует в брокер напрямую. Это убирает dual-write: платёж и событие живут в одной транзакции PostgreSQL.

## Последовательность обработки

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant DB as PostgreSQL
    participant Outbox as Outbox publisher
    participant MQ as RabbitMQ
    participant Worker as Consumer
    participant GW as Payment gateway
    participant Hook as Webhook

    Client->>API: POST /api/v1/payments
    API->>DB: INSERT payment + outbox
    API-->>Client: 202 Accepted, pending
    Outbox->>MQ: publish payments.new (publisher confirm)
    Outbox->>DB: mark outbox published
    MQ->>Worker: payment.created
    Worker->>DB: lock payment
    Worker->>GW: emulate charge (2-5s)
    Worker->>DB: status succeeded/failed
    Worker->>Hook: POST webhook
    Worker->>DB: webhook delivered
    Worker->>MQ: ack
```

## Outbox

Таблица `outbox` пишется в той же транзакции, что и `payments`. Publisher в процессе consumer:

1. Берёт пачку неопубликованных строк `FOR UPDATE SKIP LOCKED`.
2. Публикует persistent-сообщение с publisher confirms.
3. Ставит `published_at` только после confirm.
4. При недоступности брокера оставляет запись, увеличивает `publish_attempts` и откладывает `available_at`.

После рестарта unpublished-события уходят повторно. Consumer идемпотентен, поэтому повторная публикация безопасна.

## Идемпотентность

- `UNIQUE(idempotency_key)` в PostgreSQL.
- Повтор с тем же ключом и тем же каноническим payload возвращает исходный платёж (`202`).
- Повтор с другим payload даёт `409 Conflict`.
- Fingerprint нормализует Decimal, URL и порядок ключей в `metadata`.
- Гонки обрабатываются через `IntegrityError`, а не через предварительный `SELECT`.
- Для одного платежа создаётся одно событие `payment.created`.
- Повтор RabbitMQ-сообщения не переписывает финальный `succeeded`/`failed`.

## Retry и DLQ

Очереди:

- `payments.new` — основная
- `payments.retry.1` — TTL, затем возврат в `payments.new`
- `payments.retry.2` — больший TTL
- `payments.dlq` — после исчерпания 3 попыток

Сообщения persistent. Исходное сообщение подтверждается только после успешной публикации в retry/DLQ.

Бизнес-результат `failed` — штатный исход шлюза, не retry. Техническая ошибка webhook не меняет статус платежа. После 3 неудачных webhook-попыток уведомление уходит в DLQ, платёж остаётся `succeeded` или `failed`.

Повторный webhook при at-least-once полностью исключить нельзя: получатель должен быть идемпотентным.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

Сервисы: `postgres`, `rabbitmq`, `migrate`, `api`, `consumer`, `webhook-mock`.

- API: http://localhost:8000
- OpenAPI: http://localhost:8000/docs
- RabbitMQ UI: http://localhost:15672 (payments / payments)
- Webhook mock: http://localhost:8080/events

Миграции применяет сервис `migrate` перед стартом API и consumer.

## Примеры запросов

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key-change-me" \
  -H "Idempotency-Key: order-123" \
  -d '{
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Order payment",
    "metadata": {"order_id": "123"},
    "webhook_url": "http://webhook-mock:8080/webhook"
  }'
```

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: dev-api-key-change-me"
```

`webhook_url` для Docker-consumer должен быть доступен из сети compose, поэтому в примере указан `webhook-mock`.

## Webhook payload

```json
{
  "payment_id": "11111111-1111-1111-1111-111111111111",
  "status": "succeeded",
  "amount": "1500.50",
  "currency": "RUB",
  "description": "Order payment",
  "metadata": {"order_id": "123"},
  "processed_at": "2026-11-16T10:00:05Z"
}
```

Успехом считается любой HTTP 2xx.

## Тесты

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Unit-тесты не требуют инфраструктуры. API, outbox, consumer и RabbitMQ-тесты поднимают PostgreSQL/RabbitMQ через Testcontainers, либо используют:

```bash
export TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments
export TEST_RABBITMQ_URL=amqp://payments:payments@localhost:5672/
pytest
```

Smoke после `docker compose up --build`:

```bash
sh scripts/smoke.sh
```

## Решения и компромиссы

- Outbox publisher живёт в процессе consumer, чтобы compose оставался простым: API, consumer, postgres, rabbitmq.
- Retry сделан отдельными TTL-очередями, а не in-memory FastStream retry: счётчик живёт в заголовке `x-attempt` и переживает рестарт.
- Publisher confirms включены явно (`Channel(publisher_confirms=True)`). Событие помечается опубликованным только после confirm.
- Короткий `FOR UPDATE` на платеже удерживается на время эмуляции шлюза (2–5 с). Для тестового сервиса это проще и безопаснее, чем отдельный статус `processing`.
- Webhook отправляется после commit статуса платежа. Если процесс умрёт между commit и HTTP-вызовом, redelivery догонит уведомление и не изменит финансовый результат.
- Аутентификация — статический `X-API-Key` из окружения, без захардкоженного production-секрета.
- Ровно один логический consumer handler. Нет отдельного микросервиса для webhook.

Локальный запуск без Docker: PostgreSQL и RabbitMQ должны быть доступны, затем `alembic upgrade head`, `uvicorn payment_service.api.app:app` и `python -m payment_service.worker`.
