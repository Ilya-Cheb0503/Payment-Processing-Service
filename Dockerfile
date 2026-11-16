FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
COPY scripts ./scripts

RUN pip install --no-cache-dir . \
    && chmod +x /app/scripts/entrypoint.sh /app/scripts/smoke.sh

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["uvicorn", "payment_service.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
