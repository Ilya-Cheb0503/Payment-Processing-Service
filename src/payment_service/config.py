from functools import lru_cache
from urllib.parse import urlparse

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_key: SecretStr = SecretStr("dev-api-key-change-me")
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    log_level: str = "INFO"

    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = 0.9

    webhook_timeout_seconds: float = 5.0
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 50

    retry_ttl_ms_1: int = 1000
    retry_ttl_ms_2: int = 4000
    max_processing_attempts: int = 3
    max_webhook_attempts: int = 3
    rabbit_name_prefix: str = ""

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    graceful_timeout_seconds: float = 30.0
    broker_prefetch_count: int = 16

    @field_validator("gateway_success_rate")
    @classmethod
    def validate_success_rate(cls, value: float) -> float:
        if not 0 <= value <= 1:
            raise ValueError("GATEWAY_SUCCESS_RATE must be between 0 and 1")
        return value

    @field_validator("gateway_max_delay_seconds")
    @classmethod
    def validate_delay_range(cls, value: float, info) -> float:
        min_delay = info.data.get("gateway_min_delay_seconds", 0)
        if value < min_delay:
            raise ValueError("GATEWAY_MAX_DELAY_SECONDS must be >= GATEWAY_MIN_DELAY_SECONDS")
        return value

    @property
    def retry_ttls_ms(self) -> tuple[int, int]:
        return (self.retry_ttl_ms_1, self.retry_ttl_ms_2)


@lru_cache
def get_settings() -> Settings:
    return Settings()


def redact_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.password:
        netloc = parsed.netloc.replace(f":{parsed.password}", ":***")
        return parsed._replace(netloc=netloc).geturl()
    return url


def format_settings(settings: Settings) -> str:
    return "\n".join(
        [
            "Configuration:",
            f"  log_level: {settings.log_level}",
            f"  database_url: {redact_url(settings.database_url)}",
            f"  rabbitmq_url: {redact_url(settings.rabbitmq_url)}",
            "  api_key: ***",
            f"  gateway_delay: {settings.gateway_min_delay_seconds}-{settings.gateway_max_delay_seconds}s",
            f"  gateway_success_rate: {settings.gateway_success_rate}",
            f"  webhook_timeout_seconds: {settings.webhook_timeout_seconds}",
            f"  retry_ttl_ms: {settings.retry_ttls_ms}",
            f"  max_processing_attempts: {settings.max_processing_attempts}",
            f"  max_webhook_attempts: {settings.max_webhook_attempts}",
        ]
    )
