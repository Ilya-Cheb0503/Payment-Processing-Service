from collections.abc import AsyncIterator

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import Settings
from payment_service.exceptions import UnauthorizedError, ValidationAppError
from payment_service.services.payments import PaymentService


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def require_api_key(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    expected = request.app.state.settings.api_key.get_secret_value()
    if x_api_key is None or x_api_key == "":
        raise UnauthorizedError("X-API-Key header is required")
    if not _safe_equals(x_api_key, expected):
        raise UnauthorizedError("Invalid API key")


def require_idempotency_key(
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> str:
    if idempotency_key is None or not idempotency_key.strip():
        raise ValidationAppError("Idempotency-Key header is required")
    key = idempotency_key.strip()
    if len(key) > 128:
        raise ValidationAppError("Idempotency-Key must be at most 128 characters")
    return key


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_payment_service(session: AsyncSession = Depends(get_session)) -> PaymentService:
    return PaymentService(session)


def _safe_equals(left: str, right: str) -> bool:
    if len(left) != len(right):
        return False
    result = 0
    for left_char, right_char in zip(left, right, strict=True):
        result |= ord(left_char) ^ ord(right_char)
    return result == 0
