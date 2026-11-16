class AppError(Exception):
    status_code = 500
    code = "internal_error"

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class UnauthorizedError(AppError):
    status_code = 401
    code = "unauthorized"


class PaymentNotFound(AppError):
    status_code = 404
    code = "payment_not_found"


class IdempotencyConflict(AppError):
    status_code = 409
    code = "idempotency_conflict"


class ValidationAppError(AppError):
    status_code = 400
    code = "validation_error"


class RetryableProcessingError(Exception):
    """Infrastructure failure that should be retried via RabbitMQ."""


class ProcessingExhaustedError(Exception):
    """Retry budget exhausted; the message must go to DLQ."""
