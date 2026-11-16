from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass

from payment_service.config import Settings
from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus


@dataclass(frozen=True, slots=True)
class GatewayResult:
    status: PaymentStatus


class PaymentGateway:
    """Emulated payment gateway. Tests replace this class with a deterministic fake."""

    def __init__(
        self,
        settings: Settings,
        *,
        rng: random.Random | None = None,
    ) -> None:
        self._settings = settings
        self._rng = rng or random.Random()

    async def charge(self, payment: Payment) -> GatewayResult:
        delay = self._rng.uniform(
            self._settings.gateway_min_delay_seconds,
            self._settings.gateway_max_delay_seconds,
        )
        if delay > 0:
            await asyncio.sleep(delay)
        succeeded = self._rng.random() < self._settings.gateway_success_rate
        return GatewayResult(PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED)
