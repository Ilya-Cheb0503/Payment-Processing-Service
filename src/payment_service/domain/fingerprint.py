from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any


def _normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize(value[key]) for key in sorted(value, key=lambda item: str(item))}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, Decimal):
        return format(value.quantize(Decimal("0.01")), "f")
    return value


def canonical_request_payload(
    *,
    amount: Decimal,
    currency: str,
    description: str,
    metadata: dict[str, Any] | None,
    webhook_url: str,
) -> dict[str, Any]:
    return {
        "amount": format(amount.quantize(Decimal("0.01")), "f"),
        "currency": str(currency),
        "description": description,
        "metadata": _normalize(metadata or {}),
        "webhook_url": str(webhook_url).rstrip("/"),
    }


def request_fingerprint(
    *,
    amount: Decimal,
    currency: str,
    description: str,
    metadata: dict[str, Any] | None,
    webhook_url: str,
) -> str:
    payload = canonical_request_payload(
        amount=amount,
        currency=currency,
        description=description,
        metadata=metadata,
        webhook_url=webhook_url,
    )
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
