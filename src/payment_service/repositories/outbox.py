from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def count_for_aggregate(self, aggregate_id: UUID, event_type: str) -> int:
        stmt = select(OutboxEvent).where(
            OutboxEvent.aggregate_id == aggregate_id,
            OutboxEvent.event_type == event_type,
        )
        rows = (await self._session.scalars(stmt)).all()
        return len(rows)

    async def lock_unpublished_batch(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> list[OutboxEvent]:
        stmt = (
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.available_at <= now,
            )
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(await self._session.scalars(stmt))
