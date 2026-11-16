from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service import __version__
from payment_service.api.errors import register_exception_handlers
from payment_service.api.routes import health_router, payments_router
from payment_service.config import Settings, format_settings, get_settings
from payment_service.db.session import create_engine, create_session_factory
from payment_service.logging import setup_logging


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    engine: AsyncEngine | None = None,
) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        setup_logging(settings.log_level)
        print(format_settings(settings), flush=True)
        owned_engine = engine or create_engine(settings)
        app.state.settings = settings
        app.state.engine = owned_engine
        app.state.session_factory = session_factory or create_session_factory(owned_engine)
        try:
            yield
        finally:
            if engine is None:
                await owned_engine.dispose()

    app = FastAPI(
        title="Payment Processing Service",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = settings
    if session_factory is not None:
        app.state.session_factory = session_factory
    if engine is not None:
        app.state.engine = engine
    register_exception_handlers(app)
    app.include_router(health_router)
    app.include_router(payments_router)
    return app


app = create_app()
