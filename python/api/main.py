from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from api.autonomous_paper import router as autonomous_paper_router
from api.control import router as control_router
from api.event_consumer import EngineEventConsumer
from api.logging import configure_logging
from api.micro_live import router as micro_live_analytics_router
from api.opportunities import router as opportunity_analytics_router
from api.operations import router as operations_router
from api.paper_trading import router as paper_trading_router
from api.performance_analytics import router as performance_analytics_router
from api.settings import settings
from api.security import SecurityMiddleware
from api.shadow import router as shadow_analytics_router

configure_logging(settings.arb_log_level)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if settings.arb_env == "production":
        from analytics.db import get_engine
        from sqlalchemy import text
        def restricted_role():
            with get_engine().connect() as connection:
                unsafe = connection.execute(text("SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM pg_roles WHERE rolname=current_user")).scalar_one()
                if unsafe:
                    raise RuntimeError("production API requires a restricted PostgreSQL runtime role")
        await asyncio.to_thread(restricted_role)
    consumer = EngineEventConsumer()
    task = asyncio.create_task(
        consumer.run(),
        name="engine-event-consumer",
    )
    try:
        yield
    finally:
        await consumer.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


app = FastAPI(
    title="Arbitrage Bot API",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None if settings.arb_env == "production" else "/docs",
    redoc_url=None if settings.arb_env == "production" else "/redoc",
    openapi_url=None if settings.arb_env == "production" else "/openapi.json",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
app.add_middleware(SecurityMiddleware)
if settings.arb_env == "production":
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.arb_trusted_hosts.split(","))


@app.get("/livez")
def livez():
    return {"status": "alive"}

app.include_router(opportunity_analytics_router)
app.include_router(paper_trading_router)
app.include_router(autonomous_paper_router)
app.include_router(performance_analytics_router)
app.include_router(shadow_analytics_router)
app.include_router(micro_live_analytics_router)
app.include_router(operations_router)
app.include_router(control_router)
