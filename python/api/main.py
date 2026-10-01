from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.control import router as control_router
from api.event_consumer import EngineEventConsumer
from api.logging import configure_logging
from api.micro_live import router as micro_live_analytics_router
from api.opportunities import router as opportunity_analytics_router
from api.operations import router as operations_router
from api.paper_trading import router as paper_trading_router
from api.settings import settings
from api.shadow import router as shadow_analytics_router

configure_logging(settings.arb_log_level)


@asynccontextmanager
async def lifespan(_app: FastAPI):
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
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(opportunity_analytics_router)
app.include_router(paper_trading_router)
app.include_router(shadow_analytics_router)
app.include_router(micro_live_analytics_router)
app.include_router(operations_router)
app.include_router(control_router)
