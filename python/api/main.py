from fastapi import FastAPI

from api.logging import configure_logging
from api.opportunities import router as opportunity_analytics_router
from api.paper_trading import router as paper_trading_router
from api.settings import settings

configure_logging(settings.arb_log_level)

app = FastAPI(title="Arbitrage Bot API", version="0.1.0")
app.include_router(opportunity_analytics_router)
app.include_router(paper_trading_router)


@app.get("/health")
def health() -> dict[str, str | bool]:
    return {
        "status": "ok",
        "environment": settings.arb_env,
        "live_trading_enabled": settings.arb_live_trading_enabled,
    }
