from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    arb_env: str = "development"
    arb_log_level: str = "INFO"
    arb_api_host: str = "0.0.0.0"
    arb_api_port: int = 8000
    arb_live_trading_enabled: bool = False
    arb_cors_origins: str = "http://localhost:5173"
    arb_control_api_token: str = ""
    arb_control_state_file: str = "data/control/trading_state.json"

    arb_database_url: str = (
        "postgresql+psycopg://arbitrage:arbitrage@localhost:5432/arbitrage"
    )
    arb_opportunity_min_net_bps: float = 0.0
    arb_opportunity_max_gap_ms: int = 2_000
    arb_opportunity_batch_size: int = 250
    arb_shadow_batch_size: int = 250
    arb_micro_live_batch_size: int = 100

    bybit_testnet: bool = True
    bybit_api_key: str = ""
    bybit_api_secret: str = ""

    def cors_origins(self) -> list[str]:
        return [
            value.strip()
            for value in self.arb_cors_origins.split(",")
            if value.strip()
        ]

    def safe_summary(self) -> dict[str, object]:
        return {
            "arb_env": self.arb_env,
            "arb_log_level": self.arb_log_level,
            "arb_live_trading_enabled": self.arb_live_trading_enabled,
            "arb_control_auth_configured": bool(self.arb_control_api_token),
            "arb_control_state_file": self.arb_control_state_file,
            "arb_database_configured": bool(self.arb_database_url),
            "arb_opportunity_min_net_bps": self.arb_opportunity_min_net_bps,
            "arb_opportunity_max_gap_ms": self.arb_opportunity_max_gap_ms,
            "arb_shadow_batch_size": self.arb_shadow_batch_size,
            "arb_micro_live_batch_size": self.arb_micro_live_batch_size,
            "arb_cors_origins": self.cors_origins(),
            "bybit_testnet": self.bybit_testnet,
            "bybit_api_key_configured": bool(self.bybit_api_key),
            "bybit_api_secret_configured": bool(self.bybit_api_secret),
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
