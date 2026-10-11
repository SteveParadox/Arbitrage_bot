from functools import lru_cache
from pathlib import Path
import json
import os
import re
from urllib.parse import unquote, urlsplit

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, model_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    arb_env: str = "development"
    arb_log_level: str = "INFO"
    arb_api_host: str = "0.0.0.0"
    arb_api_port: int = 8000
    arb_live_trading_enabled: bool = False
    arb_trading_mode: str = "observe"
    arb_paper_trading_enabled: bool = False
    arb_cors_origins: str = "http://localhost:5173"
    arb_control_api_token: str = ""
    arb_paper_api_keys_json: str = "{}"
    arb_api_rate_limit_per_minute: int = Field(default=0, ge=0, le=10000)
    arb_api_max_body_bytes: int = Field(default=65536, ge=1024, le=1048576)
    arb_trusted_hosts: str = "localhost,127.0.0.1,testserver"
    arb_risk_config: str = "shared/config/risk.json"
    arb_control_state_file: str = "data/control/trading_state.json"
    arb_control_state_max_age_seconds: int = Field(default=3600, ge=1, le=86400)
    arb_health_event_max_age_ms: int = Field(default=15000, ge=1000, le=60000)
    arb_market_data_max_age_ms: int = Field(default=10000, ge=1, le=60000)
    arb_market_required_symbols: str = "BTCUSDT,ETHUSDT,ETHBTC"
    arb_reconciliation_lock_timeout_ms: int = Field(default=2000, ge=1, le=10000)
    arb_engine_grpc_target: str = "127.0.0.1:50051"
    arb_engine_grpc_token: str = ""
    arb_engine_grpc_timeout_seconds: float = 2.0
    arb_engine_grpc_max_retries: int = 2
    arb_engine_grpc_retry_initial_seconds: float = 0.1
    arb_engine_grpc_retry_max_seconds: float = 1.0
    arb_redis_url: str = "redis://127.0.0.1:6379/0"
    arb_event_stream: str = "arb.events"
    arb_event_consumer_group: str = "python-api"
    arb_event_dead_letter_stream: str = "arb.events.dlq"
    arb_event_batch_size: int = 100
    arb_event_retry_seconds: float = 2.0

    arb_database_url: str = "postgresql+psycopg://arbitrage:arbitrage@localhost:5432/arbitrage"
    arb_opportunity_min_net_bps: float = 0.0
    arb_opportunity_max_gap_ms: int = 2_000
    arb_opportunity_batch_size: int = 250
    arb_shadow_batch_size: int = 250
    arb_micro_live_batch_size: int = 100
    arb_performance_sample_limit: int = 100_000

    bybit_testnet: bool = True
    bybit_api_key: str = ""
    bybit_api_secret: str = ""

    @model_validator(mode="before")
    @classmethod
    def secret_files(cls, values):
        values = dict(values)
        for field in ("arb_database_url", "arb_redis_url", "arb_control_api_token",
                      "arb_engine_grpc_token", "arb_paper_api_keys_json"):
            source = os.getenv(field.upper()+"_FILE")
            if not source:
                continue
            if values.get(field):
                raise ValueError("inline and file-based secret sources conflict")
            path = Path(source)
            if path.stat().st_size > 65536 or path.stat().st_mode & 0o022:
                raise ValueError("secret file is oversized or writable by another user")
            values[field] = path.read_text().strip()
        return values

    @model_validator(mode="after")
    def validate_security(self):
        if self.arb_env not in {"development", "test", "production"}:
            raise ValueError("unknown environment")
        if len(self.arb_paper_api_keys_json) > 65536:
            raise ValueError("paper credential catalog too large")
        grants = json.loads(self.arb_paper_api_keys_json)
        if not isinstance(grants, dict) or len(grants) > 1000:
            raise ValueError("invalid paper credential catalog")
        for digest, grant in grants.items():
            if not re.fullmatch(r"[0-9a-f]{64}", digest) or not isinstance(grant, dict):
                raise ValueError("paper credentials require SHA-256 keys")
            if set(grant) != {"subject", "role", "accounts"} or grant["role"] not in {"viewer", "operator", "admin"}:
                raise ValueError("invalid paper credential permissions")
            if not isinstance(grant["subject"], str) or not 1 <= len(grant["subject"]) <= 128:
                raise ValueError("invalid paper credential subject")
            accounts = grant["accounts"]
            if not isinstance(accounts, list) or not 1 <= len(accounts) <= 1000 or any(
                not isinstance(a, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", a) for a in accounts
            ):
                raise ValueError("invalid paper account permissions")
        if self.arb_env == "production":
            if self.arb_live_trading_enabled or os.getenv("ARB_LIVE_TRADING_ENABLED") != "false":
                raise ValueError("production paper deployment requires explicit live=false")
            if self.arb_trading_mode not in {"observe", "paper"}:
                raise ValueError("unsupported production execution mode")
            tokens = (self.arb_control_api_token, self.arb_engine_grpc_token)
            if any(len(t.encode()) < 32 or any(word in t.lower() for word in ("example", "changeme", "test-token")) for t in tokens) or tokens[0] == tokens[1]:
                raise ValueError("production requires distinct strong control credentials")
            if self.bybit_api_key or self.bybit_api_secret:
                raise ValueError("paper deployment must not contain private exchange credentials")
            if any(os.getenv(k) for k in ("ARB_OBSERVER_REPLAY_FILE", "ARB_PAPER_CRASH_AT", "ARB_PAPER_CRASH_AFTER_LEG", "ARB_TEST_GRPC_RESPONSE_DELAY_MS")) or os.getenv("ARB_REPLAY_CLOCK") == "virtual":
                raise ValueError("development replay and fault hooks forbidden in production")
            if self.arb_api_rate_limit_per_minute <= 0:
                raise ValueError("production requires distributed request limits")
            if not self.cors_origins() or any(not origin.startswith("https://") or "*" in origin for origin in self.cors_origins()):
                raise ValueError("production requires explicit HTTPS origins")
            if not self.arb_trusted_hosts.strip() or "*" in self.arb_trusted_hosts:
                raise ValueError("production requires explicit trusted hosts")
            for url in (self.arb_database_url.replace("postgresql+psycopg:", "postgresql:"), self.arb_redis_url):
                if len(unquote(urlsplit(url).password or "")) < 16:
                    raise ValueError("production requires authenticated data services")
        return self

    def cors_origins(self) -> list[str]:
        return [value.strip() for value in self.arb_cors_origins.split(",") if value.strip()]

    def safe_summary(self) -> dict[str, object]:
        return {
            "arb_env": self.arb_env,
            "arb_log_level": self.arb_log_level,
            "arb_live_trading_enabled": self.arb_live_trading_enabled,
            "arb_control_auth_configured": bool(self.arb_control_api_token),
            "arb_control_state_file": self.arb_control_state_file,
            "arb_engine_grpc_target": self.arb_engine_grpc_target,
            "arb_engine_grpc_max_retries": self.arb_engine_grpc_max_retries,
            "arb_engine_grpc_auth_configured": (
                len(self.arb_engine_grpc_token.encode("utf-8")) >= 32
            ),
            "arb_event_stream": self.arb_event_stream,
            "arb_event_dead_letter_stream": self.arb_event_dead_letter_stream,
            "arb_database_configured": bool(self.arb_database_url),
            "arb_opportunity_min_net_bps": self.arb_opportunity_min_net_bps,
            "arb_opportunity_max_gap_ms": self.arb_opportunity_max_gap_ms,
            "arb_shadow_batch_size": self.arb_shadow_batch_size,
            "arb_micro_live_batch_size": self.arb_micro_live_batch_size,
            "arb_performance_sample_limit": self.arb_performance_sample_limit,
            "arb_cors_origins": self.cors_origins(),
            "bybit_testnet": self.bybit_testnet,
            "bybit_api_key_configured": bool(self.bybit_api_key),
            "bybit_api_secret_configured": bool(self.bybit_api_secret),
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
