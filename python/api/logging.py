"""Structured logs and final rendered-output redaction, including third-party exceptions."""

import logging
import sys
from urllib.parse import unquote, urlsplit

import structlog


def redact(value):
    from api.settings import settings

    secrets = []
    for name in (
        "arb_database_url",
        "arb_redis_url",
        "arb_control_api_token",
        "arb_engine_grpc_token",
        "arb_metrics_token",
        "bybit_api_key",
        "bybit_api_secret",
    ):
        secret = getattr(settings, name, "")
        if secret and len(secret) >= 8:
            secrets.append(secret)
            try:
                password = urlsplit(secret).password
            except ValueError:
                password = None
            if password:
                secrets.extend((password, unquote(password)))
    if isinstance(value, str):
        for secret in sorted(secrets, key=len, reverse=True):
            value = value.replace(secret, "[REDACTED]")
    elif isinstance(value, dict):
        value = {key: redact(item) for key, item in value.items()}
    elif isinstance(value, (list, tuple)):
        value = [redact(item) for item in value]
    return value


class SafeFormatter(logging.Formatter):
    def format(self, record):
        return redact(super().format(record))


def configure_logging(level="INFO"):
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, level.upper(), logging.INFO))
    for logger in (
        logging.getLogger(),
        logging.getLogger("uvicorn"),
        logging.getLogger("uvicorn.error"),
    ):
        for handler in logger.handlers:
            handler.setFormatter(SafeFormatter("%(message)s"))
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            lambda _logger, _method, event: redact(event),
            structlog.processors.JSONRenderer(),
        ]
    )
