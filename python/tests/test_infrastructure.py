"""Transport-policy, private file and financial admission regressions."""

import json
import logging
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from api.infrastructure import read_secret, postgres_options, redis_options, production_readiness
from api.logging import SafeFormatter
from api.settings import Settings


def config(**overrides):
    return SimpleNamespace(
        arb_env="production",
        arb_database_url="postgresql://runtime:long-database-password@localhost/a",
        arb_database_sslmode="verify-full",
        arb_database_tls_ca_cert="",
        arb_redis_url="rediss://:long-redis-password@localhost:6379/0",
        arb_redis_tls_ca_cert="",
        **overrides,
    )


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o622, 0o666])
def test_secret_rejects_shared_permissions(tmp_path, mode):
    path = tmp_path / "secret"
    path.write_text("synthetic-private-value")
    path.chmod(mode)
    with pytest.raises(ValueError, match="group and other"):
        read_secret(path)


@pytest.mark.parametrize("value", ["", " leading", "trailing ", "a\nb", "\x00", "a" * 65537])
def test_secret_rejects_malformed_files(tmp_path, value):
    path = tmp_path / "secret"
    path.write_text(value)
    path.chmod(0o600)
    with pytest.raises(ValueError):
        read_secret(path)


def test_secret_rejects_symlink_and_conflicting_sources(tmp_path, monkeypatch):
    path = tmp_path / "secret"
    path.write_text("private-value\n")
    path.chmod(0o600)
    assert read_secret(path) == "private-value"
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(ValueError):
        read_secret(link)
    monkeypatch.setenv("ARB_CONTROL_API_TOKEN_FILE", str(path))
    with pytest.raises(ValidationError, match="conflict"):
        Settings(_env_file=None, arb_control_api_token="inline-value")


@pytest.mark.parametrize("mode", ["disable", "prefer", "allow", "require", "verify-ca"])
def test_production_denies_database_downgrade(mode):
    value = config()
    value.arb_database_sslmode = mode
    with pytest.raises(ValueError):
        postgres_options(value)


def test_database_url_cannot_override_policy(tmp_path):
    value = config()
    path = tmp_path / "ca"
    path.write_text("public-certificate")
    value.arb_database_tls_ca_cert = str(path)
    options = postgres_options(value)
    assert options["sslmode"] == "verify-full" and options["connect_timeout"] == 3
    for query in ("sslmode=disable", "sslmode=verify-full&sslmode=disable", "sslrootcert=attacker"):
        value.arb_database_url = "postgresql://runtime:password@localhost/a?" + query
        with pytest.raises(ValueError):
            postgres_options(value)


@pytest.mark.parametrize(
    "url",
    [
        "redis://:password@localhost/0",
        "rediss://localhost/0",
        "rediss://:password@localhost/0#insecure",
        "rediss://:password@localhost/0?ssl_cert_reqs=none",
    ],
)
def test_redis_denies_insecure_configuration(url):
    value = config()
    value.arb_redis_url = url
    with pytest.raises(ValueError):
        redis_options(value)


def test_admission_requires_fresh_typed_proof_and_restore_barrier(tmp_path):
    value = config()
    value.arb_restore_marker = str(tmp_path / "RESTORE_REQUIRED")
    value.arb_financial_integrity_file = str(tmp_path / "integrity.json")
    value.arb_monitoring_readiness_file = str(tmp_path / "monitoring.json")
    assert not production_readiness(value)[0]
    Path(value.arb_financial_integrity_file).write_text(
        json.dumps({"checked_at": time.time(), "balanced": True})
    )
    for stamp, ready in (
        (time.time() + 60, True),
        (time.time() - 121, True),
        (time.time(), "true"),
        (time.time(), False),
    ):
        Path(value.arb_monitoring_readiness_file).write_text(
            json.dumps({"checked_at": stamp, "ready_for_new_exposure": ready})
        )
        assert not production_readiness(value)[0]
    Path(value.arb_monitoring_readiness_file).write_text(
        json.dumps({"checked_at": time.time(), "ready_for_new_exposure": True})
    )
    assert production_readiness(value)[0]
    Path(value.arb_restore_marker).write_text("{}")
    assert not production_readiness(value)[0]


def test_rendered_exception_logs_redact_synthetic_secret(monkeypatch):
    from api.settings import settings

    secret = "synthetic-secret-" + str(time.time_ns())
    monkeypatch.setattr(settings, "arb_control_api_token", secret)
    record = logging.LogRecord(
        "test", logging.ERROR, "", 1, "request rejected: %s", (secret,), None
    )
    assert secret not in SafeFormatter("%(message)s").format(record)
