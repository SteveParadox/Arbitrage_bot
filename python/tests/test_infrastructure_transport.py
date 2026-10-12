"""Real TLS endpoints. Opt in only against local disposable fixtures, never Bybit."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
from types import SimpleNamespace
import uuid

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
import grpc
import psycopg
import pytest
from redis import Redis

from api.infrastructure import postgres_options, redis_options
from api.grpc import engine_control_pb2, engine_control_pb2_grpc

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.getenv("ARB_RUN_TLS_INTEGRATION") != "1",
    reason="requires local PostgreSQL/Redis tools and built Rust workspace",
)


def port():
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return server.getsockname()[1]


def certificates(directory, expired=False):
    authority = rsa.generate_private_key(65537, 2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "isolated-test-ca")])
    now = datetime.now(UTC)
    ca = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(authority.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=10))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(authority, hashes.SHA256())
    )
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "ca.pem").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    for role in ("server", "client"):
        key = rsa.generate_private_key(65537, 2048)
        builder = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=2))
            .not_valid_after(now - timedelta(hours=1) if expired else now + timedelta(days=2))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .add_extension(
                x509.ExtendedKeyUsage(
                    [
                        ExtendedKeyUsageOID.SERVER_AUTH
                        if role == "server"
                        else ExtendedKeyUsageOID.CLIENT_AUTH
                    ]
                ),
                critical=False,
            )
        )
        certificate = builder.sign(authority, hashes.SHA256())
        (directory / f"{role}.pem").write_bytes(
            certificate.public_bytes(serialization.Encoding.PEM)
        )
        (directory / f"{role}.key").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        (directory / f"{role}.key").chmod(0o600)
    return directory


def wait(check, timeout=15):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            if check():
                return
        except Exception:
            pass
        time.sleep(0.05)
    raise AssertionError("local TLS fixture failed startup")


@pytest.fixture(scope="module")
def services(tmp_path_factory):
    directory = tmp_path_factory.mktemp("tls-infra")
    ca = certificates(directory / "trusted")
    wrong = certificates(directory / "wrong")
    expired = certificates(directory / "expired", True)
    pg_port, redis_port = port(), port()
    pg_bin = Path(os.environ.get("ARB_TEST_PG_BIN", "/usr/lib/postgresql/16/bin"))
    pg_share = os.environ.get("ARB_TEST_PG_SHARE", "/usr/share/postgresql/16")
    pg_env = dict(os.environ)
    if preload := os.getenv("ARB_TEST_PG_PRELOAD"):
        pg_env["LD_PRELOAD"] = preload
    elif os.getuid() == 0:
        pytest.fail(
            "run these tests as a non-root OS user, or supply the explicitly test-only sandbox shim"
        )
    data = directory / "pg"
    subprocess.run(
        [
            str(pg_bin / "initdb"),
            "-D",
            str(data),
            "-L",
            pg_share,
            "-A",
            "trust",
            "-U",
            "audit",
            "--no-locale",
            "--encoding=UTF8",
        ],
        env=pg_env,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    (data / "pg_hba.conf").write_text(
        "hostnossl all all 0.0.0.0/0 reject\nhostssl all all 127.0.0.1/32 trust\n"
    )
    (data / "postgresql.conf").write_text(
        f"listen_addresses='127.0.0.1'\nport={pg_port}\nunix_socket_directories=''\nssl=on\nssl_cert_file='{ca / 'server.pem'}'\nssl_key_file='{ca / 'server.key'}'\n"
    )
    log = (directory / "services.log").open("w")
    postgres = subprocess.Popen(
        [str(pg_bin / "postgres"), "-D", str(data)],
        env=pg_env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    password = "isolated-redis-" + uuid.uuid4().hex
    redis = subprocess.Popen(
        [
            os.environ.get("ARB_TEST_REDIS_BIN", "redis-server"),
            "--port",
            "0",
            "--tls-port",
            str(redis_port),
            "--bind",
            "127.0.0.1",
            "--tls-cert-file",
            str(ca / "server.pem"),
            "--tls-key-file",
            str(ca / "server.key"),
            "--tls-ca-cert-file",
            str(ca / "ca.pem"),
            "--tls-auth-clients",
            "no",
            "--requirepass",
            password,
            "--save",
            "",
            "--dir",
            str(directory),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    config = SimpleNamespace(
        arb_env="production",
        arb_database_url=f"postgresql://audit@localhost:{pg_port}/postgres",
        arb_database_sslmode="verify-full",
        arb_database_tls_ca_cert=str(ca / "ca.pem"),
        arb_redis_url=f"rediss://:{password}@localhost:{redis_port}/0",
        arb_redis_tls_ca_cert=str(ca / "ca.pem"),
        arb_engine_grpc_tls_ca_cert=str(ca / "ca.pem"),
        arb_engine_grpc_tls_cert=str(ca / "client.pem"),
        arb_engine_grpc_tls_key=str(ca / "client.key"),
    )

    def database_ready():
        with psycopg.connect(config.arb_database_url, **postgres_options(config)) as connection:
            return connection.execute("SELECT 1").fetchone() == (1,)

    try:
        wait(database_ready)
        wait(lambda: Redis.from_url(config.arb_redis_url, **redis_options(config)).ping())
        yield config, ca, wrong, expired, directory, postgres
    finally:
        for process in (redis, postgres):
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        log.close()


def rust(config, operation, **overrides):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ARB_", "BYBIT_"))}
    env.update(
        ARB_ENV="production",
        ARB_DATABASE_URL=config.arb_database_url,
        ARB_DATABASE_SSLMODE=config.arb_database_sslmode,
        ARB_DATABASE_TLS_CA_CERT=config.arb_database_tls_ca_cert,
        ARB_REDIS_URL=config.arb_redis_url,
        ARB_REDIS_TLS_CA_CERT=config.arb_redis_tls_ca_cert,
    )
    env.update(overrides)
    return subprocess.run(
        [str(ROOT / "rust/target/debug/transport_probe"), operation],
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
    )


def test_verified_postgres_from_both_runtimes(services):
    config, *_ = services
    with psycopg.connect(config.arb_database_url, **postgres_options(config)) as connection:
        assert connection.execute(
            "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"
        ).fetchone() == (True,)
    result = rust(config, "postgres")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["encrypted"] is True


@pytest.mark.parametrize("kind", ["untrusted", "missing", "malformed", "hostname", "disabled"])
def test_postgres_certificate_and_downgrade_rejection(services, kind, tmp_path):
    config, ca, wrong, *_ = services
    value = SimpleNamespace(**vars(config))
    overrides = {}
    if kind == "untrusted":
        value.arb_database_tls_ca_cert = str(wrong / "ca.pem")
    if kind == "missing":
        value.arb_database_tls_ca_cert = str(tmp_path / "absent")
    if kind == "malformed":
        path = tmp_path / "malformed"
        path.write_text("invalid PEM")
        value.arb_database_tls_ca_cert = str(path)
    if kind == "hostname":
        value.arb_database_url = config.arb_database_url.replace("localhost", "127.0.0.1")
    if kind == "disabled":
        value.arb_database_sslmode = "disable"
    with pytest.raises((ValueError, psycopg.Error)):
        with psycopg.connect(value.arb_database_url, **postgres_options(value)):
            pass
    overrides.update(
        ARB_DATABASE_URL=value.arb_database_url,
        ARB_DATABASE_TLS_CA_CERT=value.arb_database_tls_ca_cert,
        ARB_DATABASE_SSLMODE=value.arb_database_sslmode,
    )
    assert rust(config, "postgres", **overrides).returncode != 0


def test_expired_server_certificate_rejected(services):
    config, ca, wrong, expired, directory, postgres = services
    import shutil

    saved_cert = (ca / "server.pem").read_bytes()
    saved_key = (ca / "server.key").read_bytes()
    try:
        shutil.copyfile(expired / "server.pem", ca / "server.pem")
        shutil.copyfile(expired / "server.key", ca / "server.key")
        # Expired leaf has its own trusted root, so failure is expiry rather than unknown issuer.
        postgres.send_signal(signal.SIGHUP)
        time.sleep(0.15)
        value = SimpleNamespace(**vars(config))
        value.arb_database_tls_ca_cert = str(expired / "ca.pem")
        with pytest.raises(psycopg.Error):
            with psycopg.connect(value.arb_database_url, **postgres_options(value)):
                pass
        assert (
            rust(config, "postgres", ARB_DATABASE_TLS_CA_CERT=str(expired / "ca.pem")).returncode
            != 0
        )
    finally:
        (ca / "server.pem").write_bytes(saved_cert)
        (ca / "server.key").write_bytes(saved_key)
        postgres.send_signal(signal.SIGHUP)
        time.sleep(0.15)


def test_authenticated_verified_redis_and_negative_controls(services):
    config, ca, wrong, *_ = services
    with Redis.from_url(config.arb_redis_url, **redis_options(config)) as connection:
        assert connection.ping()
    assert rust(config, "redis").returncode == 0
    for url, certificate in (
        (config.arb_redis_url, str(wrong / "ca.pem")),
        (config.arb_redis_url.replace("localhost", "127.0.0.1"), str(ca / "ca.pem")),
        (config.arb_redis_url.replace("isolated-redis-", "wrong-"), str(ca / "ca.pem")),
    ):
        value = SimpleNamespace(**vars(config))
        value.arb_redis_url = url
        value.arb_redis_tls_ca_cert = certificate
        with pytest.raises(Exception):
            with Redis.from_url(url, **redis_options(value)) as connection:
                connection.ping()
        assert (
            rust(config, "redis", ARB_REDIS_URL=url, ARB_REDIS_TLS_CA_CERT=certificate).returncode
            != 0
        )


def test_real_rust_grpc_mutual_tls(services):
    import asyncio

    config, ca, wrong, expired, directory, *_ = services
    grpc_port = port()
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ARB_", "BYBIT_"))}
    token = "isolated-internal-" + uuid.uuid4().hex
    env.update(
        ARB_ENV="development",
        ARB_LIVE_TRADING_ENABLED="false",
        ARB_ENGINE_AUTOSTART="false",
        ARB_ENGINE_GRPC_ADDR=f"127.0.0.1:{grpc_port}",
        ARB_ENGINE_GRPC_TOKEN=token,
        ARB_REDIS_URL=config.arb_redis_url,
        ARB_REDIS_TLS_CA_CERT=config.arb_redis_tls_ca_cert,
        ARB_ENGINE_GRPC_TLS_CA_CERT=str(ca / "ca.pem"),
        ARB_ENGINE_GRPC_TLS_CERT=str(ca / "server.pem"),
        ARB_ENGINE_GRPC_TLS_KEY=str(ca / "server.key"),
        ARB_CONTROL_STATE_FILE=str(directory / "control.json"),
        ARB_EVENT_OUTBOX_PATH=str(directory / "outbox"),
        ARB_GRPC_IDEMPOTENCY_STORE=str(directory / "commands"),
    )
    with (directory / "engine.log").open("w") as log:
        process = subprocess.Popen(
            [str(ROOT / "rust/target/debug/engine-service")],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        async def call(credentials):
            target = f"localhost:{grpc_port}"
            channel = (
                grpc.aio.secure_channel(target, credentials)
                if credentials
                else grpc.aio.insecure_channel(target)
            )
            async with channel:
                return await engine_control_pb2_grpc.EngineControlStub(channel).GetStatus(
                    engine_control_pb2.StatusRequest(request_id="tls-fixture"),
                    metadata=(("x-engine-token", token),),
                    timeout=1,
                )

        trusted = grpc.ssl_channel_credentials(
            (ca / "ca.pem").read_bytes(),
            (ca / "client.key").read_bytes(),
            (ca / "client.pem").read_bytes(),
        )
        try:
            wait(lambda: asyncio.run(call(trusted)))
            for credentials in (
                None,
                grpc.ssl_channel_credentials((ca / "ca.pem").read_bytes()),
                grpc.ssl_channel_credentials(
                    (ca / "ca.pem").read_bytes(),
                    (wrong / "client.key").read_bytes(),
                    (wrong / "client.pem").read_bytes(),
                ),
            ):
                with pytest.raises(grpc.aio.AioRpcError):
                    asyncio.run(call(credentials))
        finally:
            process.terminate()
            process.wait(timeout=10)
