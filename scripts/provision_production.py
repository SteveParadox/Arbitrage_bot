"""Provision private mounted secrets and isolated PKI; never authorizes exchange orders.

Run on the deployment host. CA signing key stays outside service mounts.
Replace HTTPS leaf with an operator-trusted certificate before public exposure.
"""

from __future__ import annotations
import argparse
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import secrets
import sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID


def provision(directory, host, assign_owners):
    if directory.exists():
        raise ValueError("provisioning destination must not already exist")
    directory.mkdir(parents=True, mode=0o711)
    offline = directory / "offline"
    offline.mkdir(mode=0o700)
    now = datetime.now(UTC)

    def write(name, value, owner=10001, private=True):
        path = directory / name
        path.write_bytes(value if isinstance(value, bytes) else value.encode())
        path.chmod(0o400 if private else 0o444)
        if assign_owners:
            os.chown(path, owner, owner)
        return path

    ca_key = rsa.generate_private_key(65537, 3072)
    ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "Arbitrage isolated production CA")]
    )
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), True)
        .sign(ca_key, hashes.SHA256())
    )
    write("ca.pem", ca.public_bytes(serialization.Encoding.PEM), private=False)
    private = offline / "ca-signing.key"
    private.write_bytes(
        ca_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    private.chmod(0o400)
    for role, names, owner, client in [
        ("postgres", ["postgres"], 999, False),
        ("redis", ["redis"], 999, False),
        ("engine", ["engine-control"], 10001, False),
        ("api", ["api"], 10001, True),
        ("https", [host, "localhost"], 10001, False),
    ]:
        key = rsa.generate_private_key(65537, 2048)
        certificate = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0])])
            )
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + timedelta(days=90))
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName(n) for n in names]), False
            )
            .add_extension(
                x509.ExtendedKeyUsage(
                    [
                        ExtendedKeyUsageOID.CLIENT_AUTH
                        if client
                        else ExtendedKeyUsageOID.SERVER_AUTH
                    ]
                ),
                False,
            )
            .sign(ca_key, hashes.SHA256())
        )
        write(
            role + ".pem",
            certificate.public_bytes(serialization.Encoding.PEM),
            private=False,
        )
        write(
            role + ".key",
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
            owner,
        )
    admin_password = secrets.token_hex(32)
    runtime_password = secrets.token_hex(32)
    redis_password = secrets.token_hex(32)
    write("postgres-password", admin_password, 999)
    write(
        "database-admin-url",
        "postgresql+psycopg://arb_admin:" + admin_password + "@postgres:5432/arbitrage",
    )
    write(
        "database-runtime-url",
        "postgresql+psycopg://arb_runtime:"
        + runtime_password
        + "@postgres:5432/arbitrage",
    )
    write("redis-url", "rediss://arb:" + redis_password + "@redis:6379/0")
    write(
        "redis-users.acl",
        "user default off\nuser arb on >" + redis_password + " ~* &* +@all\n",
        999,
    )
    for name in ("control-token", "grpc-token", "metrics-token", "backup-key"):
        write(name, secrets.token_hex(32))
    write("paper-grants.json", "{}")
    write("execution-grants.json", "{}")
    write(
        "runtime-role.sql",
        "CREATE ROLE arb_runtime LOGIN PASSWORD '"
        + runtime_password
        + "' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;\nGRANT CONNECT ON DATABASE arbitrage TO arb_runtime;\nGRANT USAGE ON SCHEMA public TO arb_runtime;\nALTER DEFAULT PRIVILEGES FOR ROLE arb_admin IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO arb_runtime;\nALTER DEFAULT PRIVILEGES FOR ROLE arb_admin IN SCHEMA public GRANT USAGE,SELECT ON SEQUENCES TO arb_runtime;\n",
        999,
    )
    write("metrics-token-monitor", (directory / "metrics-token").read_text(), 65534)
    write(
        "monitoring-webhook-url",
        "https://monitoring.invalid/replace-with-approved-receiver",
        65534,
    )
    configuration = {
        "ARB_ENV": "production",
        "ARB_TRADING_MODE": "observe",
        "ARB_LIVE_TRADING_ENABLED": "false",
        "ARB_TESTNET_TRADING_ENABLED": "false",
        "ARB_PAPER_TRADING_ENABLED": "false",
        "ARB_ENGINE_AUTOSTART": "false",
        "ARB_CORS_ORIGINS": "https://" + host,
        "ARB_TRUSTED_HOSTS": host + ",api,localhost,127.0.0.1",
        "ARB_API_RATE_LIMIT_PER_MINUTE": "120",
    }
    write(
        "deployment.env",
        "".join(k + "=" + v + "\n" for k, v in configuration.items()),
        private=False,
    )
    print(
        json.dumps(
            {
                "directory": str(directory),
                "live_trading_enabled": False,
                "exchange_credentials_provisioned": False,
                "https_trust": "private CA; operator replacement/trust required",
            }
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--https-host", required=True)
    parser.add_argument("--assign-container-owners", action="store_true")
    args = parser.parse_args()
    if not args.https_host or any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-"
        for c in args.https_host
    ):
        parser.error("HTTPS hostname must be a DNS name")
    provision(args.directory, args.https_host, args.assign_container_owners)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError):
        print(
            "provisioning failed; inspect configuration and host ownership privileges",
            file=sys.stderr,
        )
        raise SystemExit(1)
