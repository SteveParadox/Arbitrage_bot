"""Encrypted quiesced PostgreSQL/custom-format + runtime-state recovery.

Never resumes execution. Restore requires an empty database and state directory.
"""

from __future__ import annotations
from datetime import UTC, datetime

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.parse import unquote, urlsplit, urlunsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
import psycopg
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from api.infrastructure import atomic_json, postgres_options, read_secret
from api.settings import settings
from analytics.integrity import audit

MAGIC = b"ARB_BACKUP_V1\x00"
CHUNK = 1024 * 1024


def key(path):
    try:
        value = bytes.fromhex(read_secret(path))
    except ValueError:
        raise ValueError(
            "backup key must be a private 64-character hexadecimal secret"
        ) from None
    if len(value) != 32:
        raise ValueError("backup key must contain 256 bits")
    return value


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(CHUNK):
            result.update(block)
    return result.hexdigest()


def encrypt(source, destination, secret):
    nonce = os.urandom(12)
    cipher = Cipher(algorithms.AES(secret), modes.GCM(nonce)).encryptor()
    cipher.authenticate_additional_data(MAGIC)
    with Path(source).open("rb") as plain, Path(destination).open("xb") as target:
        os.chmod(destination, 0o600)
        target.write(MAGIC + nonce)
        while block := plain.read(CHUNK):
            target.write(cipher.update(block))
        target.write(cipher.finalize() + cipher.tag)
        target.flush()
        os.fsync(target.fileno())


def decrypt(source, destination, secret):
    with Path(source).open("rb") as encrypted:
        if encrypted.read(len(MAGIC)) != MAGIC:
            raise ValueError("unsupported backup format")
        nonce = encrypted.read(12)
        size = encrypted.seek(0, 2)
        if size < len(MAGIC) + 12 + 16:
            raise ValueError("truncated backup")
        encrypted.seek(-16, 2)
        tag = encrypted.read(16)
        encrypted.seek(len(MAGIC) + 12)
        cipher = Cipher(algorithms.AES(secret), modes.GCM(nonce, tag)).decryptor()
        cipher.authenticate_additional_data(MAGIC)
        remaining = size - len(MAGIC) - 12 - 16
        try:
            with Path(destination).open("xb") as target:
                os.chmod(destination, 0o600)
                while remaining:
                    block = encrypted.read(min(CHUNK, remaining))
                    if not block:
                        raise ValueError("truncated backup")
                    target.write(cipher.update(block))
                    remaining -= len(block)
                target.write(cipher.finalize())
                target.flush()
                os.fsync(target.fileno())
        except Exception:
            Path(destination).unlink(missing_ok=True)
            raise


def pg_environment(directory):
    uri = urlsplit(
        settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:")
    )
    password = unquote(uri.password or "")

    def escape(value):
        return value.replace("\\", "\\\\").replace(":", "\\:")

    passfile = Path(directory) / "pgpass"
    passfile.write_text(
        ":".join(
            map(
                escape,
                [
                    uri.hostname or "",
                    str(uri.port or 5432),
                    unquote(uri.path[1:]),
                    unquote(uri.username or ""),
                    password,
                ],
            )
        )
        + "\n"
    )
    passfile.chmod(0o600)
    clean = urlunsplit(
        uri._replace(netloc=f"{uri.hostname}:{uri.port or 5432}", query="")
    )
    options = postgres_options(settings)
    environment = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    environment.update(
        PGPASSFILE=str(passfile),
        PGUSER=unquote(uri.username or ""),
        PGSSLMODE=options["sslmode"],
        PGCONNECT_TIMEOUT="3",
    )
    if "sslrootcert" in options:
        environment["PGSSLROOTCERT"] = options["sslrootcert"]
    return environment, clean


def pg_run(command, environment):
    # Driver diagnostics can include credentials. Keep bounded private stderr; expose category only.
    result = subprocess.run(
        command,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=900,
    )
    if result.returncode:
        raise RuntimeError(
            f"{Path(command[0]).name} failed with exit {result.returncode}"
        )
    return result.stdout


def backup(args):
    started = time.monotonic()
    secret = key(args.key_file)
    args.output.mkdir(parents=True, exist_ok=True)
    args.output.chmod(0o700)
    with tempfile.TemporaryDirectory(prefix="arb-backup-") as temp:
        temp = Path(temp)
        environment, database = pg_environment(temp)
        with psycopg.connect(
            settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
            **postgres_options(settings),
        ) as connection:
            connection.execute("SET LOCAL idle_in_transaction_session_timeout=0")
            connection.execute(
                "LOCK TABLE paper_accounts,execution_accounts IN SHARE MODE"
            )
            for table, domain in (
                ("paper_accounts", 8149),
                ("execution_accounts", 9150),
            ):
                for identity, enabled in connection.execute(
                    f"SELECT id,enabled FROM {table} ORDER BY id"
                ):
                    if enabled:
                        raise RuntimeError(
                            "backup requires stopped execution authorization"
                        )
                    owned = connection.execute(
                        "SELECT pg_try_advisory_xact_lock(hashtextextended(%s,%s))",
                        (identity, domain),
                    ).fetchone()[0]
                    if not owned:
                        raise RuntimeError("backup requires stopped execution owners")
            pg_run(
                [
                    "pg_dump",
                    "--format=custom",
                    "--no-owner",
                    "--no-acl",
                    "--file",
                    str(temp / "database.dump"),
                    database,
                ],
                environment,
            )
            pg_run(["pg_restore", "--list", str(temp / "database.dump")], environment)
            with psycopg.connect(
                settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
                autocommit=True,
                **postgres_options(settings),
            ) as verifier:
                integrity = audit(verifier)
            if not integrity["balanced"]:
                raise RuntimeError("backup financial integrity check failed")
            archive = temp / "archive.tar"
            manifest = {
                "version": 1,
                "checked_at": time.time(),
                "dump_sha256": digest(temp / "database.dump"),
                "source_commit": args.source_commit,
                "schema": connection.execute(
                    "SELECT version_num FROM alembic_version"
                ).fetchone()[0],
                "server_version": connection.info.server_version,
                "integrity": integrity,
                "state_files": {},
            }
            with tarfile.open(archive, "w") as tar:
                tar.add(temp / "database.dump", arcname="database.dump")
                for path in sorted(args.state.rglob("*")):
                    if path.is_symlink():
                        raise ValueError(
                            "runtime state must not contain symbolic links"
                        )
                    if path.is_dir():
                        continue
                    if not path.is_file():
                        raise ValueError(
                            "runtime state must contain regular files only"
                        )
                    relative = path.relative_to(args.state)
                    if relative.name == "deployment-permit.json":
                        continue
                    if path.suffix in {".pem", ".key"} or "secret" in relative.parts:
                        raise ValueError(
                            "secret material must not be placed in runtime state"
                        )
                    manifest["state_files"][str(relative)] = digest(path)
                    tar.add(path, arcname="state/" + str(relative), recursive=False)
                encoded = json.dumps(manifest, allow_nan=False).encode()
                from io import BytesIO

                entry = tarfile.TarInfo("manifest.json")
                entry.size = len(encoded)
                tar.addfile(entry, BytesIO(encoded))
            destination = args.output / f"financial-{time.time_ns()}.arbbackup"
            pending = destination.with_suffix(".pending")
            encrypt(archive, pending, secret)
            decrypt(pending, temp / "authenticated.tar", secret)
            if digest(archive) != digest(temp / "authenticated.tar"):
                raise ValueError("encrypted backup verification failed")
            pending.replace(destination)
        retained = sorted(args.output.glob("financial-*.arbbackup"))
        for old in retained[: -args.retain]:
            old.unlink()
        report = {
            "checked_at": time.time(),
            "verified": True,
            "backup": destination.name,
            "sha256": digest(destination),
            "bytes": destination.stat().st_size,
            "actual_seconds": time.monotonic() - started,
            "source_commit": args.source_commit,
            "integrity": integrity,
            "recovery_characteristics": "Quiesced consistent snapshot plus runtime state; no WAL/PITR or off-site claim",
        }
        atomic_json(args.status_file, report)
        print(json.dumps(report))


def restore(args):
    started = time.monotonic()
    secret = key(args.key_file)
    if args.state.exists() and any(
        p.is_symlink() or not p.is_dir() for p in args.state.rglob("*")
    ):
        raise ValueError("restore state directory must be empty")
    with psycopg.connect(
        settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
        autocommit=True,
        **postgres_options(settings),
    ) as connection:
        if connection.execute(
            "SELECT count(*) FROM pg_tables WHERE schemaname='public'"
        ).fetchone()[0]:
            raise ValueError("restore database must be empty")
    args.state.mkdir(parents=True, exist_ok=True)
    marker = args.state / "RESTORE_REQUIRED"
    # Durable barrier precedes EVERY restore side effect and survives failed restore.
    atomic_json(marker, {"restored_at": time.time(), "requires_reauthorization": True})
    with tempfile.TemporaryDirectory(prefix="arb-restore-") as temp:
        temp = Path(temp)
        decrypt(args.backup, temp / "authenticated.tar", secret)
        extract = temp / "extract"
        extract.mkdir()
        with tarfile.open(temp / "authenticated.tar") as tar:
            members = tar.getmembers()
            if len(members) > 100000 or sum(m.size for m in members) > 100 * 1024**3:
                raise ValueError("backup archive exceeds restoration bounds")
            for member in members:
                path = Path(member.name)
                if (
                    not member.isfile()
                    or path.is_absolute()
                    or ".." in path.parts
                    or not (
                        member.name in {"database.dump", "manifest.json"}
                        or member.name.startswith("state/")
                    )
                ):
                    raise ValueError("unsafe backup archive member")
            if len({m.name for m in members}) != len(members):
                raise ValueError("duplicate backup archive member")
            tar.extractall(extract, filter="data")
        manifest = json.loads((extract / "manifest.json").read_text())
        if (
            manifest["version"] != 1
            or digest(extract / "database.dump") != manifest["dump_sha256"]
        ):
            raise ValueError("backup manifest/dump mismatch")
        state_files = {
            str(p.relative_to(extract / "state")): p
            for p in (extract / "state").rglob("*")
            if p.is_file()
        }
        if set(state_files) != set(manifest["state_files"]):
            raise ValueError("backup state manifest mismatch")
        for relative, path in state_files.items():
            if digest(path) != manifest["state_files"][relative]:
                raise ValueError("backup runtime-state digest mismatch")
        environment, database = pg_environment(temp)
        pg_run(
            [
                "pg_restore",
                "--single-transaction",
                "--exit-on-error",
                "--no-owner",
                "--no-acl",
                "--dbname",
                database,
                str(extract / "database.dump"),
            ],
            environment,
        )
        with psycopg.connect(
            settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
            **postgres_options(settings),
        ) as connection:
            connection.execute(
                "UPDATE paper_accounts SET enabled=false,blocked=true,recovering=true,heartbeat=NULL,epoch=epoch+1"
            )
            connection.execute(
                "UPDATE execution_accounts SET enabled=false,blocked=true,recovering=true,state='RECOVERING',heartbeat=NULL,epoch=epoch+1"
            )
        for relative, path in state_files.items():
            target = args.state / relative
            if target.name in {"RESTORE_REQUIRED", "deployment-permit.json"}:
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            target.chmod(0o600)
        # Runtime controls are reinitialized disabled, retaining the historical command journal.
        atomic_json(
            args.state / "control/trading_state.json",
            {
                "enabled": False,
                "reason": "restore requires recovery and fresh authorization",
                "version": 1,
                "updated_at": datetime.now(UTC).isoformat(),
                "source": "disaster_recovery",
                "request_id": None,
            },
        )
        with psycopg.connect(
            settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
            autocommit=True,
            **postgres_options(settings),
        ) as connection:
            integrity = audit(connection)
        report = {
            "checked_at": time.time(),
            "restored": True,
            "authorized": False,
            "restore_barrier": True,
            "actual_seconds": time.monotonic() - started,
            "source_commit": manifest["source_commit"],
            "integrity": integrity,
        }
        atomic_json(args.status_file, report)
        print(json.dumps(report))
        if not integrity["balanced"]:
            raise RuntimeError(
                "restored financial integrity failed; barrier remains active"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("backup", "restore"))
    parser.add_argument("--key-file", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--status-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("backups"))
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--retain", type=int, default=14)
    parser.add_argument("--source-commit", default="unknown")
    args = parser.parse_args()
    if not 1 <= args.retain <= 365:
        parser.error("retention must be 1..365 snapshots")
    if args.operation == "restore" and not args.backup:
        parser.error("restore requires --backup")
    (backup if args.operation == "backup" else restore)(args)


if __name__ == "__main__":
    main()
