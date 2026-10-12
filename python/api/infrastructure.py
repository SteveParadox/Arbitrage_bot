"""Shared transport policy. Production never negotiates an insecure fallback."""
from __future__ import annotations

import json
import os
import ssl
import stat
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import grpc


def read_file(path, *, private=False, limit=65536) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(descriptor, 'rb') as source:
            metadata = os.fstat(source.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
                raise ValueError('configuration file must be a bounded regular file')
            if private and metadata.st_mode & 0o077:
                raise ValueError('secret file must deny group and other access')
            value = source.read(limit + 1)
            if len(value) > limit:
                raise ValueError('configuration file exceeds size limit')
            return value
    except OSError:
        raise ValueError('required configuration file unavailable') from None


def read_secret(path) -> str:
    try:
        value = read_file(path, private=True).decode('utf-8').rstrip('\r\n')
    except UnicodeError:
        raise ValueError('secret file must contain UTF-8') from None
    if not value or '\x00' in value or '\n' in value or '\r' in value or value != value.strip():
        raise ValueError('secret file contains invalid content')
    return value


def postgres_options(settings) -> dict:
    uri = urlsplit(settings.arb_database_url.replace('postgresql+psycopg:', 'postgresql:'))
    if uri.scheme not in {'postgresql', 'postgres'} or not uri.hostname:
        raise ValueError('invalid PostgreSQL endpoint')
    query = parse_qs(uri.query, keep_blank_values=True)
    mode = settings.arb_database_sslmode
    if query.get('sslmode', [mode]) != [mode] or any(k in query for k in ('sslrootcert', 'sslcert', 'sslkey')):
        raise ValueError('database URL contradicts transport policy')
    if settings.arb_env == 'production' and mode != 'verify-full':
        raise ValueError('production PostgreSQL requires verify-full')
    options = {'connect_timeout': 3, 'sslmode': mode,
               'options': '-c statement_timeout=5000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=10000'}
    if mode == 'verify-full':
        if not settings.arb_database_tls_ca_cert:
            raise ValueError('PostgreSQL trusted CA required')
        read_file(settings.arb_database_tls_ca_cert, limit=1048576)
        options['sslrootcert'] = settings.arb_database_tls_ca_cert
    elif mode != 'disable':
        raise ValueError('supported database policies are disable or verify-full')
    return options


def redis_options(settings) -> dict:
    uri = urlsplit(settings.arb_redis_url)
    if uri.scheme not in {'redis', 'rediss'} or not uri.hostname or uri.query or uri.fragment:
        raise ValueError('invalid Redis endpoint')
    if settings.arb_env == 'production' and (uri.scheme != 'rediss' or not unquote(uri.password or '')):
        raise ValueError('production Redis requires authenticated TLS')
    options = {'socket_connect_timeout': 2, 'socket_timeout': 2, 'health_check_interval': 15}
    if uri.scheme == 'rediss':
        if not settings.arb_redis_tls_ca_cert:
            raise ValueError('Redis trusted CA required')
        read_file(settings.arb_redis_tls_ca_cert, limit=1048576)
        options.update(ssl_cert_reqs=ssl.CERT_REQUIRED, ssl_check_hostname=True,
                       ssl_ca_certs=settings.arb_redis_tls_ca_cert)
    return options


def grpc_channel(settings, target=None):
    fields = (settings.arb_engine_grpc_tls_ca_cert, settings.arb_engine_grpc_tls_cert,
              settings.arb_engine_grpc_tls_key)
    if any(fields):
        if not all(fields):
            raise ValueError('gRPC mutual TLS requires CA, certificate and key')
        credentials = grpc.ssl_channel_credentials(read_file(fields[0], limit=1048576),
                                                   read_file(fields[2], private=True),
                                                   read_file(fields[1], limit=1048576))
        return grpc.aio.secure_channel(target or settings.arb_engine_grpc_target, credentials)
    if settings.arb_env == 'production':
        raise ValueError('production gRPC requires mutual TLS')
    return grpc.aio.insecure_channel(target or settings.arb_engine_grpc_target)


def production_readiness(settings) -> tuple[bool, str]:
    marker = Path(settings.arb_restore_marker)
    try:
        marker.lstat()
    except FileNotFoundError:
        pass
    except OSError:
        return False, 'restore barrier unavailable'
    else:
        return False, 'restored state requires explicit recovery'
    if settings.arb_env != 'production':
        return True, 'development transport boundary'
    for path, field, age in ((settings.arb_financial_integrity_file, 'balanced', 120),
                             (settings.arb_monitoring_readiness_file, 'ready_for_new_exposure', 60)):
        try:
            evidence = json.loads(read_file(path))
            stamp = evidence.get('checked_at')
            if type(stamp) not in (int, float) or not 0 <= time.time()-stamp <= age or evidence.get(field) is not True:
                return False, 'financial or monitoring evidence unhealthy or stale'
        except (ValueError, TypeError, OSError):
            return False, 'financial or monitoring evidence unavailable'
    return True, 'fresh independent financial and monitoring evidence'


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.pending')
    with temporary.open('w') as stream:
        json.dump(value, stream, separators=(',', ':'), allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
