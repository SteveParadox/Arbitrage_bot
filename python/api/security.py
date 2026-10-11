"""Account-scoped paper permissions and bounded, distributed request protection."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
from dataclasses import dataclass

from fastapi import HTTPException, Request
from redis import Redis
from starlette.responses import JSONResponse

from api.settings import settings


@dataclass(frozen=True)
class Principal:
    subject: str
    role: str
    accounts: frozenset[str]
    global_admin: bool = False


def principal(token: str) -> Principal | None:
    if len(token.encode()) < 32:
        return None
    expected = settings.arb_control_api_token
    if len(expected.encode()) >= 32 and hmac.compare_digest(token.encode(), expected.encode()):
        return Principal("control-admin", "admin", frozenset(), True)
    grant = json.loads(settings.arb_paper_api_keys_json).get(hashlib.sha256(token.encode()).hexdigest())
    if grant is not None:
        return Principal(grant["subject"], grant["role"], frozenset(grant["accounts"]))
    return None


def bearer(headers) -> str:
    raw = headers.get("authorization", "")
    scheme, _, token = raw.partition(" ")
    return token if scheme.lower() == "bearer" else ""


def require_paper_auth(request: Request) -> None:
    user = principal(bearer(request.headers))
    if user is None:
        raise HTTPException(401, "invalid paper credential", headers={"WWW-Authenticate": "Bearer"})
    if not user.global_admin:
        account = request.path_params.get("account")
        if account is None or account not in user.accounts:
            raise HTTPException(403, "paper account access denied")
        if request.method != "GET":
            action = request.path_params.get("action")
            if user.role == "viewer" or action not in {"start", "stop"}:
                raise HTTPException(403, "paper action denied")
    request.state.paper_role = user.role


def _rate_count(identity: str) -> int:
    # One Redis transaction, one expiry, shared by all API workers.
    client = Redis.from_url(settings.arb_redis_url, socket_timeout=.5, socket_connect_timeout=.5)
    try:
        return client.eval("local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],60) end; return n",
                           1, "arb.api.rate:" + hashlib.sha256(identity.encode()).hexdigest())
    finally:
        client.close()


class SecurityMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        request = Request(scope)
        path, method = request.url.path, request.method
        user = principal(bearer(request.headers))

        async def safe_send(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend([(b"x-content-type-options", b"nosniff"), (b"x-frame-options", b"DENY"),
                                (b"cache-control", b"no-store"), (b"referrer-policy", b"no-referrer")])
                message = {**message, "headers": headers}
            await send(message)

        async def reject(code, detail, headers=None):
            await JSONResponse({"detail": detail}, status_code=code, headers=headers)(scope, receive, safe_send)

        production = settings.arb_env == "production"
        if production and path != "/livez" and not path.startswith("/paper/") and method != "OPTIONS":
            if user is None or not user.global_admin:
                return await reject(401, "administrator credential required")
        origin = request.headers.get("origin")
        if production and method not in {"GET", "HEAD", "OPTIONS"} and origin and origin not in settings.cors_origins():
            return await reject(403, "untrusted origin")
        # A verified emergency stop must remain usable during Redis failure or quota exhaustion.
        account_stop = re.fullmatch(r"/paper/([A-Za-z0-9_-]+)/control/stop", path)
        emergency = method == "POST" and user is not None and (
            user.global_admin and path == "/trading/stop" or
            account_stop is not None and user.role != "viewer" and
            (user.global_admin or account_stop[1] in user.accounts))
        rate = settings.arb_api_rate_limit_per_minute
        if rate and path != "/livez" and not emergency and method != "OPTIONS":
            token = bearer(request.headers)
            identity = "credential:" + token if user else "source:" + (request.client.host if request.client else "unknown")
            try:
                count = await asyncio.to_thread(_rate_count, identity)
            except Exception:
                return await reject(503, "request protection unavailable")
            if count > rate:
                return await reject(429, "request limit exceeded", {"Retry-After": "60"})
        limit = settings.arb_api_max_body_bytes
        raw_length = request.headers.get("content-length")
        if raw_length is not None:
            try:
                size = int(raw_length)
                if size < 0:
                    raise ValueError
            except ValueError:
                return await reject(400, "invalid content length")
            if size > limit:
                return await reject(413, "request body too large")
        messages, total = [], 0
        while True:
            try:
                message = await asyncio.wait_for(receive(), timeout=10)
            except TimeoutError:
                return await reject(408, "request body timeout")
            if message["type"] == "http.disconnect":
                return
            total += len(message.get("body", b""))
            if total > limit:
                return await reject(413, "request body too large")
            messages.append(message)
            if not message.get("more_body", False):
                break
        iterator = iter(messages)

        async def bounded_receive():
            return next(iterator, None) or await receive()

        await self.app(scope, bounded_receive, safe_send)
