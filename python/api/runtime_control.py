from __future__ import annotations

import json
import os
import re
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from api.settings import settings

REPO_ROOT = Path(__file__).resolve().parents[2]


class ControlState(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    version: int = Field(ge=1, le=1)
    enabled: bool
    updated_at: str
    reason: str = Field(min_length=1, max_length=256)
    source: Literal["fastapi_control", "rust_grpc_control"]
    request_id: str | None = Field(
        default=None, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$"
    )

    @field_validator("updated_at")
    @classmethod
    def valid_timestamp(cls, value: str) -> str:
        if not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value
        ):
            raise ValueError("timestamp must be RFC3339")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timestamp must include timezone")
        return value

    @field_validator("reason")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("reason must not be blank")
        return value


class StopIntent(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    version: int = Field(ge=1, le=1)
    request_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    reason: str = Field(min_length=1, max_length=256)
    status: Literal["STOP_REQUESTED", "STOP_UNCONFIRMED", "CONFIRMED_STOPPED"]
    updated_at: str

    _timestamp = field_validator("updated_at")(ControlState.valid_timestamp.__func__)
    _reason = field_validator("reason")(ControlState.nonblank.__func__)


def control_state_path() -> Path:
    configured = Path(settings.arb_control_state_file)
    return configured if configured.is_absolute() else REPO_ROOT / configured


def stop_intent_path() -> Path:
    return control_state_path().with_suffix(".stop.json")


def _read_model(path: Path, model):
    def unique_fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate state field")
            result[key] = value
        return result

    if path.stat().st_size > 16384:
        raise ValueError("runtime control record exceeds size limit")
    payload = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_fields)
    return model.model_validate(payload)


def read_stop_intent() -> dict[str, Any] | None:
    try:
        state = _read_model(stop_intent_path(), StopIntent)
        timestamp = datetime.fromisoformat(state.updated_at.replace("Z", "+00:00"))
        if (timestamp - datetime.now(UTC)).total_seconds() > 5:
            raise ValueError("future-dated stop intent")
        return state.model_dump()
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, ValueError):
        return {
            "status": "STOP_UNCONFIRMED",
            "request_id": None,
            "reason": "stop intent is unreadable",
        }


def _closed(reason: str, *, missing: bool = False) -> dict[str, Any]:
    return {
        "enabled": False,
        "updated_at": None,
        "reason": reason,
        "source": "default_fail_closed" if missing else "invalid_fail_closed",
        "request_id": None,
        "valid": False,
    }


def read_control_state() -> dict[str, Any]:
    try:
        state = _read_model(control_state_path(), ControlState)
    except FileNotFoundError:
        return _closed("runtime control state has not been initialized", missing=True)
    except (OSError, UnicodeError, ValueError):
        return _closed("runtime control state is unreadable or invalid")
    updated = datetime.fromisoformat(state.updated_at.replace("Z", "+00:00"))
    age = (datetime.now(UTC) - updated).total_seconds()
    if age < -5 or (state.enabled and age > settings.arb_control_state_max_age_seconds):
        return _closed("runtime control timestamp is future-dated or enabled state has expired")
    result = {**state.model_dump(), "valid": True}
    intent = read_stop_intent()
    if intent and intent["status"] != "CONFIRMED_STOPPED":
        result.update(enabled=False, reason="emergency stop remains unconfirmed")
    return result


@contextmanager
def control_lock(timeout_seconds: float = 2.0):
    """Same OS advisory lock used by Rust; released by the OS on process death."""
    path = control_state_path().with_suffix(".lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt

            if path.stat().st_size == 0:
                handle.write(b"0")
                handle.flush()

            def lock():
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

            def unlock():
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            def lock():
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            def unlock():
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                lock()
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("runtime control lock unavailable") from None
                time.sleep(0.01)
        try:
            yield
        finally:
            unlock()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def write_control_state(
    *, enabled: bool, reason: str, request_id: str | None = None
) -> dict[str, Any]:
    payload = ControlState(
        version=1,
        enabled=enabled,
        updated_at=datetime.now(UTC).isoformat(),
        reason=reason.strip(),
        source="fastapi_control",
        request_id=request_id,
    ).model_dump()
    write_json_atomic(control_state_path(), payload)
    return payload


def request_stop(reason: str, request_id: str) -> None:
    with control_lock():
        existing = read_stop_intent()
        if existing and existing.get("request_id") == request_id and existing["reason"] != reason:
            raise ValueError("request_id already belongs to a different stop reason")
        intent = StopIntent(
            version=1,
            request_id=request_id,
            reason=reason,
            status="STOP_REQUESTED",
            updated_at=datetime.now(UTC).isoformat(),
        )
        write_json_atomic(stop_intent_path(), intent.model_dump())
        try:
            state = _read_model(control_state_path(), ControlState)
        except (OSError, UnicodeError, ValueError):
            state = None
        # Preserve proof of an already applied operation across a cached gRPC reply.
        if not (
            state
            and not state.enabled
            and state.source == "rust_grpc_control"
            and state.request_id == request_id
        ):
            write_control_state(enabled=False, reason=reason, request_id=request_id)


def mark_stop_unconfirmed(request_id: str) -> None:
    with control_lock():
        intent = read_stop_intent()
        if (
            intent
            and intent.get("request_id") == request_id
            and intent["status"] != "CONFIRMED_STOPPED"
        ):
            intent.update(status="STOP_UNCONFIRMED", updated_at=datetime.now(UTC).isoformat())
            write_json_atomic(stop_intent_path(), intent)


def confirm_stop(request_id: str) -> bool:
    with control_lock():
        intent = read_stop_intent()
        if not intent or intent.get("request_id") != request_id:
            return False
        try:
            state = _read_model(control_state_path(), ControlState)
        except (OSError, UnicodeError, ValueError):
            return False
        if state.enabled or state.request_id != request_id or state.source != "rust_grpc_control":
            return False
        intent.update(status="CONFIRMED_STOPPED", updated_at=datetime.now(UTC).isoformat())
        write_json_atomic(stop_intent_path(), intent)
        return True
