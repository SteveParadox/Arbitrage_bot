from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from api.settings import settings

REPO_ROOT = Path(__file__).resolve().parents[2]


def control_state_path() -> Path:
    configured = Path(settings.arb_control_state_file)
    if configured.is_absolute():
        return configured
    return REPO_ROOT / configured


def read_control_state() -> dict[str, Any]:
    path = control_state_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {
            "enabled": False,
            "updated_at": None,
            "reason": "runtime control state has not been initialized",
            "source": "default_fail_closed",
        }
    except (OSError, json.JSONDecodeError):
        return {
            "enabled": False,
            "updated_at": None,
            "reason": "runtime control state is unreadable",
            "source": "invalid_fail_closed",
        }

    if payload.get("version") != 1:
        return {
            "enabled": False,
            "updated_at": payload.get("updated_at"),
            "reason": "runtime control state has an unsupported version",
            "source": "invalid_fail_closed",
        }

    enabled = payload.get("enabled")
    if not isinstance(enabled, bool):
        return {
            "enabled": False,
            "updated_at": payload.get("updated_at"),
            "reason": "runtime control state is missing a boolean enabled field",
            "source": "invalid_fail_closed",
        }

    return {
        "enabled": enabled,
        "updated_at": payload.get("updated_at"),
        "reason": payload.get("reason"),
        "source": payload.get("source", "control_api"),
        "request_id": payload.get("request_id"),
    }


def write_control_state(
    *,
    enabled: bool,
    reason: str,
    request_id: str | None = None,
) -> dict[str, Any]:
    path = control_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "version": 1,
        "enabled": enabled,
        "updated_at": datetime.now(UTC).isoformat(),
        "reason": reason.strip(),
        "source": "fastapi_control",
        "request_id": request_id,
    }

    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    try:
        os.replace(temporary, path)
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    return payload
