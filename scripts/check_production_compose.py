"""Check a resolved Compose model; this is NOT a deployment/startup test."""

import argparse
import json
from pathlib import Path


def check(model):
    failures = []
    for name, service in model["services"].items():
        if name == "initialize":
            if service.get("network_mode") != "none":
                failures.append("initializer must have no network")
            continue
        if service.get("user", "").split(":")[0] in ("", "0", "root"):
            failures.append(name + ": non-root user required")
        if not service.get("read_only"):
            failures.append(name + ": read-only root required")
        if "ALL" not in service.get("cap_drop", []):
            failures.append(name + ": capabilities must be dropped")
        if "no-new-privileges:true" not in service.get("security_opt", []):
            failures.append(name + ": no-new-privileges required")
        if not service.get("mem_limit") or not service.get("pids_limit"):
            failures.append(name + ": resource bounds required")
        environment = service.get("environment", {})
        if environment.get("ARB_LIVE_TRADING_ENABLED") not in (None, "false"):
            failures.append(name + ": live must remain false")
        for key in (
            "ARB_DATABASE_URL",
            "ARB_REDIS_URL",
            "ARB_CONTROL_API_TOKEN",
            "ARB_ENGINE_GRPC_TOKEN",
            "BYBIT_API_KEY",
            "BYBIT_API_SECRET",
            "BYBIT_LIVE_API_KEY",
            "BYBIT_LIVE_API_SECRET",
            "BYBIT_TESTNET_API_KEY",
            "BYBIT_TESTNET_API_SECRET",
        ):
            if environment.get(key):
                failures.append(name + ": inline credential forbidden")
        if name in ("postgres", "redis", "engine-control", "api") and service.get(
            "ports"
        ):
            failures.append(name + ": no published administrative port")
        for port in service.get("ports", []):
            if port.get("host_ip") != "127.0.0.1":
                failures.append(name + ": bind published ports to literal loopback")
        if (
            "ARB_DATABASE_URL_FILE" in environment
            and environment.get("ARB_DATABASE_SSLMODE") != "verify-full"
        ):
            failures.append(name + ": verified PostgreSQL policy required")
    if not model.get("networks", {}).get("private", {}).get("internal"):
        failures.append("data-service network must be internal")
    return {
        "configuration_only": True,
        "container_startup_verified": False,
        "services": len(model["services"]),
        "passed": not failures,
        "failures": failures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model", type=Path)
    result = check(json.loads(parser.parse_args().model.read_text()))
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
