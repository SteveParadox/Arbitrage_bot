"""Regression for malformed tmpfs options that Compose config alone can accept."""

import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("compose_guard", ROOT / "scripts/check_production_compose.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def model(tmpfs):
    return {"services": {"sample": {"user": "10001:10001", "read_only": True,
            "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"],
            "mem_limit": 100, "pids_limit": 16, "tmpfs": tmpfs}},
            "networks": {"private": {"internal": True}}}


def test_split_tmpfs_options_are_rejected():
    assert not guard.check(model(["/tmp:rw", "nosuid", "nodev", "size=64m"]))["passed"]
    assert guard.check(model(["/tmp:rw,nosuid,nodev,size=64m,mode=1777"]))["passed"]


def test_committed_production_tmpfs_are_bounded_absolute_mounts():
    configuration = yaml.safe_load((ROOT / "docker/production/compose.yml").read_text())
    for service in configuration["services"].values():
        if "tmpfs" in service:
            assert guard.check(model(service["tmpfs"]))["passed"]
