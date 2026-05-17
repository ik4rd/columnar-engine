#!/usr/bin/env python3
"""
action.py — starts Docker (if not running) and then runs act
for a GitHub Actions workflow.

Examples:
  python3 scripts/action.py
  python3 scripts/action.py --workflow .github/workflows/cmake-single-platform.yml
  python3 scripts/action.py -- --verbose
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional


DEFAULT_WORKFLOW = ".github/workflows/cmake-single-platform.yml"
DEFAULT_PLATFORM_MAP = "ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest"
DEFAULT_ARCH = "linux/amd64"
DEFAULT_DOCKER_TIMEOUT_SECONDS = 180

DEFAULT_REPO_ROOT_SENTINEL = ""

DOCKER_DESKTOP_APP_NAME_MAC = "Docker"

ACT_EXE = "act"
DOCKER_EXE = "docker"
GIT_EXE = "git"
SYSTEMCTL_EXE = "systemctl"
SERVICE_EXE = "service"
SUDO_EXE = "sudo"
OPEN_EXE_MAC = "open"

APT_PACKAGES_FILE = ".github/apt-packages.txt"


ERR_DOCKER_NOT_FOUND = (
    "Docker executable not found in PATH. Install Docker Desktop (macOS/Windows) "
    "or Docker Engine (Linux)."
)
ERR_ACT_NOT_FOUND = (
    "'act' executable not found in PATH. Install nektos/act (e.g. 'brew install act') "
    "or follow the official installation docs."
)
ERR_WORKFLOW_NOT_FOUND = "Workflow file not found: {path}"
ERR_DOCKER_NOT_READY_TIMEOUT = (
    "Docker did not become ready in time. Ensure Docker Desktop is running and does not "
    "require manual confirmation."
)
ERR_APT_PACKAGES_NOT_FOUND = (
    "Required file not found: {path}\n"
    "Create it (repo-root/.github/apt-packages.txt) or update the workflow to not depend on it."
)

INFO_DOCKER_STARTING_MAC = "Docker daemon is not responding — starting Docker Desktop…"
INFO_DOCKER_STARTING_LINUX = (
    "Docker daemon is not responding — trying to start docker service…"
)
INFO_DOCKER_UNSUPPORTED_OS = (
    "Docker daemon is not responding. OS '{os_name}'. Start Docker manually and retry."
)
INFO_RUNNING_ACT = "Running:"
INFO_USING_REPO_ROOT = "Using repo root:"
INFO_AUTO_DETECT_REPO_ROOT_FAILED = (
    "Auto-detecting repo root via git failed; falling back to script parent directory."
)


def run(
    cmd: List[str], check: bool = False, capture: bool = False
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def have_exe(name: str) -> bool:
    return shutil.which(name) is not None


def resolve_path(base: Path, maybe_path: str) -> Path:
    p = Path(maybe_path)
    return p if p.is_absolute() else (base / p)


def detect_repo_root() -> Path:
    if have_exe(GIT_EXE):
        p = run([GIT_EXE, "rev-parse", "--show-toplevel"], check=False, capture=True)
        if p.returncode == 0 and p.stdout:
            return Path(p.stdout.strip()).resolve()

    print(INFO_AUTO_DETECT_REPO_ROOT_FAILED, flush=True)
    return Path(__file__).resolve().parent.parent


def docker_daemon_ready() -> bool:
    if not have_exe(DOCKER_EXE):
        return False
    p = run([DOCKER_EXE, "info"], check=False, capture=True)
    return p.returncode == 0


def start_docker_desktop_mac() -> None:
    run(
        [OPEN_EXE_MAC, "-g", "-a", DOCKER_DESKTOP_APP_NAME_MAC],
        check=False,
        capture=False,
    )


def start_docker_linux() -> None:
    if have_exe(SYSTEMCTL_EXE):
        p = run([SYSTEMCTL_EXE, "start", DOCKER_EXE], check=False, capture=True)
        if p.returncode != 0 and have_exe(SUDO_EXE):
            run(
                [SUDO_EXE, SYSTEMCTL_EXE, "start", DOCKER_EXE],
                check=False,
                capture=False,
            )
    elif have_exe(SERVICE_EXE):
        p = run([SERVICE_EXE, DOCKER_EXE, "start"], check=False, capture=True)
        if p.returncode != 0 and have_exe(SUDO_EXE):
            run(
                [SUDO_EXE, SERVICE_EXE, DOCKER_EXE, "start"], check=False, capture=False
            )


def ensure_docker_running(
    timeout_seconds: int = DEFAULT_DOCKER_TIMEOUT_SECONDS,
) -> None:
    if not have_exe(DOCKER_EXE):
        raise RuntimeError(ERR_DOCKER_NOT_FOUND)

    if docker_daemon_ready():
        return

    system = platform.system().lower()
    if system == "darwin":
        print(INFO_DOCKER_STARTING_MAC, flush=True)
        start_docker_desktop_mac()
    elif system == "linux":
        print(INFO_DOCKER_STARTING_LINUX, flush=True)
        start_docker_linux()
    else:
        print(INFO_DOCKER_UNSUPPORTED_OS.format(os_name=platform.system()), flush=True)

    start = time.time()
    while time.time() - start < timeout_seconds:
        if docker_daemon_ready():
            return
        time.sleep(2)

    raise RuntimeError(ERR_DOCKER_NOT_READY_TIMEOUT)


def ensure_act_installed() -> None:
    if have_exe(ACT_EXE):
        return
    raise RuntimeError(ERR_ACT_NOT_FOUND)


def run_act(workflow: str, arch: str, platform_map: str, extra_args: List[str]) -> int:
    cmd = [
        ACT_EXE,
        "-W",
        workflow,
        "--container-architecture",
        arch,
        "-P",
        platform_map,
    ]
    cmd.extend(extra_args)

    print(f"\n{INFO_RUNNING_ACT} {' '.join(cmd)}\n", flush=True)
    p = subprocess.run(cmd)
    return p.returncode


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Start Docker (if needed) and run act workflow."
    )
    parser.add_argument(
        "--workflow",
        default=DEFAULT_WORKFLOW,
        help="Path to workflow yml (relative to repo root)",
    )
    parser.add_argument(
        "--arch", default=DEFAULT_ARCH, help="act --container-architecture value"
    )
    parser.add_argument(
        "--platform-map",
        default=DEFAULT_PLATFORM_MAP,
        help='act -P mapping, e.g. "ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest"',
    )
    parser.add_argument(
        "--repo-root",
        default=DEFAULT_REPO_ROOT_SENTINEL,
        help="Repo root. If empty, auto-detect using git (default).",
    )
    parser.add_argument(
        "--docker-timeout",
        type=int,
        default=DEFAULT_DOCKER_TIMEOUT_SECONDS,
        help="Seconds to wait for Docker to become ready",
    )
    parser.add_argument(
        "act_args",
        nargs=argparse.REMAINDER,
        help="Extra args passed to act (prefix with --), e.g. -- -v",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    extra = args.act_args
    if extra and extra[0] == "--":
        extra = extra[1:]

    repo_root = Path(args.repo_root).resolve() if args.repo_root else detect_repo_root()
    print(f"{INFO_USING_REPO_ROOT} {repo_root}", flush=True)
    os.chdir(repo_root)

    apt_file = resolve_path(repo_root, APT_PACKAGES_FILE)
    if not apt_file.exists():
        raise RuntimeError(ERR_APT_PACKAGES_NOT_FOUND.format(path=apt_file))

    workflow_path = resolve_path(repo_root, args.workflow)
    if not workflow_path.exists():
        raise RuntimeError(ERR_WORKFLOW_NOT_FOUND.format(path=workflow_path.resolve()))

    ensure_act_installed()
    ensure_docker_running(timeout_seconds=args.docker_timeout)

    return run_act(str(workflow_path), args.arch, args.platform_map, extra)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"\nERROR: {e}\n", file=sys.stderr)
        raise SystemExit(1)
