"""The ``fabric-jupyter`` command line interface."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from .broker import BrokerClient, BrokerServer, load_endpoint
from .config import inspect_profiles
from .installer import install_kernels
from .kernel import launch_kernel
from .models import redact_mapping


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fabric-jupyter", description="Local fabric-jupyter kernel and broker"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    install = subcommands.add_parser("install-kernels", help="install user-scoped kernelspecs")
    install.add_argument(
        "--replace", action="store_true", help="replace existing Fabric kernelspecs"
    )
    kernel = subcommands.add_parser("kernel", help="run a Jupyter kernel process")
    kernel.add_argument("-f", "--connection-file", required=True)
    kernel.add_argument("--profile", required=True)
    broker = subcommands.add_parser("broker", help="run owner-local broker in the foreground")
    broker.add_argument("--idle-timeout", type=int, default=900)
    profile = subcommands.add_parser("profile", help="inspect safe profile configuration")
    profile_subcommands = profile.add_subparsers(dest="profile_command", required=True)
    show = profile_subcommands.add_parser("show", help="show credential-redacted profiles")
    show.add_argument("--config", type=Path)
    status = subcommands.add_parser(
        "broker-status", help="show broker session state without credentials"
    )
    status.add_argument("--endpoint", type=Path)
    return parser


async def _serve_broker(idle_timeout: int) -> int:
    if idle_timeout < 60 or idle_timeout > 86_400:
        raise ValueError("--idle-timeout must be between 60 and 86400")
    server = BrokerServer(idle_timeout_seconds=idle_timeout)
    endpoint = await server.start(persist_endpoint=True)
    # Safe: this intentionally excludes the auth secret.
    print(json.dumps({"status": "ready", **endpoint.public_dict()}, sort_keys=True), flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await server.close()
    return 0


async def _broker_status(endpoint: Path | None) -> int:
    sessions = await BrokerClient(load_endpoint(endpoint)).status()
    print(
        json.dumps(
            {"sessions": [redact_mapping(item) for item in sessions]}, indent=2, sort_keys=True
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "install-kernels":
            print(json.dumps({"installed": install_kernels(replace=args.replace)}, sort_keys=True))
            return 0
        if args.command == "kernel":
            launch_kernel(args.connection_file, args.profile)
            return 0
        if args.command == "broker":
            return asyncio.run(_serve_broker(args.idle_timeout))
        if args.command == "profile" and args.profile_command == "show":
            print(json.dumps(inspect_profiles(args.config), indent=2, sort_keys=True))
            return 0
        if args.command == "broker-status":
            return asyncio.run(_broker_status(args.endpoint))
    except (FileExistsError, OSError, RuntimeError, ValueError) as exc:
        print(f"fabric-jupyter: {exc}", file=sys.stderr)
        return 2
    return 2
