"""User-scoped Jupyter kernelspec installation."""

from __future__ import annotations

import sys
from pathlib import Path
from shutil import rmtree

from jupyter_client.kernelspec import KernelSpecManager

from .config import load_profiles
from .models import Profile, TransportKind


def kernel_name(profile: Profile) -> str:
    return profile.name


def kernel_display_name(profile: Profile) -> str:
    return profile.name


def install_kernels(*, replace: bool = False) -> list[str]:
    """Install configured real Fabric user kernelspecs."""

    profiles = load_profiles()
    manager = KernelSpecManager()
    installed: list[str] = []
    for profile in profiles.values():
        name = kernel_name(profile)
        spec_dir = Path(manager.user_kernel_dir) / name
        if profile.transport is not TransportKind.FABRIC:
            if replace and spec_dir.exists():
                rmtree(spec_dir)
            continue
        if spec_dir.exists() and not replace:
            raise FileExistsError(f"kernelspec already exists: {name}; use --replace")
        if spec_dir.exists():
            rmtree(spec_dir)
        spec_dir.mkdir(parents=True, exist_ok=True)
        kernel_json = {
            "argv": [
                sys.executable,
                "-m",
                "fabric_jupyter",
                "kernel",
                "-f",
                "{connection_file}",
                "--profile",
                profile.name,
            ],
            "display_name": kernel_display_name(profile),
            "language": profile.language.jupyter_language,
            "interrupt_mode": "message",
            "metadata": {
                "debugger": False,
                "fabric_jupyter": {
                    "profile": profile.name,
                    "transport": profile.transport.value,
                    "executionMode": "fabric",
                    "remoteFabricSessionSupported": True,
                    "installedKernelValidated": True,
                    "runtimeValidation": "trident-runtime-protocol",
                    "capabilities": {
                        "execute": True,
                        "interrupt": True,
                        "shutdown": True,
                        "completion": False,
                        "inspect": False,
                        "widgets": False,
                        "richComm": False,
                        "synapseDataFrameWidgets": profile.language.is_spark,
                    },
                },
            },
        }
        (spec_dir / "kernel.json").write_text(
            __import__("json").dumps(kernel_json, indent=2), encoding="utf-8"
        )
        installed.append(name)
    return installed
