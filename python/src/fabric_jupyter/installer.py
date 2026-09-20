"""User-scoped Jupyter kernelspec installation."""

from __future__ import annotations

import sys
from pathlib import Path

from jupyter_client.kernelspec import KernelSpecManager

from .config import load_profiles
from .models import Profile, TransportKind


def kernel_name(profile: Profile) -> str:
    return profile.name


def kernel_display_name(profile: Profile) -> str:
    language = "PySpark" if profile.language.value == "pyspark" else "Python"
    if profile.transport is TransportKind.FAKE:
        mode = "offline fake"
    elif profile.transport is TransportKind.FABRIC:
        mode = "Fabric"
    else:
        mode = "unavailable"
    return f"fabric-jupyter ({language}; {mode})"


def install_kernels(*, replace: bool = False) -> list[str]:
    """Install built-in Fabric Python and PySpark user kernelspecs."""

    profiles = load_profiles()
    manager = KernelSpecManager()
    installed: list[str] = []
    for profile in profiles.values():
        name = kernel_name(profile)
        spec_dir = Path(manager.user_kernel_dir) / name
        if spec_dir.exists() and not replace:
            raise FileExistsError(f"kernelspec already exists: {name}; use --replace")
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
            "language": "python",
            "interrupt_mode": "message",
            "metadata": {
                "debugger": False,
                "fabric_jupyter": {
                    "profile": profile.name,
                    "transport": profile.transport.value,
                    "executionMode": (
                        "offline-simulation"
                        if profile.transport is TransportKind.FAKE
                        else (
                            "fabric"
                            if profile.transport is TransportKind.FABRIC
                            else "unavailable"
                        )
                    ),
                    "remoteFabricSessionSupported": (
                        profile.transport is TransportKind.FABRIC
                        and profile.language.value == "pyspark"
                    ),
                    "installedKernelValidated": (
                        profile.transport is TransportKind.FABRIC
                        and profile.language.value == "pyspark"
                    ),
                    "runtimeValidation": (
                        "direct-transport-pyspark"
                        if profile.transport is TransportKind.FABRIC
                        and profile.language.value == "pyspark"
                        else "not-applicable"
                    ),
                    "capabilities": {
                        "execute": profile.transport
                        in {TransportKind.FAKE, TransportKind.FABRIC},
                        "interrupt": profile.transport
                        in {TransportKind.FAKE, TransportKind.FABRIC},
                        "shutdown": True,
                        "completion": False,
                        "inspect": False,
                        "widgets": False,
                        "richComm": False,
                        "synapseDataFrameWidgets": (
                            profile.transport is TransportKind.FABRIC
                            and profile.language.value == "pyspark"
                        ),
                    },
                },
            },
        }
        (spec_dir / "kernel.json").write_text(
            __import__("json").dumps(kernel_json, indent=2), encoding="utf-8"
        )
        installed.append(name)
    return installed
