"""User-scoped Jupyter kernelspec installation."""

from __future__ import annotations

import sys
from pathlib import Path

from jupyter_client.kernelspec import KernelSpecManager

from .config import load_profiles
from .models import Profile


def kernel_name(profile: Profile) -> str:
    return f"fabric-{profile.language.value}"


def kernel_display_name(profile: Profile) -> str:
    language = "PySpark" if profile.language.value == "pyspark" else "Python"
    return f"fabric-jupyter ({language})"


def install_kernels(*, replace: bool = False) -> list[str]:
    """Install built-in Fabric Python and PySpark user kernelspecs."""

    profiles = load_profiles()
    manager = KernelSpecManager()
    installed: list[str] = []
    for profile_name in ("fabric-pyspark", "fabric-python"):
        profile = profiles.get(profile_name)
        if profile is None:
            continue
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
            "metadata": {
                "debugger": False,
                "fabric_jupyter": {
                    "profile": profile.name,
                    "transport": profile.transport.value,
                    "capabilities": {
                        "execute": True,
                        "interrupt": True,
                        "shutdown": True,
                        "completion": False,
                        "inspect": False,
                        "widgets": False,
                        "richComm": False,
                    },
                },
            },
        }
        (spec_dir / "kernel.json").write_text(
            __import__("json").dumps(kernel_json, indent=2), encoding="utf-8"
        )
        installed.append(name)
    return installed
