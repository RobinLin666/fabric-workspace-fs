"""Local capability reporting; never performs a remote Fabric health check."""

from __future__ import annotations

from typing import Any


def runtime_status() -> dict[str, Any]:
    return {
        "remoteFabricSessionSupported": True,
        "remoteCheckPerformed": False,
        "defaultTransport": "fake",
        "security": {
            "serverBoundTargetPolicy": True,
            "privateEndpointRequired": True,
        },
        "transports": {
            "fake": {"available": True, "executesCode": False, "createsRemoteSession": False},
            "fabric": {
                "available": True,
                "optIn": True,
                "requiresExplicitTarget": True,
                "executesCode": True,
                "createsRemoteSession": True,
                "installedKernelValidated": True,
                "validatedLanguages": ["pyspark"],
                "pythonStatus": "unavailable-unverified",
            },
            "experimental": {
                "available": False,
                "executesCode": False,
                "createsRemoteSession": False,
            },
        },
        "blockers": [],
    }
