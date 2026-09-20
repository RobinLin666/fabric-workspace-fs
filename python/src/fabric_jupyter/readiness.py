"""Local capability reporting; never performs a remote Fabric health check."""

from __future__ import annotations

from typing import Any


def runtime_status() -> dict[str, Any]:
    return {
        "remoteFabricSessionSupported": True,
        "remoteCheckPerformed": False,
        "defaultTransport": "fabric",
        "security": {
            "serverBoundTargetPolicy": True,
            "privateEndpointRequired": True,
        },
        "transports": {
            "fabric": {
                "available": True,
                "optIn": True,
                "requiresExplicitTarget": True,
                "executesCode": True,
                "createsRemoteSession": True,
                "installedKernelValidated": True,
                "validatedLanguages": [
                    "pyspark",
                    "spark",
                    "sparkr",
                    "python3.11",
                    "python3.12",
                ],
            },
        },
        "blockers": [],
    }
