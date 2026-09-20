"""Offline capability reporting; never a remote Fabric health check."""

from __future__ import annotations

from typing import Any


def runtime_status() -> dict[str, Any]:
    return {
        "remoteFabricSessionSupported": False,
        "remoteCheckPerformed": False,
        "defaultTransport": "fake",
        "security": {
            "serverBoundTargetPolicy": True,
            "privateEndpointRequired": True,
        },
        "transports": {
            "fake": {"available": True, "executesCode": False, "createsRemoteSession": False},
            "experimental": {
                "available": False,
                "executesCode": False,
                "createsRemoteSession": False,
            },
        },
        "blockers": [
            {
                "code": "NOTEBOOK_RUNTIME_CONTRACT",
                "message": (
                    "No supported Notebook-bound runtime startup, readiness, channel-authentication "
                    "and stop contract is implemented. Lakehouse Livy is a different target."
                ),
            },
        ],
    }
