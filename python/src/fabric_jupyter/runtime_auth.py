"""In-memory, target-bound public-cloud credential and runtime discovery."""

from __future__ import annotations

import base64
import binascii
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from .models import FabricTarget

FABRIC_ORIGIN = "https://api.fabric.microsoft.com"
FABRIC_SCOPE = FABRIC_ORIGIN + "/.default"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
MAX_RESPONSE = 4 * 1024 * 1024


class RuntimeFailure(RuntimeError):
    """Safe diagnostics never contain raw service bodies, URLs, or credentials."""


class AccessToken(Protocol):
    token: str
    expires_on: int


class Credential(Protocol):
    async def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken: ...
    async def close(self) -> None: ...


def uuid_text(value: Any) -> str:
    if not isinstance(value, str):
        raise RuntimeFailure("runtime response contains an invalid identifier")
    try:
        return str(UUID(value))
    except ValueError:
        raise RuntimeFailure("runtime response contains an invalid identifier") from None


def trusted_origin(value: Any) -> str:
    if not isinstance(value, str) or not value or any(ord(c) <= 32 or ord(c) >= 127 for c in value):
        raise RuntimeFailure("runtime discovery returned an invalid HTTPS origin")
    if "\\" in value or "#" in value:
        raise RuntimeFailure("runtime discovery returned an invalid HTTPS origin")
    raw = value if "://" in value else "https://" + value
    try:
        parsed = urlsplit(raw)
        host = parsed.hostname or ""
        if (
            parsed.scheme != "https" or parsed.username is not None or parsed.password is not None
            or parsed.port not in (None, 443) or parsed.query or parsed.fragment
            or parsed.path not in ("", "/")
            or not re.fullmatch(r"[a-z0-9]+(?:[a-z0-9.-]*[a-z0-9])?", host)
            or any(not label or len(label) > 63 or label.startswith("-") or label.endswith("-")
                   for label in host.split("."))
            or not any(host.endswith(suffix) for suffix in (
                ".analysis.windows.net", ".pbidedicated.windows.net",
                ".fabric.microsoft.com", ".powerbi.com",
            ))
        ):
            raise ValueError()
    except ValueError:
        raise RuntimeFailure("runtime origin is outside supported public-cloud HTTPS hosts") from None
    return "https://" + host


@dataclass(repr=False)
class RuntimeGrant:
    origin: str
    capacity: str
    token: str = field(repr=False)
    expires: float


def grant_expiry(value: Any, token: str) -> float:
    try:
        if value is None:
            parts = token.split(".")
            if len(parts) != 3:
                raise ValueError()
            claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
            value = claims["exp"]
        if isinstance(value, str) and "T" in value:
            expiry = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        else:
            expiry = float(value)
        if not time.time() + 30 < expiry < time.time() + 86400 * 7:
            raise ValueError()
        return expiry
    except (ValueError, TypeError, KeyError, OverflowError, binascii.Error):
        raise RuntimeFailure("runtime grant has missing, invalid, or expired lifetime") from None


class RuntimeAccess:
    """Azure CLI credentials are acquired only on explicit remote startup."""

    def __init__(
        self, target: FabricTarget, *, client: httpx.AsyncClient | None = None,
        credential: Credential | None = None,
    ) -> None:
        self.target = target
        self.client = client or httpx.AsyncClient(
            timeout=30, follow_redirects=False, trust_env=False,
        )
        self._credential = credential
        self._grant: RuntimeGrant | None = None

    async def bearer(self, scope: str) -> str:
        if self._credential is None:
            from azure.identity.aio import AzureCliCredential

            self._credential = AzureCliCredential(process_timeout=30)
        try:
            token = await self._credential.get_token(scope)
        except Exception:
            raise RuntimeFailure(
                "Azure CLI credential acquisition failed; run az login for the target tenant "
                "outside the kernel, then retry"
            ) from None
        if not token.token or any(ord(c) <= 32 or ord(c) >= 127 for c in token.token):
            raise RuntimeFailure("credential provider returned an invalid token")
        return token.token

    async def request(
        self, method: str, url: str, *, authorization: str, body: Any = None,
        expected: tuple[int, ...] = (200,),
    ) -> tuple[int, Any]:
        # Every URL is built internally from a validated discovery grant and UUIDs.
        try:
            async with self.client.stream(
                method, url, headers={"Authorization": authorization, "Accept": "application/json"},
                json=body, follow_redirects=False,
            ) as response:
                if response.status_code not in expected:
                    request_id = response.headers.get("x-ms-request-id", "")
                    suffix = f" (request {request_id})" if re.fullmatch(r"[a-zA-Z0-9-]{1,80}", request_id) else ""
                    raise RuntimeFailure(f"Fabric runtime HTTP {response.status_code}{suffix}; operation {method}")
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > MAX_RESPONSE:
                        raise RuntimeFailure("Fabric runtime response exceeds 4 MiB limit")
                if not data:
                    return response.status_code, None
                try:
                    return response.status_code, json.loads(data)
                except (ValueError, UnicodeDecodeError):
                    raise RuntimeFailure("Fabric runtime returned invalid JSON") from None
        except httpx.HTTPError:
            raise RuntimeFailure(f"Fabric runtime {method} network operation failed or timed out") from None

    async def grant(self, *, force: bool = False) -> RuntimeGrant:
        if self._grant is not None and not force and time.time() + 120 < self._grant.expires:
            return self._grant
        bearer = await self.bearer(FABRIC_SCOPE)
        _, workspace = await self.request(
            "GET", f"{FABRIC_ORIGIN}/v1/workspaces/{self.target.workspace_id}",
            authorization="Bearer " + bearer,
        )
        if not isinstance(workspace, dict) or uuid_text(workspace.get("id")) != self.target.workspace_id.lower():
            raise RuntimeFailure("workspace discovery identity mismatch")
        capacity = uuid_text(workspace.get("capacityId"))
        # A Notebook identity is verified independently before any runtime mutation.
        _, notebook = await self.request(
            "GET", f"{FABRIC_ORIGIN}/v1/workspaces/{self.target.workspace_id}/notebooks/{self.target.notebook_id}",
            authorization="Bearer " + bearer,
        )
        if not isinstance(notebook, dict) or uuid_text(notebook.get("id")) != self.target.notebook_id.lower():
            raise RuntimeFailure("Notebook discovery identity mismatch")
        pbi = await self.bearer(POWERBI_SCOPE)
        _, cluster = await self.request(
            "GET", FABRIC_ORIGIN + "/metadata/cluster", authorization="Bearer " + pbi,
        )
        if not isinstance(cluster, dict):
            raise RuntimeFailure("cluster discovery response is not an object")
        origin = trusted_origin(cluster.get("backendUrl"))
        _, raw = await self.request(
            "POST", origin + "/metadata/v201606/generatemwctoken",
            authorization="Bearer " + pbi,
            body={
                "capacityObjectId": capacity, "workspaceObjectId": self.target.workspace_id,
                "workloadType": "Notebook", "artifactObjectIds": [self.target.notebook_id],
            },
        )
        if not isinstance(raw, dict) or uuid_text(raw.get("CapacityObjectId")) != capacity:
            raise RuntimeFailure("runtime grant capacity mismatch")
        token = raw.get("Token")
        if not isinstance(token, str) or not token or any(ord(c) <= 32 or ord(c) >= 127 for c in token):
            raise RuntimeFailure("runtime grant contains an invalid credential")
        grant = RuntimeGrant(
            trusted_origin(raw.get("TargetUriHost")), capacity, token,
            grant_expiry(raw.get("Expiry"), token),
        )
        if self._grant and (grant.origin, grant.capacity) != (self._grant.origin, self._grant.capacity):
            raise RuntimeFailure("runtime origin/capacity changed; stop and reconnect explicitly")
        self._grant = grant
        return grant

    def base(self, grant: RuntimeGrant) -> str:
        return (
            f"{grant.origin}/webapi/capacities/{grant.capacity}/workloads/Notebook/Data/Direct"
            f"/api/workspaces/{self.target.workspace_id}/artifacts/{self.target.notebook_id}"
            "/jupyterApi/versions/1"
        )

    async def runtime_request(
        self, method: str, path: str, *, body: Any = None, expected: tuple[int, ...] = (200,),
    ) -> tuple[int, Any]:
        grant = await self.grant()
        return await self.request(
            method, self.base(grant) + path, authorization="MwcToken " + grant.token,
            body=body, expected=expected,
        )

    async def close(self) -> None:
        self._grant = None
        await self.client.aclose()
        if self._credential is not None:
            await self._credential.close()
