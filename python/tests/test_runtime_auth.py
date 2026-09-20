from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from fabric_jupyter.models import FabricLanguage, FabricTarget
from fabric_jupyter.runtime_auth import (
    FABRIC_SCOPE,
    POWERBI_SCOPE,
    RuntimeAccess,
    RuntimeFailure,
    grant_expiry,
    trusted_origin,
)

WORKSPACE = "11111111-1111-1111-1111-111111111111"
NOTEBOOK = "22222222-2222-2222-2222-222222222222"
CAPACITY = "33333333-3333-3333-3333-333333333333"
TARGET = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)


class Credential:
    def __init__(self):
        self.scopes = []

    async def get_token(self, scope):
        self.scopes.append(scope)
        return SimpleNamespace(token="synthetic-credential", expires_on=int(time.time() + 3600))

    async def close(self):
        pass


@pytest.mark.parametrize("origin", [
    "http://test.analysis.windows.net", "https://evil.example",
    "https://test.analysis.windows.net.evil.example", "https://user@test.analysis.windows.net",
    "https://test.analysis.windows.net/path", "https://test.analysis.windows.net?token=bad",
    "https://127.0.0.1", "https://test.analysis.windows.net:8443",
    "https://test.analysis.windows.net\\@evil.example",
])
def test_untrusted_runtime_origins_rejected(origin):
    with pytest.raises(RuntimeFailure):
        trusted_origin(origin)


def test_grant_is_target_bound_cached_and_audience_separated():
    async def run():
        calls = []
        credential = Credential()

        def handle(request):
            calls.append(request)
            path = request.url.path
            if path.endswith("/workspaces/" + WORKSPACE):
                return httpx.Response(200, json={"id": WORKSPACE, "capacityId": CAPACITY})
            if path.endswith("/notebooks/" + NOTEBOOK):
                return httpx.Response(200, json={"id": NOTEBOOK})
            if path == "/metadata/cluster":
                return httpx.Response(200, json={"backendUrl": "https://test.analysis.windows.net"})
            if path.endswith("/generatemwctoken"):
                assert json.loads(request.content) == {
                    "capacityObjectId": CAPACITY, "workspaceObjectId": WORKSPACE,
                    "workloadType": "Notebook", "artifactObjectIds": [NOTEBOOK],
                }
                return httpx.Response(200, json={
                    "Token": "synthetic-mwc", "CapacityObjectId": CAPACITY,
                    "TargetUriHost": "test.pbidedicated.windows.net", "Expiry": time.time() + 3600,
                })
            raise AssertionError("unexpected request")

        access = RuntimeAccess(TARGET, client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
                               credential=credential)
        first = await access.grant()
        assert first is await access.grant()
        assert len(calls) == 4
        assert credential.scopes == [FABRIC_SCOPE, POWERBI_SCOPE]
        assert access.base(first).endswith(f"/artifacts/{NOTEBOOK}/jupyterApi/versions/1")
        assert "synthetic-mwc" not in repr(first)
        await access.close()

    asyncio.run(run())


def test_redirect_and_service_body_never_leak_credentials():
    async def run():
        calls = []

        def handle(request):
            calls.append(request)
            return httpx.Response(307, headers={"Location": "https://evil.example"},
                                  text="synthetic-secret-in-response")

        access = RuntimeAccess(TARGET, client=httpx.AsyncClient(transport=httpx.MockTransport(handle)))
        with pytest.raises(RuntimeFailure) as error:
            await access.request("GET", "https://api.fabric.microsoft.com/test",
                                 authorization="Bearer synthetic-secret-in-header")
        assert "secret" not in str(error.value)
        assert "evil.example" not in str(error.value)
        assert len(calls) == 1
        await access.close()

    asyncio.run(run())


def test_expired_or_missing_grant_lifetime_fails_closed():
    for expiry in (0, time.time() - 1, "not-a-date", None):
        with pytest.raises(RuntimeFailure):
            grant_expiry(expiry, "not-a-jwt")


def test_authenticated_discovery_cannot_delegate_to_non_microsoft_origin():
    async def run():
        calls = []

        def handle(request):
            calls.append(str(request.url))
            if request.url.path.endswith("/workspaces/" + WORKSPACE):
                return httpx.Response(200, json={"id": WORKSPACE, "capacityId": CAPACITY})
            if request.url.path.endswith("/notebooks/" + NOTEBOOK):
                return httpx.Response(200, json={"id": NOTEBOOK})
            return httpx.Response(200, json={"backendUrl": "https://attacker.example"})

        access = RuntimeAccess(TARGET, client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
                               credential=Credential())
        with pytest.raises(RuntimeFailure, match="outside supported"):
            await access.grant()
        assert all(url.startswith("https://api.fabric.microsoft.com/") for url in calls)
        await access.close()

    asyncio.run(run())
