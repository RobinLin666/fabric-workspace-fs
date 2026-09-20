from __future__ import annotations

import asyncio
import json

import pytest

from fabric_jupyter.cli import main
from fabric_jupyter.models import ExecutionRequest, FabricLanguage, FabricTarget, Profile, TransportKind
from fabric_jupyter.transport import make_transport


def test_runtime_status_reports_installed_pyspark_validation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["runtime-status", "--require-fabric"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["remoteFabricSessionSupported"] is True
    assert report["remoteCheckPerformed"] is False
    assert report["transports"]["fake"]["executesCode"] is False
    assert report["transports"]["fabric"]["requiresExplicitTarget"] is True
    assert report["transports"]["fabric"]["installedKernelValidated"] is True
    assert report["transports"]["fabric"]["validatedLanguages"] == ["pyspark"]
    assert report["transports"]["fabric"]["pythonStatus"] == "unavailable-unverified"
    assert report["transports"]["experimental"]["available"] is False
    assert report["blockers"] == []
    assert report["security"]["serverBoundTargetPolicy"] is True
    assert main(["runtime-status"]) == 0


def test_fabric_profile_without_target_is_rejected_not_silently_simulated() -> None:
    with pytest.raises(ValueError, match="explicit target"):
        Profile.from_dict({"name": "real", "language": "pyspark", "transport": "fabric"})


def test_experimental_transport_cannot_report_success() -> None:
    async def attempt() -> None:
        request = ExecutionRequest(
            "offline-test",
            FabricTarget(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222",
                FabricLanguage.PYSPARK,
            ),
            "must not execute",
        )
        transport = make_transport(TransportKind.EXPERIMENTAL)
        with pytest.raises(RuntimeError, match="runtime-status"):
            async for _ in transport.execute(request):
                pytest.fail("unavailable transport yielded an event")

    asyncio.run(attempt())


def test_fabric_transport_factory_is_explicit_and_lazy() -> None:
    target = FabricTarget(
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
        FabricLanguage.PYSPARK,
    )
    with pytest.raises(ValueError, match="explicit target"):
        make_transport(TransportKind.FABRIC)
    transport = make_transport(TransportKind.FABRIC, target, 120)
    assert type(transport).__name__ == "NotebookRuntimeTransport"
    assert transport.status()["remoteSession"] is False


def test_unavailable_broker_cli_fails_before_binding(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    from fabric_jupyter import cli

    profile = Profile(
        name="unavailable",
        language=FabricLanguage.PYSPARK,
        transport=TransportKind.EXPERIMENTAL,
    )
    monkeypatch.setattr(cli, "load_profiles", lambda: {profile.name: profile})
    assert main(["broker", "--profile", "unavailable"]) == 2
    assert "broker will not start" in capsys.readouterr().err
