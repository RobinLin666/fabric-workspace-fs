from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from fabric_jupyter.broker import BrokerServer
from fabric_jupyter.cli import _broker_status, main
from fabric_jupyter.paths import ensure_private, secure_private_file, state_dir


def test_missing_broker_status(
    local_state: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    # Exercises the real GetFileAttributesW path on Windows, not a mocked loader.
    assert main(["broker-status"]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"status": "not-running", "sessions": []}
    assert captured.err == ""
    assert not (state_dir() / "broker.json").exists()


def test_missing_explicit_endpoint(
    local_state: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    endpoint = ensure_private(state_dir()) / "missing.json"
    assert main(["broker-status", "--endpoint", str(endpoint)]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["status"] == "not-running"
    assert captured.err == ""


def test_malformed_endpoint_is_an_error(
    local_state: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    endpoint = ensure_private(state_dir()) / "broker.json"
    with secure_private_file(endpoint) as stream:
        stream.write("not json")
    assert main(["broker-status"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "fabric-jupyter:" in captured.err
    assert "Traceback" not in captured.err


def test_permission_failure_is_not_absence(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    def denied(path: Path | None) -> None:
        raise PermissionError("endpoint access denied")

    monkeypatch.setattr("fabric_jupyter.cli.load_endpoint", denied)
    assert main(["broker-status"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "endpoint access denied" in captured.err


def test_running_broker_status(
    local_state: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    async def check() -> None:
        server = BrokerServer()
        await server.start(persist_endpoint=True)
        try:
            assert await _broker_status(None) == 0
        finally:
            await server.close()

    asyncio.run(check())
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"status": "running", "sessions": []}
    assert captured.err == ""


def test_profile_show_supported_invocation(
    local_state: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["profile", "show"]) == 0
    captured = capsys.readouterr()
    assert "fabric-pyspark" in captured.out
    assert "fabric-python" in captured.out
    assert json.loads(captured.out)
    assert captured.err == ""


def test_profile_show_explicit_config(
    local_state: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    config = ensure_private(state_dir()) / "profiles.json"
    assert main([
        "profile", "configure", "--config", str(config),
        "--name", "custom-python", "--transport", "fake",
    ]) == 0
    capsys.readouterr()
    assert main(["profile", "show", "--config", str(config)]) == 0
    captured = capsys.readouterr()
    assert "custom-python" in captured.out
    assert json.loads(captured.out)
    assert captured.err == ""


@pytest.mark.skipif(os.name != "nt", reason="Native Win32 error mapping")
@pytest.mark.parametrize(
    ("code", "exception"), [(2, FileNotFoundError), (3, FileNotFoundError), (5, PermissionError)],
)
def test_windows_error_categories(
    code: int, exception: type[OSError], monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fabric_jupyter import _win_acl

    monkeypatch.setattr(_win_acl, "_get_last_error", lambda: code)
    with pytest.raises(exception):
        _win_acl._raise_last_error("status probe")
    with pytest.raises(exception):
        _win_acl._check_win32_error(code, "status probe")
