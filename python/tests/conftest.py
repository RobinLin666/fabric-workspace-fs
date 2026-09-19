from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def local_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    config = tmp_path / "config"
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config))
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    monkeypatch.setenv("APPDATA", str(config))
    monkeypatch.setenv("LOCALAPPDATA", str(state))
    return tmp_path
