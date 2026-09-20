from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager

from fabric_jupyter.config import write_profiles
from fabric_jupyter.installer import install_kernels
from fabric_jupyter.models import FabricLanguage, FabricTarget, Profile
from fabric_jupyter.paths import broker_endpoint_path

WORKSPACE = "11111111-1111-1111-1111-111111111111"
NOTEBOOK = "22222222-2222-2222-2222-222222222222"


def _get_shell_reply(client, message_id: str) -> dict:
    while True:
        reply = client.get_shell_msg(timeout=15)
        if reply["parent_header"].get("msg_id") == message_id:
            return reply


def _isolated_kernel_env(local_state: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["JUPYTER_DATA_DIR"] = str(local_state / "jupyter")
    env["XDG_CONFIG_HOME"] = str(local_state / "config")
    env["XDG_STATE_HOME"] = str(local_state / "state")
    env["APPDATA"] = str(local_state / "config")
    env["LOCALAPPDATA"] = str(local_state / "state")
    return env


def test_user_kernelspec_install(local_state: Path, monkeypatch) -> None:
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(local_state / "jupyter"))
    installed = install_kernels()
    assert installed == ["fabric-pyspark", "fabric-python"]
    kernels = local_state / "jupyter" / "kernels"
    spec = json.loads((kernels / "fabric-pyspark" / "kernel.json").read_text())
    assert spec["argv"][2:4] == ["fabric_jupyter", "kernel"]
    assert spec["argv"][4:6] == ["-f", "{connection_file}"]
    assert spec["display_name"] == "fabric-jupyter (PySpark)"
    assert spec["metadata"]["fabric_jupyter"]["capabilities"]["widgets"] is False


def test_generated_default_kernelspec_starts_without_profile_or_broker(
    local_state: Path, monkeypatch
) -> None:
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(local_state / "jupyter"))
    install_kernels()
    assert not broker_endpoint_path().exists()

    env = _isolated_kernel_env(local_state)

    manager = KernelManager(
        kernel_name="fabric-pyspark", kernel_spec_manager=KernelSpecManager()
    )
    client = None
    try:
        manager.start_kernel(env=env)
        client = manager.client()
        client.start_channels()
        client.wait_for_ready(timeout=15)
        message_id = client.execute("print('not evaluated')")
        shell_reply = _get_shell_reply(client, message_id)
        assert shell_reply["content"]["status"] == "ok"

        messages: list[dict] = []
        while True:
            message = client.get_iopub_msg(timeout=15)
            if message["parent_header"].get("msg_id") == message_id:
                messages.append(message)
                if (
                    message["msg_type"] == "status"
                    and message["content"]["execution_state"] == "idle"
                ):
                    break
        assert any(message["msg_type"] == "stream" for message in messages)
        assert any(message["msg_type"] == "execute_result" for message in messages)
    finally:
        if client is not None:
            with suppress(Exception):
                client.stop_channels()
        with suppress(Exception):
            manager.shutdown_kernel(now=True)


def test_generated_default_kernelspec_control_shutdown_has_clean_stderr(
    local_state: Path, monkeypatch
) -> None:
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(local_state / "jupyter"))
    install_kernels()
    env = _isolated_kernel_env(local_state)

    manager = KernelManager(
        kernel_name="fabric-pyspark", kernel_spec_manager=KernelSpecManager()
    )
    client = None
    process = None
    try:
        manager.start_kernel(env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process = getattr(manager.provisioner, "process", None)
        client = manager.client()
        client.start_channels()
        client.wait_for_ready(timeout=15)
        message_id = client.execute("print('not evaluated')")
        assert _get_shell_reply(client, message_id)["content"]["status"] == "ok"

        shutdown_reply = client.shutdown(restart=False, reply=True, timeout=15)
        assert shutdown_reply["content"] == {"status": "ok", "restart": False}

        deadline = time.monotonic() + 15
        while manager.is_alive() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not manager.is_alive()
    finally:
        if client is not None:
            with suppress(Exception):
                client.stop_channels()
        if manager.has_kernel and manager.is_alive():
            with suppress(Exception):
                manager.shutdown_kernel(now=True)

    stderr = ""
    if process is not None and process.stderr is not None:
        raw_stderr = process.stderr.read()
        stderr = raw_stderr.decode("utf-8", errors="replace") if isinstance(raw_stderr, bytes) else raw_stderr
    assert "Future attached to a different loop" not in stderr
    assert "Traceback" not in stderr
    assert "ERROR" not in stderr


def test_real_jupyter_client_with_fake_broker(
    local_state: Path, monkeypatch
) -> None:
    profile = Profile(
        name="test",
        language=FabricLanguage.PYSPARK,
        target=FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK),
    )
    write_profiles({profile.name: profile})
    kernels = local_state / "jupyter" / "kernels" / "test"
    kernels.mkdir(parents=True)
    (kernels / "kernel.json").write_text(
        json.dumps(
            {
                "argv": [
                    sys.executable,
                    "-m",
                    "fabric_jupyter",
                    "kernel",
                    "-f",
                    "{connection_file}",
                    "--profile",
                    "test",
                ],
                "display_name": "test",
                "language": "python",
            }
        )
    )
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(local_state / "jupyter"))
    env = _isolated_kernel_env(local_state)
    broker = subprocess.Popen(
        [sys.executable, "-m", "fabric_jupyter", "broker", "--idle-timeout", "60"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 15
    while not broker_endpoint_path().exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert broker_endpoint_path().exists(), broker.stderr.read()
    manager = KernelManager(kernel_name="test", kernel_spec_manager=KernelSpecManager())
    client = None
    try:
        manager.start_kernel(env=env)
        client = manager.client()
        client.start_channels()
        client.wait_for_ready(timeout=15)
        message_id = client.execute("print('not evaluated')")
        messages: list[dict] = []
        while True:
            message = client.get_iopub_msg(timeout=15)
            if message["parent_header"].get("msg_id") == message_id:
                messages.append(message)
                if (
                    message["msg_type"] == "status"
                    and message["content"]["execution_state"] == "idle"
                ):
                    break
        assert any(message["msg_type"] == "stream" for message in messages)
        assert any(message["msg_type"] == "execute_result" for message in messages)
    finally:
        if client is not None:
            with suppress(Exception):
                client.stop_channels()
        with suppress(Exception):
            manager.shutdown_kernel(now=True)
        broker.terminate()
        with suppress(subprocess.TimeoutExpired):
            broker.wait(timeout=10)
