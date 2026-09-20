from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager

from fabric_jupyter.broker import BrokerClient, load_endpoint
from fabric_jupyter.config import write_profiles
from fabric_jupyter.installer import install_kernels
from fabric_jupyter.kernel import FabricKernel
from fabric_jupyter.models import (
    EventKind,
    ExecutionEvent,
    FabricLanguage,
    FabricTarget,
    Profile,
    TransportKind,
)
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
    assert install_kernels() == []
    assert not (local_state / "jupyter" / "kernels" / "fabric-pyspark").exists()


def test_fabric_kernelspec_marks_pyspark_as_live_validated(
    local_state: Path, monkeypatch
) -> None:
    target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)
    pyspark = Profile(
        name="real-pyspark",
        language=FabricLanguage.PYSPARK,
        transport=TransportKind.FABRIC,
        target=target,
    )
    write_profiles({pyspark.name: pyspark})
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(local_state / "jupyter"))
    install_kernels()
    kernels = local_state / "jupyter" / "kernels"
    pyspark_spec = json.loads((kernels / "real-pyspark" / "kernel.json").read_text())
    assert pyspark_spec["metadata"]["fabric_jupyter"]["installedKernelValidated"] is True
    assert pyspark_spec["metadata"]["fabric_jupyter"]["remoteFabricSessionSupported"] is True
    assert pyspark_spec["display_name"] == "fabric-jupyter (PySpark; Fabric)"


def test_replace_removes_an_existing_fake_kernelspec(local_state: Path, monkeypatch) -> None:
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(local_state / "jupyter"))
    fake_spec = local_state / "jupyter" / "kernels" / "fabric-pyspark"
    fake_spec.mkdir(parents=True)
    (fake_spec / "kernel.json").write_text("{}")
    assert install_kernels(replace=True) == []
    assert not fake_spec.exists()


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
        [sys.executable, "-m", "fabric_jupyter", "broker", "--profile", "test", "--idle-timeout", "60"],
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
        sessions = asyncio.run(BrokerClient(load_endpoint()).status())
        assert len(sessions) == 1
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


def test_kernel_passes_through_display_updates_without_buffering() -> None:
    async def run() -> None:
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)

        class Client:
            async def stream_execute(self, **kwargs):
                assert kwargs["transport"] is TransportKind.FABRIC
                yield ExecutionEvent(
                    EventKind.DISPLAY_DATA,
                    {"data": {"text/plain": "initial"}, "transient": {"display_id": "one"}},
                )
                yield ExecutionEvent(
                    EventKind.UPDATE_DISPLAY_DATA,
                    {"data": {"text/plain": "updated"}, "transient": {"display_id": "one"}},
                )
                yield ExecutionEvent(EventKind.CLEAR_OUTPUT, {"wait": True})

        kernel = FabricKernel()
        kernel._profile = Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        kernel._target = target
        kernel._connect_profile = AsyncMock(return_value=Client())
        kernel.send_response = MagicMock()
        kernel.iopub_socket = object()
        kernel.execution_count = 1
        reply = await kernel.do_execute("display", silent=False)
        assert reply["status"] == "ok"
        assert [
            call.args[1] for call in kernel.send_response.call_args_list
        ] == ["display_data", "update_display_data", "clear_output"]

    asyncio.run(run())


def test_kernel_adds_standard_mime_fallbacks_for_fabric_outputs() -> None:
    async def run() -> None:
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)

        class Client:
            async def stream_execute(self, **kwargs):
                assert kwargs["transport"] is TransportKind.FABRIC
                yield ExecutionEvent(
                    EventKind.COMM_OPEN,
                    {
                        "comm_id": "widget-comm",
                        "target_name": "synapse:widget",
                        "data": {
                            "widget_id": "5e1df07d-5123-44a5-b14b-8b02b4cb09a8",
                            "widget_type": "Synapse.DataFrame",
                            "state": {
                                "table": {
                                    "schema": [
                                        {"key": "0", "name": "name", "type": "string"},
                                        {"key": "1", "name": "count", "type": "long"},
                                    ],
                                    "rows": [{"0": "alpha", "1": 3}],
                                    "truncated": False,
                                },
                                "language": "pyspark",
                            },
                        },
                    },
                )
                yield ExecutionEvent(
                    EventKind.DISPLAY_DATA,
                    {
                        "data": {
                            "text/plain": (
                                "StatementMeta(, d3afa8c9-34e2-459d-9721-d9ffa6d8bcfd, "
                                "11, Finished, Available, Finished, False)"
                            )
                        }
                    },
                )
                yield ExecutionEvent(
                    EventKind.RESULT,
                    {
                        "data": {
                            "text/plain": (
                                "SynapseWidget(Synapse.DataFrame, "
                                "5e1df07d-5123-44a5-b14b-8b02b4cb09a8)"
                            )
                        },
                        "execution_count": 1,
                        "metadata": {},
                    },
                )

        kernel = FabricKernel()
        kernel._profile = Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        kernel._target = target
        kernel._connect_profile = AsyncMock(return_value=Client())
        kernel.send_response = MagicMock()
        kernel.iopub_socket = object()
        kernel.execution_count = 1
        reply = await kernel.do_execute("display", silent=False)
        assert reply["status"] == "ok"

        display = kernel.send_response.call_args_list[0].args[2]["data"]
        assert "text/html" in display
        assert "application/vnd.fabric.statement-meta+json" not in display
        assert "text/plain" not in display
        assert "display:none" in display["text/html"]
        statement_marker = kernel.send_response.call_args_list[0].args[2]["metadata"]["fabric_jupyter"]
        assert statement_marker["restore_data"]["text/plain"].startswith("StatementMeta(")
        result = kernel.send_response.call_args_list[1].args[2]["data"]
        assert "text/html" in result
        assert "application/vnd.synapse.widget-view+json" not in result
        marker = kernel.send_response.call_args_list[1].args[2]["metadata"]["fabric_jupyter"]
        assert marker["restore_data"]["application/vnd.synapse.widget-view+json"] == {
            "widget_type": "Synapse.DataFrame",
            "widget_id": "5e1df07d-5123-44a5-b14b-8b02b4cb09a8",
        }
        assert set(result) == {"text/html"}
        assert marker["generated_mime_types"] == ["text/html"]
        assert "<th>name</th><th>count</th>" in result["text/html"]
        assert "<td>alpha</td>" in result["text/html"]
        assert "<td>3</td>" in result["text/html"]
        assert marker["widget_state"]["sync_state"]["table"]["rows"] == [
            {"0": "alpha", "1": 3}
        ]

    asyncio.run(run())


def test_kernel_filters_fabric_markers_from_stream_outputs() -> None:
    async def run() -> None:
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)

        class Client:
            async def stream_execute(self, **kwargs):
                assert kwargs["transport"] is TransportKind.FABRIC
                yield ExecutionEvent(
                    EventKind.STREAM,
                    {
                        "name": "stdout",
                        "text": (
                            "StatementMeta(, f2f725cb-c0f5-43a3-99be-eccd0a26349c, "
                            "9, Finished, Available, Finished, False)\n"
                        ),
                    },
                )
                yield ExecutionEvent(
                    EventKind.STREAM,
                    {
                        "name": "stdout",
                        "text": (
                            "SynapseWidget(Synapse.DataFrame, "
                            "c37ea038-d57f-4610-826f-a112f3446c15)\n"
                        ),
                    },
                )
                yield ExecutionEvent(
                    EventKind.STREAM,
                    {"name": "stdout", "text": "[MountPointInfo(mountPoint=/nb_resource)]\n"},
                )

        kernel = FabricKernel()
        kernel._profile = Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        kernel._target = target
        kernel._connect_profile = AsyncMock(return_value=Client())
        kernel.send_response = MagicMock()
        kernel.iopub_socket = object()
        kernel.execution_count = 1
        reply = await kernel.do_execute("display", silent=False)
        assert reply["status"] == "ok"

        messages = [(call.args[1], call.args[2]) for call in kernel.send_response.call_args_list]
        assert [kind for kind, _ in messages] == ["display_data", "stream"]
        assert "application/vnd.synapse.widget-view+json" not in messages[0][1]["data"]
        assert "SynapseWidget(" in messages[0][1]["data"]["text/plain"]
        assert "table preview is unavailable" in messages[0][1]["data"]["text/html"]
        assert messages[0][1]["metadata"]["fabric_jupyter"]["generated_mime_types"] == ["text/html"]
        restore = messages[0][1]["metadata"]["fabric_jupyter"]["restore_data"]
        assert restore["application/vnd.synapse.widget-view+json"] == {
            "widget_type": "Synapse.DataFrame",
            "widget_id": "c37ea038-d57f-4610-826f-a112f3446c15",
        }
        assert messages[1][1]["text"] == "[MountPointInfo(mountPoint=/nb_resource)]\n"

    asyncio.run(run())


def test_kernel_preserves_mount_point_info_result_without_a_second_table() -> None:
    async def run() -> None:
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)

        class Client:
            async def stream_execute(self, **kwargs):
                assert kwargs["transport"] is TransportKind.FABRIC
                yield ExecutionEvent(
                    EventKind.RESULT,
                    {
                        "data": {
                            "text/plain": (
                                "[MountPointInfo(mountPoint=/nb_resource/builtin, "
                                "source=Notebook Working Directory, scope=nb_resource, "
                                "localPath=/synfs/notebook/builtin)]"
                            )
                        },
                        "execution_count": 1,
                        "metadata": {},
                    },
                )

        kernel = FabricKernel()
        kernel._profile = Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        kernel._target = target
        kernel._connect_profile = AsyncMock(return_value=Client())
        kernel.send_response = MagicMock()
        kernel.iopub_socket = object()
        kernel.execution_count = 1
        reply = await kernel.do_execute("notebookutils.fs.mounts()", silent=False)
        assert reply["status"] == "ok"

        result = kernel.send_response.call_args.args[2]["data"]
        assert result == {
            "text/plain": (
                "[MountPointInfo(mountPoint=/nb_resource/builtin, "
                "source=Notebook Working Directory, scope=nb_resource, "
                "localPath=/synfs/notebook/builtin)]"
            )
        }

    asyncio.run(run())
