from __future__ import annotations

import asyncio
import copy
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from fabric_jupyter.kernel import FabricKernel, _normalize_fabric_display_content
from fabric_jupyter.models import (
    EventKind,
    ExecutionEvent,
    FabricLanguage,
    FabricTarget,
    Profile,
    TransportKind,
)

MIME = "application/vnd.synapse.mssparkutilsrunmultiple-result+json"


def run_payload(status: str, progress: int) -> dict:
    return {
        "activities": [{
            "activity_name": "0",
            "notebook_name": "Notebook_pyspark",
            "status": status,
            "status_msg": status.title(),
            "progress": progress,
            "duration": 11269 if progress == 100 else 1000,
            "exit_value": "",
            "exception": "",
            "snapshot_status": "success",
            "run_id": "run-1",
        }],
        "numbers": {
            "pending": 0,
            "running": int(status == "running"),
            "succeeded": int(status == "success"),
            "failed": int(status == "failed"),
        },
        "limit": 50,
    }


@pytest.mark.parametrize("encoded", [False, True])
def test_run_output_html_and_lossless_restore_marker(encoded: bool) -> None:
    payload = run_payload("success", 100)
    value = json.dumps(payload) if encoded else payload
    content = {
        "data": {MIME: value},
        "metadata": {"existing": {"keep": True}},
        "transient": {"display_id": "run-table"},
    }
    before = copy.deepcopy(content)
    rendered = _normalize_fabric_display_content(content)
    assert content == before
    assert set(rendered["data"]) == {"text/html"}
    output = rendered["data"]["text/html"]
    for expected in ("Notebook_pyspark", "Success", "100%", "11.3 s"):
        assert expected in output
    assert output.startswith('<table class="fabric-notebook-run">')
    assert "Activity display limit" not in output and "Succeeded:" not in output
    assert rendered["transient"] == {"display_id": "run-table"}
    assert rendered["metadata"] == {
        "existing": {"keep": True},
        "fabric_jupyter": {
            "restore_data": {MIME: value},
            "generated_mime_types": ["text/html"],
        },
    }


def test_run_failure_values_are_escaped_and_all_activities_are_shown() -> None:
    payload = run_payload("failed", 50)
    activity = payload["activities"][0]
    activity["exception"] = '<script>alert("error")</script>'
    activity["notebook_name"] = '<img src=x onerror="bad">'
    activity["exit_value"] = "<failed & stopped>"
    payload["activities"].append({"activity_name": "waiting", "status": "pending"})
    output = _normalize_fabric_display_content({"data": {MIME: payload}})["data"]["text/html"]
    assert "<td>Failed</td>" in output
    assert "<script>" not in output and "<img " not in output
    assert "&lt;script&gt;" in output and "&lt;failed &amp; stopped&gt;" in output
    assert "<td>waiting</td>" in output and "<td>pending</td>" in output
    assert output.count("<tbody>") == 1 and output.count("</tr>") == 3


@pytest.mark.parametrize("payload", [None, [], "not json", {"activities": [None]}])
def test_unknown_payload_shows_explicit_diagnostic_and_preserves_native_data(payload) -> None:
    output = _normalize_fabric_display_content({"data": {MIME: payload}})
    assert "unsupported result payload" in output["data"]["text/html"]
    assert output["metadata"]["fabric_jupyter"]["restore_data"][MIME] == payload


def test_empty_run_waits_without_inventing_success() -> None:
    output = _normalize_fabric_display_content({"data": {MIME: {"activities": []}}})
    assert "Waiting for notebook activities" in output["data"]["text/html"]
    assert "Succeeded" not in output["data"]["text/html"]


@pytest.mark.parametrize("end,expected", [(0, "<td></td>"), (12345, "<td>2.3 s</td>")])
def test_run_duration_supports_timestamp_payloads(end: int, expected: str) -> None:
    payload = {"activities": [{"start_time": 10000, "end_time": end}]}
    output = _normalize_fabric_display_content({"data": {MIME: payload}})
    assert expected in output["data"]["text/html"]
    assert "-10.0" not in output["data"]["text/html"]


def test_existing_standard_representation_is_not_overwritten_or_stripped_on_save() -> None:
    output = _normalize_fabric_display_content({
        "data": {MIME: run_payload("running", 0), "text/html": "<p>backend HTML</p>", "text/plain": "run"},
    })
    assert output["data"] == {"text/html": "<p>backend HTML</p>", "text/plain": "run"}
    assert "generated_mime_types" not in output["metadata"]["fabric_jupyter"]


def test_kernel_streams_run_updates_in_place_before_completion() -> None:
    async def run() -> None:
        target = FabricTarget(
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
            FabricLanguage.PYSPARK,
        )
        kernel = FabricKernel()
        kernel._profile = Profile(
            name="real", language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC, target=target,
        )
        kernel._target = target
        kernel.send_response = MagicMock()
        kernel.iopub_socket = object()
        kernel.execution_count = 1

        class Client:
            async def stream_execute(self, **kwargs):
                for index, (status, progress) in enumerate(
                    [("pending", 0), ("running", 50), ("success", 100)]
                ):
                    assert kernel.send_response.call_count == index
                    yield ExecutionEvent(
                        EventKind.DISPLAY_DATA if index == 0 else EventKind.UPDATE_DISPLAY_DATA,
                        {
                            "data": {MIME: run_payload(status, progress)},
                            "transient": {"display_id": "same-run"},
                            "metadata": {},
                        },
                    )
                yield ExecutionEvent(EventKind.CLEAR_OUTPUT, {"wait": True})

        kernel._connect_profile = AsyncMock(return_value=Client())
        assert (await kernel.do_execute('notebookutils.notebook.run("Notebook_pyspark")', False))[
            "status"
        ] == "ok"
        calls = kernel.send_response.call_args_list
        assert [call.args[1] for call in calls] == [
            "display_data", "update_display_data", "update_display_data", "clear_output",
        ]
        for call, status in zip(calls[:3], ("pending", "running", "success"), strict=True):
            content = call.args[2]
            assert content["transient"] == {"display_id": "same-run"}
            assert status.title() in content["data"]["text/html"]
            assert MIME not in content["data"]
            assert content["metadata"]["fabric_jupyter"]["restore_data"][MIME] == run_payload(
                status, {"pending": 0, "running": 50, "success": 100}[status]
            )
        assert calls[-1].args[2] == {"wait": True}

    asyncio.run(run())


def test_snapshot_link_uses_parent_not_child_artifact_and_escapes_values() -> None:
    payload = run_payload("success", 100)
    activity = payload["activities"][0]
    activity.update({
        "workspace_id": "workspace-id",
        "root_artifact_id": "parent-notebook",
        "artifact_id": "child-notebook",
        "run_id": 'run/"<>',
        "notebook_name": "<Notebook & name>",
    })
    output = _normalize_fabric_display_content({"data": {MIME: payload}})["data"]["text/html"]
    assert (
        'href="https://app.powerbi.com/groups/workspace-id/synapsenotebooks/'
        'parent-notebook/snapshots/run%2F%22%3C%3E?experience=power-bi"'
    ) in output
    assert "child-notebook" not in output
    assert 'rel="noopener noreferrer">&lt;Notebook &amp; name&gt;</a>' in output


@pytest.mark.parametrize("missing", ["workspace_id", "root_artifact_id", "run_id", "snapshot_status"])
def test_snapshot_link_requires_success_and_all_identifiers(missing: str) -> None:
    payload = run_payload("running", 50)
    activity = payload["activities"][0]
    activity.update({"workspace_id": "workspace-id", "root_artifact_id": "parent-notebook"})
    activity.pop(missing)
    output = _normalize_fabric_display_content({"data": {MIME: payload}})["data"]["text/html"]
    assert "<a " not in output and "Notebook_pyspark" in output


@pytest.mark.parametrize("status", ["pending", "failed", "Success"])
def test_unavailable_snapshot_does_not_generate_a_link(status: str) -> None:
    payload = run_payload("running", 50)
    payload["activities"][0].update({
        "workspace_id": "workspace-id", "root_artifact_id": "parent-notebook", "snapshot_status": status,
    })
    output = _normalize_fabric_display_content({"data": {MIME: payload}})["data"]["text/html"]
    assert "<a " not in output
