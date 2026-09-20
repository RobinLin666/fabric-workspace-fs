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
from fabric_jupyter.table_output import (
    INLINE_TABLE_MIMES,
    JUPYTER_DISPLAY_MIME,
    SPARK_SQL_MIME,
    render_table,
)


def payload_for(mime: str, value: str = "alpha") -> dict:
    if mime == JUPYTER_DISPLAY_MIME:
        return {
            "table": {
                "schema": [{"key": "label", "name": "Name"}, {"key": "score", "name": "Score"}],
                "rows": [{"label": value, "score": 1.25}, {"label": "beta", "score": None}],
                "truncated": True,
            },
            "isSummary": False,
        }
    return {
        "schema": {"type": "struct", "fields": [{"name": "Name"}, {"name": "Score"}]},
        "data": [[value, 1.25], ["beta", None]],
    }


@pytest.mark.parametrize("mime", INLINE_TABLE_MIMES)
@pytest.mark.parametrize("encoded", [False, True])
def test_inline_tables_restore_exact_payload_and_metadata(mime: str, encoded: bool) -> None:
    payload = payload_for(mime)
    value = json.dumps(payload) if encoded else payload
    content = {
        "data": {mime: value},
        "metadata": {"keep": {"nested": [1, 2]}},
        "transient": {"display_id": "table-id"},
        "execution_count": 3,
    }
    before = copy.deepcopy(content)
    result = _normalize_fabric_display_content(content)
    assert content == before
    assert set(result["data"]) == {"text/html"}
    output = result["data"]["text/html"]
    assert "<th>Name</th><th>Score</th>" in output
    assert "<td>alpha</td><td>1.25</td>" in output
    assert "<td>beta</td><td></td>" in output
    assert ("truncated preview" in output) == (mime == JUPYTER_DISPLAY_MIME)
    assert result["transient"] == content["transient"]
    assert result["execution_count"] == 3
    assert result["metadata"] == {
        "keep": {"nested": [1, 2]},
        "fabric_jupyter": {
            "generated_mime_types": ["text/html"],
            "restore_data": {mime: value},
        },
    }
    assert _normalize_fabric_display_content(result) == result


@pytest.mark.parametrize("mime", INLINE_TABLE_MIMES)
def test_inline_tables_preserve_existing_html_and_unknown_mime(mime: str) -> None:
    content = {
        "data": {
            mime: payload_for(mime),
            "text/html": "<table>backend</table>",
            "application/vnd.example+json": {"keep": True},
        },
    }
    result = _normalize_fabric_display_content(content)
    assert result["data"] == {
        "text/html": "<table>backend</table>", "application/vnd.example+json": {"keep": True},
    }
    assert result["metadata"]["fabric_jupyter"] == {"restore_data": {mime: payload_for(mime)}}


@pytest.mark.parametrize("mime", INLINE_TABLE_MIMES)
@pytest.mark.parametrize("payload", [None, [], "bad json", {}, {"table": {"schema": [None]}}])
def test_invalid_inline_payload_is_visible_and_reversible(mime: str, payload) -> None:
    result = _normalize_fabric_display_content({"data": {mime: payload}})
    assert "unsupported payload" in result["data"]["text/html"]
    assert result["metadata"]["fabric_jupyter"]["restore_data"] == {mime: payload}


@pytest.mark.parametrize("rows", [
    [["first", {"nested": "<unsafe>"}], ["short"], [None, True, "extra"]],
    [{"0": "first", "1": {"nested": "<unsafe>"}}, {"0": "short"}, {"0": None, "1": True}],
])
def test_sql_row_arrays_and_numeric_keys_and_nested_values(rows) -> None:
    payload = payload_for(SPARK_SQL_MIME)
    payload["schema"]["fields"][0]["name"] = "<Name &>"
    payload["data"] = rows
    output = _normalize_fabric_display_content({"data": {SPARK_SQL_MIME: payload}})["data"]["text/html"]
    assert "<th>&lt;Name &amp;&gt;</th>" in output
    assert "{&quot;nested&quot;: &quot;&lt;unsafe&gt;&quot;}" in output
    assert "<td>short</td><td></td>" in output
    assert "<td></td><td>true</td>" in output
    assert "extra" not in output and "<unsafe>" not in output


@pytest.mark.parametrize("mime", INLINE_TABLE_MIMES)
def test_empty_and_missing_rows_render_headers(mime: str) -> None:
    payload = payload_for(mime)
    if mime == JUPYTER_DISPLAY_MIME:
        payload["table"].pop("rows")
    else:
        payload.pop("data")
    output = _normalize_fabric_display_content({"data": {mime: payload}})["data"]["text/html"]
    assert "<th>Name</th>" in output and "<tbody></tbody>" in output


@pytest.mark.parametrize("table", [
    {"schema": [], "rows": [None]},
    {"schema": [], "rows": "invalid"},
    {"schema": [None], "rows": []},
])
def test_malformed_rows_are_not_silently_dropped(table) -> None:
    assert render_table(table) is None


@pytest.mark.parametrize("mime", [
    "application/vnd.livy.statement-meta+json",
    "application/vnd.jupyter.statement-meta+json",
])
def test_statement_mime_does_not_depend_on_repr_field_count(mime: str) -> None:
    data = {mime: {"state": "finished"}, "text/plain": ["StatementMeta(, session, 1, Finished, Available)"]}
    result = _normalize_fabric_display_content({"data": data})
    assert set(result["data"]) == {"text/html"}
    assert "display:none" in result["data"]["text/html"]
    assert result["metadata"]["fabric_jupyter"]["restore_data"] == data


def test_declared_but_unimplemented_formats_are_not_claimed_as_supported() -> None:
    for mime in (
        "application/vnd.synapse.mssparkutilsrun-result+json",
        "application/vnd.mlflow.run-widget+json",
        "text/vnd.synapse.lsmagic-result",
    ):
        content = {"data": {mime: {"unknown": True}}}
        assert _normalize_fabric_display_content(content) is content


@pytest.mark.parametrize("mime", INLINE_TABLE_MIMES)
def test_inline_tables_flow_through_kernel_display_updates(mime: str) -> None:
    async def run() -> None:
        target = FabricTarget(
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
            FabricLanguage.PYSPARK,
        )
        kernel = FabricKernel()
        kernel._profile = Profile(
            name="test",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        kernel._target = target
        kernel.send_response = MagicMock()
        kernel.iopub_socket = object()

        class Client:
            async def stream_execute(self, **kwargs):
                for index, value in enumerate(("initial", "updated")):
                    assert kernel.send_response.call_count == index
                    yield ExecutionEvent(
                        EventKind.DISPLAY_DATA if index == 0 else EventKind.UPDATE_DISPLAY_DATA,
                        {
                            "data": {mime: payload_for(mime, value)},
                            "transient": {"display_id": "table"},
                        },
                    )

        kernel._connect_profile = AsyncMock(return_value=Client())
        assert (await kernel.do_execute("display", False))["status"] == "ok"
        calls = kernel.send_response.call_args_list
        assert [call.args[1] for call in calls] == ["display_data", "update_display_data"]
        for call, value in zip(calls, ("initial", "updated"), strict=True):
            content = call.args[2]
            assert content["transient"] == {"display_id": "table"}
            assert f"<td>{value}</td>" in content["data"]["text/html"]
            assert content["metadata"]["fabric_jupyter"]["restore_data"][mime] == payload_for(
                mime, value
            )

    asyncio.run(run())
