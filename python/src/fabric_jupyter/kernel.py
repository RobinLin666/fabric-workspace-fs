"""Jupyter ZeroMQ adapter backed by the authenticated local broker."""

from __future__ import annotations

import asyncio
import html
import json
import os
import re
import sys
from typing import Any, cast
from urllib.parse import quote
from uuid import uuid4

from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelapp import IPKernelApp

from . import __version__
from .broker import BrokerClient, BrokerServer, load_endpoint
from .config import load_profiles
from .models import EventKind, FabricLanguage, FabricTarget, Profile
from .table_output import INLINE_TABLE_MIMES, render_inline_table, render_table
from .targets import resolve_target

_NOTEBOOK_RUN_MIME = "application/vnd.synapse.mssparkutilsrunmultiple-result+json"
_STATEMENT_META_MIMES = (
    "application/vnd.livy.statement-meta+json",
    "application/vnd.jupyter.statement-meta+json",
)


class FabricKernel(IPythonKernel):
    """A local adapter: Jupyter speaks only to this process, never to Fabric."""

    implementation = "fabric-jupyter"
    implementation_version = __version__
    language_info = {
        "name": "python",
        "mimetype": "text/x-python",
        "file_extension": ".py",
        "pygments_lexer": "python",
    }
    banner = "fabric-jupyter local broker adapter"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        name = os.environ.get("FABRIC_JUPYTER_PROFILE", "fabric-pyspark")
        self._profile: Profile | None = None
        self._target: FabricTarget | None = None
        self._client: BrokerClient | None = None
        self._embedded_broker: BrokerServer | None = None
        self._embedded_broker_loop: asyncio.AbstractEventLoop | None = None
        self._startup_error: str | None = None
        self._connection_error: str | None = None
        self._widget_states: dict[str, dict[str, Any]] = {}
        try:
            profile = load_profiles().get(name)
            if profile is None:
                raise RuntimeError(f"unknown fabric-jupyter profile: {name}")
            self._profile = profile
            self._target = resolve_target(profile)
            self.banner = "fabric-jupyter Fabric notebook runtime"
            self.language_info = {
                **self.language_info,
                "name": profile.language.jupyter_language,
                "pygments_lexer": (
                    "scala"
                    if profile.language is FabricLanguage.SPARK
                    else ("r" if profile.language is FabricLanguage.SPARKR else "python")
                ),
            }
        except (OSError, RuntimeError, ValueError) as exc:
            self._startup_error = _diagnostic("startup configuration failed", exc)

    async def _broker_client(self) -> BrokerClient:
        if self._startup_error is not None:
            raise RuntimeError(self._startup_error)
        if self._profile is None or self._target is None:
            raise RuntimeError("fabric-jupyter startup did not produce a runnable profile")
        if self._client is not None:
            return self._client
        try:
            self._client = BrokerClient(load_endpoint())
            return self._client
        except FileNotFoundError:
            self._embedded_broker = BrokerServer(
                idle_timeout_seconds=self._profile.idle_timeout_seconds,
                profile=self._profile,
            )
            endpoint = await self._embedded_broker.start(persist_endpoint=False)
            self._embedded_broker_loop = asyncio.get_running_loop()
            self._client = BrokerClient(endpoint)
            return self._client
        except (OSError, RuntimeError, ValueError) as exc:
            raise RuntimeError(_diagnostic("broker endpoint is not usable", exc)) from exc

    async def _connect_profile(self) -> BrokerClient:
        client = await self._broker_client()
        if self._profile is None or self._target is None:
            raise RuntimeError("fabric-jupyter startup did not produce a runnable profile")
        await client.connect(self._target, self._profile.transport)
        self._connection_error = None
        return client

    async def kernel_info_request(
        self, stream: Any, ident: Any, parent: dict[str, Any]
    ) -> None:
        if not self.session:
            return
        try:
            await self._connect_profile()
        except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
            self._connection_error = _diagnostic("Fabric connection failed", exc)
        content: dict[str, Any] = {"status": "ok"}
        content.update(self.kernel_info)
        if self._connection_error is not None:
            content["status"] = "error"
            content["ename"] = "FabricConnectionError"
            content["evalue"] = self._connection_error
            content["banner"] = f"{content['banner']}\n\n{self._connection_error}"
        self.session.send(stream, "kernel_info_reply", content, parent, ident)

    async def do_execute(
        self,
        code: str,
        silent: bool,
        store_history: bool = True,
        user_expressions: dict[str, str] | None = None,
        allow_stdin: bool = False,
        *,
        cell_meta: dict[str, Any] | None = None,
        cell_id: str | None = None,
    ) -> dict[str, Any]:
        del store_history, user_expressions, allow_stdin, cell_meta, cell_id
        error_payload: dict[str, Any] | None = None
        try:
            client = await self._connect_profile()
            if self._profile is None or self._target is None:
                raise RuntimeError("fabric-jupyter startup did not produce a runnable profile")
            async for event in client.stream_execute(
                request_id=str(uuid4()),
                target=self._target,
                code=code,
                silent=silent,
                transport=self._profile.transport,
            ):
                if event.kind is EventKind.STREAM and not silent:
                    stream_content = cast(dict[str, Any], dict(event.content))
                    normalized_stream, display_contents = _normalize_fabric_stream_content(
                        stream_content
                    )
                    if normalized_stream is not None:
                        self.send_response(self.iopub_socket, "stream", normalized_stream)
                    for display_content in display_contents:
                        self.send_response(self.iopub_socket, "display_data", display_content)
                elif event.kind is EventKind.RESULT and not silent:
                    result_content = cast(dict[str, Any], dict(event.content))
                    result_content = _normalize_fabric_display_content(
                        result_content, self._widget_states
                    )
                    self.send_response(self.iopub_socket, "execute_result", result_content)
                elif event.kind is EventKind.DISPLAY_DATA and not silent:
                    display_content = cast(dict[str, Any], dict(event.content))
                    display_content = _normalize_fabric_display_content(
                        display_content, self._widget_states
                    )
                    self.send_response(self.iopub_socket, "display_data", display_content)
                elif event.kind is EventKind.UPDATE_DISPLAY_DATA and not silent:
                    update_content = cast(dict[str, Any], dict(event.content))
                    update_content = _normalize_fabric_display_content(
                        update_content, self._widget_states
                    )
                    self.send_response(self.iopub_socket, "update_display_data", update_content)
                elif event.kind is EventKind.CLEAR_OUTPUT and not silent:
                    clear_content = cast(dict[str, Any], dict(event.content))
                    self.send_response(self.iopub_socket, "clear_output", clear_content)
                elif event.kind is EventKind.COMM_OPEN:
                    self._remember_synapse_widget(
                        cast(dict[str, Any], dict(event.content))
                    )
                elif event.kind is EventKind.ERROR:
                    content = cast(dict[str, Any], dict(event.content))
                    self.send_response(self.iopub_socket, "error", content)
                    error_payload = {"status": "error"}
                    error_payload.update(content)
        except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
            content = {
                "ename": type(exc).__name__,
                "evalue": str(exc),
                "traceback": [str(exc)],
            }
            self.send_response(self.iopub_socket, "error", content)
            return {"status": "error", **content}
        if error_payload is not None:
            return error_payload
        return {
            "status": "ok",
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }

    async def do_interrupt(self) -> dict[str, Any]:
        try:
            client = await self._connect_profile()
            if self._target is None:
                raise RuntimeError("fabric-jupyter startup did not produce a runnable target")
            await client.interrupt(self._target)
            return {"status": "ok"}
        except (OSError, RuntimeError, ValueError) as exc:
            return {"status": "error", "ename": type(exc).__name__, "evalue": str(exc)}

    def _remember_synapse_widget(self, content: dict[str, Any]) -> None:
        if content.get("target_name") != "synapse:widget":
            return
        data = content.get("data")
        if not isinstance(data, dict):
            return
        widget_id = data.get("widget_id")
        widget_type = data.get("widget_type")
        state = data.get("state")
        if not isinstance(widget_id, str) or not isinstance(widget_type, str):
            return
        if not isinstance(state, dict):
            return
        widget_state: dict[str, Any] = {
            "type": widget_type,
            "sync_state": state,
        }
        persist_state = data.get("persist_state")
        if isinstance(persist_state, dict):
            widget_state["persist_state"] = persist_state
        self._widget_states[widget_id] = widget_state

    async def interrupt_request(self, stream: Any, ident: Any, parent: dict[str, Any]) -> None:
        if self.session is not None:
            content = await self.do_interrupt()
            self.session.send(stream, "interrupt_reply", content, parent, ident=ident)

    async def do_shutdown(self, restart: bool) -> dict[str, Any]:
        failure: BaseException | None = None
        if self._client is not None or self._embedded_broker is not None:
            try:
                if self._client is not None and self._target is not None:
                    await self._client.shutdown(self._target)
            except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                failure = exc
            finally:
                try:
                    await self._close_embedded_broker()
                except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                    failure = failure or exc
        if failure is not None:
            return {
                "status": "error",
                "restart": restart,
                "ename": type(failure).__name__,
                "evalue": str(failure),
            }
        return {"status": "ok", "restart": restart}

    async def do_complete(self, code: str, cursor_pos: int) -> dict[str, Any]:
        del code
        return {
            "status": "ok",
            "matches": [],
            "cursor_start": cursor_pos,
            "cursor_end": cursor_pos,
            "metadata": {},
        }

    async def do_inspect(
        self,
        code: str,
        cursor_pos: int,
        detail_level: int = 0,
        omit_sections: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        del code, cursor_pos, detail_level, omit_sections
        return {"status": "ok", "found": False, "data": {}, "metadata": {}}

    async def _close_embedded_broker(self) -> None:
        broker = self._embedded_broker
        if broker is None:
            return
        self._embedded_broker = None
        self._client = None
        broker_loop = self._embedded_broker_loop
        self._embedded_broker_loop = None
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if broker_loop is None or broker_loop is running_loop:
            await broker.close()
            return
        if broker_loop.is_closed():
            return
        future = asyncio.run_coroutine_threadsafe(broker.close(), broker_loop)
        await asyncio.wrap_future(future)


def _diagnostic(context: str, exc: BaseException) -> str:
    return f"{context}: {type(exc).__name__}: {exc}"


def _normalize_fabric_display_content(
    content: dict[str, Any],
    widget_states: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    data = content.get("data")
    if not isinstance(data, dict):
        return content
    text = data.get("text/plain")
    if isinstance(text, list) and all(isinstance(line, str) for line in text):
        text = "".join(text)
    if not isinstance(text, str):
        text = ""
    widget = _widget_descriptor(data, text)
    widget_state = None
    if widget is not None and widget_states is not None:
        widget_state = widget_states.get(widget["id"])
    extra: dict[str, Any] | None
    inline_mime = next((mime for mime in INLINE_TABLE_MIMES if mime in data), None)
    statement_meta = any(mime in data for mime in _STATEMENT_META_MIMES)
    if _NOTEBOOK_RUN_MIME in data:
        extra = {"text/html": _render_notebook_run(data[_NOTEBOOK_RUN_MIME])}
    elif inline_mime is not None:
        extra = {"text/html": render_inline_table(inline_mime, data[inline_mime])}
    elif widget is not None:
        extra = _fabric_synapse_widget_mime(
            f"SynapseWidget({widget['type']}, {widget['id']})", widget_state
        )
    elif statement_meta:
        extra = {"text/html": '<span class="fabric-statement-meta" style="display:none"></span>'}
    else:
        extra = _fabric_display_mime(text, widget_state)
    if extra is None:
        return content
    normalized = dict(content)
    normalized_data = {**data}
    generated = {
        key: value
        for key, value in extra.items()
        if key not in data
    }
    normalized_data.update(generated)
    restore_data: dict[str, Any] = {}
    for mime in (
        "application/vnd.synapse.widget-view+json",
        *_STATEMENT_META_MIMES,
        _NOTEBOOK_RUN_MIME,
        *INLINE_TABLE_MIMES,
    ):
        if mime in normalized_data:
            restore_data[mime] = normalized_data.pop(mime)
    if (
        (statement_meta or _parse_statement_meta(text) is not None
         or (widget is not None and widget_state is not None))
        and "text/plain" in normalized_data
    ):
        restore_data["text/plain"] = normalized_data.pop("text/plain")
    normalized["data"] = normalized_data
    generated_fallbacks = [
        mime
        for mime in ("application/vnd.dataresource+json", "text/html")
        if mime in generated
    ]
    if generated_fallbacks or restore_data:
        metadata = dict(content.get("metadata") or {})
        marker: dict[str, Any] = {}
        if generated_fallbacks:
            marker["generated_mime_types"] = generated_fallbacks
        if restore_data:
            marker["restore_data"] = restore_data
        if widget is not None and widget_state is not None:
            marker["widget_id"] = widget["id"]
            marker["widget_state"] = widget_state
        metadata["fabric_jupyter"] = marker
        normalized["metadata"] = metadata
    return normalized


def _normalize_fabric_stream_content(
    content: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    text = content.get("text")
    if not isinstance(text, str):
        return content, []
    stream_parts: list[str] = []
    display_contents: list[dict[str, Any]] = []
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if _parse_statement_meta(stripped) is not None:
            continue
        if _parse_synapse_widget(stripped) is not None:
            display_contents.append(_normalize_fabric_display_content({
                "data": {"text/plain": stripped},
                "metadata": {},
            }))
            continue
        stream_parts.append(line)
    if not stream_parts:
        return None, display_contents
    normalized = dict(content)
    normalized["text"] = "".join(stream_parts)
    return normalized, display_contents


def _fabric_display_mime(
    text: str, widget_state: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    statement = _parse_statement_meta(text)
    if statement is not None:
        return {
            "text/html": '<span class="fabric-statement-meta" style="display:none"></span>',
        }
    return _fabric_synapse_widget_mime(text, widget_state)


def _fabric_synapse_widget_mime(
    text: str, widget_state: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    widget = _parse_synapse_widget(text)
    if widget is not None:
        result: dict[str, Any] = {
            "application/vnd.synapse.widget-view+json": {
                "widget_type": widget["type"],
                "widget_id": widget["id"],
            },
        }
        rendered = _render_dataframe_widget(widget_state)
        if rendered is not None:
            result["text/html"] = rendered
        else:
            result["text/html"] = (
                '<div class="fabric-widget-unavailable">'
                f"<p>{html.escape(widget['type'])}: table preview is unavailable; "
                "rerun this cell to receive the widget state.</p></div>"
            )
        return result
    return None


def _widget_descriptor(data: dict[str, Any], text: str) -> dict[str, str] | None:
    value = data.get("application/vnd.synapse.widget-view+json")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            value = None
    if isinstance(value, dict):
        widget_id, widget_type = value.get("widget_id"), value.get("widget_type")
        if isinstance(widget_id, str) and isinstance(widget_type, str):
            return {"id": widget_id, "type": widget_type}
    return _parse_synapse_widget(text)


def _render_dataframe_widget(
    widget_state: dict[str, Any] | None,
) -> str | None:
    if not isinstance(widget_state, dict) or widget_state.get("type") != "Synapse.DataFrame":
        return None
    sync_state = widget_state.get("sync_state")
    table = sync_state.get("table") if isinstance(sync_state, dict) else None
    return render_table(table)


def _render_notebook_run(payload: Any) -> str:
    original = payload
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    activities = payload.get("activities") if isinstance(payload, dict) else None
    if not isinstance(activities, list) or not all(
        isinstance(activity, dict) for activity in activities
    ):
        return (
            '<div class="fabric-notebook-run"><p>'
            "Fabric notebook run: unsupported result payload.</p><pre>"
            f"{html.escape(json.dumps(original, ensure_ascii=False))}</pre></div>"
        )

    def text(value: Any) -> str:
        return html.escape("" if value is None else str(value))

    header = "".join(
        f"<th>{name}</th>"
        for name in ("Activity", "Snapshot", "Status", "Progress", "Duration", "Exit value", "Exception")
    )
    rows: list[str] = []
    for activity in activities:
        progress = activity.get("progress")
        progress_text = "" if progress is None else f"{progress}%"
        duration = activity.get("duration")
        if duration is None:
            start, end = activity.get("start_time"), activity.get("end_time")
            if (
                isinstance(start, (int, float)) and not isinstance(start, bool)
                and isinstance(end, (int, float)) and not isinstance(end, bool)
                and start > 0 and end >= start
            ):
                duration = end - start
        duration_text = (
            f"{duration / 1000:.1f} s"
            if isinstance(duration, (int, float)) and not isinstance(duration, bool)
            else duration
        )
        status = activity.get("status", "")
        status_msg = activity.get("status_msg", "")
        status_text = status_msg or status
        notebook = text(activity.get("notebook_name"))
        identifiers = [
            activity.get(key) for key in ("workspace_id", "root_artifact_id", "run_id")
        ]
        if activity.get("snapshot_status") == "success" and all(
            isinstance(value, str) and value for value in identifiers
        ):
            workspace, root, run = (quote(str(value), safe="") for value in identifiers)
            url = (
                f"https://app.powerbi.com/groups/{workspace}/synapsenotebooks/{root}"
                f"/snapshots/{run}?experience=power-bi"
            )
            notebook = (
                f'<a href="{html.escape(url, quote=True)}" target="_blank" '
                f'rel="noopener noreferrer">{notebook}</a>'
            )
        values = (
            text(activity.get("activity_name")),
            notebook,
            text(status_text),
            text(progress_text),
            text(duration_text),
            text(activity.get("exit_value")),
            text(activity.get("exception")),
        )
        rows.append("<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>")
    if not activities:
        rows.append('<tr><td colspan="7">Waiting for notebook activities.</td></tr>')
    return (
        f'<table class="fabric-notebook-run"><thead><tr>{header}</tr></thead>'
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _parse_statement_meta(text: str) -> dict[str, str] | None:
    match = re.fullmatch(r"StatementMeta\((.*)\)", text.strip())
    if match is None:
        return None
    values = [part.strip() for part in match.group(1).split(",")]
    if len(values) < 7:
        return None
    return {
        "application_id": values[0],
        "session_id": values[1],
        "statement_id": values[2],
        "state": values[3],
        "available_state": values[4],
        "result_state": values[5],
        "cancelled": values[6],
    }


def _parse_synapse_widget(text: str) -> dict[str, str] | None:
    match = re.fullmatch(r"SynapseWidget\(([^,]+),\s*([^)]+)\)", text.strip())
    if match is None:
        return None
    return {"type": match.group(1).strip(), "id": match.group(2).strip()}


def launch_kernel(connection_file: str, profile: str) -> None:
    """Start a standard ipykernel process with the requested Fabric profile."""

    os.environ["FABRIC_JUPYTER_PROFILE"] = profile
    try:
        IPKernelApp.launch_instance(argv=["-f", connection_file], kernel_class=FabricKernel)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"fabric-jupyter: {_diagnostic('kernel launch failed', exc)}", file=sys.stderr)
        raise
