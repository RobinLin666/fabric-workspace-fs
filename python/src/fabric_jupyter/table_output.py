"""Portable tables for Fabric's inline display and Spark SQL MIME contracts."""

from __future__ import annotations

import html
import json
from typing import Any

JUPYTER_DISPLAY_MIME = "application/vnd.synapse-jupyter.display-view+json"
SPARK_SQL_MIME = "application/vnd.synapse.sparksql-result+json"
INLINE_TABLE_MIMES = (JUPYTER_DISPLAY_MIME, SPARK_SQL_MIME)


def render_table(table: Any) -> str | None:
    if not isinstance(table, dict):
        return None
    schema, rows = table.get("schema"), table.get("rows", [])
    if not isinstance(schema, list) or not isinstance(rows, list):
        return None
    if not all(isinstance(column, dict) for column in schema):
        return None
    if not all(isinstance(row, (dict, list)) for row in rows):
        return None
    columns = [
        (str(column.get("key", index)), str(column.get("name", column.get("key", index))))
        for index, column in enumerate(schema)
    ]

    def cell(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list, bool)):
            return html.escape(json.dumps(value, ensure_ascii=False))
        return html.escape(str(value))

    body = []
    for row in rows:
        keyed_row = row if isinstance(row, dict) else {
            str(index): value for index, value in enumerate(row)
        }
        values = []
        for key, _ in columns:
            values.append(f"<td>{cell(keyed_row.get(key))}</td>")
        body.append("<tr>" + "".join(values) + "</tr>")
    header = "".join(f"<th>{html.escape(name)}</th>" for _, name in columns)
    truncated = (
        "<p><em>Fabric returned a truncated preview.</em></p>"
        if table.get("truncated") is True else ""
    )
    return (
        '<div class="fabric-dataframe">'
        f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"
        f"{truncated}</div>"
    )


def render_inline_table(mime: str, payload: Any) -> str:
    original = payload
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    table = None
    if isinstance(payload, dict):
        if mime == JUPYTER_DISPLAY_MIME:
            table = payload.get("table")
        elif mime == SPARK_SQL_MIME:
            schema = payload.get("schema")
            fields = schema.get("fields") if isinstance(schema, dict) else None
            if isinstance(fields, list) and all(isinstance(field, dict) for field in fields):
                table = {
                    "schema": [
                        {"key": str(index), "name": field.get("name", str(index))}
                        for index, field in enumerate(fields)
                    ],
                    "rows": payload.get("data", []),
                }
    rendered = render_table(table)
    if rendered is not None:
        return rendered
    return (
        '<div class="fabric-output-unavailable"><p>'
        f"Fabric table: unsupported payload for {html.escape(mime)}.</p><pre>"
        f"{html.escape(json.dumps(original, ensure_ascii=False))}</pre></div>"
    )
