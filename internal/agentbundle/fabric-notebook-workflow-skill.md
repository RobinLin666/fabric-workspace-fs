---
name: fabric-notebook-workflow
description: >
  Use for ANY Microsoft Fabric notebook task in a CLI/terminal context — load this
  skill BEFORE writing, generating, editing, or executing notebook code, even when the user
  provides full workspace context and only wants code as text output. Match patterns:
  "In notebook 'X', add a cell ...", "write code that ...", "provide the PySpark code
  snippet for ...", "generate notebook code", "create / list / edit notebooks", "run /
  execute this cell", "start spark session", "set / attach default lakehouse", "configure
  spark / %%configure", "use notebookutils / mssparkutils", "variable library", "read a
  lakehouse / Delta table", "cross-lakehouse read", "visualization / chart in notebook",
  "install package in notebook", "monitor / logs / OOM / shuffle skew / why did my spark
  job fail", "fntk", "headless notebook execution". Covers Fabric notebook authoring
  (PySpark / Python / Scala / SparkR / Spark SQL / notebookutils / magic / Delta / visualization)
  AND lifecycle operations.
---

# fabric-notebook-workflow

You are the **terminal-host adapter for Fabric notebooks**. Drive `fntk` to operate notebooks (create, edit, run, debug) and route every Fabric-knowledge question to fabric-skills. You do **not** own Fabric code patterns — those live upstream.

## Critical rules

1. **For any Fabric topic beyond `fntk` mechanics**, consult [Knowledge routing](#knowledge-routing) **before** falling back to internal knowledge. `fntk` handles operations; fabric-skills handles knowledge.

2. **Before writing any notebook code, load `spark-cli` and read its `SPARK-NOTEBOOK-AUTHORING-CORE.md`.** This covers context/parameters, lakehouse paths and tables, file system, credentials, fabric item/artifact management, orchestration, notebook resources, visualization, Delta optimization, Spark runtime config, ML workflows, and troubleshooting. Internal knowledge of Fabric notebook APIs is frequently outdated. Pure data Q&A and pure `fntk` mechanics are exempt.

3. **Declare the terminal host on every FNTK invocation.** Use the canonical
   `--client-name` installed for this host (`github-copilot-cli`, `claude-code`,
   or `codex`). This is self-asserted telemetry metadata. Add `--client-version`
   only when the host already exposes it reliably; never ask the terminal user
   to configure these values or run a version probe.

## Knowledge routing

This adapter owns only the `fntk` CLI tool surface. Everything else — Fabric code patterns, data queries, diagnostics, cross-workload questions — lives in the fabric-skills plugin. You don't need a routing table here: each fabric-skills skill has its own description, and the agent matches against those. Your job is just to pick the right destination by intent.

### How to route

- **`fntk` mechanics** (sessions, cells, monitoring, logs, the JSON envelope, ID lifecycle) — handled here. See the sections below.
- **Writing notebook code** (PySpark / Python / Scala / SparkR / Spark SQL / notebookutils / magic commands / visualization / package install) — load `spark-cli` and read its `SPARK-NOTEBOOK-AUTHORING-CORE.md` before writing any cell.
- **Anything else Fabric-related**— let the agent pick the matching fabric-skills skill from its description. For cross-workload requests that span multiple skills (e.g., Spark + SQL + KQL together), delegate to the **FabricDataEngineer** agent and prefix your response with "**Delegating to FabricDataEngineer agent**" so the user can see the routing.
- **Fallback chain** if fabric-skills isn't installed: `fntk skill get <name>` (narrower built-in variants below) → `microsoft-learn-mcp` → internal knowledge (last resort).

### Principles

- **Don't generate notebook code for pure data Q&A.** Route to fabric-skills and return the answer directly. Embed code in the notebook only when the user wants the query persisted.
- **Discover, don't invent.** For paths or column names, query via fabric-skills (`SHOW TABLES` through the SQL endpoint is one round-trip) or run a probe cell: `fntk code run --code "spark.catalog.listTables('default').show(truncate=False)" --json`.
- **Don't lean on internal knowledge for Fabric-specific APIs.** Route first; fall back to `fntk skill get <name>`, then `microsoft-learn-mcp`, then internal knowledge.

### `fntk` built-in skills (fallback only)

Use only when fabric-skills isn't installed, or for fntk-tool-coupled topics no upstream skill covers:

| Skill | Load when |
|---|---|
| `fabric-session-ids` | You hit 404 or wrong-ID errors on `monitor` / `logs` commands. Maps `session_id` ↔ `livy_id` ↔ `app_id` ↔ `activity_id`. |
| `spark-debug` | Pair with fabric-skills' diagnostic skill for the `fntk monitor` / `logs` command-level playbook. |
| `pyspark` / `python` / `scala` / `sparksql` / `python-kernel` / `notebookutils` | Deprecated — fabric-skills covers these better. Last resort. |

## Tool surface

`fntk` is a ~70-command CLI across 9 domains. **Pass `--json` to every operational command** (everything except `fntk help` and `fntk skill get`) — you need the structured envelope and `next_actions`.

```
fntk discover   → whoami, list-workspaces, list-notebooks, list-lakehouses, get-capacity
fntk notebook   → create, delete, get-content, put-content,
                  start-session, get-session, list-sessions, delete-session,
                  restart-kernel, interrupt-kernel,
                  set-lakehouse, get-lakehouse, list-livy-sessions
fntk cell       → list, get, update, append, insert, delete, get-output,
                  set-language, set-parameter
fntk code       → run, get-result, cancel, cancel-all, ping
fntk monitor    → list-apps, get-diagnostic, get-stages, get-progress, get-executors,
                  get-environment, get-sql, get-resources, get-advice, get-advice-cell,
                  get-data-io, get-graph, get-replmaps, get-session-status,
                  get-activity-summary, list-jobs, list-metrics, get-anomalies,
                  get-activity-metrics
fntk logs       → get-driver, get-executor, list-executors, get-livy, get-cell
fntk resource   → list, get, put, mkdir, delete, usage
fntk server     → start, stop, status              # local daemon (usually transparent)
fntk skill      → list, get                        # built-in narrower skills
fntk help       → human or --json manifest
```

Discover capabilities anytime via `fntk help --json`. If a subcommand returns an arg-parse error, run `fntk <domain> <cmd> --help` rather than guessing the flags again — `fntk` always returns the canonical flag set on demand.

## JSON envelope

Every operational `--json` response has the same shape:

```json
{
  "status": "success | error | running | starting | completed",
  "stage": "...", "progress": 0.0,
  "message": "...",
  "data": { ... },
  "next_actions": [
    {"type": "poll | once | conditional",
     "command": "fntk ... --json",
     "retry_after": 5, "stop_condition": "status == 'ready'"}
  ],
  "context": {"workspace_id": "...", "session_id": "..."},
  "error": {"code": "...", "message": "...", "suggestion": "..."},
  "trace_id": "fntk-...", "elapsed_ms": 234
}
```

**Read `next_actions` before deciding what to do next.** The toolkit already knows its own state machine; `poll` types include `retry_after` and `stop_condition` — follow them literally rather than guessing intervals.

On error, check `error.suggestion` first — the toolkit usually proposes the exact recovery command in `next_actions` (e.g. `fab auth login`).

## Notebook development loop

```
DISCOVER → CREATE / OPEN → EDIT → SESSION → EXECUTE → INSPECT → ITERATE
```

| Phase | Commands | Notes |
|---|---|---|
| Discover | `fntk discover whoami`, `list-workspaces`, `list-notebooks`, `list-lakehouses` | Always start here — it captures the IDs every downstream command needs. |
| Lakehouse setup | `fntk notebook get-lakehouse` → `set-lakehouse` (only if changing) | **Set the lakehouse before starting the session.** `set-lakehouse` after `notebook start-session` triggers a restart and wastes the cold start. |
| Open / create | `fntk notebook get-content` (read existing) or `fntk notebook create` | `get-content` returns the `.ipynb` JSON. Use `cell` commands for surgical edits; for whole-notebook rewrites, edit the JSON locally then `put-content`. |
| Edit | `fntk cell list / get / update / append / insert / delete / set-language / set-parameter` | Targeted, persistent server-side edits. No local file needed. |
| Session | `fntk notebook start-session` (Jupyter kernel up immediately; Livy Spark cold start 2–5 min, lazy on first code run) → `notebook get-session` to check `livy_session_state` | `code run` will auto-create a session if none exists. Inspect `list-sessions` / `get-session` first if you want to keep an existing session; otherwise explicit `start-session` surfaces lakehouse mistakes early. |
| Execute | `fntk code run --code "..."` → returns `execution_id` → poll `code get-result` | Use `--code` for ad-hoc runs. For persisted-cell runs, `cell update` first, then `code run --code` against the updated cell content. |
| Inspect | `cell get-output --cell-index <INDEX>`; `monitor get-progress / get-advice / get-diagnostic`; `logs get-driver / get-cell` | See [Debugging](#debugging-spark-jobs) below. |
| Iterate | Loop back to Edit. For fresh state: `notebook restart-kernel` (keeps Spark warm) or `notebook delete-session` + `start-session` (cold start). |

### Editing patterns

- **Use `fntk cell ...` for surgical edits.** Each operation is one round-trip and the notebook stays consistent on the server. No local file to manage.
- **Use `fntk notebook get-content` → edit locally → `put-content`** for large structural rewrites (≥5 cells reshuffled, format migration). Verify with `fntk cell list` afterward.
- **`%%configure` must be the first code cell.** Running it terminates the session, so place it before `notebook start-session` whenever possible.

## ID lifecycle

Several IDs flow through the workflow, and confusing them is the single biggest source of 404s. Keep this mapping in mind, and **echo IDs back to the user** as you capture them.

| ID | Get from | Used by |
|---|---|---|
| `workspace_id` | `discover list-workspaces` | every command |
| `artifact_id` | `discover list-notebooks` | every `notebook` / `cell` / `code` / `monitor` / `logs` command |
| `lakehouse_id` | `discover list-lakehouses` | `notebook set-lakehouse` |
| `session_id` | `notebook start-session` | `notebook get-session / delete-session` |
| `kernel_id` | `notebook get-session` | `notebook restart-kernel / interrupt-kernel` |
| `livy_id` | `notebook get-session` (after Livy is up) or `notebook list-livy-sessions` | `monitor list-apps`, `logs get-driver / get-executor / get-livy` |
| `app_id` | `monitor list-apps` (format `application_NNNN_NNNN`) | most `monitor get-*`, `logs get-driver / get-executor` |
| `job_group_id` | `monitor get-replmaps` | `monitor get-advice-cell`, `logs get-cell` |
| `container_id` | `logs list-executors` | `logs get-executor` |
| `execution_id` | `code run` | `code get-result / cancel` |
| `cell_index` | zero-based position from `cell list` | `cell get / update / insert / delete / get-output / set-language / set-parameter` |

Note: `activity_id ≠ livy_id`, even when both appear in monitor URLs. If you hit unexpected 404s on `monitor` / `logs`, load `fntk skill get fabric-session-ids` for the full lifecycle map.

## Debugging Spark jobs

Delegate to **FabricDataEngineer** for the diagnostic playbook, and also load `fntk skill get spark-debug --json` for the `fntk monitor` / `logs` command-level recipes. Don't duplicate diagnostic content here — those two sources together cover every case.

## Working style

- **Announce before acting.** Tell the user what you're about to do, then run it and report. Terminal users want to see commands, not just outcomes.
- **Capture and echo IDs** (`session_id`, `livy_id`, `app_id`, `execution_id`) as you get them so the user can verify or reproduce.
- **Don't retry the same POST on failure.** If `code run` returned an `execution_id`, only `get-result` it — don't re-POST. Same for `notebook start-session`.
- **Self-recover on flag errors via `--help`.** If a command rejects your flags, `fntk <domain> <cmd> --help` returns the canonical set. Re-issuing the same wrong flags wastes turns.
- **Trust `next_actions`.** Follow `retry_after` and `stop_condition` literally; don't guess polling intervals.

## Hard constraints (and why)

- **Always pass `--json` to operational commands.** Without it you lose `next_actions` and structured fields, and downstream parsing breaks. Exception: `fntk help` and `fntk skill get` are meant for human reading.
- **Prefer explicit `notebook start-session` before `code run`.** `code run` will auto-create a session if needed, but the Livy cold start is still 2–5 minutes — inspect existing sessions first if you want to keep one, then start explicitly when a new session is needed.
- **Set the lakehouse before `notebook start-session`, not after.** Changing it later forces a session restart, wasting the cold start.
- **Don't call extension LM tools** (`fabricCreateNotebook`, `fabricNotebookContext`, etc.) — they don't exist in CLI. The pair skill `fabric-notebook-vscode` covers VS Code Chat.
- **Don't generate notebook code for pure data Q&A.** Route to fabric-skills.
- **Don't write notebook code without first loading `spark-cli` and its `SPARK-NOTEBOOK-AUTHORING-CORE.md`.** Internal knowledge of Fabric notebook APIs is not authoritative.
- **Don't invent paths or columns.** Probe via `fntk code run` or query fabric-skills — guesses become silent bugs in user data.
- **Don't lean on internal knowledge for Fabric-specific APIs.** Route via [Knowledge routing](#knowledge-routing) first.
