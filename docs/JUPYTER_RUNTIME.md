# Opt-in Fabric Notebook runtime

`fabric-jupyter` can connect to a Notebook-bound remote runtime using an
explicit `fabric` profile. **This is a private-protocol
integration**, not a supported Microsoft public API contract. Service changes
can break it. No Jupyter kernel is installed until a real profile is explicitly
configured and installed.

Real execution supports the following configured profiles:

| Profile | Runtime protocol selection |
| --- | --- |
| `fabric-pyspark` | `synapse_pyspark` with `pyspark` |
| `fabric-spark` | `synapse_pyspark` with `spark` (Scala) |
| `fabric-sparkr` | `synapse_pyspark` with `sparkr` |
| `fabric-python-3.11` | `jupyter` with `python3.11` |
| `fabric-python-3.12` | `jupyter` with `python3.12` |

## Authentication and configuration

Install the package in a user-local environment and authenticate Azure CLI
separately:

```sh
python -m pip install ./python
az login --tenant <your-tenant-id>
fabric-jupyter profile configure --name fabric-pyspark --transport fabric \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --notebook 22222222-2222-2222-2222-222222222222
fabric-jupyter profile configure --name fabric-spark --transport fabric \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --notebook 22222222-2222-2222-2222-222222222222
fabric-jupyter profile configure --name fabric-sparkr --transport fabric \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --notebook 22222222-2222-2222-2222-222222222222
fabric-jupyter profile configure --name fabric-python-3.11 --transport fabric \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --notebook 22222222-2222-2222-2222-222222222222
fabric-jupyter profile configure --name fabric-python-3.12 --transport fabric \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --notebook 22222222-2222-2222-2222-222222222222
fabric-jupyter install-kernels --replace
```

The IDs above are fictional. Select a Notebook you own and are authorized to
execute. Starting the configured kernel can allocate billable Fabric compute.
The package uses `AzureCliCredential`; it neither performs interactive login
nor saves Azure or workload tokens. Fabric and Power BI token audiences are
separate. Runtime grants refresh in memory, with target and origin checks.

Profiles contain identities, language and transport, **not tokens**. Keep them
in the protected user configuration directory, never in a notebook, repository,
mount, or kernelspec. A kernelspec selects a profile by name. Explicit target
IDs take precedence over optional mounted Notebook `.fabric.json` discovery.
The broker snapshots that resolved target at startup: a client cannot change
its workspace, Notebook, language, transport, Lakehouse, or Environment.

To remove a Fabric kernel, delete its kernelspec through Jupyter. `runtime-status`
describes implemented capabilities without authenticating; it is not evidence of
remote readiness.

## Lifecycle and supported messages

The transport independently verifies the Notebook/workspace identities, obtains
a Notebook-scoped workload grant, allocates a uniquely named Jupyter session,
opens its authenticated WebSocket, requests remote `kernel_info`, configures
the runtime control/status channels, and waits for compute readiness. It does
not attach to or stop an arbitrary existing session.

Execute requests are sent once. Stream, result, display-data, clear-output and
error messages are forwarded; completion requires both the correlated
`execute_reply` and idle status. A network error never triggers cell replay.
The transport supports a REST interrupt and orderly stop/delete of its owned
session. Idle cleanup uses the configured broker TTL.

Startup is bounded to 10 minutes, execution to 5 minutes, HTTP/handshake to
30 seconds, and compute-stop observation to 60 seconds. Incoming HTTP responses
are limited to 4 MiB and individual channel/IPC messages to 1 MiB. Oversized or
unsupported responses fail explicitly rather than being silently discarded.

Shutdown requests stop compute, observe its terminal state, delete the exact
owned Jupyter session, and check its absence. If cleanup cannot be confirmed,
the error is surfaced: use Fabric's session management to inspect/stop that
owned session before retrying. Forced process termination cannot guarantee
remote cleanup; the configured compute idle timeout is a backstop, not a
substitute for orderly shutdown.

## Trust boundary and limitations

- HTTPS origins come only from authenticated discovery and must be supported
  public-cloud Microsoft hosts. REST and WebSocket redirects are disabled.
  No endpoint/tenant/region override can redirect credentials to an arbitrary
  host. Sovereign clouds are not supported.
- WebSocket credentials are sent through the Notebook protocol's authenticated
  handshake, not URLs. Credential-bearing handshake logging is disabled.
  Service error bodies are not included in transport diagnostics.
- Local IPC requires owner-private state and immutable server-side target
  authorization. Other processes running as the same user and administrators
  remain outside the single-user isolation boundary.
- The runtime may maintain remote execution history. The local adapter does not
  persist code history, runtime session identifiers, outputs, or tokens.
- Synapse DataFrame widget state received through `synapse:widget` comm-open
  messages is rendered as a static standard HTML table for Jupyter clients,
  without a client plugin. Generated Data Resource MIME is deliberately omitted:
  VS Code can prefer its Data Resource renderer over HTML and show a blank output.
  Missing widget state produces an explicit unavailable-preview message.
  The original `application/vnd.synapse.widget-view+json` output is retained in
  output metadata under `fabric_jupyter.restore_data` during local rendering.
  The workspace filesystem restores it and removes locally generated HTML during Notebook save
  while `metadata.synapse_widget.state` is updated for Fabric portal
  compatibility. Reopening the native saved output in a client without a Fabric
  renderer requires rerunning the cell to regenerate its HTML preview.
- Notebook `run`/`runMultiple` results with MIME
  `application/vnd.synapse.mssparkutilsrunmultiple-result+json` are rendered as
  standard HTML activity tables (status, progress, duration, exit value and error).
  Each incoming `update_display_data` regenerates the table with the same
  `transient.display_id`, so clients update the existing output while execution is
  running; no JavaScript or renderer extension is required. Durations use the
  backend's millisecond values and update only when a new result arrives.
  Only the activity table is displayed. On snapshot success, the notebook name
  links to the public-cloud Portal snapshot using `workspace_id`,
  `root_artifact_id` (the parent notebook, not the child `artifact_id`) and `run_id`:
  `https://app.powerbi.com/groups/{workspace_id}/synapsenotebooks/{root_artifact_id}/snapshots/{run_id}?experience=power-bi`.
  Pending/failed snapshots or missing identifiers remain plain text.
  The original payload is retained in `fabric_jupyter.restore_data` and restored
  by the workspace filesystem on save, just like the DataFrame output. This is
  not the Portal's interactive DAG viewer.
- Inline `application/vnd.synapse-jupyter.display-view+json` tables and
  `application/vnd.synapse.sparksql-result+json` results also render as standard
  HTML without a client extension. The former uses `table.schema/rows`; the latter
  uses `schema.fields/data`, supporting row arrays and numeric-key objects.
  Column order, null/short rows, nested JSON values and truncated-preview notices
  are handled by the shared DataFrame table renderer. Malformed payloads produce
  a visible diagnostic instead of silently dropping data. Native MIME, unrelated
  metadata and existing backend HTML are preserved; generated HTML is removed
  on workspace-filesystem save. Standard display-ID updates apply to both formats.
  Livy and Jupyter statement-meta MIME bundles are hidden locally and restored
  on save, even without a recognized `StatementMeta(...)` text representation.
- This covers the four custom MIME renderer families actually implemented by
  the Trident desktop/remote renderer, at the portable table level. It does not
  reproduce interactive chart editing, Azure Maps, Data Wrangler, or chart-view
  persistence. Existing chart/view metadata is left untouched. Registered-only
  `application/vnd.synapse.mssparkutilsrun-result+json`, MLflow run widgets and
  `text/vnd.synapse.lsmagic-result` have no verified renderer contract here and
  remain unchanged, not advertised as supported.
- General rich comms/widgets, binary buffers, completion, inspection,
  debugging, interactive stdin, custom environment/Lakehouse attachment,
  high-concurrency session sharing, and notebook-reference artifact serving
  are not supported. No client-side code evaluation or fallback is used
  by a `fabric` profile.

The documented [Fabric Livy API](https://learn.microsoft.com/fabric/data-engineering/api-livy-overview)
is Lakehouse-scoped and is a different integration. Notebook definition APIs
manage stored content and are not used as an execution endpoint. This transport
uses the observed Notebook runtime REST/WebSocket lifecycle; it does not copy
another client's implementation.

## Validation

Offline tests cover credential audiences, target isolation, origin/redirect
rejection, secret-safe errors, frame parsing, reply/idle correlation, local IPC,
and genuine local Jupyter clients. They never contact Fabric.

Portable-output tests cover all four table families, HTML escaping, malformed
payload diagnostics, display-ID updates, and native-MIME restoration on save.
Inline Jupyter-display and Spark-SQL acceptance in VS Code uses explicitly
constructed protocol samples sent through a real Fabric kernel; these are not
captured native-producer fixtures. The SQL sample can use a read-only Spark
`SELECT` for its rows. Keep such acceptance notebooks separate from user notebooks.

Real validation requires separate explicit authorization and a disposable
owned Notebook. Verify remote readiness, a harmless print/arithmetic result,
interrupt where supported, orderly shutdown and exact-ID fixture cleanup.
Local heartbeat alone must never be reported as a
real Fabric session.
