# Opt-in Fabric Notebook runtime

`fabric-jupyter` can connect to a Notebook-bound remote runtime using an
explicit `fabric` profile. **This is an experimental private-protocol
integration**, not a supported Microsoft public API contract. Service changes
can break it. Default profiles remain offline `fake`; no cloud activity occurs
until a real profile is explicitly selected and started.

Real execution currently supports **PySpark only**. The Fabric Python
kernelspec remains useful in offline fake mode; a real Python runtime profile
is rejected before authentication until that protocol is separately validated.

## Authentication and configuration

Install the package in a user-local environment and authenticate Azure CLI
separately:

```sh
python -m pip install ./python
az login --tenant <your-tenant-id>
fabric-jupyter profile configure --name fabric-pyspark --transport fabric \
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

To return to offline operation, configure the profile with `--transport fake`
and reinstall its kernelspec. `runtime-status` describes implemented
capabilities without authenticating; it is not evidence of remote readiness.

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
- Rich comms/widgets, binary buffers, completion, inspection, debugging,
  interactive stdin, custom environment/Lakehouse attachment, high-concurrency
  session sharing, and notebook-reference artifact serving are not supported.
  No client-side code evaluation or fake fallback is used by a `fabric` profile.

The documented [Fabric Livy API](https://learn.microsoft.com/fabric/data-engineering/api-livy-overview)
is Lakehouse-scoped and is a different integration. Notebook definition APIs
manage stored content and are not used as an execution endpoint. This transport
uses the observed Notebook runtime REST/WebSocket lifecycle; it does not copy
another client's implementation.

## Validation

Offline tests cover credential audiences, target isolation, origin/redirect
rejection, secret-safe errors, frame parsing, reply/idle correlation, local IPC,
and genuine local Jupyter clients. They never contact Fabric.

Real validation requires separate explicit authorization and a disposable
owned Notebook. Verify remote readiness, a harmless print/arithmetic result,
interrupt where supported, orderly shutdown and exact-ID fixture cleanup.
Local heartbeat or fake protocol success alone must never be reported as a
real Fabric session.
