# `fabric-jupyter`

`fabric-jupyter` is an optional Python package under [`python/`](../python).
It complements `fabric-workspace-fs`: the mount can provide optional notebook
identity discovery, while the kernel/broker never reads mount credentials,
fntk state, recovery files, or notebook contents.

## Installation

Use Python 3.11 or later in the environment that runs Jupyter:

```sh
python -m pip install ./python
fabric-jupyter install-kernels
```

The command installs user-scoped `fabric-pyspark` and `fabric-python`
kernelspecs. It does not modify a system kernelspec or start a service. A new
install is immediately usable with the default `fake` transport: the kernel
starts a private per-kernel broker endpoint when no foreground broker is
running, replies to Jupyter protocol startup messages, and returns deterministic
fake execution results without contacting Fabric, Spark, fntk, or a mount.

For a foreground local broker, bind it to one configured profile:

```sh
fabric-jupyter broker --profile fabric-pyspark
```

Select **fabric-jupyter (PySpark; offline fake)** or
**fabric-jupyter (Python; offline fake)** from a Jupyter client. A foreground
broker accepts only its startup profile's resolved target/language/transport;
a Python kernel cannot use a PySpark-bound broker. Prefer the default private
per-kernel brokers when using both languages. Stop the foreground broker before
changing profiles. Non-fake broker startup fails before binding a listener;
a non-fake kernel remains alive to report an explicit execution error.

The distribution and command are both named `fabric-jupyter`. The importable
Python module remains `fabric_jupyter` because Python package names cannot use
hyphens. Existing kernelspec identifiers (`fabric-pyspark` and
`fabric-python`) are retained for compatibility; reinstalling updates their
visible display names without breaking saved Jupyter kernel references.
Run `fabric-jupyter install-kernels --replace` after an upgrade.

## Real Fabric Runtime status

**This release cannot create or attach a real Fabric Notebook runtime.**
Local heartbeat, `kernel_info`, and deterministic fake replies prove only the
local Jupyter protocol adapter, not a cloud connection or Spark readiness.

```sh
fabric-jupyter runtime-status
fabric-jupyter runtime-status --require-fabric
```

Both commands report offline capabilities without authenticating or contacting
Fabric; the second exits **2** while real sessions are unavailable. The status
includes `remoteFabricSessionSupported: false` and `remoteCheckPerformed: false`.
There is no usable `transport: fabric` configuration; unknown transports are
rejected and `experimental` fails closed. Do not add tokens to kernelspecs or
profiles. See [runtime prerequisites](JUPYTER_RUNTIME.md) for the specific
protocol gap and the distinction from the documented Lakehouse Livy API.

## Profiles and target resolution

Profiles are credential-free strict JSON. Inspect the effective safe view:

```sh
fabric-jupyter profile show
fabric-jupyter broker-status
```

A target is resolved in this order:

1. Explicit workspace and Notebook IDs supplied to the library resolver.
2. The profile's explicit `target`.
3. A profile's optional `fuseNotebookPath/.fabric.json`.

The generated default profiles include fixed local fake targets so a fresh
install has no hidden mount or Fabric identity prerequisite. Replace those
targets only for local tests; configuring real IDs does not enable cloud execution.

Resolution does not grant execution authority. The broker resolves its own
startup profile once, stores an immutable target policy, and never accepts
client changes to that policy. Requests for other workspace/Notebook IDs,
languages, transports, Lakehouses, or Environments are rejected before dispatch.
Changing the configuration requires restarting the broker; editing a profile
or optional mount identity cannot retarget a running broker.

The FUSE path is only a convenience identity source. It is optional and is not
assumed to be mounted. Its `.fabric.json` must identify a `Notebook`; the
resolver does not inspect any credential files or infer a target from names.

Example profile schema (all UUIDs are fictional):

```json
{
  "profiles": [
    {
      "name": "fabric-pyspark",
      "language": "pyspark",
      "transport": "fake",
      "target": {
        "workspaceId": "11111111-1111-1111-1111-111111111111",
        "notebookId": "22222222-2222-2222-2222-222222222222",
        "language": "pyspark"
      },
      "idleTimeoutSeconds": 900
    }
  ]
}
```

Unknown keys, duplicate profile names, malformed UUIDs, and secret-like
configuration inspection fields are rejected or redacted. Do not place
tokens, cookies, passwords, or client secrets in profiles.

## Broker security boundary

The broker accepts only local connections:

- Unix uses owner-only (`0700`) runtime directories and (`0600`) Unix-domain
  sockets/descriptors; owner and permissions are checked before use.
- Windows uses a random loopback port plus a cryptographically random
  per-broker authentication secret. Runtime state must have a verified
  owner-only protected DACL; insecure, redirected/shared, or unverifiable state
  fails closed rather than starting an unprotected broker.

The secret is not written into kernelspecs, notebooks, mounts, command output,
or logs. The endpoint descriptor is private local runtime state; it is removed
when its owning foreground broker shuts down normally. Embedded brokers do not
persist a descriptor. The broker does not listen on an external network
interface and does not provide multi-user access. Local administrators and
processes already running as the same user are outside this isolation boundary.

The local protocol has a version, strict method/field validation, bounded
1 MiB messages, and a constant-time authentication comparison. It maintains
server-authorized, target-bound local sessions, supports `execute`, `interrupt`,
`shutdown`, and idle-session cleanup. Possessing a broker token does not grant
access to an arbitrary target. The token is a per-broker capability, not an
Azure credential.

On Windows, IPC state is under `%LOCALAPPDATA%\fabric-jupyter\runtime`,
separate from a user-local `venv`. Profiles remain under
`%APPDATA%\fabric-jupyter`. Version 0.1.1 validates existing directory ACLs
instead of silently accepting inherited access. If an upgrade reports an
insecure directory, stop its broker and review the exact path in the error.
An owner may explicitly repair only that dedicated directory using the
suggested PowerShell command, then retry. Never apply such repairs recursively
to `%APPDATA%`, `%LOCALAPPDATA%`, a home directory, or a virtual environment.
If an existing ACL or filesystem cannot be verified, the broker stays stopped.

## Execution behavior and limitations

The initial release maps canonical Jupyter execute, stream, result, error,
interrupt, and shutdown semantics to the broker. The bundled `fake` transport
is the default and never evaluates code or contacts Fabric; it exists for
deterministic local and Jupyter protocol tests.

`experimental` is intentionally inert. It does **not** guess a private
Fabric endpoint, hardcode a tenant/region, acquire credentials, create a
remote session, or execute code. A real Fabric execution transport requires a
separately reviewed and explicitly configured implementation plus separate
runtime authorization.

Current capability metadata truthfully reports no completion, inspect,
widgets, rich comms, stdin prompts, or debugger integration. Notebook
execution history, access tokens, and session IDs are not persisted. Session
reuse only exists while the broker process is running; idle sessions are
released after the configured timeout.

## Development and testing

```sh
cd python
python -m pip install -e ".[dev]"
ruff check .
mypy src
pytest
python -m build
```

Tests use only the fake transport and local loopback/Unix IPC. They do not
contact Fabric, start Spark, or modify remote resources.
