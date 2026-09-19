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
kernelspecs. It does not modify a system kernelspec or start a service.

Start the broker in a terminal owned by the same local user:

```sh
fabric-jupyter broker
```

Then select **fabric-jupyter (PySpark)** or **fabric-jupyter (Python)** from a
Jupyter client. The broker runs in the foreground; stopping it stops local
kernel-to-broker requests.

The distribution and command are both named `fabric-jupyter`. The importable
Python module remains `fabric_jupyter` because Python package names cannot use
hyphens. Existing kernelspec identifiers (`fabric-pyspark` and
`fabric-python`) are retained for compatibility; reinstalling updates their
visible display names without breaking saved Jupyter kernel references.

## Profiles and target resolution

Profiles are credential-free strict JSON. Inspect the effective safe view:

```sh
fabric-jupyter profile show
fabric-jupyter broker-status
```

A target is resolved in this order:

1. Explicit workspace and Notebook IDs supplied by an embedding client.
2. The profile's explicit `target`.
3. A profile's optional `fuseNotebookPath/.fabric.json`.

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

- Unix uses one owner-only (`0600`) Unix-domain socket.
- Windows uses a random loopback port plus a cryptographically random
  per-broker authentication secret in owner-local runtime state.

The secret is not written into kernelspecs, notebooks, mounts, command output,
or logs. The endpoint descriptor is private local runtime state; it is removed
when the broker shuts down on Unix. The broker does not listen on a network
interface and does not provide multi-user access.

The local protocol has a version, strict method/field validation, bounded
1 MiB messages, and a constant-time authentication comparison. It maintains
target-bound sessions, supports `execute`, `interrupt`, `shutdown`, and
idle-session cleanup.

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
