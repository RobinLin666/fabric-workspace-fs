# `fabric-jupyter`

`fabric-jupyter` is the optional Python companion to the Go
`fabric-workspace-fs` mount. It installs local Jupyter kernels and uses a
single-user local broker to isolate Jupyter ZeroMQ traffic from a future Fabric
execution transport.

It has no default cloud execution path. The default `fake` transport is
deterministic and is intended for local protocol testing. An explicit `fabric`
profile enables Notebook runtime execution through an experimental,
version-sensitive private protocol with Azure CLI authentication.
The legacy `experimental` placeholder remains unavailable.

Run `fabric-jupyter runtime-status` for offline capability inspection, not a
remote connection test. Default kernel names explicitly include **offline
fake**; only an explicitly configured Fabric profile performs remote operations.

See [`../docs/JUPYTER.md`](../docs/JUPYTER.md) for installation, security
boundaries, profile resolution, and compatibility.
See [`../docs/JUPYTER_RUNTIME.md`](../docs/JUPYTER_RUNTIME.md) before enabling
real runtime sessions, which may allocate billable compute.
