# `fabric-jupyter`

`fabric-jupyter` is the optional Python companion to the Go
`fabric-workspace-fs` mount. It installs local Jupyter kernels and uses a
single-user local broker to isolate Jupyter ZeroMQ traffic from a future Fabric
execution transport.

It installs only explicitly configured `fabric` profiles as selectable Jupyter
kernels. The internal fake transport is reserved for local protocol tests.
A `fabric` profile enables Notebook runtime execution through an experimental,
version-sensitive private protocol with Azure CLI authentication.
The legacy `experimental` placeholder remains unavailable.

Run `fabric-jupyter runtime-status` for offline capability inspection, not a
remote connection test. Only an explicitly configured Fabric profile installs
a kernel or performs remote operations.

See [`../docs/JUPYTER.md`](../docs/JUPYTER.md) for installation, security
boundaries, profile resolution, and compatibility.
See [`../docs/JUPYTER_RUNTIME.md`](../docs/JUPYTER_RUNTIME.md) before enabling
real runtime sessions, which may allocate billable compute.
