# `fabric-jupyter`

`fabric-jupyter` is the optional Python companion to the Go
`fabric-workspace-fs` mount. It installs local Jupyter kernels and uses a
single-user local broker to isolate Jupyter ZeroMQ traffic from a future Fabric
execution transport.

It installs only explicitly configured `fabric` profiles as selectable Jupyter
kernels. A `fabric` profile enables Notebook runtime execution through a
version-sensitive private protocol with Azure CLI authentication.

Run `fabric-jupyter runtime-status` for offline capability inspection, not a
remote connection test. Only an explicitly configured Fabric profile installs
a kernel or performs remote operations. Supported profile names are
`fabric-pyspark`, `fabric-spark`, `fabric-sparkr`, `fabric-python-3.11`, and
`fabric-python-3.12`.

See [`../docs/JUPYTER.md`](../docs/JUPYTER.md) for installation, security
boundaries, profile resolution, and compatibility.
See [`../docs/JUPYTER_RUNTIME.md`](../docs/JUPYTER_RUNTIME.md) before enabling
real runtime sessions, which may allocate billable compute.
