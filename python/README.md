# `fabric-jupyter`

`fabric-jupyter` is the optional Python companion to the Go
`fabric-workspace-fs` mount. It installs local Jupyter kernels and uses a
single-user local broker to isolate Jupyter ZeroMQ traffic from a future Fabric
execution transport.

It has no default cloud execution path. The default `fake` transport is
deterministic and is intended for local protocol testing. The `experimental`
transport is deliberately unavailable until an explicitly configured, reviewed
implementation is supplied; it never guesses private Fabric endpoints.

Run `fabric-jupyter runtime-status --require-fabric` to check this limitation
without network access (exit code 2 means real Fabric sessions are unavailable).
Kernel display names explicitly include **offline fake**; startup and fake
execute replies do not demonstrate a Fabric session.

See [`../docs/JUPYTER.md`](../docs/JUPYTER.md) for installation, security
boundaries, profile resolution, and compatibility.
