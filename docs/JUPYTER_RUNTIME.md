# Fabric Notebook runtime readiness

## Current result

`fabric-jupyter` implements a local Jupyter kernel and authenticated local
broker, **not a real Fabric runtime transport**. Default fake execution never
evaluates code. `experimental` is unavailable; `fabric` is not a supported
transport value. All fail before credential acquisition or remote session
operations. Reinstalling a kernelspec or mounting a workspace does not change
this limitation.

`fabric-jupyter runtime-status --require-fabric` returns JSON and exit code 2.
It is a capability check, not a network health check. A successful local
`kernel_info` or heartbeat is not evidence that Fabric or Spark started.

## Why the documented Livy API is not a drop-in Notebook transport

Microsoft documents interactive sessions in the
[Fabric Livy overview](https://learn.microsoft.com/fabric/data-engineering/api-livy-overview)
and [session guide](https://learn.microsoft.com/fabric/data-engineering/get-started-api-livy-session).
Those APIs operate on a **Lakehouse** and do not require a Notebook. They are
not a documented Notebook-bound Jupyter channel/startup contract. They could
support a separately designed Lakehouse-targeted integration, but silently
substituting one would change target, permissions, lifecycle, and client behavior.

Notebook item/definition APIs describe stored content, not an interactive
Jupyter runtime. An allocated REST session alone is insufficient evidence that
its execution engine is ready. The missing supported contract covers:

1. Notebook-targeted session create/attach and idempotent recovery after timeout.
2. Supported Azure credential audience and runtime endpoint discovery, including
   explicit REST/WebSocket origin and redirect rules.
3. Channel authentication, startup negotiation, authoritative readiness/status,
   and the mapping to canonical Jupyter messages without running a cell.
4. Interrupt, cancellation, idle expiry, orderly shutdown, and authoritative
   proof that allocated remote resources stopped after an error.

This release does not infer private endpoint paths, encode an undocumented
authentication convention, or copy another client's implementation. No remote
session or test Notebook was created to probe an unverified protocol.

## Requirements before enabling a real transport

A reviewed contract for the operations above must be available, together with
an opt-in credential-free profile schema and explicit authorization for a
bounded target. The broker's immutable server-side target policy and verified
private IPC storage must remain in force. Authentication must refresh in memory,
never serialize tokens, and never follow an untrusted redirect with credentials.

Transport validation must cover timeout/cancellation, retry safety, startup
failure cleanup, channel errors, target isolation, and lifecycle limits.
Separate authorized live validation must prove create/attach, readiness,
`kernel_info`, and stop using an owned disposable target, with exact cleanup
verification. Fake tests cannot substitute for that evidence. Rich comms,
widgets, completion, and inspection must remain disabled unless independently
implemented and verified.

Until then, use the local fake kernels for protocol development only and use a
supported Fabric client for real runtime work. There is no token or profile
setting that enables real Fabric execution in this package today.
