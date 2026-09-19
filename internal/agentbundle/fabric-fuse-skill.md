---
name: fabric-fuse
description: Safe Fabric FUSE guidance for workspace discovery, .fabric.json identity, read/write boundaries, notebook save conflicts, and OneLake Files/Tables handling.
---

# Fabric FUSE

Bundle version: **{{VERSION}}**. This bundled guidance is injected by the mounted filesystem and remains read-only. The configured external Fabric Notebook Toolkit executable is `{{FNTK}}`.

This mount exposes Fabric workspace items by stable workspace-relative paths, not by remote IDs or local overlays.

- Start from the workspace root and use `.fabric.json` to resolve the real item/folder IDs for a path.
- Keep identity data in `.fabric.json`; do not invent IDs, create fake display names, or silently re-use a prior collision suffix.
- Do not write outside the authorized item, workspace, or Files subtree. Tables, Environment metadata, and protected roots stay read-only.
- Notebook save must preserve the actual definition format and remote metadata; refresh before flush and fail on conflicts instead of discarding the user’s local edit.
- Lakehouse writes are only valid under the user-owned `Files/` subtree, and only with the required conditional checks and safe rename/delete behavior.
- Builtin or resource aliases are not public execution endpoints; if the provider is unavailable, report the limit explicitly and do not fabricate a directory tree.
- Execution is outside this filesystem. After explicit user approval, use `{{FNTK}} help --json` and the installed fntk contract; use `.fabric.json` only to obtain immutable workspace and Notebook IDs. Never auto-run or replay code, and never pass credentials or tokens through the mount or logs.
- Do not leak bearer tokens, secrets, or cache payloads. Keep any private resource provider implementation in memory only and bound to the correct audience.
- All filesystem writes must respect the same read-only and error semantics as the mounted FUSE layer: no silent success, no unsupported chmod/chown/symlink/hardlink semantics, and no implied CAS or atomicity beyond the documented service guarantees.
