# fabric-workspace-fs

Mount Microsoft Fabric workspaces as a local filesystem. Browse Fabric folders,
edit existing Notebooks in place, and work with Lakehouse `Files` through one
cross-platform Go CLI. Environment definitions and Lakehouse `Tables` remain
read-only.

An optional Python companion, **`fabric-jupyter`**, provides local Jupyter
kernels backed by an owner-local broker. Its default fake transport is offline;
an explicit `fabric` profile enables experimental remote Notebook execution.

## Status

| Platform | Filesystem support |
|---|---|
| Linux | Supported with FUSE 3 |
| Windows | Supported with WinFsp 2.1 |
| macOS | Builds with macFUSE; real mounts are not yet validated |

This is an early release, not a full POSIX filesystem or a
production-certified OneLake driver. Notebook saves use read/compare/write
rather than a server-side atomic CAS, and unsupported filesystem operations
fail explicitly.

## Quick start

Prerequisites:

- Go 1.26 or later
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) for local
  development authentication
- FUSE 3 on Linux, [WinFsp 2.1](https://github.com/winfsp/winfsp/releases/tag/v2.1)
  on Windows, or [macFUSE](https://macfuse.github.io/) on macOS

```sh
go build -trimpath -o bin/fabric-workspace-fs ./cmd/fabric-workspace-fs
az login --tenant <your-tenant-id>

mkdir -p "$HOME/fabric-mount"
./bin/fabric-workspace-fs mount \
  --workspace 11111111-1111-1111-1111-111111111111 \
  "$HOME/fabric-mount"
```

On Windows, build `bin\fabric-workspace-fs.exe` and use an unused drive such as
`M:` as the mountpoint. The binary runs in the foreground; press Ctrl+C to
unmount. See the [filesystem guide](docs/FILESYSTEM.md) for platform-specific
commands, authentication scopes, namespace layout, write semantics, limits,
and recovery guidance.

### Optional `fabric-jupyter`

```sh
python -m pip install ./python
fabric-jupyter install-kernels
fabric-jupyter runtime-status
```

Select a **fabric-jupyter (PySpark; offline fake)** or
**fabric-jupyter (Python; offline fake)** kernel in your Jupyter client.
For real execution, configure an opt-in `fabric` profile; see the
[`fabric-jupyter` guide](docs/JUPYTER.md) for profiles, security boundaries,
capabilities, and limitations.

## Safety

The filesystem uses separate Fabric and OneLake credentials in memory and does
not store access tokens. Treat mounted paths as remote data: test with a
disposable workspace, keep a single writer for Notebook edits, and do not
assume local POSIX operations are atomic in Fabric. Remote execution is never
authorized merely by mounting a workspace.

## Documentation

- [Filesystem guide](docs/FILESYSTEM.md)
- [`fabric-jupyter` guide](docs/JUPYTER.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [MIT License](LICENSE)
