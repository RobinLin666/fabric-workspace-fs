# fabric-workspace-fs

A standalone Go filesystem for Microsoft Fabric workspaces. The first release
mounts on **Linux, Windows, and macOS**. Linux uses `go-fuse/v2`; Windows and
macOS use a thin `cgofuse` adapter over WinFsp and macFUSE respectively. Fabric
public REST APIs provide
workspace/item discovery and definitions; the **OneLake ADLS Gen2-compatible
API** provides Lakehouse data. Optional, explicitly enabled MWC resource APIs
provide Notebook `builtin` and Environment resource files behind a replaceable
backend. There are no invented Fabric `/files` endpoints or VS Code dependencies.

**Scope:** real Fabric folders, typed Notebook/Lakehouse/Environment item
directories, Notebook in-place editing, and Lakehouse `Files` CRUD. `mkdir`
creates supported items or Fabric folders; guarded `rmdir` supports empty
Lakehouses and empty folders. Notebook/Environment item deletion is deliberately
unsupported because public definitions do not prove hidden resources empty.
The mount injects a versioned, read-only `/.agents` guidance bundle. Arbitrary
dot-directory overlays are no longer supported. Environment definitions,
identity metadata and Lakehouse `Tables` are read-only. Workspaces
cannot be created/deleted; remote item/folder rename is explicitly unsupported.
With `--resource-provider mwc`, Notebook builtin resources are writable, while
Environment resources remain read-only. Notebook `env` and all top-level
`.platform` paths are hidden, without deleting remote metadata.

This is an initial implementation, not a claim of full POSIX semantics or a
production-certified OneLake driver. Default automated tests use local HTTP
services, including real Linux FUSE mounts. A separate, explicitly authorized
live smoke command creates only uniquely named test fixtures. Validate in a
disposable workspace before relying on it for remote data.

## Optional Jupyter kernel and local broker

The repository also ships an optional standalone Python 3.11+ package at
[`python/`](python) named `fabric-jupyter`. It installs user-scoped Fabric
Python/PySpark kernels and connects them to an owner-local authenticated broker.
It complements the FUSE mount: a mounted Notebook's `.fabric.json` can supply
optional target identity, but it is never assumed and no credentials are read
from a mount.

The default transport is deterministic and fake: it does not execute code or
contact Fabric. The experimental transport is intentionally inert until a
separately reviewed, explicitly configured real execution transport exists.
There is no hardcoded tenant, endpoint, token flow, background service, or
remote execution by default.

```sh
python -m pip install ./python
fabric-jupyter install-kernels
fabric-jupyter broker
```

See [`docs/JUPYTER.md`](docs/JUPYTER.md) for architecture, profile resolution,
IPC security, compatible Jupyter messages, limitations, and offline testing.

## Build and prerequisites

Use Go **1.26 or later**. Install the native filesystem runtime separately:

| Platform | Runtime and build boundary | Validation |
|---|---|---|
| Linux | FUSE 3 (`/dev/fuse` and `fusermount3`, normally the `fuse3` package). Normal builds do not need CGO. | Unit/race tests and real kernel mounts against offline HTTP mocks. |
| Windows | [WinFsp 2.1](https://github.com/winfsp/winfsp/releases/tag/v2.1). The normal `CGO_ENABLED=0` binary dynamically loads the installed WinFsp runtime. Use an unused drive such as `M:` or an existing empty directory. | Native callback tests on every Windows build; CI installs WinFsp and performs a real mount. A WinFsp 2.1 mount was also validated end-to-end against owned Fabric fixtures. |
| macOS | [macFUSE](https://macfuse.github.io/) plus Xcode command-line tools. Build with `CGO_ENABLED=1`; macFUSE must remain installed at runtime. | Native adapter tests and compilation only. A real macOS mount has not been validated in this release. |

Linux:

```sh
# Debian/Ubuntu prerequisites, installed by the machine's administrator:
sudo apt-get install fuse3

go mod download
go build -trimpath -o bin/fabric-workspace-fs ./cmd/fabric-workspace-fs
./bin/fabric-workspace-fs --help
```

Windows (PowerShell, after installing WinFsp from its signed installer):

```powershell
$env:CGO_ENABLED = '0'
go build -trimpath -o bin\fabric-workspace-fs.exe .\cmd\fabric-workspace-fs
.\bin\fabric-workspace-fs.exe --help
```

macOS (after installing macFUSE separately and accepting its documented
system-extension requirements):

```sh
xcode-select --install
brew install --cask macfuse
CGO_ENABLED=1 go build -trimpath -o bin/fabric-workspace-fs ./cmd/fabric-workspace-fs
```

Dependencies are pinned in `go.mod` and `go.sum`: `go-fuse/v2 v2.11.0`,
`cgofuse v1.6.0`,
Azure Identity `v1.14.1`, Azure Core `v1.23.1`, and
`microsoft/fabric-sdk-go v0.20.0`. The Fabric SDK requires Go 1.25.12 and is
compatible with this project's Go 1.26 minimum. The binary runs in the
foreground; it does not install a service, change system settings, or daemonize.
Linux writable mounts require the kernel's FUSE atomic `O_TRUNC` capability.
Directory mountpoints must already exist, be empty, and not be symlinks. Do not use a
mountpoint on a filesystem that `fusermount3` disallows, such as WSL's Windows
DrvFS; use a directory on WSL's native Linux filesystem instead.

The application does not install WinFsp/macFUSE, request elevation, register a
service, or bypass an operating-system driver warning. Missing runtimes produce
an explicit mount startup error. macOS binaries built with `CGO_ENABLED=0`
retain the CLI but reject `mount` with a clear dependency error.

## Authentication and permissions

`DefaultAzureCredential` supports development credentials such as Azure CLI,
and configured environment/workload/managed identities supported by the Azure
Identity SDK. For local interactive use, authenticate in the **same operating
system environment** as the filesystem:

```sh
az login --tenant <your-tenant-id>
# Optional: restrict DefaultAzureCredential to the signed-in CLI.
export AZURE_TOKEN_CREDENTIALS=AzureCLICredential
./bin/fabric-workspace-fs workspaces
```

No credentials or bearer tokens are stored by this application. It keeps at
most three access tokens in memory, separately for
`https://api.fabric.microsoft.com/.default` and
`https://storage.azure.com/.default`, plus
`https://analysis.windows.net/powerbi/api/.default` for opt-in MWC discovery and
token exchange. Each audience has single-flight refresh
two minutes before expiry; failed refreshes are surfaced, not replaced with an
expired token. This cache avoids spawning `az account get-access-token` for
every individual range request when Azure CLI is the selected credential.
Azure CLI/Identity manage their own externally configured credential state.

Workspace discovery requires the applicable Fabric workspace permissions and
delegated scopes (`Workspace.Read.All` or `Workspace.ReadWrite.All`). Importantly,
**Notebook and Environment `getDefinition` require item read AND write
permission**, and the documented corresponding
`Notebook.ReadWrite.All` / `Environment.ReadWrite.All` or `Item.ReadWrite.All`
scopes. A local read-only mount does not turn these into Viewer-readable APIs.
Encrypted sensitivity labels can prevent Notebook definition export. OneLake
data access uses its separate Fabric/OneLake permissions and storage audience.
Service-principal/managed-identity availability also depends on tenant settings
and the particular Fabric API; this tool grants no roles or permissions.

HTTP 401/403 are errors, not empty directories. Files access does not probe
Tables: a denied optional `Tables` subtree cannot prevent authorized use of
`Files`.

## Mount and unmount

All UUIDs below are fictional placeholders; substitute IDs from `workspaces`.

```sh
mkdir -p "$HOME/fabric-mount"

./bin/fabric-workspace-fs mount \
  --workspace 11111111-1111-1111-1111-111111111111 \
  "$HOME/fabric-mount"

# Several selected workspaces:
./bin/fabric-workspace-fs mount \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --workspace 55555555-5555-5555-5555-555555555555 \
  --read-only \
  "$HOME/fabric-mount"

# Alternatively, expose every workspace visible to this identity:
./bin/fabric-workspace-fs mount --all-workspaces "$HOME/fabric-mount"

# From another terminal, after closing files:
fusermount3 -u "$HOME/fabric-mount"
```

Windows PowerShell:

```powershell
# Pick an unused drive letter.
.\bin\fabric-workspace-fs.exe mount --all-workspaces M:

# Ctrl+C in the foreground process is preferred. If recovery is needed:
& "$env:ProgramFiles(x86)\WinFsp\bin\fsptool-x64.exe" unmount M:
```

macOS:

```sh
mkdir -p "$HOME/fabric-mount"
./bin/fabric-workspace-fs mount --all-workspaces "$HOME/fabric-mount"
# Ctrl+C is preferred; otherwise:
diskutil unmount "$HOME/fabric-mount"
```

Ctrl+C/SIGTERM also requests unmount and waits for the FUSE server to stop.
Losing a stdout/stderr consumer does not terminate the daemon with SIGPIPE;
use private file-backed logs for unattended mounts to preserve error details.
Shutdown waits for the **entire serving goroutine**, not merely the reader
loops, and fails explicitly after 10 seconds if unmount/shutdown stalls.
Close programs using the mount if unmount reports busy. A timeout is not a
successful unmount: retain the mountpoint and recovery files and check the
reported error before cleanup. Never recursively delete a mounted directory
as a cleanup mechanism.

Options must precede the mountpoint. `mount --help` documents the full set.
Exit codes are `0` for success/help, `2` for invalid arguments, and `1` for
runtime/authentication/unsupported-platform failures.

## Actual namespace

```text
<mount>/
  .agents/
    AGENT.md
    skills/
      <bundled-skill>/SKILL.md
  Analytics/
    .fabric.json
    Project/                              # a real Fabric folder
      .fabric.json
      Transform.Notebook/
        .fabric.json
        content.ipynb
        builtin/                          # fixed descriptor; MWC needed to enter
    Data.Lakehouse/
      .fabric.json
      Files/
        incoming/
          data.csv
      Tables/                             # actual OneLake data, read-only
        <service-returned schema/table paths>
    Spark.Environment/
      .fabric.json
      Libraries/
        CustomLibraries/
          <actual definition parts>
        PublicLibraries/
          environment.yml
      Setting/
        Sparkcompute.yml
      resources/                          # independent MWC surface, read-only
```

Notebook fixed children are `builtin`, `content.ipynb` and `.fabric.json`.
Lakehouse fixed children are `Files`, `Tables` and `.fabric.json`. Merely
enumerating these names does not export a definition or probe optional resource
permissions. Accurate first-time Notebook content lookup/stat/open still needs
a service export; no fake zero size or remote mtime is substituted.

Environment synthesizes only documented directory categories: `Libraries`,
its `CustomLibraries`/`PublicLibraries` children, and singular `Setting`, plus
`.fabric.json` and the `resources` descriptor. Optional YAML/library files
**only come from returned definition parts**. Cold fixed-category enumeration
does not export a definition. Entering a dynamic subtree or explicitly looking
up an unknown part loads the definition and discovers additional names;
subsequent root listings include those names while that snapshot is fresh.
Cold fixed-only enumeration is not a claim to have discovered unknown future
top-level parts. The example is not a promise that every environment has every
optional file. Unsupported
payload encodings, malformed definitions, and missing/denied objects are errors.
Single- and multi-workspace mounts both put workspace display names **directly
under the mount root**. There is no `Workspaces` wrapper. `/.agents` is reserved;
a real workspace with that display name receives a stable ordinal alias rather
than being hidden. Below a workspace, public Fabric folders follow their actual
`parentFolderId` hierarchy, with root items alongside root folders. There are
no `Notebooks`, `Lakehouses` or `Environments` type-group directories. Folder
pagination is complete and bounded; orphaned items/parents, cycles and duplicate
identities/parent assignments fail explicitly rather than being hidden.

Notebook content is always named **`content.ipynb` locally** and maps to the
unique `.ipynb` part in the requested `ipynb` definition:
both `notebook-content.ipynb` and `artifact.content.ipynb` are supported, without
renaming the part sent back to Fabric. Other Notebook definition parts are not
listed, but are retained and sent back unchanged with every content save.

Workspace/folder names use safe display names without GUIDs. Items add the
canonical `.Notebook`, `.Lakehouse` or `.Environment` type suffix.
Remote addressing remains ID-based. Each workspace/folder/item directory has a
generated, read-only `.fabric.json` containing `id`, `type`, and the original
`displayName`; items include `workspaceId`/`folderId`, folders include
`workspaceId`/`parentFolderId`, and Notebook metadata includes the actual
`remotePartPath` **only while its actual definition path is known in a fresh
snapshot**. Unknown/expired part paths and empty optional parent fields are
omitted. For example, after a Notebook export:

```json
{
  "id": "22222222-2222-2222-2222-222222222222",
  "type": "Notebook",
  "displayName": "Transform",
  "workspaceId": "11111111-1111-1111-1111-111111111111",
  "remotePartPath": "notebook-content.ipynb"
}
```

This generated identity file is never uploaded. Reading basic identity never
forces an export. Its optional Notebook part path comes from the same bounded
definition snapshot, not an invented name or a separately retained stale map.
Write, truncate, create-over, rename and delete are rejected;
the UUID is not squeezed into nonportable `stat` extensions. An Environment
definition part actually named `.fabric.json` is exposed as `%2Efabric.json`,
so it remains readable without shadowing generated identity metadata.

Only conflicting names receive a small ordinal, such as `Data (2).Lakehouse`
or `Project (2)`.
Initial assignment is sorted by ID, independent of API listing order; genuine
display names such as `Data (2)` are reserved before synthetic suffixes. An
alias stays bound to its ID for the mount's lifetime, even after deletion:
a later object cannot steal a path and receive a save intended for another
item. Up to 100,000 identity/name bindings are retained. Fabric renames acquire
new aliases. Collision ordinals may differ after remount if inventory changed;
use `.fabric.json` for identity. Long catalog labels have a stable hash
abbreviation. Unicode is not normalized.

Ordinary safe names remain readable. Reserved characters, `%`, controls in
catalog labels, and trailing dots/spaces are percent-escaped. For OneLake leaves,
only canonical local spellings are accepted: a literal remote `%2e%2e` is shown
as `%252e%252e`, **not** interpreted as `..`. Slash, backslash, NUL, `.`/`..`,
noncanonical escape aliases, and escaping paths are rejected. Escaped leaf names
over 255 bytes cannot be represented and produce an explicit error.

The original VFS reference (`vscode-trident` commit
`58cf950e31341c0cddf40c1748480daf0a4a9681`) informed the data surfaces; this
filesystem intentionally uses a public, folder-aware layout instead:

- Fabric folders are real hierarchy nodes and item types are filename suffixes,
  not artificial type-group directories.
- Public definitions alone do not expose builtin/resources. Fixed descriptors
  remain visible with the provider disabled, but entering them returns
  `ENOTSUP`, never a fabricated empty directory. The opt-in MWC provider lists
  real resource contents without Spark/session startup. Notebook `env`,
  `.platform`, old `dependencies` aliases and recent-run/log views are not
  exposed.
- Environment resources are available through the opt-in read-only MWC provider.
  Public definition parts remain a separate tree; the old synthesized
  staging/published YAML views are not copied or fabricated.
- `Tables` exposes real OneLake files/directories, **not** the old VFS's
  up-to-100-row JSON table previews. It must not be used for table writes.
  Real schema directories (for example `dbo`) appear only when returned by the
  service; no synthetic `dbo` is inserted.

## Creating and removing directories

At a workspace content root or real Fabric folder, `mkdir` routes by the
canonical basename (case-sensitive type suffixes):

| `mkdir` basename | Destination |
|---|---|
| `Example.Notebook` | Public Notebook create, minimum valid empty ipynb |
| `Example.Lakehouse` | Public Lakehouse create, default/minimum payload |
| `Example.Environment` | Public Environment create, default/minimum payload |
| `Project` or an unrecognized type suffix | Public Fabric folder create, subject to Fabric's naming rules |
| A leading-dot name | The same typed-item/folder routing and Fabric validation; never a local overlay |

The only injected special namespace is `/.agents`, which cannot be created,
modified or removed by filesystem clients. Names elsewhere must be canonical,
nonempty and safe; `.fabric.json`/`.platform` are reserved. A typed create sets
the current Fabric `folderId`, and a folder create sets `parentFolderId`.
202 is awaited to completion before a successful directory is returned.
API errors are not converted into phantom entries, and failed/ambiguous calls
invalidate catalog caches. Existing names return `EEXIST`. A deleted alias is
not reused for a different ID during the mount; select another name.

Routing is not permission to invent a legal remote name. Fabric's documented
[folder name requirements](https://learn.microsoft.com/fabric/fundamentals/workspaces-folders#folder-name-requirements)
forbid characters including the period. Consequently `Project.Foo` routes to
ordinary-folder creation but returns `EINVAL` for its invalid Fabric folder
name; it is not silently rewritten or treated as an unknown item type. Use a
valid name such as `Project` for remote folders. A workspace-level `.agents`
folder is not local storage and is rejected by Fabric's period restriction.

`rmdir` has explicit **managed-empty** semantics, not recursive deletion:
Fabric folders must have no folders or items (including unexposed types);
the public service also enforces empty folder deletion.
A Lakehouse must have empty `Files` and `Tables`, checked freshly. Notebook and
Environment **item-directory rmdir always returns `ENOTSUP`**, even immediately
after creation: zero cells or unchanged public parts cannot prove private
`builtin`/`Resources` are empty. No DELETE is sent for those operations.
Creation succeeds independently of whether a subsequent definition export is
temporarily available; read failures are surfaced separately and do not trigger
automatic deletion of the new item. Active handles block supported deletion.

Deletion always addresses the bound GUID, never an inferred display name.
It uses the normal public delete behavior, not `hardDelete`. External changes
between the emptiness check and an item delete remain a service concurrency
limitation; no undocumented conditional delete guarantee is claimed. Remote
item/folder rename returns `ENOTSUP`; OneLake file rename retains its documented
scope restrictions.

### Read-only agent bundle; retired overlays

Each mount injects `/.agents/AGENT.md` (singular) and a single
`/.agents/skills/fabric-fuse/SKILL.md`, bound to the running binary's version.
This compact bundle describes identity discovery, `.fabric.json` metadata,
read/write boundaries, Notebook save conflicts, Lakehouse Files/Tables
boundaries, and the requirement for explicit authorization before helper or
execution actions. It contains no tokens, tenant secrets or agent session
state, and reading it never contacts Fabric. The bundle is immutable,
including all create/rename/truncate/delete entry points.

Explicitly instruct an agent to read `<mountpoint>/.agents/AGENT.md`; this
filename is **not a promise of automatic discovery by every agent**. The
guidance is an original FUSE adaptation, not a VS Code command integration or
an automatically installed external plugin. Remote Notebook resources are a
different trust boundary and cannot grant execution authority.

The legacy `--overlay-dir`, `--overlay-max-file-size`, `--overlay-max-bytes`
and `--overlay-max-entries` options are rejected. New mounts do not open, import,
upload, migrate or delete previous overlay data. Older running mounts are
independent. Keep temporary edits and agent state in an approved native local
directory outside the mount.

### Opt-in Notebook and Environment resources

Use `--resource-provider mwc` to enable the private resource provider.
`internal/resources.Backend` separates resource paths, identities, versions and
read/write operations from the protocol so a future public implementation does
not require changing FUSE, namespace, cache or spool code.

[Microsoft's documented resource boundary](https://learn.microsoft.com/fabric/data-engineering/notebook-source-control-deployment#notebooks-resources-folder-support-in-git)
states that integration with deployment pipelines and public APIs is not
currently supported. Git resource support is not a public resource REST API.
Consequently the default public-only mode does not invent builtin files from
definition parts or local storage; entering its fixed resource descriptor
reports the unavailable provider. The explicitly enabled MWC provider is a
separate private compatibility surface, not a claim of supported public API
coverage, and does not start Spark merely to display resources.

Notebook `builtin` is a local alias for its real filesystem `workdir` resource
root; it is not an additional remote `builtin` directory. Environment
`resources` uses the target Environment's resource `workdir` root, not an
invented Notebook subpath. The former Notebook `env` alias is not shown or
looked up, and fixed-root listing does not parse Environment bindings.
Environment resources remain read-only in every write entrance, including
rename destinations.

The provider uses the Power BI audience only for authenticated cluster discovery
and MWC token exchange. The returned workload token uses `Authorization:
MwcToken`, not `Bearer`. Capacity discovery, cluster grants and workload grants
are tied to the exact workspace/artifact/workload, configured environment,
credential identity and expiry. Each target uses its own workspace capacity.
Tokens remain in bounded in-memory caches with
single-flight refresh and no disk/fntk cache access. Trusted origins come only
from the authenticated discovery chain; credentials are never sent to an
arbitrary redirect, continuation or wildcard-matching host.

Notebook content/definition access does not depend on resource permissions.
Direct lookup of an Environment's `resources` path does not require its public
definition to be ready. A failed source returns its actual error rather than an
empty tree. Fixed Environment categories remain independently enumerable;
accessing actual definition files surfaces export errors. No Spark execution
session is started for resource management.

Resource bytes use the same offset-write/truncate/spool/flush/recovery handling
as other writable files. MWC does not have a verified conditional CAS contract:
resource saves perform read/compare/write conflict detection with an explicit
race window, **not** a fabricated `If-Match` guarantee. Never assume atomic
create-if-absent or atomic replacement across clients. Reads are bounded whole
binary responses with a **version-checked snapshot pinned per read handle or
one-time spool load**; chunked reads never refetch that snapshot. Shared TTL
caching can reduce separate opens but is not required for linear download
cost: `--cache-ttl 0` still performs a constant number of full downloads, not one
per 64 KiB chunk. Snapshot lifetime/size is bounded by the resource and handle
limits, and failed saves retain their dirty spool. No undocumented
HTTP Range/HEAD endpoint is invented. `--max-resource-size` defaults to 16 MiB
(at most 128 MiB). Resource directory deletion is nonrecursive; unsupported
directory moves fail explicitly. All resource roots are protected.

Resource `readdir`/lookup/`stat` are **metadata-only** and are not subject to the
content snapshot size limit. They use returned type, `contentLength`, mtime and
opaque metadata validators; if listing metadata is incomplete, a real GET's
headers supply the missing size/mtime and its body is immediately closed without
buffering content. Missing size in both sources is an explicit error, not a
fabricated zero. Metadata ETags are separate from SHA-256 content versions:
opening a bounded snapshot establishes the content version used for conflict
checks. Thus `ls -la` can display a 20 MiB resource accurately with a 16 MiB
read/write limit, while opening its content still returns `EFBIG`. No limit is
raised or disabled to make a metadata listing work.

The private provider is based on the verified fntk source at
`34cec0a21839189d1e865102a3ce3d3ee151c952` and resource/binding contracts in the VFS
reference at `58cf950e31341c0cddf40c1748480daf0a4a9681`. Private APIs may change
without public compatibility guarantees. Recent runs, logs and additional
daemon functionality are not implemented. Session actions belong only to the
external fntk integration below, never resource browsing.

## External Fabric Notebook Toolkit integration

Notebook execution is intentionally **not implemented by this filesystem**.
Use the separately installed [Fabric Notebook Toolkit (`fntk`)] when execution
has been explicitly authorized. The integration contract was verified against
fntk commit `34cec0a21839189d1e865102a3ce3d3ee151c952`; fntk remains an
independently versioned external dependency and its installed command manifest
is authoritative.

Pass its absolute native path when mounting:

```sh
fabric-workspace-fs mount --all-workspaces \
  --fntk "$HOME/.local/bin/fntk" \
  "$HOME/fabric-mount"
```

This only advertises the executable in the read-only `/.agents` guidance. The
filesystem does not execute it, install it, inspect credential caches, start
its daemon, or proxy tokens. If `--fntk` is omitted, the guidance uses the
generic command name `fntk`; callers must resolve it on their native `PATH`.
Run `fntk help --json` before use so agents consume the installed version's
current command schema rather than a copied CLI contract.

The mounted item's `.fabric.json` supplies immutable `workspaceId` and item
`id` values for fntk commands. Reading those IDs does not authorize execution.
Starting Spark can consume billable capacity and always requires separate,
explicit approval. Never infer approval from a successful mount, filesystem
write permission, or a Notebook path.

At the referenced fntk revision, asynchronous execution is exposed through
`fntk code run`, with the returned execution ID used by `fntk code get-result`
and `fntk code cancel`. fntk owns its daemon, WebSocket/session correlation,
cross-process state, bounded outputs, cancellation, and no-replay behavior.
This project deliberately does not duplicate those state machines. Consult
`fntk help --json` for exact flags and response schemas. No real Spark
execution was performed as part of this filesystem validation.

## Permission and operation matrix

Modes are a local presentation (`0444`/`0555` read-only,
`0644`/`0755` writable, owned by the mounting user), not remote OneLake ACLs.
Mutations are also checked in the backend and transport paths, not just mode
bits. Linux uses `default_permissions` and does not expose `allow_other`;
WinFsp/macFUSE callbacks enforce the same backend policy without claiming
remote ACL, owner, chmod or chown semantics.

| Object | Read/list | Content write/truncate | Create/mkdir | Rename/delete |
|---|---|---|---|---|
| Mount root / injected `/.agents` | Yes | No | No | No |
| Workspace contents / Fabric folders | Yes | No regular-file writes | Typed items, Fabric folders | Empty folder delete; no workspace delete/remote rename |
| Notebook/Environment item directory | Yes | Managed content only | No arbitrary children | No rmdir/remote rename (`ENOTSUP`) |
| Lakehouse item directory | Yes | Managed content only | No arbitrary children | Fresh Files/Tables emptiness check before rmdir; no remote rename |
| Workspace/folder/item `.fabric.json` | Yes, generated identity metadata | No | No | No |
| Existing Notebook `.ipynb` | Yes | Yes, valid ipynb, in place | No | No |
| Notebook/Environment `.platform`; Notebook `env` | Hidden, including direct lookup | No | No | No |
| Environment definition parts | Yes when returned | No | No | No |
| Lakehouse protected `Files` root | Yes | No | Children only | Root protected |
| User paths beneath `Files` | Yes | Files: yes | Yes | Yes, conditional |
| `Tables` and every descendant | Yes | No | No | No |
| Notebook `builtin` resource subtree (MWC) | Yes | Yes, bounded writeback | Yes | Files: move/unlink; directories: empty rmdir; roots protected |
| Environment `resources` (MWC) | Yes, subject to target permissions | No | No | No |
| Any path with `--read-only` | Yes, subject to remote permissions | No | No | No |

Hardlinks, symlinks, devices, xattrs, chmod/chown/ACLs, explicit timestamp
changes, preallocation, remote file locks, and fabricated capacity/statfs values
are not implemented; unsupported operations return errors. Cross-workspace or
cross-item rename returns `EXDEV`. Replacing directories, rename exchange, and
open-file unlink/rename are unsupported (`ENOTSUP` or `EBUSY`). Directory removal
is nonrecursive and fails if children exist.

## Save, consistency, and recovery semantics

### Notebook: in-place editing only

Use an editor configured to save the existing file **in place**. A valid save
can use offset writes, `ftruncate`, `open(O_TRUNC)`, and `fsync`. Final content
must be an ipynb JSON object with `nbformat: 4` and a `cells` array, or Fabric's
real empty-export form with omitted/null `cells`. Existing exported fields are
not silently rewritten. **Editor or
Jupyter workflows requiring a temporary sibling followed by rename, checkpoint
creation, direct `.ipynb` creation, or Notebook rename do not work.** Those operations
are explicitly rejected; they are not silently discarded.
Create an empty Notebook item with `mkdir Name.Notebook`, then edit its
`content.ipynb` in place.

Before a save, the client fetches the full current `ipynb` definition and
compares it with the open-time/save-time snapshot. It changes only the existing
content part, preserves unknown JSON fields, unknown parts and `.platform`,
and sends `definition.format = "ipynb"` without `updateMetadata=true`.
An intervening content **or metadata/other-part** change returns `ESTALE`.
The exact full-definition match can reconcile an ambiguous previous response.

**Fabric does not document conditional `If-Match` for `updateDefinition`.**
Read/compare/write is not atomic across clients: an external edit between the
comparison and the update can still be lost. Use a single external owner for
Notebook editing. The tool does not claim an atomic CAS, strong consistency,
or conflict-free multi-client Notebook saves.

### Lakehouse Files: bounded spool and conditional commit

Read handles issue ETag-conditioned HTTP range reads; a changed object yields
an error rather than combining inconsistent versions. Reads do not load a
whole large file into memory. Writable handles use private local temporary
files. Existing data is downloaded in 1 MiB ranges unless the handle was
opened with `O_TRUNC`; sparse extension and offset writes happen locally.

A dirty flush creates a crypto-random sibling `.fabric-fs-upload-<random>` with
`If-None-Match: *`, uploads in 4 MiB ADLS append chunks, conditionally flushes it,
then performs **Path Create/Rename (`PUT destination`, `mode=posix`)** with both
`x-ms-source-if-match` and the destination's `If-Match` or `If-None-Match: *`.
It verifies final properties using the committed ETag. Appends have no
`If-Match` because ADLS does not support that header on append; their isolated
staging object is conditionally flushed and committed. Directory rename
continuations are followed until complete. A reported ignored safety header is
an error, not success.

Normal Lakehouse editor saves using **write temporary file -> fsync/close ->
rename over destination** are supported within one `Files` subtree, as are
in-place rewrites. Conditional behavior is exercised by HTTP contract tests and
the opt-in live smoke; it is not a guarantee across all OneLake configurations.
Do not infer universal atomicity,
transactional directory rename, storage durability beyond service
acknowledgement, or strong consistency. An interrupted directory rename may
be partially applied. An ambiguous write/rename response returns failure, even
if the server may have applied it; it is never retried blindly.

Creation immediately commits an empty remote file (create-new condition) so
subsequent lookup/open works. Later data becomes remote-visible at a successful
flush/fsync. Writes can replace a whole file; no remote POSIX metadata-preservation
or cross-file transactions are promised.

### Shared handle behavior and recovery

Only one writable handle per remote file is permitted in a mount; other paths
can have independent writers. The writer immediately reads its own spool bytes.
Separate read handles are snapshots of remote content, not a shared coherent
view of another handle's unflushed data. Path/fd attributes expose active dirty
size. Read handles validate ETags instead of mixing changed remote ranges.
Direct I/O disables the kernel page cache; this is not an offline filesystem.

`flush`, `fsync`, and close-triggered flush report remote save errors.
Duplicate-fd flushes and repeated `fsync` do not reupload a clean file.
`Release` does **not** attempt a last, unreportable upload. Failed saves stay
dirty and can be retried via `fsync` on the same open handle. Backend operations
wait up to one cancellable second for asynchronous FUSE release before
returning `EBUSY`; this lets ordinary close-then-rename/reopen work without
application-side retries. Flushing a duplicated fd does not release its lease.
WinFsp can defer its final `Release` callback after Windows has closed a file.
Before a Windows rename or delete, the adapter therefore retires matching read
handles and writers whose last flush succeeded; dirty writers remain protected
and still return `EBUSY`. This preserves the same remote writeback rule while
allowing ordinary close-then-rename/delete workflows on Windows.

If a dirty handle closes, its `0600` spool file is **retained**, and the error
log identifies the exact recovery path. No silent discard, automatic conflict
overwrite, or recovery-file garbage collection occurs. Clean spools are removed
on release. After a crash, inspect the private spool directory for orphaned
`fabric-write-*` files; they are raw user content, not tokens. Stop conflicting
editors, compare the remote state, and deliberately restore wanted content.
The spool format does not include an automatic path-to-item recovery manifest.

Failed OneLake upload cleanup checks a random ownership property and ETag before
deleting **only its own staging object**. Cleanup has a separate bounded timeout
and failures are included in the error. Crash/permission/network failures can
leave visible staging objects. The tool never recursively deletes them or
deletes an unknown remote path. Recovery spools and stale remote stages need
manual review; avoid placing sensitive recovery data on a shared disk.

## Limits and caching

| Setting | Default / behavior |
|---|---|
| `--max-notebook-size` | 16 MiB decoded editable content; configurable up to 128 MiB |
| `--max-definition-size` | 64 MiB encoded complete response/request, including base64 and all parts; up to 512 MiB |
| `--max-file-size` | 1 GiB per writable Lakehouse file; configurable up to 1 TiB |
| `--max-open-handles` / `--max-writers` | 64 total handles / 16 spooled writers |
| `--spool-dir` | User cache `fabric-workspace-fs/spool`, private `0700`, outside the mount |
| `--cache-ttl` | 2 minutes uniformly for supported filesystem/kernel caches; `0s` disables retention |
| `--cache-config` | Strict mount-time JSON layer/type/surface/identity overrides; cannot be combined with explicit `--cache-ttl` |
| Catalog caches | Workspace list: 1 entry / 8 MiB; workspace properties: 64 / 1 MiB; validated folder/item trees: 32 / 32 MiB |
| `--resource-provider` / `--max-resource-size` | `none` by default; opt-in `mwc`, 16 MiB per bounded resource |
| Definition snapshot cache | 32 snapshots / 64 MiB estimated raw definition + decoded visible bodies + part index; no second raw-definition cache |
| Active snapshot reservations | Largest of 256 MiB, six times Notebook limit, four times resource limit; includes conservative raw/decoded handle charges and writer commit buffers |
| OneLake metadata caches | 4,096 stat records / 8 MiB; 64 directory listings / 16 MiB |
| Discovery/listing | Up to 1,000 pages, 100,000 entries and 64 MiB total response bytes per list; explicit error if incomplete |
| Page envelopes | Fabric 16 MiB; OneLake 8 MiB, 5,000 paths/page |
| `--http-timeout` | 60 seconds per HTTP request |
| `--operation-timeout` | 5 minutes per Fabric definition/LRO operation |

Live spool usage is bounded by writer count times the configured per-file size;
it is not pre-reserved, so disk-full errors can still occur. Recovery files
outlive that limit and are never automatically deleted. Notebook/Environment
definition envelopes and read snapshots are kept in bounded memory; increasing
definition size or concurrent handle limits increases memory use. At most four
definition loaders run concurrently; each assembled raw/decoded snapshot is
limited to 128 MiB or three times the configured Notebook limit, whichever is
larger. Transient transport envelopes have their separate response bound.
Active handle reservations include resource snapshot copies; reaching the
byte budget returns `EBUSY` even below the handle-count limit. Closed handles
release their reservations and retained commit closures. These bounds are not
a claim that cache counters measure the Go runtime's entire heap. Large
Lakehouse read size is not limited by the writable-file limit.

Caches use entry/estimated-byte-bounded LRU retention, TTL expiry and cancellable
per-key single-flight loading. Waiting clients can cancel without waiting for a
different request's network deadline. Failed requests are not cached or turned
into empty directories; oversized successful values are returned uncached.
Values are cloned where mutable, and invalidating an in-flight key prevents
that old response from repopulating a cache or satisfying newer requests.
Local catalog inspection uses the same bounded cache's nonloading `Peek`;
there is no unbounded or nonexpiring secondary folder-tree snapshot map.

Notebook definition retrieval builds one immutable snapshot containing the
lossless multipart definition, decoded ipynb, part index and observation time.
Lookup/stat/open reuse it without repeating base64 decode. A full-definition
digest is computed lazily once per snapshot when a writer needs it. Read
handles pin one version even across expiry or eviction. Attributes and content
use the **original source observation**, not a second cache insertion time;
an attribute TTL shorter than the content TTL can therefore require another
complete export. Derived attributes/listings cannot extend source freshness.
Notebook's displayed modification time is the snapshot observation time,
not a claim that Fabric supplied a POSIX remote mtime.

Every unspecified filesystem TTL defaults to **`2m`**, including catalog,
attribute, directory, definition/decoded-content, MWC and kernel caches.
Token expiry/refresh, HTTP timeouts and LRO polling are independent and unchanged.
Explicit `0s` disables the selected layer; an absent value inherits. Catalog
folders/items always refresh together as one validated tree, never by mixing
different item-type TTL generations. Configuration is read once at mount time,
not hot-reloaded.

For example, a mount-time JSON policy can override selected surfaces while
leaving everything else at two minutes:

```json
{
  "defaults": {
    "catalog": "2m",
    "attr": "2m",
    "directory": "2m",
    "definition": "2m",
    "content": "2m",
    "kernelAttr": "2m",
    "kernelEntry": "2m",
    "kernelNegative": "2m"
  },
  "types": {
    "Notebook": {
      "surfaces": {
        "builtin": { "directory": "30s", "attr": "30s", "content": "0s" }
      }
    },
    "Lakehouse": {
      "surfaces": {
        "Files": { "directory": "15s", "attr": "15s" },
        "Tables": { "directory": "2m", "attr": "2m" }
      }
    },
    "Environment": {
      "surfaces": {
        "resources": { "directory": "1m" }
      }
    }
  }
}
```

Use `mount --cache-config /absolute/path/cache.json ...`, or use
`--cache-ttl 2m` alone for a uniform policy. Unknown fields/type/surface names,
negative durations and simultaneous explicit legacy/config options are errors.
The definition TTL is a source-freshness ceiling for derived Notebook content
and attributes, not an independent cache that can restart their clocks.

Identity overrides use workspace UUID keys and nested item UUID keys, never
display names:

```json
{
  "workspaces": {
    "11111111-1111-1111-1111-111111111111": {
      "catalog": "2m",
      "types": { "Notebook": { "attr": "1m" } },
      "items": {
        "22222222-2222-2222-2222-222222222222": {
          "type": "Notebook",
          "surfaces": { "builtin": { "content": "0s" } }
        }
      }
    }
  }
}
```

Precedence is defaults, type, type/surface, workspace, workspace/type,
workspace/type/surface, item, item/surface; each layer inherits independently.
UUID matching is case-insensitive, while type/surface names are case-sensitive.
An item override must declare its type. `catalog` may occur only in defaults
or directly inside a workspace. A `null` duration inherits just like an absent
field, unlike `"0s"`.

Successful or ambiguous write, mkdir, unlink, rmdir and rename calls invalidate
the affected metadata, descendants and parent listings, not unrelated item
trees. Definitions are invalidated after updates. Notebook **save-time
comparison always forces a fresh service export**, regardless of TTL, so cached
reads cannot bypass conflict detection. Active dirty sizes and writer reads
continue to come from the local spool. A OneLake range ETag conflict invalidates
cached metadata. Kernel attributes and positive/negative entries are notified
after local mutations, including ambiguous failures; source deadlines cap the
configured TTL. Active writer attributes use zero TTL so dirty sizes are not
hidden. Direct I/O remains enabled; there is no shared kernel content page cache.
Large Lakehouse data ranges are intentionally not retained in memory; each
range remains conditional on the read handle's ETag. Global content settings
apply to buffered definition/resource content, not these streaming ranges;
explicit Lakehouse/Files/Tables content-cache overrides are rejected.

External changes can remain visible as the prior read snapshot for up to the
configured TTL. Disable retention with `--cache-ttl 0` when freshness is more
important than API latency. In the deterministic offline measurement of twelve
repeated folder-aware list/stat/read cycles, warm caching changes catalog calls **24 -> 0**,
definition exports **24 -> 0**, storage HEADs **24 -> 0**, and directory lists
**12 -> 1**. Twelve large-file range reads remain twelve conditional requests.
`TestCachesReduceRepeatedListStatAndDefinitionCalls` measures these exact counts,
not a timing proxy: **96 -> 13** total requests, with counting beginning
**after initial path lookup prewarming**, not at cold start. The live smoke
records its own actual requests and timings.

Read/list/HEAD and read-only `POST getDefinition` can retry transient HTTP
408/429/5xx or network failures up to three times, using bounded backoff and
`Retry-After`. A delay beyond the retry ceiling is returned as an error rather
than retried too early. Mutations are never automatically replayed.

202 means **pending**, not saved. Fabric operations poll their validated
operation URL, respect `Retry-After` and cancellation/deadlines, propagate
failure, and fetch a result only when one is required (not for empty-success
updates). Continuation URLs and all outgoing requests stay on their bound
origin; redirects are not followed. Some Fabric environments return an HTTPS
regional `*.analysis.windows.net` LRO Location. When its exact
`/v1/operations/{id}` path matches the valid `x-ms-operation-id` (and it has no
userinfo, port, query or fragment), the client uses the documented
**operation-ID flow on the configured public Fabric origin** instead; it never
sends credentials to that regional URL. Unrelated origins, mismatched IDs,
scheme-relative locations and path escapes are rejected. This handles the real
regional header shape that previously surfaced as `ls: Invalid argument`.
Error types retain HTTP status and
sanitized request IDs without logging tokens, response bodies, or query secrets.

## Reproducible tests and CI

Ordinary `go test` uses fake credentials and `httptest`; it does not contact
Fabric. The separately invoked live smoke below requires explicit write consent.

```sh
go test ./...
go vet ./...
go test -race ./...               # Linux, C compiler required
go build ./cmd/...

# Actual FUSE mounts against offline HTTP mocks, not a mocked FUSE interface:
sh scripts/test-fuse.sh
# Stress asynchronous close/release and save paths:
sh scripts/test-fuse.sh -race -count=10

# Windows uses cgofuse's non-CGO WinFsp loader:
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o bin/fabric-workspace-fs.exe ./cmd/fabric-workspace-fs

# Build macOS natively with macFUSE headers/runtime available:
CGO_ENABLED=1 go build -o bin/fabric-workspace-fs-darwin ./cmd/fabric-workspace-fs
```

Ordinary `go test` skips real mounts unless `FABRICFS_FUSE_TEST=1` is set.
`scripts/test-fuse.sh` explicitly enables them and **fails**, rather than
silently skipping, if Linux/FUSE/mount privileges are unavailable. Tests create
their own `0700` `/tmp/fabric-workspace-fs-test-*` parent and mount/spool children.
They close handles, unmount, wait for server exit, and remove only that exact
directory. If unmount/exit fails, they retain and report the path rather than
recursing through a live mount.

Coverage includes dual-audience refresh/cancellation, pagination and escaping,
HTTP throttling, LRO success/failure/timeout, unknown-definition round trips,
permission boundaries, invalid paths, bounded chunk uploads, ETag conflicts,
offset writes, truncation, repeated flushes, recovery, concurrent writers, and
real kernel `O_TRUNC`, fsync/close, directory CRUD, conditional editor rename,
and read-only enforcement. Regression tests execute ordinary `ls`, `ls -la` and
shallow `find` against the real kernel mount, including regional-LRO Notebook
directories and `.fabric.json` read-only operations. A subprocess regression
checks that a closed error-log pipe cannot terminate the daemon with SIGPIPE.
`.github/workflows/ci.yml` runs Linux/Windows unit tests and vet, Linux race and
real-mount tests, Windows native callback tests plus a WinFsp-backed real mount,
and macOS CGO compile/tests with macFUSE installed. The macOS job intentionally
does not claim a real mount because hosted runners cannot load the macFUSE
system extension.

### Read-only cold/warm cache evidence (Linux)

This opt-in diagnostic reads one explicitly identified existing Notebook and
its resource metadata. It does not execute code, modify remote data or access
large resource bodies. It uses a new private temporary read-only mount, not a
user's existing mount, and retains its exact runtime path if shutdown cannot
be confirmed.

```sh
go run ./scripts/read-cache-smoke \
  --confirm-read-only \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --notebook-id 22222222-2222-2222-2222-222222222222 \
  --notebook-path 'Analytics/Transform.Notebook' \
  --evidence "$HOME/fabricfs-cache-evidence.json"
```

Evidence includes real `ls -la`/`cat` timings, HTTP/export/decode counts and
samples scheduled at source ages 0, 10, 60, 119 and 121 seconds, with actual
start/access times recorded. A long cold export can
leave the independently timed catalog older than the definition; later catalog
refreshes are reported, not hidden by resetting its deadline. If an ancestor
refresh takes a 119-second lookup across the 120-second content deadline, the
actual export timestamp must prove it was not an early refresh; a subsequent
sample can consequently run later than scheduled. Filesystem
metadata latency and code-execution/session startup are separate measurements.

### Explicitly authorized live CRUD smoke (Linux)

Do not run this against a workspace without permission to create and remove
test data. IDs below are fictional placeholders:

```sh
go run ./scripts/live-smoke \
  --allow-writes \
  --workspace 11111111-1111-1111-1111-111111111111 \
  --lakehouse 33333333-3333-3333-3333-333333333333 \
  --evidence "$HOME/fabricfs-live-evidence.json"
```

On Windows with WinFsp installed, the equivalent mount-lifecycle smoke creates
its own Folder, Notebook, Lakehouse, and Environment and removes only their
recorded IDs:

```powershell
go run .\scripts\windows-live-smoke `
  --workspace 11111111-1111-1111-1111-111111111111 `
  --evidence "$env:TEMP\fabricfs-windows-live-evidence.json"
```

The command records exact, unique test resource names before creating anything.
It creates an independently owned SDK Notebook fixture for in-place read/update,
and tests FUSE typed-item/ordinary-folder creation in its own uniquely named
Fabric folder. Lakehouse/folder deletion goes through guarded FUSE operations.
Notebook/Environment FUSE deletion is checked for `ENOTSUP` without DELETE
requests; explicit REST fixture cleanup addresses only this run's recorded IDs.
The injected root agent bundle is checked for read-only behavior, without
creating or removing local overlays. Lakehouse CRUD is limited to a uniquely
named subtree beneath the selected Lakehouse's `Files`; existing user files are
never overwritten. Identity metadata, Tables, Environment and protected roots
are checked for read-only rejection. API counts, timings, failures and exact
cleanup status are retained in the evidence file. The smoke never targets or
unmounts an already running user mount.

### Official Fabric Go SDK boundary

`github.com/microsoft/fabric-sdk-go v0.20.0` supplies **typed create-request
models for managed item/folder creation** and the live
fixture's Notebook `BeginCreateNotebook` and `DeleteNotebook` lifecycle,
behind a test-only origin/operation guard, overall deadline, bounded I/O,
safe errors, and disabled non-idempotent retries. Production creates serialize
only newly constructed known fields through SDK request models, then use the
guarded transport and create poller. Production listing, lossless definitions,
authentication, polling, OneLake, caching and FUSE do not delegate behavior
to its generated request pipeline. Upstream marks this SDK experimental.

The pinned SDK was evaluated rather than substituted blindly:

| Surface | SDK support and integration decision |
|---|---|
| Workspace, item and folder discovery | SDK clients exist, but native pager `More` checks only `continuationUri`, not token-only pages; its convenience iterator has no page/byte/cycle budget. Keep the guarded production pagination. |
| Notebook/Environment definitions | Typed models exist, but serde discards unknown definition/part fields. Keep the lossless multipart models and `ipynb` format handling. |
| Item/folder create and delete | SDK create-request models cover known new-object payloads, avoiding typed round trips of existing definitions. Production lifecycle uses the guarded non-retrying transport; the separate owned Notebook fixture also exercises SDK client methods behind a restrictive adapter. |
| LRO and regional Location | `locasync` rewrites the host but does not enforce our full operation-ID, path, query, scheme and origin contract. Production retains its guarded poller; fixture responses/requests are checked before SDK polling. |
| Authentication/retries/errors | SDK supports Azure Identity and custom transports. The fixture delegates tokens to the existing origin-bound transport and disables the SDK retry layer; raw SDK error bodies are not logged. |
| Raw headers | Available at the custom transport boundary, where operation IDs, conditional behavior, response sizes and sanitization are enforced. |

SDK-characterization tests document token-only pager behavior and unknown-field
loss; production contract tests independently require both forms of pagination,
full metadata preservation, regional-LRO safety and explicit error propagation.
Updating the SDK version must re-evaluate these assumptions. Go MVS keeps this
project's higher pinned Azure Core/Identity and crypto dependency versions.

## Package and platform boundaries

`internal/auth` and `internal/transport` handle credentials and origin-bound
requests. `internal/fabric` and `internal/onelake` implement the two public
protocols. `internal/cache` provides bounded, cancellable TTL/single-flight
storage; `internal/cachepolicy` resolves strict mount-time policies.
`internal/agentbundle` renders the versioned immutable guidance.
`internal/namespace`, `internal/workspacefs` and `internal/writeback`
own naming, policies, discovery, handles and persistence without importing
FUSE. Only `internal/fusefs/*_linux.go` imports `go-fuse` and implements the
Linux inode adapter. `internal/fusefs/mount_cgofuse.go` is a path/handle
adapter shared by Windows and CGO-enabled macOS builds; it delegates every
lookup, permission check, mutation, conflict check and writeback decision to
the same `workspacefs.FS`. Kernel entry/attribute/negative caching is disabled
in this adapter so successful mutations are immediately observable; the
existing bounded two-minute user-space cache remains configurable. Open file
contents remain pinned through `workspacefs.Handle`, and cgofuse requests
direct I/O where the host honors it. WinFsp/macFUSE do not add ACL, chmod,
chown, xattr, symlink or hardlink guarantees: unsupported operations return an
error rather than silently succeeding. Cgofuse has no per-request cancellation
context, so unmount cancels the shared mount context; Linux retains native
per-operation cancellation.

## API references

- [cgofuse v1.6.0](https://github.com/winfsp/cgofuse/tree/v1.6.0)
- [WinFsp 2.1](https://github.com/winfsp/winfsp/releases/tag/v2.1)
- [macFUSE](https://macfuse.github.io/)
- [Azure Identity for Go / local developer credentials](https://learn.microsoft.com/azure/developer/go/sdk/authentication/local-development-dev-accounts)
- [List Fabric workspaces](https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspaces)
- [List items and recursive folder discovery](https://learn.microsoft.com/rest/api/fabric/core/items/list-items)
- [Notebook definition format](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/notebook-definition)
- [Get Notebook definition and permissions](https://learn.microsoft.com/rest/api/fabric/notebook/items/get-notebook-definition)
- [Update Notebook definition](https://learn.microsoft.com/rest/api/fabric/notebook/items/update-notebook-definition)
- [Environment definition](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/environment-definition)
- [Get Environment definition and permissions](https://learn.microsoft.com/rest/api/fabric/environment/items/get-environment-definition)
- [Fabric long-running operations](https://learn.microsoft.com/rest/api/fabric/articles/long-running-operation)
- [OneLake access, GUID addressing and storage audience](https://learn.microsoft.com/fabric/onelake/onelake-access-api)
- [OneLake protected roots and ADLS differences](https://learn.microsoft.com/fabric/onelake/onelake-api-parity)
- [ADLS Path Create/Rename and conditional headers](https://learn.microsoft.com/rest/api/storageservices/datalakestoragegen2/path/create)
- [ADLS append and flush](https://learn.microsoft.com/rest/api/storageservices/datalakestoragegen2/path/update)
- [ADLS Path List](https://learn.microsoft.com/rest/api/storageservices/datalakestoragegen2/path/list)
- [ADLS Path Read](https://learn.microsoft.com/rest/api/storageservices/datalakestoragegen2/path/read)

## License

Licensed under the [MIT License](LICENSE).
