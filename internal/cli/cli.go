package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fusefs"
	"fabric-workspace-fs/internal/mwc"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/workspacefs"
)

const usage = `fabric-workspace-fs - public Fabric API / OneLake filesystem

Usage:
  fabric-workspace-fs workspaces
  fabric-workspace-fs mount --workspace <UUID> [--workspace <UUID> ...] <mountpoint>
  fabric-workspace-fs mount --all-workspaces [--read-only] <mountpoint>
  fabric-workspace-fs version

Authentication: DefaultAzureCredential (for example, az login).
Mounting requires FUSE 3 on Linux, WinFsp on Windows, or macFUSE on macOS.
Notebook saves must be in-place; create items with mkdir Name.Notebook/.Lakehouse/.Environment.
Workspace display-name directories and read-only /.agents are exposed at the mount root.
Fabric folders follow each workspace's hierarchy. Local dot-directory overlays are unsupported.
Notebook atomic-save and remote item/folder rename are unsupported.
Lakehouse Files are writable; Tables, Environment definitions and metadata are read-only.
Run "fabric-workspace-fs mount --help" for limits and writeback options.
`

type workspaceFlags []string

func (f *workspaceFlags) String() string { return strings.Join(*f, ",") }
func (f *workspaceFlags) Set(value string) error {
	if err := fabric.ValidateID(value); err != nil {
		return fmt.Errorf("--workspace requires a UUID")
	}
	*f = append(*f, strings.ToLower(value))
	return nil
}

// Run returns conventional CLI status codes: 0 success/help, 2 usage, 1 runtime.
// It never requests a credential for help/version or unsupported platforms.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer, version string) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "help", "--help", "-h":
		fmt.Fprint(stdout, usage)
		return 0
	case "version", "--version":
		fmt.Fprintln(stdout, "fabric-workspace-fs", version)
		return 0
	case "workspaces":
		if len(args) == 2 && (args[1] == "--help" || args[1] == "-h") {
			fmt.Fprintln(stdout, "Usage: fabric-workspace-fs workspaces\nLists workspace UUIDs, display names and local directory names using DefaultAzureCredential.")
			return 0
		}
		if len(args) != 1 {
			fmt.Fprintln(stderr, "workspaces takes no arguments")
			return 2
		}
		return workspaces(ctx, stdout, stderr)
	case "mount":
		return mount(ctx, args[1:], stdout, stderr, version)
	default:
		fmt.Fprintln(stderr, "unknown command; use --help")
		return 2
	}
}

func clients(httpTimeout, operationTimeout time.Duration, definitionLimit int64) (*fabric.Client, *onelake.Client, transport.TokenSource, error) {
	tokens, err := auth.NewDefault()
	if err != nil {
		return nil, nil, nil, err
	}
	client := &http.Client{Timeout: httpTimeout}
	makeTransport := func(origin, scope string) (*transport.Client, error) {
		return transport.New(transport.Options{
			BaseURL: origin, Scope: scope, Tokens: tokens, HTTPClient: client,
			MaxRetries: 3, RetryDelay: time.Second, MaxRetryDelay: 30 * time.Second,
		})
	}
	fabricTransport, err := makeTransport("https://api.fabric.microsoft.com", auth.FabricScope)
	if err != nil {
		return nil, nil, nil, err
	}
	lakeTransport, err := makeTransport("https://onelake.dfs.fabric.microsoft.com", auth.OneLakeScope)
	if err != nil {
		return nil, nil, nil, err
	}
	return fabric.New(fabricTransport, fabric.Options{
		OperationTimeout: operationTimeout, MaxDefinitionBytes: definitionLimit,
	}), onelake.New(lakeTransport, onelake.Options{ChunkSize: 4 << 20, MaxPages: 1000}), tokens, nil
}

func workspaces(ctx context.Context, stdout, stderr io.Writer) int {
	fab, _, _, err := clients(time.Minute, 5*time.Minute, 64<<20)
	if err != nil {
		return runtimeError(stderr, err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	items, err := fab.ListWorkspaces(ctx)
	if err != nil {
		return runtimeError(stderr, err)
	}
	if err := writeWorkspaceListing(stdout, items); err != nil {
		return runtimeError(stderr, err)
	}
	return 0
}

func writeWorkspaceListing(stdout io.Writer, items []fabric.Workspace) error {
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	labels := make([]namespace.Label, len(items))
	for i, item := range items {
		labels[i] = namespace.Label{ID: item.ID, DisplayName: item.DisplayName}
	}
	var catalog namespace.Catalog
	if err := catalog.ReserveName("workspaces", namespace.AgentRootName); err != nil {
		return err
	}
	names, err := catalog.Names("workspaces", labels)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "WORKSPACE UUID\tDISPLAY NAME\tDIRECTORY")
	for i, item := range items {
		fmt.Fprintf(stdout, "%s\t%q\t%q\n", item.ID, item.DisplayName, names[i])
	}
	return nil
}

func mount(ctx context.Context, args []string, stdout, stderr io.Writer, version string) int {
	flags := flag.NewFlagSet("mount", flag.ContinueOnError)
	flags.SetOutput(stderr)
	opts := workspacefs.DefaultOptions()
	opts.Version = version
	var ids workspaceFlags
	flags.Var(&ids, "workspace", "workspace UUID to expose (repeat up to 64; IDs, not names)")
	flags.BoolVar(&opts.AllWorkspaces, "all-workspaces", false, "expose every workspace visible to the identity")
	flags.BoolVar(&opts.ReadOnly, "read-only", false, "reject all local mutations")
	flags.StringVar(&opts.SpoolDirectory, "spool-dir", "", "private local writeback/recovery directory (default user cache/fabric-workspace-fs/spool)")
	flags.StringVar(&opts.FNTKExecutable, "fntk", "", "absolute path to an external fntk executable advertised by the read-only /.agents bundle")
	resourceProvider := flags.String("resource-provider", "none", "item resources: none or mwc (private API, explicit opt-in)")
	flags.Int64Var(&opts.MaxResourceSize, "max-resource-size", opts.MaxResourceSize, "maximum bounded Notebook/Environment resource bytes")
	flags.DurationVar(&opts.CacheTTL, "cache-ttl", cachepolicy.DefaultTTL, "uniform TTL for supported filesystem/kernel caches; 0s disables retention; exclusive with --cache-config")
	cacheConfig := flags.String("cache-config", "", "mount-time cache policy JSON file (maximum 1 MiB); exclusive with --cache-ttl")
	flags.Int64Var(&opts.MaxFileSize, "max-file-size", opts.MaxFileSize, "maximum writable Lakehouse file size in bytes")
	flags.Int64Var(&opts.MaxNotebookSize, "max-notebook-size", opts.MaxNotebookSize, "maximum decoded editable ipynb size in bytes")
	flags.IntVar(&opts.MaxOpenHandles, "max-open-handles", opts.MaxOpenHandles, "maximum open read and write handles")
	flags.IntVar(&opts.MaxWriters, "max-writers", opts.MaxWriters, "maximum simultaneous spooled writers")
	httpTimeout := flags.Duration("http-timeout", time.Minute, "per-request HTTP timeout")
	operationTimeout := flags.Duration("operation-timeout", 5*time.Minute, "Fabric definition/LRO timeout")
	definitionLimit := flags.Int64("max-definition-size", 64<<20, "maximum encoded definition envelope bytes (all parts)")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: fabric-workspace-fs mount (--workspace UUID ... | --all-workspaces) [options] <mountpoint>")
		fmt.Fprintln(flags.Output(), "Foreground mount (Linux FUSE 3, Windows WinFsp, or macOS macFUSE). Notebook files support in-place saves only; no notebook atomic-save.")
		fmt.Fprintln(flags.Output(), "Mount root contains workspace display-name directories and the injected read-only /.agents bundle.")
		fmt.Fprintln(flags.Output(), "Legacy --overlay-* options are unsupported; existing overlay data is left untouched.")
		flags.PrintDefaults()
		fmt.Fprint(flags.Output(), cacheConfigUsage)
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 || (len(ids) == 0 && !opts.AllWorkspaces) ||
		(len(ids) > 0 && opts.AllWorkspaces) || len(ids) > 64 {
		fmt.Fprintln(stderr, "provide one mountpoint and either --workspace UUID (repeatable) or --all-workspaces; options must precede the mountpoint")
		return 2
	}
	policy, err := mountCachePolicy(flags, opts.CacheTTL, *cacheConfig)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	opts.CachePolicy = policy
	if err := validateFNTKExecutable(opts.FNTKExecutable); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if opts.MaxNotebookSize <= 0 || opts.MaxNotebookSize > 128<<20 || opts.MaxFileSize <= 0 ||
		opts.MaxFileSize > 1<<40 || opts.CacheTTL < 0 || opts.MaxOpenHandles <= 0 ||
		opts.MaxOpenHandles > 1024 || opts.MaxWriters <= 0 || opts.MaxWriters > opts.MaxOpenHandles ||
		*httpTimeout <= 0 || *operationTimeout <= 0 || *definitionLimit <= 0 || *definitionLimit > 512<<20 ||
		*definitionLimit < (opts.MaxNotebookSize/3+1)*4 {
		fmt.Fprintln(stderr, "invalid limits: notebook 1..128 MiB, encoded definition up to 512 MiB and large enough for base64, writable file up to 1 TiB, positive timeouts/handles, writers <= handles <= 1024")
		return 2
	}

	if (*resourceProvider != "none" && *resourceProvider != "mwc") || opts.MaxResourceSize <= 0 || opts.MaxResourceSize > 128<<20 {
		fmt.Fprintln(stderr, "resource provider must be none or mwc; resource size must be 1..128 MiB")
		return 2
	}
	if !fusefs.Supported() {
		if runtime.GOOS == "darwin" {
			fmt.Fprintln(stderr, "this macOS binary was built without CGO; rebuild with CGO enabled and macFUSE installed")
		} else {
			fmt.Fprintf(stderr, "mounting is not supported by this %s build\n", runtime.GOOS)
		}
		return 1
	}
	if err := fusefs.CheckPrerequisites(); err != nil {
		return runtimeError(stderr, err)
	}
	opts.WorkspaceIDs = ids
	point, err := fusefs.NormalizeMountpoint(flags.Arg(0))
	if err != nil {
		return runtimeError(stderr, err)
	}
	if !opts.ReadOnly {
		opts.SpoolDirectory, err = spoolDirectory(opts.SpoolDirectory)
		if err != nil {
			return runtimeError(stderr, err)
		}
		if !outsideMount(point, opts.SpoolDirectory) {
			fmt.Fprintln(stderr, "spool directory must be outside the mountpoint")
			return 2
		}
	}
	fab, lake, tokens, err := clients(*httpTimeout, *operationTimeout, *definitionLimit)
	if err != nil {
		return runtimeError(stderr, err)
	}
	if *resourceProvider == "mwc" {
		opts.ResourceBackend, err = mwc.New(mwc.Options{
			FabricOrigin: fabric.BaseURL, Tokens: tokens, Workspaces: fab,
			HTTPClient: &http.Client{Timeout: *httpTimeout}, MaxFileSize: opts.MaxResourceSize, CacheTTL: opts.CacheTTL,
			CachePolicy: opts.CachePolicy,
		})
		if err != nil {
			return runtimeError(stderr, err)
		}
	}
	backend, err := workspacefs.New(fab, lake, opts)
	if err != nil {
		return runtimeError(stderr, err)
	}
	closeBackend := true
	defer func() {
		if !closeBackend {
			return
		}
		if err := backend.Close(); err != nil {
			fmt.Fprintf(stderr, "close filesystem: %v\n", err)
		}
	}()
	// Fail immediately on invalid selection/auth rather than mounting an
	// apparently empty catalog that silently hides permission failures.
	probeCtx, cancel := context.WithTimeout(ctx, *operationTimeout)
	_, err = backend.ReadDir(probeCtx, workspacefs.Entry{Kind: workspacefs.Root, Directory: true})
	cancel()
	if err != nil {
		return runtimeError(stderr, err)
	}
	server, err := fusefs.Mount(point, backend, fusefs.Options{ReadOnly: opts.ReadOnly, Logger: log.New(stderr, "fabric-workspace-fs: ", log.LstdFlags)})
	if err != nil {
		if server != nil {
			closeBackend = false
			fmt.Fprintf(stderr, "mount shutdown is not confirmed; retain and inspect %q before removing it\n", point)
		}
		return runtimeError(stderr, err)
	}
	if err := server.WaitMount(); err != nil {
		unmountErr := server.Unmount()
		if unmountErr != nil {
			closeBackend = false
		}
		return runtimeError(stderr, errors.Join(err, unmountErr))
	}
	fmt.Fprintf(stdout, "Mounted Fabric workspaces at %q (foreground; Ctrl+C to unmount).\n", point)
	if !opts.ReadOnly {
		fmt.Fprintf(stdout, "Notebook saves must be in-place. Unsaved recovery files are retained in %q.\n", opts.SpoolDirectory)
	}
	stopped := make(chan struct{})
	go func() { server.Wait(); close(stopped) }()
	select {
	case <-stopped:
		return 0
	case <-ctx.Done():
		if err := server.Unmount(); err != nil {
			closeBackend = false
			fmt.Fprintln(stderr, "unmount failed; "+fusefs.UnmountHelp(point))
			return runtimeError(stderr, err)
		}
		<-stopped
		return 0
	}
}

func validateFNTKExecutable(path string) error {
	if path == "" {
		return nil
	}
	if !filepath.IsAbs(path) {
		return errors.New("--fntk must be an absolute native executable path")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("--fntk executable is unavailable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("--fntk must reference a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return errors.New("--fntk file is not executable")
	}
	return nil
}

func spoolDirectory(path string) (string, error) {
	if path == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("locate private cache directory: %w", err)
		}
		path = filepath.Join(cache, "fabric-workspace-fs", "spool")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(absolute, 0700); err != nil {
		return "", fmt.Errorf("create private spool directory: %w", err)
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		(runtime.GOOS == "linux" && info.Mode().Perm()&0077 != 0) {
		return "", fmt.Errorf("spool directory must be a private directory (0700), not a symlink")
	}
	return absolute, nil
}

func outsideMount(mountpoint, path string) bool {
	if runtime.GOOS == "windows" {
		mountVolume := filepath.VolumeName(mountpoint)
		pathVolume := filepath.VolumeName(path)
		if len(mountpoint) == 2 && mountpoint[1] == ':' && !strings.EqualFold(mountVolume, pathVolume) {
			return true
		}
	}
	var err error
	mountpoint, err = canonicalLocation(mountpoint)
	if err != nil {
		return false
	}
	path, err = canonicalLocation(path)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(mountpoint, path)
	return err == nil && (relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)))
}

func canonicalLocation(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	var suffix []string
	for {
		if _, err := os.Lstat(path); err == nil {
			resolved, err := filepath.EvalSymlinks(path)
			if err != nil {
				return "", err
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "", fmt.Errorf("cannot resolve local storage root")
		}
		suffix = append(suffix, filepath.Base(path))
		path = parent
	}
}

func runtimeError(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "fabric-workspace-fs: %v\n", err)
	return 1
}
