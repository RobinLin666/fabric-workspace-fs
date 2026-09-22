package cli

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
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
  fabric-workspace-fs workspaces                 List accessible workspaces
  fabric-workspace-fs mount --workspace <UUID> [--workspace <UUID> ...] <mountpoint>
  fabric-workspace-fs mount --all-workspaces [--read-only] <mountpoint>
                                                   Mount selected or all workspaces
  fabric-workspace-fs unmount <mountpoint>         Request a clean unmount
  fabric-workspace-fs version                    Print the installed version

Authentication: DefaultAzureCredential (for example, az login).
Mounting requires FUSE 3 on Linux, WinFsp on Windows, or macFUSE on macOS.
Notebook saves must be in-place; create items with mkdir Name.Notebook/.Lakehouse/.Environment.
Workspace display-name directories and read-only /.agents are exposed at the mount root.
Fabric folders follow each workspace's hierarchy. Local dot-directory overlays are unsupported.
Notebook atomic-save and remote item/folder rename are unsupported.
Lakehouse Files are writable; Tables, Environment definitions and metadata are read-only.
Run "fabric-workspace-fs mount --help" for every mount option and its default.
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
		return mount(ctx, []string{"--help"}, stdout, stdout, version)
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
	case "unmount":
		return unmount(args[1:], stdout, stderr)
	default:
		fmt.Fprintln(stderr, "unknown command; use --help")
		return 2
	}
}

type mountControlState struct {
	Mountpoint string `json:"mountpoint"`
	Secret     string `json:"secret"`
}

type mountControl struct {
	statePath   string
	requestPath string
	secret      string
	cancel      context.CancelFunc
	stop        chan struct{}
	done        chan struct{}
	once        sync.Once
}

func mountControlPaths(mountpoint string) (string, string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", "", fmt.Errorf("locate mount control directory: %w", err)
	}
	key := mountpoint
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	sum := sha256.Sum256([]byte(key))
	root := filepath.Join(cacheDir, "fabric-workspace-fs", "mount-control")
	return filepath.Join(root, hex.EncodeToString(sum[:])+".json"),
		filepath.Join(root, hex.EncodeToString(sum[:])+".unmount"), nil
}

func startMountControl(mountpoint string, cancel context.CancelFunc) (*mountControl, error) {
	if cancel == nil {
		return nil, errors.New("mount cancellation is required")
	}
	statePath, requestPath, err := mountControlPaths(mountpoint)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(statePath), 0700); err != nil {
		return nil, fmt.Errorf("create mount control directory: %w", err)
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return nil, fmt.Errorf("create mount control secret: %w", err)
	}
	state := mountControlState{Mountpoint: mountpoint, Secret: hex.EncodeToString(secretBytes)}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	// A successful mount owns this exact mountpoint, so stale state cannot
	// identify another live instance at the same location.
	if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("replace stale mount control state: %w", err)
	}
	if err := os.Remove(requestPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale unmount request: %w", err)
	}
	if err := os.WriteFile(statePath, data, 0600); err != nil {
		return nil, fmt.Errorf("write mount control state: %w", err)
	}
	control := &mountControl{
		statePath: statePath, requestPath: requestPath, secret: state.Secret,
		cancel: cancel, stop: make(chan struct{}), done: make(chan struct{}),
	}
	go control.watch()
	return control, nil
}

func (c *mountControl) watch() {
	defer close(c.done)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			request, err := os.ReadFile(c.requestPath)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				continue
			}
			if subtle.ConstantTimeCompare(request, []byte(c.secret)) != 1 {
				continue
			}
			_ = os.Remove(c.requestPath)
			c.cancel()
			return
		}
	}
}

func (c *mountControl) Close() {
	if c == nil {
		return
	}
	c.once.Do(func() {
		close(c.stop)
		<-c.done
		_ = os.Remove(c.requestPath)
		_ = os.Remove(c.statePath)
	})
}

func unmount(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		fmt.Fprintln(stdout, "Usage: fabric-workspace-fs unmount <mountpoint>")
		fmt.Fprintln(stdout, "Requests a clean flush and shutdown from a mount started by this binary and user.")
		return 0
	}
	if len(args) != 1 {
		fmt.Fprintln(stderr, "unmount requires exactly one mountpoint; use `fabric-workspace-fs unmount --help`")
		return 2
	}
	mountpoint, err := fusefs.NormalizeMountpoint(args[0])
	if err != nil {
		return runtimeError(stderr, err)
	}
	statePath, requestPath, err := mountControlPaths(mountpoint)
	if err != nil {
		return runtimeError(stderr, err)
	}
	data, err := os.ReadFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(stderr, "no active fabric-workspace-fs mount control state exists for %q; use Ctrl+C in the original mount terminal or %s\n", mountpoint, fusefs.UnmountHelp(mountpoint))
		return 1
	}
	if err != nil {
		return runtimeError(stderr, fmt.Errorf("read mount control state: %w", err))
	}
	var state mountControlState
	if err := json.Unmarshal(data, &state); err != nil || state.Mountpoint != mountpoint || len(state.Secret) != 64 {
		return runtimeError(stderr, errors.New("mount control state is invalid; refusing to signal the mount"))
	}
	if err := os.WriteFile(requestPath, []byte(state.Secret), 0600); err != nil {
		return runtimeError(stderr, fmt.Errorf("request unmount: %w", err))
	}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(statePath); errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(stdout, "Unmounted Fabric workspaces at %q.\n", mountpoint)
			return 0
		} else if err != nil {
			return runtimeError(stderr, fmt.Errorf("check unmount status: %w", err))
		}
		select {
		case <-deadline.C:
			fmt.Fprintf(stderr, "unmount request for %q did not complete within 15s; %s\n", mountpoint, fusefs.UnmountHelp(mountpoint))
			return 1
		case <-ticker.C:
		}
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
	flags.IntVar(&opts.PrewarmNotebookCount, "prewarm-notebooks", 32, "Notebook content prewarm count after mount (0 disables; maximum 32)")
	notebookDiagnostics := flags.Bool("notebook-diagnostics", false, "log content-free Notebook phase durations and prewarm totals")
	flags.BoolVar(&opts.AllWorkspaces, "all-workspaces", false, "expose every workspace visible to the identity")
	background := flags.Bool("background", false, "run mount in the background; use unmount for clean shutdown")
	flags.BoolVar(&opts.ReadOnly, "read-only", false, "reject all local mutations")
	flags.StringVar(&opts.SpoolDirectory, "spool-dir", "", "private local writeback/recovery directory (default user cache/fabric-workspace-fs/spool)")
	flags.StringVar(&opts.FNTKExecutable, "fntk", "", "absolute path to an external fntk executable advertised by the read-only /.agents bundle")
	flags.StringVar(&opts.NotebookFormat, "notebook-format", opts.NotebookFormat, "local Notebook content format: ipynb or py")
	notebookContentAPI := flags.String("notebook-content-api", "mwc", "Notebook content API: mwc (fntk-compatible) or public (definition/LRO)")
	resourceProvider := flags.String("resource-provider", "mwc", "item resource provider: mwc (default) or none")
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
		fmt.Fprintln(flags.Output(), "Mount selected Fabric workspaces in the foreground (Linux FUSE 3, Windows WinFsp, or macOS macFUSE).")
		fmt.Fprintln(flags.Output(), "Specify either one or more --workspace UUID values or --all-workspaces. Notebook saves are in-place.")
		fmt.Fprintln(flags.Output(), "The mount root contains workspace display-name directories and the read-only /.agents bundle.")
		fmt.Fprintln(flags.Output(), "")
		fmt.Fprintln(flags.Output(), "Options:")
		printLongDefaults(flags)
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	mountCtx, cancelMount := context.WithCancel(ctx)
	defer cancelMount()
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
	opts.WorkspaceIDs = ids
	if err := workspacefs.ValidatePrewarmCount(opts.PrewarmNotebookCount); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if opts.NotebookFormat != "ipynb" && opts.NotebookFormat != "py" {
		fmt.Fprintln(stderr, "notebook-format must be ipynb or py")
		return 2
	}
	opts.PrewarmTimeout = *operationTimeout
	logger := log.New(stderr, "fabric-workspace-fs: ", log.LstdFlags)
	opts.LogPrewarmError = func(err error) {
		logger.Printf("notebook prewarm failed: %v", err)
	}
	if *notebookDiagnostics {
		opts.LogNotebookEvent = func(event workspacefs.NotebookEvent) {
			logger.Printf("notebook stage=%s elapsed=%s result=%s", event.Stage, event.Elapsed, event.Result)
		}
	}
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

	if (*resourceProvider != "none" && *resourceProvider != "mwc") ||
		(*notebookContentAPI != "mwc" && *notebookContentAPI != "public") ||
		opts.MaxResourceSize <= 0 || opts.MaxResourceSize > 128<<20 {
		fmt.Fprintln(stderr, "resource provider must be none or mwc; notebook content API must be mwc or public; resource size must be 1..128 MiB")
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
	if *background && os.Getenv("FABRICFS_BACKGROUND_CHILD") != "1" {
		return startBackgroundMount(args, point, stdout, stderr)
	}
	fab, lake, tokens, err := clients(*httpTimeout, *operationTimeout, *definitionLimit)
	if err != nil {
		return runtimeError(stderr, err)
	}
	if *resourceProvider == "mwc" || *notebookContentAPI == "mwc" {
		mwcClient, err := mwc.New(mwc.Options{
			FabricOrigin: fabric.BaseURL, Tokens: tokens, Workspaces: fab,
			HTTPClient: &http.Client{Timeout: *httpTimeout}, MaxFileSize: max(opts.MaxResourceSize, opts.MaxNotebookSize), CacheTTL: opts.CacheTTL,
			CachePolicy: opts.CachePolicy,
		})
		if err != nil {
			return runtimeError(stderr, err)
		}
		if *resourceProvider == "mwc" {
			opts.ResourceBackend = mwcClient
		}
		if *notebookContentAPI == "mwc" {
			opts.NotebookContentAPI = mwcClient
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
		if *notebookDiagnostics {
			logger.Printf("notebook prewarm totals: %+v", backend.PrewarmStats())
			logger.Printf("definition cache totals: %+v", backend.DefinitionCacheStats())
			logger.Printf("definition snapshot totals: %+v", backend.SnapshotStats())
			logger.Printf("notebook stage totals: %+v", backend.NotebookStats())
		}
	}()
	// Fail immediately on invalid selection/auth rather than mounting an
	// apparently empty catalog that silently hides permission failures.
	probeCtx, cancel := context.WithTimeout(mountCtx, *operationTimeout)
	_, err = backend.ReadDir(probeCtx, workspacefs.Entry{Kind: workspacefs.Root, Directory: true})
	cancel()
	if err != nil {
		return runtimeError(stderr, err)
	}
	server, err := fusefs.Mount(point, backend, fusefs.Options{ReadOnly: opts.ReadOnly, Logger: logger})
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
	control, err := startMountControl(point, cancelMount)
	if err != nil {
		unmountErr := server.Unmount()
		if unmountErr != nil {
			closeBackend = false
		}
		return runtimeError(stderr, errors.Join(err, unmountErr))
	}
	defer control.Close()
	if opts.PrewarmNotebookCount != 0 {
		if err := backend.StartPrewarm(mountCtx); err != nil {
			unmountErr := server.Unmount()
			if unmountErr != nil {
				closeBackend = false
			}
			return runtimeError(stderr, errors.Join(err, unmountErr))
		}
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
	case <-mountCtx.Done():
		if err := server.Unmount(); err != nil {
			closeBackend = false
			fmt.Fprintln(stderr, "unmount failed; "+fusefs.UnmountHelp(point))
			return runtimeError(stderr, err)
		}
		<-stopped
		return 0
	}
}

func startBackgroundMount(args []string, mountpoint string, stdout, stderr io.Writer) int {
	executable, err := os.Executable()
	if err != nil {
		return runtimeError(stderr, fmt.Errorf("locate executable for background mount: %w", err))
	}
	logPath, err := backgroundLogPath(mountpoint)
	if err != nil {
		return runtimeError(stderr, err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return runtimeError(stderr, fmt.Errorf("open background mount log: %w", err))
	}
	childArgs := append([]string{"mount"}, removeBackgroundFlag(args)...)
	command := exec.Command(executable, childArgs...)
	command.Env = append(os.Environ(), "FABRICFS_BACKGROUND_CHILD=1")
	command.Stdin = nil
	command.Stdout = logFile
	command.Stderr = logFile
	configureBackgroundProcess(command)
	if err := command.Start(); err != nil {
		logFile.Close()
		return runtimeError(stderr, fmt.Errorf("start background mount: %w", err))
	}
	if err := logFile.Close(); err != nil {
		return runtimeError(stderr, fmt.Errorf("close background mount log: %w", err))
	}
	statePath, _, err := mountControlPaths(mountpoint)
	if err != nil {
		return runtimeError(stderr, err)
	}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if data, err := os.ReadFile(statePath); err == nil {
			var state mountControlState
			if json.Unmarshal(data, &state) == nil && state.Mountpoint == mountpoint {
				fmt.Fprintf(stdout, "Mounted Fabric workspaces at %q in the background (PID %d).\nLogs: %s\nUse `fabric-workspace-fs unmount %s` for clean shutdown.\n",
					mountpoint, command.Process.Pid, logPath, mountpoint)
				return 0
			}
		}
		select {
		case <-deadline.C:
			return runtimeError(stderr, fmt.Errorf("background mount did not become ready within 15s; inspect %q", logPath))
		case <-ticker.C:
		}
	}
}

func removeBackgroundFlag(args []string) []string {
	filtered := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "--background" || arg == "-background" ||
			strings.HasPrefix(arg, "--background=") || strings.HasPrefix(arg, "-background=") {
			continue
		}
		filtered = append(filtered, arg)
	}
	return filtered
}

func backgroundLogPath(mountpoint string) (string, error) {
	statePath, _, err := mountControlPaths(mountpoint)
	if err != nil {
		return "", err
	}
	logDirectory := filepath.Join(filepath.Dir(filepath.Dir(statePath)), "logs")
	if err := os.MkdirAll(logDirectory, 0700); err != nil {
		return "", fmt.Errorf("create background mount log directory: %w", err)
	}
	return filepath.Join(logDirectory, strings.TrimSuffix(filepath.Base(statePath), ".json")+".log"), nil
}

func printLongDefaults(flags *flag.FlagSet) {
	output := flags.Output()
	var formatted strings.Builder
	flags.SetOutput(&formatted)
	flags.PrintDefaults()
	flags.SetOutput(output)
	text := formatted.String()
	if strings.HasPrefix(text, "  -") {
		text = "  --" + strings.TrimPrefix(text, "  -")
	}
	text = strings.ReplaceAll(text, "\n  -", "\n  --")
	fmt.Fprint(output, text)
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
