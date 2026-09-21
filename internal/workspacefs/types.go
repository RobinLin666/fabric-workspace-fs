package workspacefs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fabric-workspace-fs/internal/agentbundle"
	"fabric-workspace-fs/internal/cache"
	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/overlay"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/writeback"
)

type FabricAPI interface {
	ListWorkspaces(context.Context) ([]fabric.Workspace, error)
	GetWorkspace(context.Context, string) (fabric.Workspace, error)
	ListItems(context.Context, string) ([]fabric.Item, error)
	ListFolders(context.Context, string) ([]fabric.Folder, error)
	GetDefinition(context.Context, string, string, string, string) (fabric.Definition, error)
	UpdateNotebook(context.Context, string, string, fabric.Definition) error
}

type LakeAPI interface {
	Stat(context.Context, onelake.Path) (onelake.Info, error)
	List(context.Context, onelake.Path) ([]onelake.Info, error)
	Read(context.Context, onelake.Path, int64, []byte, string) (int, error)
	Put(context.Context, onelake.Path, io.ReaderAt, int64, string) (onelake.Info, error)
	Mkdir(context.Context, onelake.Path) error
	Remove(context.Context, onelake.Path, bool, string) error
	Rename(context.Context, onelake.Path, onelake.Path, string, string, bool) (onelake.Info, error)
}

type Kind uint8

const (
	Root Kind = iota
	Workspaces
	Workspace
	FabricFolder
	Notebook
	Lakehouse
	Environment
	NotebookContent
	DefinitionFile
	DefinitionDirectory
	LakeFile
	LakeDirectory
	IdentityFile
	OverlayDirectory
	OverlayFile
	ResourceDirectory
	ResourceFile
	AgentDirectory
	AgentFile
)

const (
	definitionSnapshotMaxEntries = 32
	definitionSnapshotCacheBytes = 64 << 20
	// Background prewarming leaves half of the retention budget available for
	// foreground snapshots instead of repeatedly evicting useful entries.
	prewarmRetainedByteLimit = definitionSnapshotCacheBytes / 2
)

type Entry struct {
	Name           string
	Label          string
	Kind           Kind
	Directory      bool
	Workspace      string
	Item           fabric.Item
	Folder         fabric.Folder
	RemotePartPath string
	Local          overlay.Location
	Resource       resources.Path
	Remote         string
	Part           string
	Size           int64
	Modified       time.Time
	ValidUntil     time.Time
	Fixed          bool
}

func (e Entry) LakePath() onelake.Path {
	return onelake.Path{Workspace: e.Workspace, Item: e.Item.ID, Relative: e.Remote}
}

func (e Entry) Key() string {
	if e.Kind == AgentDirectory || e.Kind == AgentFile {
		return "injected-agents/" + e.Part
	}
	if e.Kind == NotebookContent {
		return strings.ToLower(e.Workspace) + "/" + strings.ToLower(e.Item.ID) + "/" + namespace.NotebookContentFileName(e.Item.DisplayName)
	}
	if e.Kind == ResourceDirectory || e.Kind == ResourceFile {
		return "resource/" + e.Resource.Target.WorkspaceID + "/" + e.Resource.Target.ItemID + "/" + e.Resource.Target.Kind + "/" + e.Resource.Relative
	}
	if e.Kind == OverlayDirectory || e.Kind == OverlayFile {
		return "overlay/" + e.Local.Workspace + "/" + e.Local.Parent + "/" + e.Local.Relative
	}
	if e.Folder.ID != "" && e.Item.ID == "" {
		return strings.ToLower(e.Workspace) + "/folders/" + strings.ToLower(e.Folder.ID) + "/" + e.Part
	}
	key := strings.ToLower(e.Workspace) + "/" + strings.ToLower(e.Item.ID)
	if e.Kind == LakeFile || e.Kind == LakeDirectory {
		return key + "/" + e.Remote
	}
	return key + "/" + e.Part
}

type Options struct {
	WorkspaceIDs       []string
	AllWorkspaces      bool
	ReadOnly           bool
	SpoolDirectory     string
	CacheTTL           time.Duration
	CachePolicy        *cachepolicy.Policy
	Now                func() time.Time
	Version            string
	FNTKExecutable     string
	MaxNotebookSize    int64
	MaxFileSize        int64
	MaxOpenHandles     int
	MaxWriters         int
	OverlayDirectory   string
	OverlayMaxFileSize int64
	OverlayMaxBytes    int64
	OverlayMaxEntries  int
	ResourceBackend    resources.Backend
	MaxResourceSize    int64
	// PrewarmNotebookCount enables bounded mount-level Notebook prewarming.
	// Zero leaves prewarming disabled.
	PrewarmNotebookCount int
	PrewarmTimeout       time.Duration
	// LogNotebookEvent receives bounded, content-free diagnostics.
	LogNotebookEvent func(NotebookEvent)
	LogPrewarmError  func(error)
}

func DefaultOptions() Options {
	return Options{
		CacheTTL: cachepolicy.DefaultTTL, MaxNotebookSize: 16 << 20,
		MaxFileSize: 1 << 30, MaxOpenHandles: 64, MaxWriters: 16,
		MaxResourceSize: 16 << 20,
	}
}

type lease struct {
	readers int
	writers int
}

type FS struct {
	fabric *cachedFabricAPI
	lake   *cachedLakeAPI
	opts   Options
	start  time.Time
	now    func() time.Time

	mu       sync.Mutex
	active   map[string]lease
	spools   map[string]*writeback.File
	handles  int
	writers  int
	mutating bool
	changed  chan struct{}

	names          namespace.Catalog
	catalogs       *cache.Cache[*folderTree]
	snapshots      *cache.Cache[*definitionSnapshot]
	resources      resources.Backend
	decodes        atomic.Uint64
	digests        atomic.Uint64
	pinMu          sync.Mutex
	pinnedBytes    int64
	pinnedCount    int
	agentFiles     map[string][]byte
	definitionGate definitionGate
	prewarm        *notebookPrewarmer
	notebookPerf   notebookPerf
}

func New(fab FabricAPI, lake LakeAPI, opts Options) (*FS, error) {
	if fab == nil || lake == nil {
		return nil, fmt.Errorf("both API clients are required: %w", fs.ErrInvalid)
	}
	if len(opts.WorkspaceIDs) == 0 && !opts.AllWorkspaces {
		return nil, fmt.Errorf("select workspace IDs or all workspaces: %w", fs.ErrInvalid)
	}
	if len(opts.WorkspaceIDs) > 64 || (len(opts.WorkspaceIDs) > 0 && opts.AllWorkspaces) {
		return nil, fmt.Errorf("invalid workspace selection: %w", fs.ErrInvalid)
	}
	if opts.MaxNotebookSize <= 0 || opts.MaxNotebookSize > 128<<20 || opts.MaxFileSize <= 0 || opts.CacheTTL < 0 ||
		opts.MaxOpenHandles <= 0 || opts.MaxWriters <= 0 || opts.MaxWriters > opts.MaxOpenHandles ||
		(!opts.ReadOnly && opts.SpoolDirectory == "") {
		return nil, fmt.Errorf("invalid filesystem limits or spool directory: %w", fs.ErrInvalid)
	}
	if opts.ResourceBackend != nil && (opts.MaxResourceSize <= 0 || opts.MaxResourceSize > 128<<20) {
		return nil, fmt.Errorf("resource size limit must be 1..128 MiB: %w", fs.ErrInvalid)
	}
	if opts.OverlayDirectory != "" || opts.OverlayMaxFileSize != 0 || opts.OverlayMaxBytes != 0 || opts.OverlayMaxEntries != 0 {
		return nil, fmt.Errorf("local dot-directory overlays were removed; old overlay data is not modified: %w", fserrors.ErrUnsupported)
	}
	ids := make([]string, 0, len(opts.WorkspaceIDs))
	seen := make(map[string]bool)
	for _, id := range opts.WorkspaceIDs {
		if _, err := namespace.CatalogName("", id); err != nil {
			return nil, err
		}
		id = strings.ToLower(id)
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	opts.WorkspaceIDs = ids
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.CachePolicy == nil {
		var err error
		opts.CachePolicy, err = cachepolicy.Uniform(opts.CacheTTL)
		if err != nil {
			return nil, err
		}
	}
	filesystem := &FS{
		fabric: newCachedFabric(fab, opts.CachePolicy, opts.Now), lake: newCachedLake(lake, opts.CachePolicy, opts.Now),
		opts: opts, start: opts.Now(), now: opts.Now, resources: opts.ResourceBackend,
		active: make(map[string]lease), spools: make(map[string]*writeback.File),
		changed: make(chan struct{}),
	}
	if err := ValidatePrewarmCount(opts.PrewarmNotebookCount); err != nil {
		return nil, err
	}
	if opts.PrewarmTimeout < 0 {
		return nil, fmt.Errorf("prewarm timeout must not be negative: %w", fs.ErrInvalid)
	}
	if opts.PrewarmTimeout == 0 {
		filesystem.opts.PrewarmTimeout = 5 * time.Minute
	}
	if opts.PrewarmNotebookCount > 0 {
		filesystem.prewarm = newNotebookPrewarmer(filesystem)
	}
	filesystem.catalogs = cache.New(cache.Options[*folderTree]{
		TTL: cachepolicy.DefaultTTL, MaxEntries: 32, MaxBytes: 32 << 20, Now: opts.Now,
		Size:       func(tree *folderTree) int64 { return tree.bytes },
		ObservedAt: func(tree *folderTree) time.Time { return tree.observedAt },
	})
	filesystem.snapshots = cache.New(cache.Options[*definitionSnapshot]{
		TTL: cachepolicy.DefaultTTL, MaxEntries: definitionSnapshotMaxEntries, MaxBytes: definitionSnapshotCacheBytes, Now: opts.Now,
		Size:       func(snapshot *definitionSnapshot) int64 { return snapshot.bytes },
		ObservedAt: func(snapshot *definitionSnapshot) time.Time { return snapshot.observedAt },
	})
	agentFiles, err := agentbundle.Files(opts.Version, opts.FNTKExecutable)
	if err != nil {
		return nil, err
	}
	filesystem.agentFiles = agentFiles
	if err := filesystem.names.ReserveName("workspaces", namespace.AgentRootName); err != nil {
		return nil, err
	}
	if err := filesystem.names.ReserveName("workspaces", namespace.AgentGuideName); err != nil {
		return nil, err
	}
	return filesystem, nil
}

func (s *FS) Close() error {
	if s.prewarm != nil {
		return s.prewarm.close()
	}
	return nil
}

func (s *FS) Root() Entry {
	return Entry{Kind: Root, Directory: true, Modified: s.start}
}

func (s *FS) Writable(e Entry) bool {
	if s.opts.ReadOnly {
		return false
	}
	if e.Kind == ResourceDirectory || e.Kind == ResourceFile {
		return e.Resource.Target.Kind == "Notebook"
	}
	if managedContainer(e) {
		return true
	}
	if e.Kind == NotebookContent && e.Item.Type == "Notebook" {
		return true
	}
	return (e.Kind == LakeFile || e.Kind == LakeDirectory) && e.Item.Type == "Lakehouse" &&
		(e.Remote == "Files" || strings.HasPrefix(e.Remote, "Files/"))
}

func (s *FS) writableFile(e Entry) error {
	if !s.Writable(e) {
		return fserrors.ErrReadOnly
	}
	if e.Directory {
		return fserrors.ErrIsDir
	}
	if e.Kind == OverlayFile {
		return fserrors.ErrUnsupported
	}
	if e.Kind == ResourceFile {
		return resources.ValidatePath(e.Resource, true)
	}
	if e.Kind == LakeFile {
		return onelake.ValidatePath(e.LakePath(), true)
	}
	if e.Kind != NotebookContent {
		return fserrors.ErrReadOnly
	}
	return nil
}

func (s *FS) mutableDirectory(e Entry) error {
	if e.Kind == Notebook {
		return fmt.Errorf("notebook directories are read-only; save the .ipynb in place (atomic-save/create/rename is unsupported): %w", fserrors.ErrReadOnly)
	}
	if !s.Writable(e) {
		return fserrors.ErrReadOnly
	}
	if e.Kind != LakeDirectory || !e.Directory {
		return fserrors.ErrNotDir
	}
	return onelake.ValidatePath(e.LakePath(), false)
}

func (s *FS) begin(ctx context.Context, e Entry, write bool) (func(), error) {
	key := e.Key()
	return s.acquire(ctx, func() func() {
		current := s.active[key]
		if s.mutating || s.handles >= s.opts.MaxOpenHandles ||
			(write && (current.writers != 0 || s.writers >= s.opts.MaxWriters)) {
			return nil
		}
		if write {
			current.writers++
			s.writers++
		} else {
			current.readers++
		}
		s.handles++
		s.active[key] = current
		var once sync.Once
		return func() {
			once.Do(func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				state := s.active[key]
				if write {
					state.writers--
					s.writers--
				} else {
					state.readers--
				}
				s.handles--
				if state.readers == 0 && state.writers == 0 {
					delete(s.active, key)
				} else {
					s.active[key] = state
				}
				s.notify()
			})
		}
	})
}

// Namespace mutations cannot race open handles in the affected subtree. Unlike
// a local POSIX filesystem, OneLake has no stable open-file inode after rename.
func (s *FS) mutation(ctx context.Context, keys ...string) (func(), error) {
	return s.acquire(ctx, func() func() {
		if s.mutating {
			return nil
		}
		for active := range s.active {
			for _, key := range keys {
				if active == key || strings.HasPrefix(active, key+"/") {
					return nil
				}
			}
		}
		s.mutating = true
		var once sync.Once
		return func() {
			once.Do(func() {
				s.mu.Lock()
				s.mutating = false
				s.notify()
				s.mu.Unlock()
			})
		}
	})
}

// Linux sends RELEASE asynchronously after close/FLUSH. Wait briefly inside
// the filesystem so ordinary close-then-rename/reopen needs no caller retry.
// Never release a lease on FLUSH: another duplicated fd can still be alive.
func (s *FS) acquire(ctx context.Context, try func() func()) (func(), error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		release := try()
		changed := s.changed
		s.mu.Unlock()
		if release != nil {
			return release, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fserrors.ErrBusy
		case <-changed:
		}
	}
}

func (s *FS) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func IsNotExist(err error) bool {
	var httpErr *transport.HTTPError
	return errors.Is(err, fs.ErrNotExist) || (errors.As(err, &httpErr) && httpErr.StatusCode == 404)
}

func (s *FS) registerSpool(e Entry, file *writeback.File) {
	s.mu.Lock()
	s.spools[e.Key()] = file
	s.mu.Unlock()
}

func (s *FS) closeSpool(e Entry, release func()) func() {
	return func() {
		s.mu.Lock()
		delete(s.spools, e.Key())
		s.mu.Unlock()
		release()
	}
}

func (s *FS) localSize(e Entry) (int64, bool) {
	s.mu.Lock()
	file := s.spools[e.Key()]
	s.mu.Unlock()
	if file == nil {
		return 0, false
	}
	return file.Size(), true
}

type Handle interface {
	ReadAt(context.Context, []byte, int64) (int, error)
	WriteAt(context.Context, []byte, int64) (int, error)
	Truncate(context.Context, int64) error
	Flush(context.Context) error
	Close() error
	Size() int64
}
