package overlay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"fabric-workspace-fs/internal/fserrors"
)

type Location struct {
	Workspace string
	Parent    string
	Relative  string
}

type Info struct {
	Path    string
	IsDir   bool
	Size    int64
	ModTime time.Time
}

type Options struct {
	MaxFileSize int64
	MaxBytes    int64
	MaxEntries  int
}

type storeQuota struct {
	TotalBytes int64
	Entries    int
}

type entry struct {
	path string
	info fs.FileInfo
	size int64
}

type Store struct {
	mu         sync.Mutex
	root       *os.Root
	opts       Options
	quota      storeQuota
	entries    map[string]*entry
	containers map[string]fs.FileInfo
	workspaces int
	scopes     int
	handles    map[*File]struct{}
	closed     bool
	closeErr   error
	poison     error
}

// New opens or creates private storage. Zero options select 1 GiB per file,
// 4 GiB total logical file size, and 10,000 user-visible entries.
func New(root string, opts Options) (*Store, error) {
	if opts.MaxFileSize < 0 || opts.MaxBytes < 0 || opts.MaxEntries < 0 {
		return nil, invalid("quota limits cannot be negative")
	}
	if opts.MaxFileSize == 0 {
		opts.MaxFileSize = 1 << 30
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = 4 << 30
	}
	if opts.MaxEntries == 0 {
		opts.MaxEntries = 10000
	}
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		return nil, fserrors.ErrUnsupported
	}
	r, err := openPrivateRoot(root)
	if err != nil {
		return nil, err
	}
	s := &Store{
		root: r, opts: opts, entries: make(map[string]*entry),
		containers: make(map[string]fs.FileInfo), handles: make(map[*File]struct{}),
	}
	if err := s.scan(); err != nil {
		return nil, errors.Join(err, r.Close())
	}
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	for handle := range s.handles {
		s.closeErr = errors.Join(s.closeErr, handle.closeLocked())
	}
	s.closeErr = errors.Join(s.closeErr, s.root.Close())
	return s.closeErr
}

func (s *Store) checkLocked(ctx context.Context) error {
	if s.closed {
		return errors.Join(fs.ErrClosed, fserrors.ErrClosed)
	}
	if ctx == nil {
		return invalid("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.poison != nil {
		return fmt.Errorf("overlay accounting unavailable: %w", s.poison)
	}
	return nil
}

func describe(relative string, info fs.FileInfo) Info {
	return Info{Path: relative, IsDir: info.IsDir(), Size: info.Size(), ModTime: info.ModTime()}
}

// List returns sorted immediate children. Only an absent scoped container is
// treated as an empty overlay; missing user paths and other errors are returned.
func (s *Store) List(ctx context.Context, loc Location) ([]Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return nil, err
	}
	p, err := resolve(loc, true)
	if err != nil {
		return nil, err
	}
	info, err := s.checkedStat(p.path)
	if p.relative == "" && errors.Is(err, fs.ErrNotExist) {
		return []Info{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fserrors.ErrNotDir
	}
	if err := s.known(p.path, info, p.relative == ""); err != nil {
		return nil, err
	}
	result := []Info{}
	err = s.readDir(ctx, p.path, func(name string) error {
		relative := name
		if p.relative != "" {
			relative = p.relative + "/" + name
		}
		child, err := resolve(Location{Workspace: loc.Workspace, Parent: loc.Parent, Relative: relative}, false)
		if err != nil {
			return err
		}
		record, err := s.existing(child.path)
		if err != nil {
			return err
		}
		result = append(result, describe(relative, record.info))
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(result, func(a, b Info) int { return strings.Compare(a.Path, b.Path) })
	return result, nil
}

func (s *Store) Stat(ctx context.Context, loc Location) (Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return Info{}, err
	}
	p, err := resolve(loc, true)
	if err != nil {
		return Info{}, err
	}
	if p.relative != "" {
		record, err := s.existing(p.path)
		if err != nil {
			return Info{}, err
		}
		return describe(p.relative, record.info), nil
	}
	info, err := s.checkedStat(p.path)
	if err != nil {
		return Info{}, err
	}
	if err := s.known(p.path, info, p.relative == ""); err != nil {
		return Info{}, err
	}
	return describe(p.relative, info), nil
}

func (s *Store) Mkdir(ctx context.Context, loc Location) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return err
	}
	p, err := resolve(loc, false)
	if err != nil {
		return err
	}
	if _, err := s.checkedStat(p.path); err == nil {
		return fs.ErrExist
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := s.reserveEntry(); err != nil {
		return err
	}
	if !strings.Contains(p.relative, "/") {
		if err := s.ensureContainers(p); err != nil {
			return err
		}
	}
	path, err := s.destination(p)
	if err != nil {
		return err
	}
	if s.entries[pathKey(path)] != nil {
		return fmt.Errorf("overlay entry %q disappeared or aliases another entry: %w", path, fserrors.ErrConflict)
	}
	if err := s.root.Mkdir(diskPath(path), 0o700); err != nil {
		return err
	}
	if err := s.addCreated(path); err != nil {
		s.poison = err
		return err
	}
	return nil
}

func (s *Store) Remove(ctx context.Context, loc Location, directory bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return err
	}
	p, err := resolve(loc, false)
	if err != nil {
		return err
	}
	record, err := s.existing(p.path)
	if err != nil {
		return err
	}
	if directory && !record.info.IsDir() {
		return fserrors.ErrNotDir
	}
	if !directory && record.info.IsDir() {
		return fserrors.ErrIsDir
	}
	if s.busy(record.path) {
		return fserrors.ErrBusy
	}
	if directory {
		empty, err := s.emptyDir(ctx, record.path)
		if err != nil {
			return err
		}
		if !empty {
			return fserrors.ErrNotEmpty
		}
	}
	if err := s.root.Remove(diskPath(record.path)); err != nil {
		if isNotEmpty(err) {
			return errors.Join(fserrors.ErrNotEmpty, err)
		}
		return err
	}
	s.forget(record)
	if !strings.Contains(p.relative, "/") {
		return s.pruneContainers(ctx, p)
	}
	return nil
}

func (s *Store) Rename(ctx context.Context, src, dst Location, noReplace bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return err
	}
	from, err := resolve(src, false)
	if err != nil {
		return err
	}
	to, err := resolve(dst, false)
	if err != nil {
		return err
	}
	if from.scope != to.scope {
		return fserrors.ErrCrossDevice
	}
	source, err := s.existing(from.path)
	if err != nil {
		return err
	}
	if !source.info.IsDir() && !strings.Contains(to.relative, "/") {
		return invalid("dot-roots must be directories")
	}
	targetPath, err := s.destination(to)
	if err != nil {
		return err
	}
	target, err := s.existing(targetPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if target != nil {
		targetPath = target.path
	}
	if s.busy(source.path) || s.busy(targetPath) {
		return fserrors.ErrBusy
	}
	if target != nil && noReplace {
		return fs.ErrExist
	}
	if target == source {
		return nil
	}
	if source.info.IsDir() && within(targetPath, source.path) {
		return invalid("cannot move a directory inside itself")
	}
	if target != nil && (target.info.IsDir() || source.info.IsDir()) {
		return fserrors.ErrUnsupported
	}
	moves := make(map[*entry]string)
	for _, record := range s.entries {
		if within(record.path, source.path) {
			next := targetPath + record.path[len(source.path):]
			if strings.Count(next, "/")-1 > maxDepth {
				return invalid("rename would exceed the overlay depth limit")
			}
			moves[record] = next
		}
	}
	if err := s.root.Rename(diskPath(source.path), diskPath(targetPath)); err != nil {
		return err
	}
	if target != nil {
		s.forget(target)
	}
	for record := range moves {
		delete(s.entries, pathKey(record.path))
	}
	for record, path := range moves {
		record.path = path
		s.entries[pathKey(path)] = record
	}
	return nil
}

func (s *Store) busy(path string) bool {
	for handle := range s.handles {
		if within(handle.entry.path, path) {
			return true
		}
	}
	return false
}

func (s *Store) writer(record *entry) bool {
	for handle := range s.handles {
		if handle.entry == record && handle.writable {
			return true
		}
	}
	return false
}

func (s *Store) reserveEntry() error {
	if s.quota.Entries >= s.opts.MaxEntries {
		return syscall.ENOSPC
	}
	return nil
}

func (s *Store) growth(old, next int64) error {
	if next < 0 {
		return invalid("negative or overflowing file size")
	}
	if next > s.opts.MaxFileSize {
		return fserrors.ErrTooLarge
	}
	if next > old && (s.quota.TotalBytes > s.opts.MaxBytes || next-old > s.opts.MaxBytes-s.quota.TotalBytes) {
		return syscall.ENOSPC
	}
	return nil
}

func (s *Store) refresh(record *entry, info fs.FileInfo) error {
	if record.info.IsDir() != info.IsDir() || !os.SameFile(record.info, info) {
		return fmt.Errorf("overlay entry %q changed: %w", record.path, fserrors.ErrConflict)
	}
	if !info.IsDir() {
		if info.Size() < 0 || info.Size() > math.MaxInt64-(s.quota.TotalBytes-record.size) {
			return invalid("logical file sizes overflow quota accounting")
		}
		s.quota.TotalBytes += info.Size() - record.size
		record.size = info.Size()
	}
	record.info = info
	return nil
}

func (s *Store) entryFor(path string, info fs.FileInfo) (*entry, error) {
	record := s.entries[pathKey(path)]
	if record == nil && runtime.GOOS == "windows" {
		// File identity also covers filesystem case aliases outside Go's folding.
		for _, candidate := range s.entries {
			if os.SameFile(candidate.info, info) {
				record = candidate
				break
			}
		}
	}
	if record == nil {
		return nil, fmt.Errorf("overlay entry %q was added outside this store: %w", path, fserrors.ErrConflict)
	}
	if err := s.refresh(record, info); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) existing(path string) (*entry, error) {
	file, info, err := s.openVerified(path, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return s.entryFor(path, info)
}

func (s *Store) known(path string, info fs.FileInfo, container bool) error {
	if !container {
		_, err := s.entryFor(path, info)
		return err
	}
	before := s.containers[pathKey(path)]
	if before == nil || !os.SameFile(before, info) {
		return fmt.Errorf("overlay container %q changed: %w", path, fserrors.ErrConflict)
	}
	return nil
}

func (s *Store) forget(record *entry) {
	delete(s.entries, pathKey(record.path))
	s.quota.Entries--
	if !record.info.IsDir() {
		s.quota.TotalBytes -= record.size
	}
}

func (s *Store) destination(p resolvedLocation) (string, error) {
	index := strings.LastIndexByte(p.path, '/')
	parentPath, name := p.path[:index], p.path[index+1:]
	info, err := s.checkedStat(parentPath)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fserrors.ErrNotDir
	}
	if parentPath == p.scope {
		if err := s.known(parentPath, info, true); err != nil {
			return "", err
		}
		return parentPath + "/" + name, nil
	}
	parent, err := s.entryFor(parentPath, info)
	if err != nil {
		return "", err
	}
	return parent.path + "/" + name, nil
}

func (s *Store) checkedStat(path string) (fs.FileInfo, error) {
	parts := strings.Split(path, "/")
	var info fs.FileInfo
	for i := range parts {
		prefix := strings.Join(parts[:i+1], "/")
		current, err := s.root.Lstat(diskPath(prefix))
		if err != nil {
			return nil, err
		}
		if err := checkPrivate(prefix, current); err != nil {
			return nil, err
		}
		if i < len(parts)-1 && !current.IsDir() {
			return nil, fserrors.ErrNotDir
		}
		info = current
	}
	return info, nil
}

func (s *Store) openVerified(path string, flags int) (*os.File, fs.FileInfo, error) {
	before, err := s.checkedStat(path)
	if err != nil {
		return nil, nil, err
	}
	file, err := s.root.OpenFile(diskPath(path), flags, 0)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err == nil {
		err = checkHandle(path, file, info)
	}
	if err == nil && !os.SameFile(before, info) {
		err = unsafeEntry(path, "entry changed while opening")
	}
	if err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	return file, info, nil
}

func (s *Store) addCreated(path string) error {
	file, info, err := s.openVerified(path, os.O_RDONLY)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	s.entries[pathKey(path)] = &entry{path: path, info: info, size: info.Size()}
	s.quota.Entries++
	return nil
}

func (s *Store) readDir(ctx context.Context, path string, visit func(string) error) (err error) {
	file, info, err := s.openVerified(path, os.O_RDONLY)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if !info.IsDir() {
		return fserrors.ErrNotDir
	}
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		names, readErr := file.Readdirnames(1)
		for _, name := range names {
			if count >= s.opts.MaxEntries {
				return syscall.ENOSPC
			}
			count++
			if err := visit(name); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *Store) emptyDir(ctx context.Context, path string) (empty bool, err error) {
	file, info, err := s.openVerified(path, os.O_RDONLY)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if !info.IsDir() {
		return false, fserrors.ErrNotDir
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	names, err := file.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return len(names) == 0, err
}

func (s *Store) ensureContainers(p resolvedLocation) error {
	for _, path := range []string{p.workspace, p.scope} {
		info, err := s.checkedStat(path)
		if err == nil {
			if !info.IsDir() {
				return fserrors.ErrNotDir
			}
			if err := s.known(path, info, true); err != nil {
				return err
			}
			continue
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if s.containers[pathKey(path)] != nil {
			return fmt.Errorf("overlay container %q disappeared: %w", path, fserrors.ErrConflict)
		}
		count := s.workspaces
		if path == p.scope {
			count = s.scopes
		}
		if count >= s.opts.MaxEntries {
			return syscall.ENOSPC
		}
		if err := s.root.Mkdir(diskPath(path), 0o700); err != nil {
			return err
		}
		file, info, err := s.openVerified(path, os.O_RDONLY)
		if err != nil {
			s.poison = err
			return err
		}
		if err := file.Close(); err != nil {
			s.poison = err
			return err
		}
		s.containers[pathKey(path)] = info
		if path == p.scope {
			s.scopes++
		} else {
			s.workspaces++
		}
	}
	return nil
}

func (s *Store) pruneContainers(ctx context.Context, p resolvedLocation) error {
	for _, path := range []string{p.scope, p.workspace} {
		empty, err := s.emptyDir(ctx, path)
		if err != nil {
			return err
		}
		if !empty {
			return nil
		}
		if err := s.root.Remove(diskPath(path)); err != nil {
			if isNotEmpty(err) {
				return nil
			}
			return err
		}
		delete(s.containers, pathKey(path))
		if path == p.scope {
			s.scopes--
		} else {
			s.workspaces--
		}
	}
	return nil
}

func (s *Store) scan() error {
	ctx := context.Background()
	return s.readDir(ctx, ".", func(workspace string) error {
		if !validUUID(workspace) || workspace != strings.ToLower(workspace) {
			return invalid("invalid workspace directory in storage")
		}
		if s.workspaces >= s.opts.MaxEntries {
			return syscall.ENOSPC
		}
		if err := s.scanContainer(workspace); err != nil {
			return err
		}
		s.workspaces++
		return s.readDir(ctx, workspace, func(parent string) error {
			if parent != "root" && (!validUUID(parent) || parent != strings.ToLower(parent)) {
				return invalid("invalid parent directory in storage")
			}
			if s.scopes >= s.opts.MaxEntries {
				return syscall.ENOSPC
			}
			scope := workspace + "/" + parent
			if err := s.scanContainer(scope); err != nil {
				return err
			}
			s.scopes++
			return s.scanEntries(ctx, workspace, parent, "")
		})
	})
}

func (s *Store) scanContainer(path string) error {
	file, info, err := s.openVerified(path, os.O_RDONLY)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.Join(fserrors.ErrNotDir, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	s.containers[pathKey(path)] = info
	return nil
}

func (s *Store) scanEntries(ctx context.Context, workspace, parent, relative string) error {
	path := workspace + "/" + parent
	if relative != "" {
		path += "/" + relative
	}
	return s.readDir(ctx, path, func(name string) error {
		if err := s.reserveEntry(); err != nil {
			return err
		}
		child := name
		if relative != "" {
			child = relative + "/" + name
		}
		p, err := resolve(Location{workspace, parent, child}, false)
		if err != nil {
			return err
		}
		if s.entries[pathKey(p.path)] != nil {
			return invalid("duplicate or case-aliased entry in storage")
		}
		file, info, err := s.openVerified(p.path, os.O_RDONLY)
		if err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		if relative == "" && !info.IsDir() {
			return invalid("dot-roots must be directories")
		}
		if !info.IsDir() {
			if err := s.growth(0, info.Size()); err != nil {
				return err
			}
			s.quota.TotalBytes += info.Size()
		}
		s.quota.Entries++
		s.entries[pathKey(p.path)] = &entry{path: p.path, info: info, size: info.Size()}
		if info.IsDir() {
			return s.scanEntries(ctx, workspace, parent, child)
		}
		return nil
	})
}
