//go:build windows || (darwin && cgo)

package fusefs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"net"
	"os"
	pathpkg "path"
	"runtime"
	"strings"
	"sync"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/workspacefs"

	"github.com/winfsp/cgofuse/fuse"
)

func Supported() bool { return true }

func Mount(mountpoint string, backend *workspacefs.FS, opts Options) (Server, error) {
	if backend == nil {
		return nil, errors.New("filesystem backend is required")
	}
	if err := validatePortableMountpoint(mountpoint); err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "fabric-workspace-fs: ", log.LstdFlags)
	}
	adapter := newPortableFS(backend, logger)
	host := fuse.NewFileSystemHost(adapter)
	adapter.host = host
	host.SetCapCaseInsensitive(false)
	host.SetCapDeleteAccess(true)
	host.SetCapOpenTrunc(true)
	host.SetCapReaddirPlus(false)
	host.SetDirectIO(true)
	server := &portableServer{
		host: host, adapter: adapter, ready: adapter.ready, done: make(chan struct{}),
		timeout: 10 * time.Second,
	}
	mountOptions := []string{"-o", "fsname=fabric-workspace-fs", "-o", "attr_timeout=0,entry_timeout=0,negative_timeout=0"}
	if runtime.GOOS == "windows" {
		// WinFsp maps POSIX ownership to Windows security. Mapping both owner
		// and group to the mounting user keeps writable directories accessible.
		mountOptions = append(mountOptions, "-o", "uid=-1,gid=-1,FileInfoTimeout=0")
	}
	if opts.ReadOnly {
		mountOptions = append(mountOptions, "-o", "ro")
	}
	go server.serve(mountpoint, mountOptions)
	if err := server.WaitMount(); err != nil {
		select {
		case <-server.done:
			return nil, err
		default:
			return server, fmt.Errorf("mount startup is unresolved at %q; retain and inspect the mountpoint: %w", mountpoint, err)
		}
	}
	if err := waitPortableMountVisible(server, mountpoint); err != nil {
		return failedMount(server, mountpoint, err)
	}
	return server, nil
}

func waitPortableMountVisible(server *portableServer, mountpoint string) error {
	if runtime.GOOS != "windows" || len(mountpoint) != 2 || mountpoint[1] != ':' {
		return nil
	}
	root := mountpoint + `\`
	deadline := time.NewTimer(server.timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(root); err == nil {
			return nil
		}
		select {
		case <-server.done:
			return server.exitBeforeReady()
		case <-deadline.C:
			return fmt.Errorf("WinFsp mount %q was initialized but did not become visible within %s", mountpoint, server.timeout)
		case <-ticker.C:
		}
	}
}

func validatePortableMountpoint(mountpoint string) error {
	if runtime.GOOS == "windows" && len(mountpoint) == 2 && mountpoint[1] == ':' {
		if (mountpoint[0] >= 'A' && mountpoint[0] <= 'Z') || (mountpoint[0] >= 'a' && mountpoint[0] <= 'z') {
			return nil
		}
	}
	info, err := os.Lstat(mountpoint)
	if err != nil {
		return fmt.Errorf("mountpoint: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("mountpoint must be an existing directory, not a symlink")
	}
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		return fmt.Errorf("read mountpoint: %w", err)
	}
	if len(entries) != 0 {
		return errors.New("mountpoint must be empty")
	}
	return nil
}

type portableServer struct {
	host    *fuse.FileSystemHost
	adapter *portableFS
	ready   <-chan struct{}
	done    chan struct{}
	timeout time.Duration

	mu       sync.Mutex
	serveErr error
	unmount  bool
}

func (s *portableServer) serve(mountpoint string, options []string) {
	defer close(s.done)
	defer func() {
		if recovered := recover(); recovered != nil {
			s.mu.Lock()
			s.serveErr = portableRuntimeError(recovered)
			s.mu.Unlock()
		}
	}()
	if !s.host.Mount(mountpoint, options) {
		s.mu.Lock()
		s.serveErr = errors.New("FUSE host stopped without a successful mount")
		s.mu.Unlock()
	}
}

func portableRuntimeError(recovered any) error {
	message := fmt.Sprint(recovered)
	lower := strings.ToLower(message)
	if runtime.GOOS == "windows" && strings.Contains(lower, "cannot find winfsp") {
		return fmt.Errorf("WinFsp runtime was not found; install WinFsp 2.1 and retry: %s", message)
	}
	if runtime.GOOS == "darwin" && (strings.Contains(lower, "fuse") || strings.Contains(lower, "dylib")) {
		return fmt.Errorf("macFUSE runtime was not found or could not be loaded; install macFUSE and retry: %s", message)
	}
	return fmt.Errorf("FUSE runtime startup failed: %s", message)
}

func (s *portableServer) WaitMount() error {
	select {
	case <-s.done:
		return s.exitBeforeReady()
	default:
	}
	select {
	case <-s.ready:
		select {
		case <-s.done:
			return s.exitBeforeReady()
		default:
			return nil
		}
	case <-s.done:
		return s.exitBeforeReady()
	case <-time.After(s.timeout):
		return fmt.Errorf("FUSE mount did not become ready within %s", s.timeout)
	}
}

func (s *portableServer) exitBeforeReady() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serveErr != nil {
		return s.serveErr
	}
	return errors.New("FUSE host exited before the mount became ready")
}

func (s *portableServer) Wait() { <-s.done }

func (s *portableServer) Unmount() error {
	s.mu.Lock()
	if s.unmount {
		s.mu.Unlock()
		select {
		case <-s.done:
			return nil
		case <-time.After(s.timeout):
			return fmt.Errorf("FUSE serving goroutine did not stop within %s", s.timeout)
		}
	}
	s.unmount = true
	s.mu.Unlock()
	accepted, unmountErr := requestPortableUnmount(s.host)
	if !accepted {
		s.mu.Lock()
		s.unmount = false
		s.mu.Unlock()
		if unmountErr != nil {
			return unmountErr
		}
		select {
		case <-s.done:
			s.mu.Lock()
			err := s.serveErr
			s.mu.Unlock()
			return err
		default:
			return errors.New("FUSE runtime rejected the unmount request")
		}
	}
	s.adapter.cancel()
	select {
	case <-s.done:
		return nil
	case <-time.After(s.timeout):
		return fmt.Errorf("FUSE serving goroutine did not stop within %s", s.timeout)
	}
}

func requestPortableUnmount(host *fuse.FileSystemHost) (accepted bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			accepted = false
			err = fmt.Errorf("FUSE unmount request failed: %v", recovered)
		}
	}()
	return host.Unmount(), nil
}

type portableHandle struct {
	entry  workspacefs.Entry
	handle workspacefs.Handle
	flags  int

	mu      sync.Mutex
	flushed bool
	closed  bool
}

type portableFS struct {
	fuse.FileSystemBase
	backend *workspacefs.FS
	logger  *log.Logger
	host    *fuse.FileSystemHost
	ready   chan struct{}
	once    sync.Once
	tree    sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc

	mu      sync.Mutex
	next    uint64
	handles map[uint64]*portableHandle
}

func newPortableFS(backend *workspacefs.FS, logger *log.Logger) *portableFS {
	ctx, cancel := context.WithCancel(context.Background())
	return &portableFS{
		backend: backend, logger: logger, ready: make(chan struct{}), ctx: ctx, cancel: cancel,
		next: 1, handles: make(map[uint64]*portableHandle),
	}
}

func (f *portableFS) Init() { f.once.Do(func() { close(f.ready) }) }

func (f *portableFS) Destroy() {
	f.cancel()
	f.mu.Lock()
	handles := f.handles
	f.handles = make(map[uint64]*portableHandle)
	f.mu.Unlock()
	for _, handle := range handles {
		handle.mu.Lock()
		var err error
		if !handle.closed {
			handle.closed = true
			err = handle.handle.Close()
		}
		handle.mu.Unlock()
		if err != nil {
			f.logger.Printf("release during destroy: %v", err)
		}
	}
}

func (f *portableFS) fail(operation string, err error) int {
	if err == nil {
		return 0
	}
	code := portableErrno(err)
	if code != fuse.ENOENT && code != fuse.ENOTSUP {
		f.logger.Printf("%s: %v", operation, err)
	}
	return -code
}

func (f *portableFS) resolve(ctx context.Context, name string) (workspacefs.Entry, error) {
	clean, err := cleanPortablePath(name)
	if err != nil {
		return workspacefs.Entry{}, err
	}
	if clean == "/" {
		return f.backend.Root(), nil
	}
	current := f.backend.Root()
	for _, component := range strings.Split(strings.TrimPrefix(clean, "/"), "/") {
		if component == "" || component == "." || component == ".." {
			return workspacefs.Entry{}, fs.ErrInvalid
		}
		var err error
		current, err = f.backend.Lookup(ctx, current, component)
		if err != nil {
			return workspacefs.Entry{}, err
		}
	}
	return current, nil
}

func (f *portableFS) parent(ctx context.Context, name string) (workspacefs.Entry, string, error) {
	clean, err := cleanPortablePath(name)
	if err != nil {
		return workspacefs.Entry{}, "", err
	}
	base := pathpkg.Base(clean)
	if clean == "/" || base == "." || base == "/" {
		return workspacefs.Entry{}, "", fs.ErrInvalid
	}
	parent, err := f.resolve(ctx, pathpkg.Dir(clean))
	return parent, base, err
}

func cleanPortablePath(name string) (string, error) {
	name = strings.ReplaceAll(name, `\`, "/")
	for _, component := range strings.Split(name, "/") {
		if component == ".." {
			return "", fs.ErrInvalid
		}
	}
	return pathpkg.Clean("/" + name), nil
}

func (f *portableFS) alloc(entry workspacefs.Entry, handle workspacefs.Handle, flags int) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.next
	f.next++
	if id == 0 || id == math.MaxUint64 {
		id = 1
		f.next = 2
	}
	f.handles[id] = &portableHandle{entry: entry, handle: handle, flags: flags}
	return id
}

func (f *portableFS) get(id uint64) (*portableHandle, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	handle, ok := f.handles[id]
	return handle, ok
}

func (f *portableFS) take(id uint64) (*portableHandle, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	handle, ok := f.handles[id]
	delete(f.handles, id)
	return handle, ok
}

func (f *portableFS) notify(name string, action uint32) {
	if f.host != nil {
		f.host.Notify(name, action)
	}
}

func (f *portableFS) Getattr(name string, stat *fuse.Stat_t, fh uint64) int {
	ctx := f.ctx
	f.tree.RLock()
	defer f.tree.RUnlock()
	var entry workspacefs.Entry
	var err error
	if handle, ok := f.get(fh); ok {
		entry = handle.entry
	} else {
		entry, err = f.resolve(ctx, name)
	}
	if err == nil {
		entry, err = f.backend.Stat(ctx, entry)
	}
	if err != nil {
		return f.fail("getattr", err)
	}
	portableStat(f.backend, entry, stat)
	return 0
}

func portableStat(backend *workspacefs.FS, entry workspacefs.Entry, stat *fuse.Stat_t) {
	stat.Uid, stat.Gid = portableOwner()
	stat.Mode = fuse.S_IFREG | 0444
	stat.Nlink = 1
	if backend.Writable(entry) {
		stat.Mode = fuse.S_IFREG | 0644
	}
	if entry.Directory {
		stat.Mode = fuse.S_IFDIR | 0555
		stat.Nlink = 2
		if backend.Writable(entry) {
			stat.Mode = fuse.S_IFDIR | 0755
		}
	}
	stat.Size = max(entry.Size, 0)
	stat.Blksize = 4096
	stat.Blocks = (stat.Size + 511) / 512
	if !entry.Modified.IsZero() {
		stamp := fuse.NewTimespec(entry.Modified)
		stat.Atim, stat.Mtim, stat.Ctim, stat.Birthtim = stamp, stamp, stamp, stamp
	}
}

func (f *portableFS) Access(name string, mask uint32) int {
	f.tree.RLock()
	defer f.tree.RUnlock()
	entry, err := f.resolve(f.ctx, name)
	if err != nil {
		return f.fail("access", err)
	}
	if mask&2 != 0 && !f.backend.Writable(entry) {
		return -fuse.EROFS
	}
	if mask&1 != 0 && !entry.Directory {
		return -fuse.EACCES
	}
	return 0
}

func (f *portableFS) Readdir(name string, fill func(string, *fuse.Stat_t, int64) bool, offset int64, _ uint64) int {
	f.tree.RLock()
	defer f.tree.RUnlock()
	entry, err := f.resolve(f.ctx, name)
	if err != nil {
		return f.fail("readdir lookup", err)
	}
	entries, err := f.backend.ReadDir(f.ctx, entry)
	if err != nil {
		return f.fail("readdir", err)
	}
	all := make([]workspacefs.Entry, 0, len(entries)+2)
	all = append(all, workspacefs.Entry{Name: ".", Directory: true}, workspacefs.Entry{Name: "..", Directory: true})
	all = append(all, entries...)
	for index := max(int(offset), 0); index < len(all); index++ {
		// Supplying no stat keeps plain readdir cheap while forcing an exact
		// Getattr for ls -l and other metadata-sensitive callers.
		if !fill(all[index].Name, nil, int64(index+1)) {
			break
		}
	}
	return 0
}

func (f *portableFS) OpenEx(name string, info *fuse.FileInfo_t) int {
	f.tree.RLock()
	defer f.tree.RUnlock()
	entry, err := f.resolve(f.ctx, name)
	if err != nil {
		return f.fail("open lookup", err)
	}
	handle, err := f.backend.Open(f.ctx, entry, portableFlags(info.Flags))
	if err != nil {
		return f.fail("open", err)
	}
	info.Fh = f.alloc(entry, handle, portableFlags(info.Flags))
	info.DirectIo = true
	return 0
}

func (f *portableFS) CreateEx(name string, _ uint32, info *fuse.FileInfo_t) int {
	f.tree.Lock()
	parent, base, err := f.parent(f.ctx, name)
	if err != nil {
		f.tree.Unlock()
		return f.fail("create parent", err)
	}
	entry, handle, err := f.backend.Create(f.ctx, parent, base, portableFlags(info.Flags))
	if err != nil {
		f.tree.Unlock()
		return f.fail("create", err)
	}
	info.Fh = f.alloc(entry, handle, portableFlags(info.Flags))
	info.DirectIo = true
	f.tree.Unlock()
	f.notify(name, fuse.NOTIFY_CREATE)
	return 0
}

func portableFlags(flags int) int {
	const allowed = fuse.O_WRONLY | fuse.O_RDWR | fuse.O_APPEND | fuse.O_CREAT | fuse.O_EXCL | fuse.O_TRUNC
	return flags & allowed
}

func (f *portableFS) Read(_ string, buffer []byte, offset int64, fh uint64) int {
	handle, ok := f.get(fh)
	if !ok {
		return -fuse.EBADF
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.closed {
		return -fuse.EBADF
	}
	n, err := handle.handle.ReadAt(f.ctx, buffer, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return f.fail("read", err)
	}
	return n
}

func (f *portableFS) Write(name string, buffer []byte, offset int64, fh uint64) int {
	handle, ok := f.get(fh)
	if !ok {
		return -fuse.EBADF
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.closed {
		return -fuse.EBADF
	}
	n, err := handle.handle.WriteAt(f.ctx, buffer, offset)
	if err != nil {
		return f.fail("write", err)
	}
	handle.flushed = false
	f.notify(name, fuse.NOTIFY_TRUNCATE)
	return n
}

func (f *portableFS) Truncate(name string, size int64, fh uint64) int {
	if size < 0 {
		return -fuse.EINVAL
	}
	if handle, ok := f.get(fh); ok {
		handle.mu.Lock()
		defer handle.mu.Unlock()
		if handle.closed {
			return -fuse.EBADF
		}
		if err := handle.handle.Truncate(f.ctx, size); err != nil {
			return f.fail("truncate", err)
		}
		handle.flushed = false
		f.notify(name, fuse.NOTIFY_TRUNCATE)
		return 0
	}
	f.tree.RLock()
	entry, err := f.resolve(f.ctx, name)
	if err == nil {
		err = f.backend.Truncate(f.ctx, entry, size)
	}
	f.tree.RUnlock()
	if err != nil {
		return f.fail("truncate", err)
	}
	f.notify(name, fuse.NOTIFY_TRUNCATE)
	return 0
}

func (f *portableFS) Flush(_ string, fh uint64) int {
	handle, ok := f.get(fh)
	if !ok {
		return -fuse.EBADF
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.closed {
		return 0
	}
	if err := handle.handle.Flush(f.ctx); err != nil {
		return f.fail("flush", err)
	}
	handle.flushed = true
	return 0
}

func (f *portableFS) Fsync(_ string, _ bool, fh uint64) int {
	return f.Flush("", fh)
}

func (f *portableFS) Release(_ string, fh uint64) int {
	handle, ok := f.take(fh)
	if !ok {
		return -fuse.EBADF
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.closed {
		return 0
	}
	handle.closed = true
	return f.fail("release", handle.handle.Close())
}

func (f *portableFS) Mkdir(name string, _ uint32) int {
	f.tree.Lock()
	parent, base, err := f.parent(f.ctx, name)
	if err == nil {
		_, err = f.backend.Mkdir(f.ctx, parent, base)
	}
	f.tree.Unlock()
	if err != nil {
		return f.fail("mkdir", err)
	}
	f.notify(name, fuse.NOTIFY_MKDIR)
	return 0
}

func (f *portableFS) remove(name string, directory bool) int {
	f.tree.Lock()
	entry, err := f.resolve(f.ctx, name)
	if err == nil {
		if runtime.GOOS == "windows" {
			f.closeMutationHandles(entry)
		}
		err = f.backend.Remove(f.ctx, entry, directory)
	}
	f.tree.Unlock()
	if err != nil {
		return f.fail("remove", err)
	}
	action := uint32(fuse.NOTIFY_UNLINK)
	if directory {
		action = fuse.NOTIFY_RMDIR
	}
	f.notify(name, action)
	return 0
}

func (f *portableFS) Unlink(name string) int { return f.remove(name, false) }
func (f *portableFS) Rmdir(name string) int  { return f.remove(name, true) }

func (f *portableFS) Rename(oldName, newName string) int {
	return f.Rename3(oldName, newName, 0)
}

func (f *portableFS) Rename3(oldName, newName string, flags uint32) int {
	if flags&^uint32(fuse.RENAME_NOREPLACE) != 0 {
		return -fuse.ENOTSUP
	}
	f.tree.Lock()
	source, err := f.resolve(f.ctx, oldName)
	if err != nil {
		f.tree.Unlock()
		return f.fail("rename lookup", err)
	}
	if runtime.GOOS == "windows" {
		f.closeMutationHandles(source)
	}
	parent, base, err := f.parent(f.ctx, newName)
	if err == nil {
		_, err = f.backend.Rename(f.ctx, source, parent, base, flags&fuse.RENAME_NOREPLACE != 0)
	}
	f.tree.Unlock()
	if err != nil {
		return f.fail("rename", err)
	}
	f.notify(oldName, fuse.NOTIFY_UNLINK)
	f.notify(newName, fuse.NOTIFY_CREATE)
	return 0
}

func (f *portableFS) closeMutationHandles(entry workspacefs.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, handle := range f.handles {
		if handle.entry.Key() != entry.Key() {
			continue
		}
		handle.mu.Lock()
		safe := !portableWriting(handle.flags) || handle.flushed
		if safe && !handle.closed {
			if err := handle.handle.Close(); err != nil {
				f.logger.Printf("release handle before namespace mutation: %v", err)
			} else {
				handle.closed = true
			}
		}
		handle.mu.Unlock()
	}
}

func portableWriting(flags int) bool {
	return flags&fuse.O_WRONLY != 0 || flags&fuse.O_RDWR != 0
}

func (f *portableFS) Chmod(name string, _ uint32) int { return f.updateMetadata(name) }
func (f *portableFS) Chown(name string, _, _ uint32) int {
	return f.updateMetadata(name)
}
func (f *portableFS) Utimens(name string, _ []fuse.Timespec) int {
	return f.unsupportedMutation(name)
}
func (f *portableFS) Setxattr(name, _ string, _ []byte, _ int) int {
	return f.unsupportedMutation(name)
}
func (f *portableFS) Removexattr(name, _ string) int { return f.unsupportedMutation(name) }

func (f *portableFS) updateMetadata(name string) int {
	f.tree.RLock()
	defer f.tree.RUnlock()
	entry, err := f.resolve(f.ctx, name)
	if err != nil {
		return f.fail("metadata update lookup", err)
	}
	if !f.backend.Writable(entry) {
		return -fuse.EROFS
	}
	return 0
}

func (f *portableFS) unsupportedMutation(name string) int {
	f.tree.RLock()
	defer f.tree.RUnlock()
	entry, err := f.resolve(f.ctx, name)
	if err != nil {
		return f.fail("unsupported operation lookup", err)
	}
	if !f.backend.Writable(entry) {
		return -fuse.EROFS
	}
	return -fuse.ENOTSUP
}

func portableErrno(err error) int {
	for _, mapping := range []struct {
		err  error
		code int
	}{
		{fserrors.ErrReadOnly, fuse.EROFS},
		{fserrors.ErrConflict, fuse.EBUSY},
		{fserrors.ErrBusy, fuse.EBUSY},
		{fserrors.ErrTooLarge, fuse.EFBIG},
		{fserrors.ErrCrossDevice, fuse.EXDEV},
		{fserrors.ErrNotEmpty, fuse.ENOTEMPTY},
		{fserrors.ErrUnsupported, fuse.ENOTSUP},
		{fserrors.ErrIsDir, fuse.EISDIR},
		{fserrors.ErrNotDir, fuse.ENOTDIR},
		{fserrors.ErrClosed, fuse.EBADF},
		{fs.ErrNotExist, fuse.ENOENT},
		{fs.ErrExist, fuse.EEXIST},
		{fs.ErrPermission, fuse.EACCES},
		{fs.ErrInvalid, fuse.EINVAL},
		{context.Canceled, fuse.EINTR},
		{context.DeadlineExceeded, fuse.ETIMEDOUT},
	} {
		if errors.Is(err, mapping.err) {
			return mapping.code
		}
	}
	var response *transport.HTTPError
	if errors.As(err, &response) {
		switch response.StatusCode {
		case 400, 416:
			return fuse.EINVAL
		case 401, 403:
			return fuse.EACCES
		case 404:
			return fuse.ENOENT
		case 408, 504:
			return fuse.ETIMEDOUT
		case 409:
			if strings.EqualFold(response.Code, "DirectoryNotEmpty") {
				return fuse.ENOTEMPTY
			}
			if strings.Contains(strings.ToLower(response.Code), "alreadyexists") {
				return fuse.EEXIST
			}
			return fuse.EBUSY
		case 412:
			return fuse.EBUSY
		case 429:
			return fuse.EAGAIN
		case 507:
			return fuse.ENOSPC
		}
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return fuse.ETIMEDOUT
	}
	return fuse.EIO
}

var (
	_ fuse.FileSystemInterface = (*portableFS)(nil)
	_ fuse.FileSystemOpenEx    = (*portableFS)(nil)
	_ fuse.FileSystemRename3   = (*portableFS)(nil)
)
