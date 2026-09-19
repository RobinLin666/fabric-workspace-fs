//go:build linux

package fusefs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/workspacefs"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func Supported() bool { return true }

func Mount(mountpoint string, backend *workspacefs.FS, opts Options) (Server, error) {
	info, err := os.Lstat(mountpoint)
	if err != nil {
		return nil, fmt.Errorf("mountpoint: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("mountpoint must be an existing directory, not a symlink")
	}
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		return nil, fmt.Errorf("read mountpoint: %w", err)
	}
	if len(entries) != 0 {
		return nil, fmt.Errorf("mountpoint must be empty")
	}
	if backend == nil {
		return nil, fmt.Errorf("filesystem backend is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "fabric-workspace-fs: ", log.LstdFlags)
	}
	adapter := &adapter{backend: backend, logger: logger, notifications: newInvalidator(logger, inodeNotifier{})}
	root := &node{adapter: adapter, entry: backend.Root()}
	zero := time.Duration(0)
	options := []string{"default_permissions"}
	if opts.ReadOnly {
		options = append(options, "ro")
	}
	fsOptions := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "fabric-workspace-fs", Name: "fabric-workspace-fs",
			Options: options, MaxWrite: 1 << 20, ExtraCapabilities: fuse.CAP_ATOMIC_O_TRUNC,
			// Plain readdir lists fixed roots without fetching file content.
			// LOOKUP/GETATTR still resolve exact attributes on demand.
			DisableReadDirPlus: true, Logger: logger,
		},
		// go-fuse treats an explicit per-node zero as unspecified. Keep the
		// global fallbacks zero so active writers and disabled policies stay
		// uncached; all nonzero timeouts come from fresh backend entries.
		AttrTimeout: &zero, EntryTimeout: &zero, NegativeTimeout: &zero,
		NullPermissions: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()),
		Logger: logger,
	}
	raw := &cacheRawFS{RawFileSystem: fs.NewNodeFS(root, fsOptions)}
	server, err := fuse.NewServer(raw, mountpoint, &fsOptions.MountOptions)
	if err != nil {
		adapter.notifications.close()
		return nil, fmt.Errorf("mount setup failed at %q; confirm it is unmounted before removing the directory: %w", mountpoint, err)
	}
	mounted := &mountedServer{server: server, notifications: adapter.notifications, done: make(chan struct{}), timeout: 10 * time.Second}
	go func() {
		server.Serve()
		mounted.notifications.close()
		close(mounted.done)
	}()
	if err := server.WaitMount(); err != nil {
		return failedMount(mounted, mountpoint, err)
	}
	if !opts.ReadOnly && server.KernelSettings().Flags64()&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		err := errors.New("writable mounts require kernel FUSE atomic O_TRUNC support")
		return failedMount(mounted, mountpoint, err)
	}
	if code := notificationSupport(server.KernelSettings()); code != 0 {
		return failedMount(mounted, mountpoint, fmt.Errorf("kernel FUSE cache invalidation is required: %w", code))
	}

	return mounted, nil
}

type adapter struct {
	backend       *workspacefs.FS
	logger        *log.Logger
	tree          sync.RWMutex
	notifications *invalidator
}

func (a *adapter) failure(operation string, err error) syscall.Errno {
	if err != nil {
		code := errno(err)
		if code != syscall.ENOENT {
			a.logger.Printf("%s: %v", operation, err)
		}
		return code
	}
	return 0
}

type node struct {
	fs.Inode
	adapter *adapter
	mu      sync.RWMutex
	entry   workspacefs.Entry
}

func (n *node) snapshot() workspacefs.Entry {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.entry
}

func (n *node) update(e workspacefs.Entry) {
	n.mu.Lock()
	n.entry = e
	n.mu.Unlock()
}

func (n *node) selfInvalidation() []invalidationTarget {
	targets := []invalidationTarget{{inode: &n.Inode}}
	name, parent := n.Parent()
	if parent != nil {
		targets = append(targets, invalidationTarget{inode: parent}, invalidationTarget{inode: parent, name: name})
	}
	return targets
}

func (n *node) childInvalidation(name string) []invalidationTarget {
	targets := []invalidationTarget{{inode: &n.Inode}, {inode: &n.Inode, name: name}}
	if child := n.GetChild(name); child != nil {
		targets = append(targets, invalidationTarget{inode: child})
	}
	return targets
}

func (n *node) attr(e workspacefs.Entry, out *fuse.Attr) {
	out.Mode = syscall.S_IFREG | 0444
	out.Nlink = 1
	if n.adapter.backend.Writable(e) {
		out.Mode = syscall.S_IFREG | 0644
	}
	if e.Directory {
		out.Mode = syscall.S_IFDIR | 0555
		if n.adapter.backend.Writable(e) {
			out.Mode = syscall.S_IFDIR | 0755
		}
		out.Nlink = 2
	}
	out.Size = uint64(max(e.Size, 0))
	out.Owner = fuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}
	if !e.Modified.IsZero() {
		out.SetTimes(&e.Modified, &e.Modified, &e.Modified)
	}
	out.Blksize = 4096
	out.Blocks = (out.Size + 511) / 512
}

func (n *node) inode(ctx context.Context, e workspacefs.Entry, out *fuse.EntryOut) *fs.Inode {
	if existing := n.GetChild(e.Name); existing != nil {
		if child, ok := existing.Operations().(*node); ok {
			old := child.snapshot()
			if old.Key() == e.Key() && old.Kind == e.Kind {
				child.update(e)
				child.entryOut(e, out)
				return existing
			}
		}
	}
	mode := uint32(syscall.S_IFREG)
	if e.Directory {
		mode = syscall.S_IFDIR
	}
	child := &node{adapter: n.adapter, entry: e}
	child.entryOut(e, out)
	return n.NewInode(ctx, child, fs.StableAttr{Mode: mode})
}

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.adapter.tree.RLock()
	defer n.adapter.tree.RUnlock()
	e, err := n.adapter.backend.Lookup(ctx, n.snapshot(), name)
	if err != nil {
		if errno(err) == syscall.ENOENT {
			// Negative policies are directory scoped. Stat refreshes the
			// source deadline rather than extending an old observation.
			parent, statErr := n.adapter.backend.Stat(ctx, n.snapshot())
			if statErr != nil {
				return nil, n.adapter.failure("negative lookup stat", statErr)
			}
			n.update(parent)
			n.negativeOut(parent, err, out)
		}
		return nil, n.adapter.failure("lookup", err)
	}
	if !e.Directory {
		// A remote directory/metadata cache can still describe the last
		// committed size while an open writer owns a different local size.
		e, err = n.adapter.backend.Stat(ctx, e)
		if err != nil {
			return nil, n.adapter.failure("lookup stat", err)
		}
	}
	return n.inode(ctx, e, out), 0
}

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	n.adapter.tree.RLock()
	defer n.adapter.tree.RUnlock()
	entries, err := n.adapter.backend.ReadDir(ctx, n.snapshot())
	if err != nil {
		return nil, n.adapter.failure("readdir", err)
	}
	result := make([]fuse.DirEntry, 0, len(entries)+2)
	result = append(result, fuse.DirEntry{Name: ".", Mode: syscall.S_IFDIR}, fuse.DirEntry{Name: "..", Mode: syscall.S_IFDIR})
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.Directory {
			mode = syscall.S_IFDIR
		}
		result = append(result, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	// Directory streams have no kernel-cache flag. Backend directory caches
	// are separate from the attribute, positive-entry and negative-entry TTLs.
	return fs.NewListDirStream(result), 0
}

func (n *node) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Linux GETATTR may omit Fh; go-fuse then substitutes an arbitrary open
	// handle. Its pinned reader size must not overwrite inode-wide attributes
	// or a writer's fstat size. Stat gives the live spool priority, and
	// CacheTimeouts keeps that size uncached until the writer is released.
	n.adapter.tree.RLock()
	defer n.adapter.tree.RUnlock()
	e, err := n.adapter.backend.Stat(ctx, n.snapshot())
	if err != nil {
		return n.adapter.failure("getattr", err)
	}
	n.update(e)
	n.attrOut(e, out)
	return 0
}

func (n *node) Access(_ context.Context, mask uint32) syscall.Errno {
	e := n.snapshot()
	if mask&2 != 0 && !n.adapter.backend.Writable(e) {
		return syscall.EROFS
	}
	if mask&1 != 0 && !e.Directory {
		return syscall.EACCES
	}
	return 0
}

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	n.adapter.tree.RLock()
	var change *invalidation
	retire := true
	defer func() {
		n.adapter.tree.RUnlock()
		change.changed(retire)
	}()
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC) != 0 {
		var code syscall.Errno
		change, code = n.adapter.notifications.reserve(n.selfInvalidation())
		if code != 0 {
			return nil, 0, code
		}
	}
	handle, err := n.adapter.backend.Open(ctx, n.snapshot(), portableOpenFlags(flags))
	if err != nil {
		return nil, 0, n.adapter.failure("open", err)
	}
	retire = false
	return &fileHandle{handle: handle, node: n, change: change}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *node) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	n.adapter.tree.RLock()
	var change *invalidation
	retire := true
	defer func() {
		n.adapter.tree.RUnlock()
		change.changed(retire)
	}()
	e := n.snapshot()
	if !n.adapter.backend.Writable(e) {
		return syscall.EROFS
	}
	allowed := uint32(fuse.FATTR_SIZE | fuse.FATTR_FH | fuse.FATTR_LOCKOWNER | fuse.FATTR_KILL_SUIDGID)
	if in.Valid & ^allowed != 0 {
		return syscall.ENOTSUP
	}
	if size, ok := in.GetSize(); ok {
		if size > math.MaxInt64 {
			return syscall.EFBIG
		}
		var err error
		handle, hasHandle := fh.(*fileHandle)
		if hasHandle && handle.change != nil {
			change, retire = handle.change, false
			if code := change.check(); code != 0 {
				return code
			}
		} else {
			var code syscall.Errno
			change, code = n.adapter.notifications.reserve(n.selfInvalidation())
			if code != 0 {
				return code
			}
		}
		if hasHandle {
			err = handle.handle.Truncate(ctx, int64(size))
		} else {
			err = n.adapter.backend.Truncate(ctx, e, int64(size))
		}
		if err != nil {
			return n.adapter.failure("truncate", err)
		}
	}
	e, err := n.adapter.backend.Stat(ctx, e)
	if err != nil {
		return n.adapter.failure("getattr after truncate", err)
	}
	n.update(e)
	n.attrOut(e, out)
	return 0
}

func (n *node) Create(ctx context.Context, name string, flags, _ uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	n.adapter.tree.Lock()
	var change *invalidation
	retire := true
	defer func() {
		n.adapter.tree.Unlock()
		change.changed(retire)
	}()
	var code syscall.Errno
	change, code = n.adapter.notifications.reserve(n.childInvalidation(name))
	if code != 0 {
		return nil, nil, 0, code
	}
	e, handle, err := n.adapter.backend.Create(ctx, n.snapshot(), name, portableOpenFlags(flags))
	if err != nil {
		return nil, nil, 0, n.adapter.failure("create", err)
	}
	child := n.inode(ctx, e, out)
	change.add(child)
	retire = false
	return child, &fileHandle{handle: handle, node: child.Operations().(*node), change: change}, fuse.FOPEN_DIRECT_IO, 0
}

func portableOpenFlags(flags uint32) int {
	// FUSE carries the kernel's O_LARGEFILE bit even where libc's 64-bit
	// O_LARGEFILE constant is zero. These flags do not change file contents;
	// Go already uses 64-bit offsets and close-on-exec local descriptors.
	const kernelLargeFile = 0x8000
	return int(flags) &^ (kernelLargeFile | syscall.O_CLOEXEC | syscall.O_NOCTTY)
}

func (n *node) Mkdir(ctx context.Context, name string, _ uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.adapter.tree.Lock()
	var change *invalidation
	defer func() {
		n.adapter.tree.Unlock()
		change.changed(true)
	}()
	var code syscall.Errno
	change, code = n.adapter.notifications.reserve(n.childInvalidation(name))
	if code != 0 {
		return nil, code
	}
	e, err := n.adapter.backend.Mkdir(ctx, n.snapshot(), name)
	if err != nil {
		return nil, n.adapter.failure("mkdir", err)
	}
	child := n.inode(ctx, e, out)
	change.add(child)
	return child, 0
}

func (n *node) remove(ctx context.Context, name string, directory bool) syscall.Errno {
	n.adapter.tree.Lock()
	var change *invalidation
	defer func() {
		n.adapter.tree.Unlock()
		change.changed(true)
	}()
	parent := n.snapshot()
	if !n.adapter.backend.Writable(parent) {
		return syscall.EROFS
	}
	var code syscall.Errno
	change, code = n.adapter.notifications.reserve(n.childInvalidation(name))
	if code != 0 {
		return code
	}
	e, err := n.adapter.backend.Lookup(ctx, parent, name)
	if err == nil {
		err = n.adapter.backend.Remove(ctx, e, directory)
	}
	return n.adapter.failure("remove", err)
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.remove(ctx, name, false)
}

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.remove(ctx, name, true)
}

func (n *node) Rename(ctx context.Context, name string, parent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	n.adapter.tree.Lock()
	var change *invalidation
	defer func() {
		n.adapter.tree.Unlock()
		change.changed(true)
	}()
	target, ok := parent.(*node)
	if !ok || target.adapter != n.adapter {
		return syscall.EXDEV
	}
	if flags & ^uint32(1) != 0 {
		return syscall.ENOTSUP
	}
	sourceParent := n.snapshot()
	if !n.adapter.backend.Writable(sourceParent) {
		return syscall.EROFS
	}
	var code syscall.Errno
	change, code = n.adapter.notifications.reserve(append(n.childInvalidation(name), target.childInvalidation(newName)...))
	if code != 0 {
		return code
	}
	source, err := n.adapter.backend.Lookup(ctx, sourceParent, name)
	if err != nil {
		return n.adapter.failure("rename lookup", err)
	}
	updated, err := n.adapter.backend.Rename(ctx, source, target.snapshot(), newName, flags&1 != 0)
	if err != nil {
		return n.adapter.failure("rename", err)
	}
	if inode := n.GetChild(name); inode != nil {
		if child, ok := inode.Operations().(*node); ok {
			child.relocate(source, updated)
		}
	}
	return 0
}

func (n *node) relocate(source, root workspacefs.Entry) {
	e := n.snapshot()
	if source.Kind == workspacefs.ResourceDirectory || source.Kind == workspacefs.ResourceFile {
		if e.Resource.Relative == source.Resource.Relative {
			e = root
		} else if strings.HasPrefix(e.Resource.Relative, source.Resource.Relative+"/") {
			e.Resource.Relative = root.Resource.Relative + strings.TrimPrefix(e.Resource.Relative, source.Resource.Relative)
		}
	} else if source.Kind == workspacefs.OverlayDirectory || source.Kind == workspacefs.OverlayFile {
		if e.Local.Relative == source.Local.Relative {
			e = root
		} else if strings.HasPrefix(e.Local.Relative, source.Local.Relative+"/") {
			e.Local.Relative = root.Local.Relative + strings.TrimPrefix(e.Local.Relative, source.Local.Relative)
		}
	} else {
		if e.Remote == source.Remote {
			e = root
		} else if strings.HasPrefix(e.Remote, source.Remote+"/") {
			e.Remote = root.Remote + strings.TrimPrefix(e.Remote, source.Remote)
		}
	}
	n.update(e)
	for _, inode := range n.Children() {
		if child, ok := inode.Operations().(*node); ok {
			child.relocate(source, root)
		}
	}
}

func (n *node) Getxattr(context.Context, string, []byte) (uint32, syscall.Errno) {
	return 0, syscall.ENOTSUP
}
func (n *node) Listxattr(context.Context, []byte) (uint32, syscall.Errno) {
	return 0, syscall.ENOTSUP
}
func (n *node) Setxattr(context.Context, string, []byte, uint32) syscall.Errno {
	if !n.adapter.backend.Writable(n.snapshot()) {
		return syscall.EROFS
	}
	return syscall.ENOTSUP
}
func (n *node) Removexattr(context.Context, string) syscall.Errno {
	if !n.adapter.backend.Writable(n.snapshot()) {
		return syscall.EROFS
	}
	return syscall.ENOTSUP
}
func (n *node) Mknod(context.Context, string, uint32, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}
func (n *node) Link(context.Context, fs.InodeEmbedder, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}
func (n *node) Symlink(context.Context, string, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}
func (n *node) Statfs(context.Context, *fuse.StatfsOut) syscall.Errno {
	return syscall.ENOTSUP
}

type fileHandle struct {
	handle workspacefs.Handle
	node   *node
	change *invalidation
}

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.handle.ReadAt(ctx, dest, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, h.node.adapter.failure("read", err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}
func (h *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	defer h.change.changed(false)
	if code := h.change.check(); code != 0 {
		return 0, code
	}
	n, err := h.handle.WriteAt(ctx, data, off)
	return uint32(n), h.node.adapter.failure("write", err)
}
func (h *fileHandle) Flush(ctx context.Context) syscall.Errno {
	defer h.change.changed(false)
	if code := h.change.check(); code != 0 {
		return code
	}
	// The kernel can interrupt FLUSH after it has accepted prior writes.
	// Persisting the spool remains necessary for close to be durable.
	return h.node.adapter.failure("flush", h.handle.Flush(context.WithoutCancel(ctx)))
}
func (h *fileHandle) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	defer h.change.changed(false)
	if code := h.change.check(); code != 0 {
		return code
	}
	return h.node.adapter.failure("fsync", h.handle.Flush(context.WithoutCancel(ctx)))
}
func (h *fileHandle) Release(context.Context) syscall.Errno {
	defer h.change.changed(true)
	// RELEASE alone closes the handle/lease. FLUSH may be called repeatedly
	// for dup'd descriptors while the writer remains open.
	if code := h.node.adapter.failure("release", h.handle.Close()); code != 0 {
		return code
	}
	return h.change.check()
}
func (h *fileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	// Reader contents stay pinned, but inode attributes must reflect a live
	// writable spool rather than whichever reader go-fuse chose for fstat.
	return h.node.Getattr(ctx, h, out)
}
func (h *fileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return h.node.Setattr(ctx, h, in, out)
}
func (h *fileHandle) Allocate(context.Context, uint64, uint64, uint32) syscall.Errno {
	return errno(fserrors.ErrUnsupported)
}

var (
	_ fs.NodeLookuper  = (*node)(nil)
	_ fs.NodeReaddirer = (*node)(nil)
	_ fs.NodeGetattrer = (*node)(nil)
	_ fs.NodeSetattrer = (*node)(nil)
	_ fs.NodeOpener    = (*node)(nil)
	_ fs.NodeCreater   = (*node)(nil)
	_ fs.NodeMkdirer   = (*node)(nil)
	_ fs.NodeUnlinker  = (*node)(nil)
	_ fs.NodeRmdirer   = (*node)(nil)
	_ fs.NodeRenamer   = (*node)(nil)
	_ fs.FileReader    = (*fileHandle)(nil)
	_ fs.FileWriter    = (*fileHandle)(nil)
	_ fs.FileFlusher   = (*fileHandle)(nil)
	_ fs.FileFsyncer   = (*fileHandle)(nil)
	_ fs.FileReleaser  = (*fileHandle)(nil)
	_ fs.FileGetattrer = (*fileHandle)(nil)
	_ fs.FileSetattrer = (*fileHandle)(nil)
	_ fs.FileAllocater = (*fileHandle)(nil)
)
