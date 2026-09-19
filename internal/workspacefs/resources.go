package workspacefs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/writeback"
)

func resourceOwner(e Entry) resources.Target {
	return resources.Target{WorkspaceID: e.Workspace, ItemID: e.Item.ID, Kind: e.Item.Type}
}

func (s *FS) resourceRoots(ctx context.Context, parent Entry) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if parent.Kind != Notebook && parent.Kind != Environment {
		return nil, nil
	}
	owner := resourceOwner(parent)
	if err := resources.ValidateTarget(owner); err != nil {
		return nil, err
	}
	name := "builtin"
	if parent.Kind == Environment {
		name = "resources"
	}
	return []Entry{{Name: name, Kind: ResourceDirectory, Directory: true, Fixed: true,
		Workspace: owner.WorkspaceID, Item: parent.Item, Resource: resources.Path{Target: owner}, Modified: s.start}}, nil
}

func (s *FS) lookupResourceRoot(ctx context.Context, parent Entry, name string) (Entry, error) {
	roots, err := s.resourceRoots(ctx, parent)
	if err != nil {
		return Entry{}, err
	}
	for _, root := range roots {
		if root.Name == name {
			return root, nil
		}
	}
	return Entry{}, fs.ErrNotExist
}

func resourceEntry(parent Entry, name string, path resources.Path, info resources.Info) Entry {
	kind := ResourceFile
	if info.IsDir {
		kind = ResourceDirectory
	}
	return Entry{Name: name, Kind: kind, Directory: info.IsDir, Workspace: path.Target.WorkspaceID, Resource: path, Size: info.Size, Modified: info.Modified, ValidUntil: info.ValidUntil}
}

func (s *FS) resourceChildren(ctx context.Context, parent Entry) ([]Entry, error) {
	if s.resources == nil {
		return nil, fserrors.ErrUnsupported
	}
	items, err := s.resources.List(ctx, parent.Resource)
	if err != nil {
		return nil, err
	}
	var entries []Entry
	for _, item := range items {
		leaf := item.Path
		if parent.Resource.Relative != "" {
			prefix := parent.Resource.Relative + "/"
			if !strings.HasPrefix(leaf, prefix) {
				return nil, fs.ErrInvalid
			}
			leaf = strings.TrimPrefix(leaf, prefix)
		}
		name, err := namespace.FileName(leaf)
		if err != nil {
			return nil, err
		}
		path := parent.Resource
		path.Relative = item.Path
		if err := resources.ValidatePath(path, false); err != nil {
			return nil, err
		}
		entries = append(entries, resourceEntry(parent, name, path, item))
	}
	return entries, nil
}

func (s *FS) newResourceChild(parent Entry, name string, write bool) (resources.Path, error) {
	if s.resources == nil {
		return resources.Path{}, fserrors.ErrUnsupported
	}
	if parent.Kind != ResourceDirectory {
		return resources.Path{}, fserrors.ErrNotDir
	}
	if write && !s.Writable(parent) {
		return resources.Path{}, fserrors.ErrReadOnly
	}
	raw, err := namespace.ParseFileName(name)
	if err != nil {
		return resources.Path{}, err
	}
	if raw == identityFileName {
		return resources.Path{}, fserrors.ErrReadOnly
	}
	path := parent.Resource
	if path.Relative != "" {
		path.Relative += "/"
	}
	path.Relative += raw
	if err := resources.ValidatePath(path, write); err != nil {
		return resources.Path{}, err
	}
	return path, nil
}

func (s *FS) openResource(ctx context.Context, e Entry, flags int, create bool) (Entry, Handle, error) {
	if s.resources == nil {
		return Entry{}, nil, fserrors.ErrUnsupported
	}
	write := writing(flags) || create
	if write && !s.Writable(e) {
		return Entry{}, nil, fserrors.ErrReadOnly
	}
	if err := resources.ValidatePath(e.Resource, write); err != nil {
		return Entry{}, nil, err
	}
	release, err := s.begin(ctx, e, write)
	if err != nil {
		return Entry{}, nil, err
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	info, err := s.resources.Stat(ctx, e.Resource)
	newFile := create && IsNotExist(err)
	if err != nil && !newFile {
		return Entry{}, nil, err
	}
	if newFile {
		info = resources.Info{}
	}
	if create && !newFile && flags&os.O_EXCL != 0 {
		return Entry{}, nil, fs.ErrExist
	}
	if info.IsDir {
		return Entry{}, nil, fserrors.ErrIsDir
	}
	if info.Size < 0 {
		return Entry{}, nil, fs.ErrInvalid
	}
	if info.Size > s.opts.MaxResourceSize {
		return Entry{}, nil, fserrors.ErrTooLarge
	}
	charge := info.Size
	if write {
		charge += 3 * s.opts.MaxResourceSize
	}
	unpin, err := s.reserveSnapshotBytes(charge)
	if err != nil {
		return Entry{}, nil, err
	}
	leaseRelease := release
	release = func() { unpin(); leaseRelease() }
	var snapshot *resources.Snapshot
	if !newFile {
		snapshot, err = s.resources.Snapshot(ctx, e.Resource, "")
		if err != nil {
			return Entry{}, nil, err
		}
		if snapshot.Size() != info.Size || snapshot.Version() == "" {
			_ = snapshot.Close()
			return Entry{}, nil, fmt.Errorf("resource content snapshot does not match its metadata: %w", fserrors.ErrConflict)
		}
	}
	if !write {
		success = true
		return e, &resourceReadHandle{snapshot: snapshot, release: release}, nil
	}
	version := ""
	if snapshot != nil {
		version = snapshot.Version()
		defer snapshot.Close()
	}
	file, err := writeback.Open(ctx, writeback.Options{
		Directory: s.opts.SpoolDirectory, MaxSize: s.opts.MaxResourceSize, InitialSize: info.Size,
		Truncate: flags&os.O_TRUNC != 0, Append: flags&os.O_APPEND != 0,
		Load: func(ctx context.Context, dest io.Writer) error {
			buffer := make([]byte, 64<<10)
			for offset := int64(0); offset < info.Size; {
				if err := ctx.Err(); err != nil {
					return err
				}
				chunk := buffer[:min(int64(len(buffer)), info.Size-offset)]
				n, err := snapshot.ReadAt(chunk, offset)
				if err != nil && !(errors.Is(err, io.EOF) && offset+int64(n) == info.Size) {
					return err
				}
				if n == 0 {
					return io.ErrUnexpectedEOF
				}
				if _, err := dest.Write(chunk[:n]); err != nil {
					return err
				}
				offset += int64(n)
			}
			return nil
		},
		Commit: func(ctx context.Context, reader io.ReaderAt, size int64) error {
			updated, err := s.resources.Put(ctx, e.Resource, reader, size, version)
			if err != nil {
				return err
			}
			if updated.IsDir || updated.Size != size || updated.Version == "" {
				return fserrors.ErrConflict
			}
			version = updated.Version
			return nil
		},
		OnClose: s.closeSpool(e, release),
	})
	if err != nil {
		return Entry{}, nil, err
	}
	if newFile {
		info, err = s.resources.Put(ctx, e.Resource, bytes.NewReader(nil), 0, "")
		if err == nil && (info.IsDir || info.Size != 0 || info.Version == "") {
			err = fserrors.ErrConflict
		}
		if err != nil {
			return Entry{}, nil, errors.Join(err, file.Close())
		}
		version = info.Version
	}
	e.Size, e.Modified = file.Size(), info.Modified
	s.registerSpool(e, file)
	success = true
	return e, &writeHandle{file: file, writable: writing(flags)}, nil
}

func (s *FS) createResource(ctx context.Context, parent Entry, name string, flags int) (Entry, Handle, error) {
	path, err := s.newResourceChild(parent, name, true)
	if err != nil {
		return Entry{}, nil, err
	}
	entry := resourceEntry(parent, name, path, resources.Info{})
	return s.openResource(ctx, entry, flags, true)
}

func (s *FS) mkdirResource(ctx context.Context, parent Entry, name string) (Entry, error) {
	path, err := s.newResourceChild(parent, name, true)
	if err != nil {
		return Entry{}, err
	}
	e := resourceEntry(parent, name, path, resources.Info{IsDir: true})
	done, err := s.mutation(ctx, e.Key())
	if err != nil {
		return Entry{}, err
	}
	defer done()
	if err := s.resources.Mkdir(ctx, path); err != nil {
		return Entry{}, err
	}
	return s.Stat(ctx, e)
}

func (s *FS) removeResource(ctx context.Context, e Entry, directory bool) error {
	if !s.Writable(e) {
		return fserrors.ErrReadOnly
	}
	if err := resources.ValidatePath(e.Resource, true); err != nil {
		return err
	}
	done, err := s.mutation(ctx, e.Key())
	if err != nil {
		return err
	}
	defer done()
	info, err := s.resources.Stat(ctx, e.Resource)
	if err != nil {
		return err
	}
	if info.IsDir != directory {
		if info.IsDir {
			return fserrors.ErrIsDir
		}
		return fserrors.ErrNotDir
	}
	version, err := s.resourceContentVersion(ctx, e.Resource, info)
	if err != nil {
		return err
	}
	return s.resources.Remove(ctx, e.Resource, directory, version)
}

func (s *FS) renameResource(ctx context.Context, source, parent Entry, name string, noReplace bool) (Entry, error) {
	if !s.Writable(source) {
		return Entry{}, fserrors.ErrReadOnly
	}
	if err := resources.ValidatePath(source.Resource, true); err != nil {
		return Entry{}, err
	}
	path, err := s.newResourceChild(parent, name, true)
	if err != nil {
		return Entry{}, err
	}
	if source.Resource.Target != path.Target {
		return Entry{}, fserrors.ErrCrossDevice
	}
	target := resourceEntry(parent, name, path, resources.Info{IsDir: source.Directory})
	done, err := s.mutation(ctx, source.Key(), target.Key())
	if err != nil {
		return Entry{}, err
	}
	defer done()
	src, err := s.resources.Stat(ctx, source.Resource)
	if err != nil {
		return Entry{}, err
	}
	sourceVersion, err := s.resourceContentVersion(ctx, source.Resource, src)
	if err != nil {
		return Entry{}, err
	}
	dst, err := s.resources.Stat(ctx, path)
	if err != nil && !IsNotExist(err) {
		return Entry{}, err
	}
	if err == nil && noReplace {
		return Entry{}, fs.ErrExist
	}
	if IsNotExist(err) {
		dst = resources.Info{}
	}
	destinationVersion := ""
	if err == nil {
		destinationVersion, err = s.resourceContentVersion(ctx, path, dst)
		if err != nil {
			return Entry{}, err
		}
	}
	info, err := s.resources.Rename(ctx, source.Resource, path, sourceVersion, destinationVersion, noReplace)
	if err != nil {
		return Entry{}, err
	}
	return resourceEntry(parent, name, path, info), nil
}

func (s *FS) resourceContentVersion(ctx context.Context, path resources.Path, info resources.Info) (string, error) {
	if info.IsDir {
		return "", nil
	}
	if info.Size < 0 {
		return "", fs.ErrInvalid
	}
	if info.Size > s.opts.MaxResourceSize {
		return "", fserrors.ErrTooLarge
	}
	snapshot, err := s.resources.Snapshot(ctx, path, "")
	if err != nil {
		return "", err
	}
	defer snapshot.Close()
	if snapshot.Size() != info.Size || snapshot.Version() == "" {
		return "", fserrors.ErrConflict
	}
	return snapshot.Version(), nil
}

type resourceReadHandle struct {
	mu       sync.Mutex
	snapshot *resources.Snapshot
	closed   bool
	release  func()
}

func (h *resourceReadHandle) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, fserrors.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, fs.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	return h.snapshot.ReadAt(p, off)
}
func (h *resourceReadHandle) WriteAt(context.Context, []byte, int64) (int, error) {
	return 0, fserrors.ErrReadOnly
}
func (h *resourceReadHandle) Truncate(context.Context, int64) error { return fserrors.ErrReadOnly }
func (h *resourceReadHandle) Flush(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fserrors.ErrClosed
	}
	return ctx.Err()
}
func (h *resourceReadHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		err := h.snapshot.Close()
		h.release()
		h.release = nil
		return err
	}
	return nil
}
func (h *resourceReadHandle) Size() int64 { return h.snapshot.Size() }
