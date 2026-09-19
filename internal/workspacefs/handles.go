package workspacefs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/writeback"
)

func writing(flags int) bool { return flags&(os.O_WRONLY|os.O_RDWR) != 0 }

func (s *FS) Open(ctx context.Context, e Entry, flags int) (Handle, error) {
	if e.Directory {
		return nil, fserrors.ErrIsDir
	}
	if flags&os.O_TRUNC != 0 && !writing(flags) {
		return nil, fmt.Errorf("truncate requires a writable handle: %w", fs.ErrPermission)
	}
	if e.Kind == OverlayFile {
		return nil, fserrors.ErrUnsupported
	}
	if e.Kind == ResourceFile {
		_, handle, err := s.openResource(ctx, e, flags, false)
		return handle, err
	}
	if writing(flags) {
		if err := s.writableFile(e); err != nil {
			return nil, err
		}
		if e.Kind == LakeFile {
			_, handle, err := s.openLake(ctx, e, flags, false)
			return handle, err
		}
		return s.openNotebook(ctx, e, flags)
	}
	release, err := s.begin(ctx, e, false)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	switch e.Kind {
	case AgentFile:
		data, exists := s.agentFiles[e.Part]
		if !exists {
			return nil, fs.ErrNotExist
		}
		success = true
		return &bytesHandle{data: data, release: release}, nil
	case IdentityFile:
		current, err := s.identityEntry(ctx, e)
		if err != nil {
			return nil, err
		}
		data, err := identityData(current)
		if err != nil {
			return nil, err
		}
		success = true
		return &bytesHandle{data: data, release: release}, nil
	case LakeFile:
		info, err := s.lake.Stat(ctx, e.LakePath())
		if err != nil {
			return nil, err
		}
		if info.IsDir {
			return nil, fserrors.ErrIsDir
		}
		if info.ETag == "" {
			return nil, fmt.Errorf("OneLake did not provide an ETag: %w", fserrors.ErrUnsupported)
		}
		success = true
		return &lakeReadHandle{lake: s.lake, path: e.LakePath(), size: info.Size, etag: info.ETag, release: release}, nil
	case NotebookContent, DefinitionFile:
		if hiddenPlatform(e.Part) {
			return nil, fs.ErrNotExist
		}
		snapshot, err := s.sourceSnapshot(ctx, e, s.policy(e).Content, false)
		if err != nil {
			return nil, err
		}
		partPath := e.Part
		if e.Kind == NotebookContent {
			partPath = snapshot.notebook
		}
		data, exists := snapshot.parts[partPath]
		if !exists {
			return nil, fs.ErrNotExist
		}
		unpin, err := s.pinSnapshot(snapshot, false)
		if err != nil {
			return nil, err
		}
		leaseRelease := release
		release = func() { unpin(); leaseRelease() }
		success = true
		return &bytesHandle{data: data, release: release}, nil
	default:
		return nil, fs.ErrInvalid
	}
}

func (s *FS) openNotebook(ctx context.Context, e Entry, flags int) (Handle, error) {
	release, err := s.begin(ctx, e, true)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	snapshot, err := s.sourceSnapshot(ctx, e, s.policy(e).Content, false)
	if err != nil {
		return nil, err
	}
	if e.Part != "" && snapshot.notebook != e.Part {
		return nil, fserrors.ErrConflict
	}
	data := snapshot.parts[snapshot.notebook]
	unpin, err := s.pinSnapshot(snapshot, true)
	if err != nil {
		return nil, err
	}
	leaseRelease := release
	release = func() { unpin(); leaseRelease() }
	file, err := writeback.Open(ctx, writeback.Options{
		Directory: s.opts.SpoolDirectory, MaxSize: s.opts.MaxNotebookSize,
		InitialSize: int64(len(data)), Truncate: flags&os.O_TRUNC != 0, Append: flags&os.O_APPEND != 0,
		Load:   func(_ context.Context, w io.Writer) error { _, err := w.Write(data); return err },
		Commit: s.notebookCommit(e, snapshot, snapshot.notebook), OnClose: s.closeSpool(e, release),
	})
	if err != nil {
		return nil, err
	}
	s.registerSpool(e, file)
	success = true
	return &writeHandle{file: file, writable: true}, nil
}

func (s *FS) openLake(ctx context.Context, e Entry, flags int, create bool) (Entry, Handle, error) {
	release, err := s.begin(ctx, e, true)
	if err != nil {
		return Entry{}, nil, err
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	info, err := s.lake.FreshStat(ctx, e.LakePath())
	newFile := IsNotExist(err) && create
	if err != nil && !newFile {
		return Entry{}, nil, err
	}
	if newFile {
		info = onelake.Info{}
	}
	if !newFile && create && flags&os.O_EXCL != 0 {
		return Entry{}, nil, fs.ErrExist
	}
	if info.IsDir {
		return Entry{}, nil, fserrors.ErrIsDir
	}
	if !newFile && info.ETag == "" {
		return Entry{}, nil, fmt.Errorf("OneLake did not provide an ETag: %w", fserrors.ErrUnsupported)
	}
	if info.Size > s.opts.MaxFileSize {
		return Entry{}, nil, fserrors.ErrTooLarge
	}
	etag := info.ETag
	file, err := writeback.Open(ctx, writeback.Options{
		Directory: s.opts.SpoolDirectory, MaxSize: s.opts.MaxFileSize, InitialSize: info.Size,
		Truncate: flags&os.O_TRUNC != 0, Append: flags&os.O_APPEND != 0,
		Load: func(ctx context.Context, dest io.Writer) error {
			return s.download(ctx, e.LakePath(), dest, info.Size, etag)
		},
		Commit: func(ctx context.Context, source io.ReaderAt, size int64) error {
			updated, err := s.lake.Put(ctx, e.LakePath(), source, size, etag)
			if err != nil {
				return err
			}
			if updated.ETag == "" || updated.Size != size || updated.IsDir {
				return fmt.Errorf("OneLake write returned incomplete properties: %w", fserrors.ErrConflict)
			}
			etag = updated.ETag
			return nil
		},
		OnClose: s.closeSpool(e, release),
	})
	if err != nil {
		return Entry{}, nil, err
	}
	if newFile {
		info, err = s.lake.Put(ctx, e.LakePath(), bytes.NewReader(nil), 0, "")
		if err == nil && (info.ETag == "" || info.Size != 0 || info.IsDir) {
			err = fmt.Errorf("OneLake create returned incomplete properties: %w", fserrors.ErrConflict)
		}
		if err != nil {
			return Entry{}, nil, errors.Join(err, file.Close())
		}
		etag = info.ETag
	}
	e.Size, e.Modified = file.Size(), info.ModTime
	s.registerSpool(e, file)
	success = true
	return e, &writeHandle{file: file, writable: writing(flags)}, nil
}

func (s *FS) download(ctx context.Context, path onelake.Path, dest io.Writer, size int64, etag string) error {
	buffer := make([]byte, 1<<20)
	for offset := int64(0); offset < size; {
		chunk := buffer[:min(int64(len(buffer)), size-offset)]
		n, err := s.lake.Read(ctx, path, offset, chunk, etag)
		if err != nil && !(errors.Is(err, io.EOF) && offset+int64(n) == size) {
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
}

type writeHandle struct {
	file     *writeback.File
	writable bool
}

func (h *writeHandle) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return h.file.ReadAt(p, off)
}
func (h *writeHandle) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	if !h.writable {
		return 0, fs.ErrPermission
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return h.file.WriteAt(p, off)
}
func (h *writeHandle) Truncate(ctx context.Context, size int64) error {
	if !h.writable {
		return fs.ErrPermission
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return h.file.Truncate(size)
}
func (h *writeHandle) Flush(ctx context.Context) error { return h.file.Flush(ctx) }
func (h *writeHandle) Close() error                    { return h.file.Close() }
func (h *writeHandle) Size() int64                     { return h.file.Size() }

type bytesHandle struct {
	mu      sync.Mutex
	data    []byte
	closed  bool
	release func()
}

func (h *bytesHandle) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if h.closed {
		return 0, fserrors.ErrClosed
	}
	if off < 0 {
		return 0, fs.ErrInvalid
	}
	return bytes.NewReader(h.data).ReadAt(p, off)
}
func (h *bytesHandle) Size() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return int64(len(h.data))
}
func (h *bytesHandle) WriteAt(context.Context, []byte, int64) (int, error) {
	return 0, fserrors.ErrReadOnly
}
func (h *bytesHandle) Truncate(context.Context, int64) error { return fserrors.ErrReadOnly }
func (h *bytesHandle) Flush(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fserrors.ErrClosed
	}
	return nil
}
func (h *bytesHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed, h.data = true, nil
		h.release()
		h.release = nil
	}
	return nil
}

type lakeReadHandle struct {
	mu      sync.Mutex
	lake    LakeAPI
	path    onelake.Path
	size    int64
	etag    string
	closed  bool
	release func()
}

func (h *lakeReadHandle) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
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
	if off >= h.size {
		return 0, io.EOF
	}
	clipped := min(int64(len(p)), h.size-off)
	n, err := h.lake.Read(ctx, h.path, off, p[:clipped], h.etag)
	if err == nil && n < len(p) {
		err = io.EOF
	}
	return n, err
}
func (h *lakeReadHandle) Size() int64 { return h.size }
func (h *lakeReadHandle) WriteAt(context.Context, []byte, int64) (int, error) {
	return 0, fserrors.ErrReadOnly
}
func (h *lakeReadHandle) Truncate(context.Context, int64) error { return fserrors.ErrReadOnly }
func (h *lakeReadHandle) Flush(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fserrors.ErrClosed
	}
	return nil
}
func (h *lakeReadHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		h.release()
		h.release, h.lake = nil, nil
	}
	return nil
}
