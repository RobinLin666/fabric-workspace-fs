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
	"strings"

	"fabric-workspace-fs/internal/fserrors"
)

// File is a local positional-I/O handle. An append handle ignores the nonnegative
// WriteAt offset and writes at the current end of the file.
type File struct {
	store    *Store
	file     *os.File
	entry    *entry
	readable bool
	writable bool
	append   bool
	closed   bool
	closeErr error
}

func validFlags(flags int, create bool) error {
	allowed := os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC | os.O_SYNC
	if create {
		allowed |= os.O_CREATE | os.O_EXCL
	}
	access := flags & (os.O_WRONLY | os.O_RDWR)
	if flags & ^allowed != 0 || access == os.O_WRONLY|os.O_RDWR {
		return invalid("unsupported open flags")
	}
	if access == os.O_RDONLY && flags&(os.O_TRUNC|os.O_APPEND) != 0 {
		return invalid("append and truncate require a writable handle")
	}
	return nil
}

func nativeFlags(flags int) int {
	// Windows append-only handles lack the FILE_WRITE_DATA right needed by
	// Truncate. The store lock and single-writer rule make EOF WriteAt safe.
	if runtime.GOOS == "windows" {
		flags &^= os.O_APPEND
	}
	return flags
}

// Open opens an existing regular file. Creation flags are deliberately rejected;
// all creation must go through Create so entry accounting cannot be bypassed.
// Adapters must normalize nonsemantic kernel control flags. O_SYNC, including
// the Linux O_DSYNC subset, is retained on the underlying file descriptor.
func (s *Store) Open(ctx context.Context, loc Location, flags int) (*File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return nil, err
	}
	if err := validFlags(flags, false); err != nil {
		return nil, err
	}
	p, err := resolve(loc, false)
	if err != nil {
		return nil, err
	}
	record, err := s.existing(p.path)
	if err != nil {
		return nil, err
	}
	if record.info.IsDir() {
		return nil, fserrors.ErrIsDir
	}
	writable := flags&(os.O_WRONLY|os.O_RDWR) != 0
	if writable && s.writer(record) {
		return nil, fserrors.ErrBusy
	}
	// Validate the opened object before doing any destructive truncation.
	file, info, err := s.openVerified(record.path, nativeFlags(flags & ^os.O_TRUNC))
	if err != nil {
		return nil, err
	}
	if err := s.refresh(record, info); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	handle := s.makeHandle(file, record, flags)
	if flags&os.O_TRUNC != 0 {
		truncateErr := file.Truncate(0)
		statErr := handle.refreshLocked()
		if statErr != nil {
			s.poison = statErr
		}
		if err := errors.Join(truncateErr, statErr); err != nil {
			return nil, errors.Join(err, handle.closeLocked())
		}
	}
	s.handles[handle] = struct{}{}
	return handle, nil
}

// Create creates a new regular file below an existing dot-directory. It always
// uses O_EXCL; no caller flags can truncate or reuse an existing file.
func (s *Store) Create(ctx context.Context, loc Location, flags int) (*File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return nil, err
	}
	if err := validFlags(flags, true); err != nil {
		return nil, err
	}
	p, err := resolve(loc, false)
	if err != nil {
		return nil, err
	}
	if _, err := s.checkedStat(p.path); err == nil {
		return nil, fs.ErrExist
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if !strings.Contains(p.relative, "/") {
		return nil, invalid("dot-roots must be directories")
	}
	path, err := s.destination(p)
	if err != nil {
		return nil, err
	}
	if s.entries[pathKey(path)] != nil {
		return nil, fmt.Errorf("overlay entry %q disappeared or aliases another entry: %w", path, fserrors.ErrConflict)
	}
	if err := s.reserveEntry(); err != nil {
		return nil, err
	}
	file, err := s.root.OpenFile(diskPath(path), nativeFlags(flags & ^os.O_TRUNC | os.O_CREATE | os.O_EXCL), 0o600)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	if statErr == nil {
		statErr = checkHandle(path, file, info)
	}
	if statErr == nil {
		statErr = s.growth(0, info.Size())
	}
	if statErr != nil {
		s.poison = statErr
		return nil, errors.Join(statErr, file.Close())
	}
	record := &entry{path: path, info: info, size: info.Size()}
	s.entries[pathKey(path)] = record
	s.quota.Entries++
	s.quota.TotalBytes += record.size
	handle := s.makeHandle(file, record, flags)
	s.handles[handle] = struct{}{}
	return handle, nil
}

func (s *Store) makeHandle(file *os.File, record *entry, flags int) *File {
	return &File{
		store: s, file: file, entry: record,
		readable: flags&os.O_WRONLY == 0,
		writable: flags&(os.O_WRONLY|os.O_RDWR) != 0,
		append:   flags&os.O_APPEND != 0,
	}
}

func (f *File) checkLocked(ctx context.Context) error {
	if f.closed {
		return errors.Join(fs.ErrClosed, fserrors.ErrClosed)
	}
	return f.store.checkLocked(ctx)
}

func (f *File) refreshLocked() error {
	info, err := f.file.Stat()
	if err != nil {
		return err
	}
	if err := checkHandle(f.entry.path, f.file, info); err != nil {
		return err
	}
	return f.store.refresh(f.entry, info)
}

func (f *File) ReadAt(ctx context.Context, buffer []byte, offset int64) (int, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.checkLocked(ctx); err != nil {
		return 0, err
	}
	if offset < 0 || int64(len(buffer)) > math.MaxInt64-offset {
		return 0, invalid("negative or overflowing read offset")
	}
	if !f.readable {
		return 0, fs.ErrPermission
	}
	if err := f.refreshLocked(); err != nil {
		return 0, err
	}
	return f.file.ReadAt(buffer, offset)
}

func (f *File) WriteAt(ctx context.Context, buffer []byte, offset int64) (int, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.checkLocked(ctx); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, invalid("negative write offset")
	}
	if !f.writable {
		return 0, fserrors.ErrReadOnly
	}
	if err := f.refreshLocked(); err != nil {
		return 0, err
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	old := f.entry.size
	if f.append {
		offset = old
	}
	if int64(len(buffer)) > math.MaxInt64-offset {
		return 0, invalid("overflowing write offset")
	}
	next := max(old, offset+int64(len(buffer)))
	if err := f.store.growth(old, next); err != nil {
		return 0, err
	}
	var n int
	var writeErr error
	if f.append && runtime.GOOS != "windows" {
		n, writeErr = f.file.Write(buffer)
	} else {
		n, writeErr = f.file.WriteAt(buffer, offset)
	}
	if writeErr == nil && n != len(buffer) {
		writeErr = io.ErrShortWrite
	}
	statErr := f.refreshLocked()
	if statErr != nil {
		f.store.poison = statErr
	}
	return n, errors.Join(writeErr, statErr)
}

func (f *File) Truncate(ctx context.Context, size int64) error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.checkLocked(ctx); err != nil {
		return err
	}
	if size < 0 {
		return invalid("negative truncate size")
	}
	if !f.writable {
		return fserrors.ErrReadOnly
	}
	if err := f.refreshLocked(); err != nil {
		return err
	}
	if err := f.store.growth(f.entry.size, size); err != nil {
		return err
	}
	truncateErr := f.file.Truncate(size)
	statErr := f.refreshLocked()
	if statErr != nil {
		f.store.poison = statErr
	}
	return errors.Join(truncateErr, statErr)
}

// Flush performs an actual file Sync, including for a read-only handle; any
// operating-system error is returned instead of being treated as success.
func (f *File) Flush(ctx context.Context) error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.checkLocked(ctx); err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *File) Close() error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return f.closeLocked()
}

func (f *File) closeLocked() error {
	if !f.closed {
		f.closed = true
		f.closeErr = f.file.Close()
		delete(f.store.handles, f)
	}
	return f.closeErr
}

// Size reports current local metadata while open and the last observed size
// after Close. Methods with an error result report metadata errors explicitly.
func (f *File) Size() int64 {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if !f.closed {
		if err := f.refreshLocked(); err != nil {
			f.store.poison = err
		}
	}
	return f.entry.size
}
