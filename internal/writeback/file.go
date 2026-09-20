package writeback

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"fabric-workspace-fs/internal/fserrors"
)

type Commit func(context.Context, io.ReaderAt, int64) error

type Options struct {
	Directory   string
	MaxSize     int64
	InitialSize int64
	Truncate    bool
	Append      bool
	Load        func(context.Context, io.Writer) error
	Commit      Commit
	OnClose     func()
	// Observe receives flush_lock and spool_sync timings outside the I/O lock.
	// Calls may be concurrent; observers must be concurrency-safe.
	Observe func(stage string, elapsed time.Duration, err error)
}

// RecoveryError means the remote save did not finish. Path is deliberately
// retained (0600) so an unsuccessful close never destroys the only dirty copy.
type RecoveryError struct {
	Path  string
	Cause error
}

func (e *RecoveryError) Error() string {
	return fmt.Sprintf("unsaved data retained at %q: %v", e.Path, e.Cause)
}

func (e *RecoveryError) Unwrap() error { return e.Cause }

type File struct {
	mu   sync.Mutex
	file *os.File
	path string
	// size publishes completed local mutations without waiting for a remote
	// commit. All spool I/O, including commits, remains serialized by mu.
	size     atomic.Int64
	maxSize  int64
	append   bool
	dirty    bool
	closed   bool
	lastSave error
	closeErr error
	commit   Commit
	onClose  func()
	observe  func(string, time.Duration, error)
}

func Open(ctx context.Context, opts Options) (*File, error) {
	if opts.MaxSize <= 0 || opts.InitialSize < 0 || opts.InitialSize > opts.MaxSize || opts.Commit == nil {
		return nil, fmt.Errorf("invalid spool configuration: %w", fs.ErrInvalid)
	}
	if opts.Directory == "" {
		return nil, fmt.Errorf("a private spool directory is required: %w", fs.ErrInvalid)
	}
	if err := os.MkdirAll(opts.Directory, 0700); err != nil {
		return nil, fmt.Errorf("create spool directory: %w", err)
	}
	temp, err := os.CreateTemp(opts.Directory, "fabric-write-")
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}
	cleanup := func(cause error) (*File, error) {
		return nil, errors.Join(cause, temp.Close(), os.Remove(temp.Name()))
	}
	size := opts.InitialSize
	if opts.Truncate {
		size = 0
	} else if size > 0 {
		if opts.Load == nil {
			return cleanup(fmt.Errorf("initial data loader is required: %w", fs.ErrInvalid))
		}
		limited := &boundedWriter{writer: temp, remaining: opts.MaxSize}
		if err := opts.Load(ctx, limited); err != nil {
			return cleanup(fmt.Errorf("load spool: %w", err))
		}
		stat, err := temp.Stat()
		if err != nil {
			return cleanup(err)
		}
		if stat.Size() != size {
			return cleanup(fmt.Errorf("initial size changed: expected %d, read %d: %w", size, stat.Size(), fserrors.ErrConflict))
		}
	}
	f := &File{
		file: temp, path: temp.Name(), maxSize: opts.MaxSize,
		append: opts.Append, dirty: opts.Truncate && opts.InitialSize != 0,
		commit: opts.Commit, onClose: opts.OnClose, observe: opts.Observe,
	}
	f.size.Store(size)
	return f, nil
}

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, fserrors.ErrTooLarge
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func (f *File) ReadAt(p []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, fserrors.ErrClosed
	}
	if offset < 0 {
		return 0, fs.ErrInvalid
	}
	return f.file.ReadAt(p, offset)
}

func (f *File) WriteAt(p []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, fserrors.ErrClosed
	}
	if offset < 0 {
		return 0, fs.ErrInvalid
	}
	if f.append {
		offset = f.size.Load()
	}
	if len(p) == 0 {
		return 0, nil
	}
	if offset > f.maxSize || int64(len(p)) > f.maxSize-offset {
		return 0, fserrors.ErrTooLarge
	}
	n, err := f.file.WriteAt(p, offset)
	if n > 0 {
		f.dirty = true
		f.size.Store(max(f.size.Load(), offset+int64(n)))
	}
	return n, err
}

func (f *File) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fserrors.ErrClosed
	}
	if size < 0 {
		return fs.ErrInvalid
	}
	if size > f.maxSize {
		return fserrors.ErrTooLarge
	}
	if f.size.Load() == size {
		return nil
	}
	if err := f.file.Truncate(size); err != nil {
		return err
	}
	f.size.Store(size)
	f.dirty = true
	return nil
}

func (f *File) Flush(ctx context.Context) error {
	started := time.Now()
	f.mu.Lock()
	observe := f.observe
	var lockElapsed, syncElapsed time.Duration
	var syncErr error
	var synced bool
	if observe != nil {
		lockElapsed = time.Since(started)
	}
	defer func() {
		f.mu.Unlock()
		if observe != nil {
			observe("flush_lock", lockElapsed, nil)
			if synced {
				observe("spool_sync", syncElapsed, syncErr)
			}
		}
	}()
	if f.closed {
		return fserrors.ErrClosed
	}
	if !f.dirty {
		return nil
	}
	if err := ctx.Err(); err != nil {
		f.lastSave = err
		return err
	}
	if observe != nil {
		started = time.Now()
	}
	syncErr = f.file.Sync()
	synced = true
	if observe != nil {
		syncElapsed = time.Since(started)
	}
	if syncErr != nil {
		f.lastSave = syncErr
		return syncErr
	}
	if err := f.commit(ctx, f.file, f.size.Load()); err != nil {
		f.lastSave = err
		return err
	}
	f.dirty, f.lastSave = false, nil
	return nil
}

func (f *File) Size() int64 {
	return f.size.Load()
}

func (f *File) Dirty() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dirty
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return f.closeErr
	}
	f.closed = true
	closeErr := f.file.Close()
	if f.dirty {
		cause := f.lastSave
		if cause == nil {
			cause = errors.New("close before successful flush/fsync")
		}
		f.closeErr = &RecoveryError{Path: f.path, Cause: errors.Join(cause, closeErr)}
	} else {
		f.closeErr = errors.Join(closeErr, os.Remove(f.path))
	}
	if f.onClose != nil {
		f.onClose()
	}
	// Closed descriptors must not pin old decoded definitions or resource
	// snapshots after their byte reservation has been released.
	f.commit, f.onClose, f.observe = nil, nil, nil
	return f.closeErr
}
