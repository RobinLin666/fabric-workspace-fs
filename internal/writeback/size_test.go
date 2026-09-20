package writeback

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
)

func TestSizeDuringBlockedCommit(t *testing.T) {
	for _, commitErr := range []error{nil, fserrors.ErrConflict} {
		name := "success"
		if commitErr != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			f, err := Open(context.Background(), Options{
				Directory: t.TempDir(), MaxSize: 32,
				Commit: func(_ context.Context, source io.ReaderAt, size int64) error {
					close(entered)
					<-release
					data := make([]byte, size)
					if _, err := source.ReadAt(data, 0); err != nil || string(data) != "saved" {
						t.Errorf("commit source = %q, %v", data, err)
					}
					return commitErr
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = f.Close() })
			if _, err := f.WriteAt([]byte("saved"), 0); err != nil {
				t.Fatal(err)
			}
			flushed := make(chan error, 1)
			go func() { flushed <- f.Flush(context.Background()) }()
			// Unblock the commit even when the regression makes Size time out.
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("commit did not start")
			}
			sizes := make(chan int64, 1)
			go func() { sizes <- f.Size() }()
			select {
			case size := <-sizes:
				if size != 5 {
					t.Fatalf("size during commit = %d", size)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Size waited for remote commit")
			}
			if f.mu.TryLock() {
				f.mu.Unlock()
				t.Fatal("commit no longer holds the spool I/O lock")
			}
			select {
			case err := <-flushed:
				t.Fatalf("Flush returned before commit completion: %v", err)
			default:
			}
			unblock()
			select {
			case err := <-flushed:
				if !errors.Is(err, commitErr) || f.Size() != 5 || f.Dirty() != (commitErr != nil) {
					t.Errorf("flush = %v, size = %d, dirty = %v", err, f.Size(), f.Dirty())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Flush did not finish")
			}
		})
	}
}

func TestSizeTracksCompletedLocalMutations(t *testing.T) {
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 16, InitialSize: 3,
		Load: func(_ context.Context, w io.Writer) error {
			_, err := io.WriteString(w, "abc")
			return err
		},
		Commit: func(context.Context, io.ReaderAt, int64) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	check := func(want int64) {
		t.Helper()
		if got := f.Size(); got != want {
			t.Fatalf("size = %d, want %d", got, want)
		}
	}
	check(3)
	if _, err := f.WriteAt([]byte("XY"), 1); err != nil {
		t.Fatal(err)
	}
	check(3)
	if _, err := f.WriteAt([]byte("z"), 7); err != nil {
		t.Fatal(err)
	}
	check(8)
	if err := f.Truncate(2); err != nil {
		t.Fatal(err)
	}
	check(2)
	if err := f.Truncate(10); err != nil {
		t.Fatal(err)
	}
	check(10)
	if _, err := f.WriteAt(nil, 15); err != nil {
		t.Fatal(err)
	}
	check(10)
	if _, err := f.WriteAt([]byte("x"), 16); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal(err)
	}
	if err := f.Truncate(17); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal(err)
	}
	if err := f.Truncate(-1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal(err)
	}
	check(10)
	if err := f.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	check(10)
	if _, err := f.WriteAt([]byte("x"), 10); !errors.Is(err, fserrors.ErrClosed) {
		t.Fatal(err)
	}
	if err := f.Truncate(0); !errors.Is(err, fserrors.ErrClosed) {
		t.Fatal(err)
	}
	check(10)
}

func TestSizeUnchangedAfterLocalIOFailureAndDirtyClose(t *testing.T) {
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 16,
		Commit: func(context.Context, io.ReaderAt, int64) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.file.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := f.WriteAt([]byte("extension"), 4); n != 0 || err == nil {
		t.Fatalf("failed write = %d, %v", n, err)
	}
	if err := f.Truncate(1); err == nil {
		t.Fatal("truncate unexpectedly succeeded")
	}
	if f.Size() != 4 {
		t.Fatalf("failed local I/O changed size to %d", f.Size())
	}
	var recovery *RecoveryError
	if err := f.Close(); !errors.As(err, &recovery) || f.Size() != 4 {
		t.Fatalf("dirty close = %v, size = %d", err, f.Size())
	}
}
