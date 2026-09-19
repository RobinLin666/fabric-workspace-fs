package writeback

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
)

func TestOffsetTruncateAndIdempotentFlush(t *testing.T) {
	var saved []byte
	commits := 0
	closed := 0
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 1024, InitialSize: 6,
		Load: func(_ context.Context, w io.Writer) error { _, err := io.WriteString(w, "abcdef"); return err },
		Commit: func(_ context.Context, r io.ReaderAt, n int64) error {
			commits++
			saved = make([]byte, n)
			_, err := r.ReadAt(saved, 0)
			return err
		},
		OnClose: func() { closed++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("XY"), 2); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	if _, err := f.ReadAt(buf, 0); err != nil || string(buf) != "abXYef" {
		t.Fatalf("read own writes: %q %v", buf, err)
	}
	if err := f.Truncate(4); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := f.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if commits != 1 || string(saved) != "abXY" {
		t.Fatalf("commits=%d data=%q", commits, saved)
	}
	if err := f.Truncate(8); err != nil {
		t.Fatal(err)
	}
	if err := f.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, []byte{'a', 'b', 'X', 'Y', 0, 0, 0, 0}) {
		t.Fatalf("sparse extension=%q", saved)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil || closed != 1 {
		t.Fatalf("duplicate close: %v, callbacks=%d", err, closed)
	}
	if _, err := os.Stat(f.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("clean spool not removed: %v", err)
	}
}

func TestFailureRetainsDirtyAndRetry(t *testing.T) {
	fail := true
	commits := 0
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 32,
		Commit: func(context.Context, io.ReaderAt, int64) error {
			commits++
			if fail {
				return fserrors.ErrConflict
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteAt([]byte("unsaved"), 0)
	if err := f.Flush(context.Background()); !errors.Is(err, fserrors.ErrConflict) || !f.Dirty() {
		t.Fatalf("failed save lost dirty state: %v", err)
	}
	fail = false
	if err := f.Flush(context.Background()); err != nil || f.Dirty() {
		t.Fatalf("retry: %v", err)
	}
	_ = f.Flush(context.Background())
	if commits != 2 {
		t.Fatalf("repeated upload: %d", commits)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDirtyClosePreservesRecoveryAndNeverUploads(t *testing.T) {
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 32,
		Commit: func(context.Context, io.ReaderAt, int64) error { t.Fatal("close attempted upload"); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteAt([]byte("only-copy"), 0)
	err = f.Close()
	var recovery *RecoveryError
	if !errors.As(err, &recovery) {
		t.Fatalf("missing actionable recovery error: %v", err)
	}
	data, err := os.ReadFile(recovery.Path)
	if err != nil || string(data) != "only-copy" {
		t.Fatalf("dirty copy lost: %q, %v", data, err)
	}
}

func TestAppendLimitsCancellationAndConcurrentFlush(t *testing.T) {
	var commits int
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 8, Append: true,
		Commit: func(context.Context, io.ReaderAt, int64) error { commits++; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteAt([]byte("abc"), 99)
	_, _ = f.WriteAt([]byte("def"), 0)
	if n, err := f.WriteAt([]byte("ghi"), 0); n != 0 || !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatalf("limit=%d,%v", n, err)
	}
	if _, err := f.WriteAt([]byte("x"), -1); err == nil {
		t.Fatal("accepted negative write")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.Flush(ctx); !errors.Is(err, context.Canceled) || commits != 0 {
		t.Fatalf("canceled commit: %v", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := f.Flush(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if commits != 1 || f.Size() != 6 {
		t.Fatalf("commits=%d size=%d", commits, f.Size())
	}
	_ = f.Close()
}

func TestOpenTruncateDoesNotLoad(t *testing.T) {
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 8, InitialSize: 8, Truncate: true,
		Load: func(context.Context, io.Writer) error { t.Fatal("unnecessary download"); return nil },
		Commit: func(_ context.Context, _ io.ReaderAt, n int64) error {
			if n != 0 {
				t.Fatal(n)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !f.Dirty() || f.Size() != 0 {
		t.Fatal("O_TRUNC was dropped")
	}
	if err := f.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}
