package workspacefs

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/testutil"
)

type statBlockedPut struct {
	LakeAPI
	entered chan struct{}
	release chan struct{}
}

func (l *statBlockedPut) Put(ctx context.Context, path onelake.Path, source io.ReaderAt, size int64, etag string) (onelake.Info, error) {
	close(l.entered)
	<-l.release
	return l.LakeAPI.Put(ctx, path, source, size, etag)
}

func TestStatDuringBlockedFlush(t *testing.T) {
	service := testutil.New(t)
	fab, lake := service.Clients()
	blocked := &statBlockedPut{LakeAPI: lake, entered: make(chan struct{}), release: make(chan struct{})}
	opts := DefaultOptions()
	opts.WorkspaceIDs, opts.SpoolDirectory = []string{testutil.WorkspaceID}, t.TempDir()
	backend, err := New(fab, blocked, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	_, files, _ := lakeRoots(t, backend)
	entry := lookup(t, backend, files, "demo.txt")
	ctx := context.Background()
	handle, err := backend.Open(ctx, entry, os.O_RDWR|os.O_TRUNC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	const data = "size remains local during upload"
	if _, err := handle.WriteAt(ctx, []byte(data), 0); err != nil {
		t.Fatal(err)
	}
	unblock := sync.OnceFunc(func() { close(blocked.release) })
	defer unblock()
	flushed := make(chan error, 1)
	go func() { flushed <- handle.Flush(ctx) }()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		stat, err := backend.Stat(ctx, entry)
		if err != nil || stat.Size != int64(len(data)) {
			t.Errorf("stat during upload = %+v, %v", stat, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stat waited for remote upload")
	}
	select {
	case err := <-flushed:
		t.Fatalf("Flush returned before upload completed: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not finish")
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	stat, err := backend.Stat(ctx, entry)
	if err != nil || stat.Size != int64(len(data)) {
		t.Fatalf("committed stat = %+v, %v", stat, err)
	}
	reader, err := backend.Open(ctx, entry, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := read(t, reader); got != data {
		t.Fatalf("committed data = %q", got)
	}
}
