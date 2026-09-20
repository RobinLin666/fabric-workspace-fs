package fusefs

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"
)

type getattrBlockedPut struct {
	workspacefs.LakeAPI
	entered chan struct{}
	release chan struct{}
}

func (l *getattrBlockedPut) Put(ctx context.Context, path onelake.Path, source io.ReaderAt, size int64, etag string) (onelake.Info, error) {
	close(l.entered)
	<-l.release
	return l.LakeAPI.Put(ctx, path, source, size, etag)
}

func getattrFlushFixture(t *testing.T) (*workspacefs.FS, workspacefs.Entry, workspacefs.Handle, *getattrBlockedPut) {
	t.Helper()
	service := testutil.New(t)
	fab, lake := service.Clients()
	blocked := &getattrBlockedPut{LakeAPI: lake, entered: make(chan struct{}), release: make(chan struct{})}
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs, opts.SpoolDirectory = []string{testutil.WorkspaceID}, t.TempDir()
	backend, err := workspacefs.New(fab, blocked, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	entry := workspacefs.Entry{
		Kind: workspacefs.LakeFile, Name: "demo.txt", Workspace: testutil.WorkspaceID,
		Item: fabric.Item{ID: testutil.LakehouseID, Type: "Lakehouse"}, Remote: "Files/demo.txt",
	}
	handle, err := backend.Open(context.Background(), entry, os.O_RDWR|os.O_TRUNC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	if _, err := handle.WriteAt(context.Background(), []byte("local"), 0); err != nil {
		t.Fatal(err)
	}
	return backend, entry, handle, blocked
}

func checkGetattrDuringFlush(t *testing.T, blocked *getattrBlockedPut, flush func() int, getters map[string]func() (int64, int)) {
	t.Helper()
	unblock := sync.OnceFunc(func() { close(blocked.release) })
	defer unblock()
	flushed := make(chan int, 1)
	go func() { flushed <- flush() }()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	for name, getattr := range getters {
		t.Run(name, func(t *testing.T) {
			type result struct {
				size int64
				code int
			}
			done := make(chan result, 1)
			go func() {
				size, code := getattr()
				done <- result{size, code}
			}()
			select {
			case got := <-done:
				if got.code != 0 || got.size != 5 {
					t.Fatalf("getattr during flush: size=%d, status=%d", got.size, got.code)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("getattr waited for remote upload")
			}
		})
	}
	select {
	case code := <-flushed:
		t.Fatalf("flush returned before upload completed: %d", code)
	default:
	}
	unblock()
	select {
	case code := <-flushed:
		if code != 0 {
			t.Fatalf("flush status = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not finish")
	}
}
