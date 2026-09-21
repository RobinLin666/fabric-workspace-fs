package workspacefs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/transport"
)

func repeatedReadWorkload(t *testing.T, ttl time.Duration) testutil.Counts {
	t.Helper()
	backend, remote := newTestFS(t, func(o *Options) { o.CacheTTL = ttl })
	nbDir, content := notebook(t, backend)
	_, files, _ := lakeRoots(t, backend)
	demo := lookup(t, backend, files, "demo.txt")
	group := Entry{Kind: Workspace, Directory: true, Workspace: testutil.WorkspaceID, Label: "Sample workspace"}
	before := remote.Counts()
	ctx := context.Background()
	for range 12 {
		if _, err := backend.ReadDir(ctx, group); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.ReadDir(ctx, nbDir); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.Stat(ctx, content); err != nil {
			t.Fatal(err)
		}
		notebookHandle, err := backend.Open(ctx, content, os.O_RDONLY)
		if err != nil {
			t.Fatal(err)
		}
		if read(t, notebookHandle) != testutil.InitialNotebook {
			t.Fatal("notebook changed")
		}
		_ = notebookHandle.Close()
		if _, err := backend.ReadDir(ctx, files); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.Stat(ctx, demo); err != nil {
			t.Fatal(err)
		}
		lakeHandle, err := backend.Open(ctx, demo, os.O_RDONLY)
		if err != nil {
			t.Fatal(err)
		}
		if read(t, lakeHandle) != "0123456789" {
			t.Fatal("range read changed")
		}
		_ = lakeHandle.Close()
	}
	after := remote.Counts()
	return testutil.Counts{
		CatalogReads:    after.CatalogReads - before.CatalogReads,
		DefinitionReads: after.DefinitionReads - before.DefinitionReads,
		StorageStats:    after.StorageStats - before.StorageStats,
		StorageLists:    after.StorageLists - before.StorageLists,
		StorageReads:    after.StorageReads - before.StorageReads,
	}
}

func TestCachesReduceRepeatedListStatAndDefinitionCalls(t *testing.T) {
	uncached := repeatedReadWorkload(t, 0)
	cached := repeatedReadWorkload(t, time.Minute)
	t.Logf("12 repeated ls/stat/read cycles: uncached=%+v cached=%+v", uncached, cached)
	if uncached.CatalogReads != 24 || uncached.DefinitionReads != 24 ||
		uncached.StorageStats != 24 || uncached.StorageLists != 12 || uncached.StorageReads != 12 {
		t.Fatal("baseline API count changed; update measurement with evidence", uncached)
	}
	if cached.CatalogReads != 0 || cached.DefinitionReads != 0 || cached.StorageStats != 0 ||
		cached.StorageLists != 1 || cached.StorageReads != 12 {
		t.Fatal("caches missed repeated metadata or cached large-file ranges unsafely", cached)
	}
}

func TestCacheNeverHidesDirtySizeOrFreshNotebookConflict(t *testing.T) {
	backend, remote := newTestFS(t, func(o *Options) { o.CacheTTL = time.Hour })
	_, e := notebook(t, backend)
	h, err := backend.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	data := `{"cells":[],"nbformat":4,"metadata":{"mine":1}}`
	_, _ = h.WriteAt(context.Background(), []byte(data), 0)
	_ = h.Truncate(context.Background(), int64(len(data)))
	if stat, err := backend.Stat(context.Background(), e); err != nil || stat.Size != int64(len(data)) {
		t.Fatal("cached size covered dirty writeback", stat, err)
	}
	external := remote.Definition()
	external.Parts[0].Payload = base64.StdEncoding.EncodeToString([]byte("external metadata"))
	remote.SetDefinition(external)
	before := remote.Counts().DefinitionReads
	if err := h.Flush(context.Background()); !errors.Is(err, fserrors.ErrConflict) {
		t.Fatal("cached comparison overwrote an external notebook edit", err)
	}
	if remote.Counts().DefinitionReads != before+1 || remote.Counts().NotebookUpdates != 0 {
		t.Fatal("save did not force a fresh uncached comparison")
	}
}

func TestWriteRenameAndDeleteInvalidateOnlyRelatedMetadata(t *testing.T) {
	backend, remote := newTestFS(t, func(o *Options) { o.CacheTTL = time.Hour })
	_, files, tables := lakeRoots(t, backend)
	ctx := context.Background()
	if _, err := backend.ReadDir(ctx, files); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.ReadDir(ctx, tables); err != nil {
		t.Fatal(err)
	}
	dir, err := backend.Mkdir(ctx, files, "fresh-dir")
	if err != nil {
		t.Fatal(err)
	}
	children, err := backend.ReadDir(ctx, files)
	if err != nil || len(children) != 2 {
		t.Fatal("mkdir left parent listing stale", children, err)
	}
	e, h, err := backend.Create(ctx, dir, "file", os.O_RDWR|os.O_EXCL)
	if err != nil {
		t.Fatal(err)
	}
	save(t, h, "created")
	_ = h.Close()
	if stat, err := backend.Stat(ctx, e); err != nil || stat.Size != 7 {
		t.Fatal("saved size stale", stat, err)
	}
	reader, err := backend.Open(ctx, e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	remote.SetFile(e.Remote, []byte("outside"))
	if _, err := reader.ReadAt(ctx, make([]byte, 7), 0); !errors.Is(err, fserrors.ErrConflict) {
		t.Fatal("range snapshot ignored remote change", err)
	}
	_ = reader.Close()
	reader, err = backend.Open(ctx, e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, reader) != "outside" {
		t.Fatal("range conflict did not invalidate metadata")
	}
	_ = reader.Close()
	renamed, err := backend.Rename(ctx, e, dir, "renamed", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Stat(ctx, e); !IsNotExist(err) {
		t.Fatal("rename retained old path", err)
	}
	if _, err := backend.Stat(ctx, renamed); err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(ctx, renamed, false); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Stat(ctx, renamed); !IsNotExist(err) {
		t.Fatal("deleted path was cached", err)
	}
	if err := backend.Remove(ctx, dir, true); err != nil {
		t.Fatal(err)
	}
	children, err = backend.ReadDir(ctx, files)
	if err != nil || len(children) != 1 {
		t.Fatal("rmdir left parent listing stale", children, err)
	}
	before := remote.Counts().StorageLists
	if _, err := backend.ReadDir(ctx, tables); err != nil {
		t.Fatal(err)
	}
	if remote.Counts().StorageLists != before {
		t.Fatal("Files mutation invalidated unrelated Tables listing")
	}
}

func TestConcurrentDefinitionReadsAreSingleFlightAndIsolated(t *testing.T) {
	backend, remote := newTestFS(t, func(o *Options) { o.CacheTTL = time.Minute })
	dir, _ := notebook(t, backend)
	backend.snapshots.Clear()
	before := remote.Counts().DefinitionReads
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			def, err := backend.definition(context.Background(), dir)
			if err != nil {
				t.Error(err)
				return
			}
			def.Parts[0].Payload = "caller-local-change"
			def.Extra["futureDefinitionField"][0] = 'x'
		})
	}
	wg.Wait()
	if remote.Counts().DefinitionReads != before+1 {
		t.Fatal("concurrent definition cache stampede")
	}
	def, err := backend.definition(context.Background(), dir)
	if err != nil || def.Parts[0].Payload == "caller-local-change" || def.Extra["futureDefinitionField"][0] != '{' {
		t.Fatal("cached value was mutated by a caller", err)
	}
}

type failingDefinitionAPI struct {
	FabricAPI
	status, remaining int
}

func (f *failingDefinitionAPI) GetDefinition(ctx context.Context, ws, item, kind, format string) (fabric.Definition, error) {
	if f.remaining > 0 {
		f.remaining--
		return fabric.Definition{}, &transport.HTTPError{StatusCode: f.status}
	}
	return f.FabricAPI.GetDefinition(ctx, ws, item, kind, format)
}

func TestAuthorizationAndServiceErrorsAreNeverCachedAsEmptyTrees(t *testing.T) {
	for _, status := range []int{401, 403, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			remote := testutil.New(t)
			fab, lake := remote.Clients()
			opts := DefaultOptions()
			opts.WorkspaceIDs, opts.SpoolDirectory = []string{testutil.WorkspaceID}, t.TempDir()
			opts.CacheTTL = time.Hour
			backend, err := New(&failingDefinitionAPI{FabricAPI: fab, status: status, remaining: 1}, lake, opts)
			if err != nil {
				t.Fatal(err)
			}
			dir := Entry{Kind: Notebook, Directory: true, Workspace: testutil.WorkspaceID,
				Item: fabric.Item{ID: testutil.NotebookID, Type: "Notebook", DisplayName: "Sample notebook"}}
			if entries, err := backend.ReadDir(context.Background(), dir); err != nil || len(entries) != 3 {
				t.Fatal("fixed roots depended on definition export", entries, err)
			}
			if _, err := backend.Lookup(context.Background(), dir, "Sample notebook.ipynb"); err == nil {
				t.Fatal("remote content error converted to a success-shaped file")
			}
			if entry, err := backend.Lookup(context.Background(), dir, "Sample notebook.ipynb"); err != nil || entry.Size <= 0 {
				t.Fatal("remote error was cached", entry, err)
			}
		})
	}
}
