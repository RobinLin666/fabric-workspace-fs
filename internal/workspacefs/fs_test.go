package workspacefs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/writeback"
)

func newTestFS(t *testing.T, adjust func(*Options)) (*FS, *testutil.Service) {
	t.Helper()
	service := testutil.New(t)
	fab, lake := service.Clients()
	opts := DefaultOptions()
	opts.WorkspaceIDs = []string{testutil.WorkspaceID}
	opts.SpoolDirectory = t.TempDir()
	opts.CacheTTL = 0
	if adjust != nil {
		adjust(&opts)
	}
	backend, err := New(fab, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	return backend, service
}

func lookup(t *testing.T, backend *FS, parent Entry, name string) Entry {
	t.Helper()
	e, err := backend.Lookup(context.Background(), parent, name)
	if err != nil {
		t.Fatalf("lookup %q: %v", name, err)
	}
	return e
}

func item(t *testing.T, backend *FS, group, label, id string) Entry {
	t.Helper()
	root := backend.Root()
	wsName, _ := namespace.CatalogName("Sample workspace", testutil.WorkspaceID)
	ws := lookup(t, backend, root, wsName)
	itemName, _ := namespace.CatalogName(label, id)
	return lookup(t, backend, ws, itemName+"."+strings.TrimSuffix(group, "s"))
}

func notebook(t *testing.T, backend *FS) (Entry, Entry) {
	t.Helper()
	dir := item(t, backend, "Notebooks", "Sample notebook", testutil.NotebookID)
	return dir, lookup(t, backend, dir, namespace.NotebookContentFileName("Sample notebook"))
}

func lakeRoots(t *testing.T, backend *FS) (Entry, Entry, Entry) {
	t.Helper()
	dir := item(t, backend, "Lakehouses", "Sample lakehouse", testutil.LakehouseID)
	return dir, lookup(t, backend, dir, "Files"), lookup(t, backend, dir, "Tables")
}

func read(t *testing.T, handle Handle) string {
	t.Helper()
	data := make([]byte, handle.Size()+1)
	n, err := handle.ReadAt(context.Background(), data, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return string(data[:n])
}

func save(t *testing.T, handle Handle, data string) {
	t.Helper()
	ctx := context.Background()
	if _, err := handle.WriteAt(ctx, []byte(data), 0); err != nil {
		t.Fatal(err)
	}
	if err := handle.Truncate(ctx, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if err := handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNotebookRoundTripFormatPartsAndFlush(t *testing.T) {
	for _, path := range []string{"notebook-content.ipynb", "artifact.content.ipynb"} {
		t.Run(path, func(t *testing.T) {
			backend, remote := newTestFS(t, nil)
			original := remote.Definition()
			original.Parts[1].Path = path
			remote.SetDefinition(original)
			dir, e := notebook(t, backend)
			if e.Part != path {
				t.Fatal("content path was not preserved")
			}
			children, err := backend.ReadDir(context.Background(), dir)
			if err != nil || len(children) != 3 {
				t.Fatalf("unexpected notebook view %v: %v", children, err)
			}
			handle, err := backend.Open(context.Background(), e, os.O_RDWR)
			if err != nil {
				t.Fatal(err)
			}
			if got := read(t, handle); got != testutil.InitialNotebook {
				t.Fatal(got)
			}
			if _, err := backend.Open(context.Background(), e, os.O_WRONLY); !errors.Is(err, fserrors.ErrBusy) {
				t.Fatalf("concurrent writer not rejected: %v", err)
			}
			first := `{"nbformat":4,"cells":[],"metadata":{"edited":1}}`
			save(t, handle, first)
			for range 4 {
				if err := handle.Flush(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if remote.Counts().NotebookUpdates != 1 {
				t.Fatal("clean flush uploaded again")
			}
			second := `{"nbformat":4,"cells":[],"metadata":{"edited":2}}`
			save(t, handle, second)
			if err := handle.Close(); err != nil {
				t.Fatal(err)
			}
			result := remote.Definition()
			if result.Format != "ipynb" || len(result.Parts) != len(original.Parts) {
				t.Fatalf("wrong update format/parts: %+v", result)
			}
			for _, index := range []int{0, 2} {
				a, _ := json.Marshal(original.Parts[index])
				b, _ := json.Marshal(result.Parts[index])
				if string(a) != string(b) {
					t.Fatalf("untouched metadata part changed: %s => %s", a, b)
				}
			}
			if string(result.Extra["futureDefinitionField"]) != string(original.Extra["futureDefinitionField"]) ||
				string(result.Parts[1].Extra["futurePartField"]) != string(original.Parts[1].Extra["futurePartField"]) {
				t.Fatal("unknown metadata fields lost")
			}
			reopened, err := backend.Open(context.Background(), e, os.O_RDONLY)
			if err != nil {
				t.Fatal(err)
			}
			if got := read(t, reopened); got != second {
				t.Fatalf("saved notebook not persistent: %s", got)
			}
			_ = reopened.Close()
		})
	}
}

func TestNotebookFailedFlushRetryAndConflictRecovery(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	_, e := notebook(t, backend)
	handle, err := backend.Open(context.Background(), e, os.O_RDWR|os.O_TRUNC)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if stat, err := backend.Stat(ctx, e); err != nil || stat.Size != 0 {
		t.Fatalf("dirty size hidden from path getattr: %d %v", stat.Size, err)
	}
	data := `{"nbformat":4,"cells":[],"metadata":{"saved":true}}`
	_, _ = handle.WriteAt(ctx, []byte(data), 0)
	if stat, err := backend.Stat(ctx, e); err != nil || stat.Size != int64(len(data)) {
		t.Fatalf("dirty size hidden from path getattr: %d %v", stat.Size, err)
	}
	remote.FailNotebookUpdates(1)
	if err := handle.Flush(ctx); err == nil {
		t.Fatal("injected failed flush reported success")
	}
	if got := read(t, handle); got != data {
		t.Fatal("failed flush lost local bytes", got)
	}
	if remote.Counts().NotebookAttempts != 1 {
		t.Fatal("write was automatically retried")
	}
	if err := handle.Flush(ctx); err != nil {
		t.Fatal("explicit retry", err)
	}
	_ = handle.Flush(ctx)
	if remote.Counts().NotebookAttempts != 2 || remote.Counts().NotebookUpdates != 1 {
		t.Fatal("retry/deduplication failed", remote.Counts())
	}
	external := remote.Definition()
	external.Parts[0].Payload = base64.StdEncoding.EncodeToString([]byte(`{"external":"metadata edit"}`))
	remote.SetDefinition(external)
	_, _ = handle.WriteAt(ctx, []byte(" "), int64(len(data)))
	if err := handle.Flush(ctx); !errors.Is(err, fserrors.ErrConflict) {
		t.Fatalf("external part edit not detected: %v", err)
	}
	var recovery *writeback.RecoveryError
	if err := handle.Close(); !errors.As(err, &recovery) {
		t.Fatalf("conflict close did not preserve recovery data: %v", err)
	}
	recovered, err := os.ReadFile(recovery.Path)
	if err != nil || string(recovered) != data+" " {
		t.Fatalf("dirty data was discarded: %q %v", recovered, err)
	}
	if remote.Counts().NotebookUpdates != 1 {
		t.Fatal("conflicting writer overwrote remote data")
	}
}

func TestNotebookSaveComparisonDoesNotDecodeFreshDefinition(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	_, e := notebook(t, backend)
	handle, err := backend.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()

	before := backend.SnapshotStats()
	reads := remote.Counts().DefinitionReads
	save(t, handle, `{"nbformat":4,"cells":[],"metadata":{"saved":true}}`)
	after := backend.SnapshotStats()

	if remote.Counts().DefinitionReads != reads+1 {
		t.Fatal("save did not force a fresh definition comparison")
	}
	if after.Decodes != before.Decodes {
		t.Fatalf("save comparison decoded a definition: before=%+v after=%+v", before, after)
	}
	if remote.Counts().NotebookUpdates != 1 {
		t.Fatal("save did not update the notebook")
	}
}

func TestInvalidNotebookIsVisibleAndCanBeCorrected(t *testing.T) {
	backend, _ := newTestFS(t, nil)
	_, e := notebook(t, backend)
	handle, err := backend.Open(context.Background(), e, os.O_RDWR|os.O_TRUNC)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = handle.WriteAt(context.Background(), []byte("partial JSON"), 0)
	if err := handle.Flush(context.Background()); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("invalid ipynb uploaded: %v", err)
	}
	save(t, handle, testutil.InitialNotebook)
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefinitionValidationAndEnvironmentTree(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	dir := item(t, backend, "Environments", "Sample environment", testutil.EnvironmentID)
	libraries := lookup(t, backend, dir, "Libraries")
	public := lookup(t, backend, libraries, "PublicLibraries")
	config := lookup(t, backend, public, "environment.yml")
	h, err := backend.Open(context.Background(), config, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, h) != "dependencies: []\n" {
		t.Fatal("environment definition did not come from real parts")
	}
	_ = h.Close()
	for _, parts := range [][]fabric.Part{
		{{Path: "../../evil", Payload: "eA==", PayloadType: "InlineBase64"}},
		{{Path: "a", Payload: "eA==", PayloadType: "InlineBase64"}, {Path: "a/b", Payload: "eA==", PayloadType: "InlineBase64"}},
		{{Path: "a", Payload: "eA==", PayloadType: "InlineBase64"}, {Path: "a", Payload: "eA==", PayloadType: "InlineBase64"}},
	} {
		remote.SetEnvironment(fabric.Definition{Parts: parts})
		if _, err := backend.ReadDir(context.Background(), public); err == nil {
			t.Fatalf("unsafe definition accepted: %v", err)
		}
	}
}

func TestLakeWritebackOffsetTruncateAndConditionalRetry(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	_, files, _ := lakeRoots(t, backend)
	dir, err := backend.Mkdir(context.Background(), files, "new-dir")
	if err != nil {
		t.Fatal(err)
	}
	e, handle, err := backend.Create(context.Background(), dir, "data.bin", os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := handle.WriteAt(ctx, []byte("abc"), 2); err != nil {
		t.Fatal(err)
	}
	if err := handle.Truncate(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if got := read(t, handle); got != "\x00\x00abc\x00\x00" {
		t.Fatalf("offset/sparse/truncate bytes=%q", got)
	}
	remote.FailStorageCommits(1)
	if err := handle.Flush(ctx); err == nil {
		t.Fatal("failed conditional commit not reported")
	}
	if got := read(t, handle); got != "\x00\x00abc\x00\x00" {
		t.Fatal("failed lake flush lost data")
	}
	if err := handle.Flush(ctx); err != nil {
		t.Fatal("lake retry", err)
	}
	count := remote.Counts().Renames
	for range 3 {
		if err := handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if remote.Counts().Renames != count {
		t.Fatal("duplicate flush reuploaded OneLake data")
	}
	if _, err := backend.Rename(ctx, e, dir, "renamed.bin", false); !errors.Is(err, fserrors.ErrBusy) {
		t.Fatalf("open remote file was renamed: %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	renamed, err := backend.Rename(ctx, e, dir, "renamed.bin", false)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := backend.Open(ctx, renamed, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if got := read(t, reader); got != "\x00\x00abc\x00\x00" {
		t.Fatal("rename lost data", got)
	}
	if err := backend.Remove(ctx, renamed, false); !errors.Is(err, fserrors.ErrBusy) {
		t.Fatalf("unlink-open must be explicit unsupported/busy: %v", err)
	}
	_ = reader.Close()
	if err := backend.Remove(ctx, dir, true); err == nil {
		t.Fatal("rmdir silently removed children")
	}
	if err := backend.Remove(ctx, renamed, false); err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(ctx, dir, true); err != nil {
		t.Fatal(err)
	}
	if _, found := remote.File("Files/new-dir"); found {
		t.Fatal("rmdir not persisted")
	}
}

func TestLakeConflictKeepsSpoolAndExternalBytes(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	_, files, _ := lakeRoots(t, backend)
	e := lookup(t, backend, files, "demo.txt")
	h, err := backend.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = h.WriteAt(context.Background(), []byte("local"), 0)
	remote.SetFile("Files/demo.txt", []byte("external"))
	err = h.Flush(context.Background())
	var response *transport.HTTPError
	if !errors.Is(err, fserrors.ErrConflict) && !(errors.As(err, &response) && response.StatusCode == 412) {
		t.Fatalf("expected conditional conflict: %v", err)
	}
	if data, _ := remote.File("Files/demo.txt"); string(data) != "external" {
		t.Fatal("conditional save overwrote external data")
	}
	var recovery *writeback.RecoveryError
	if err := h.Close(); !errors.As(err, &recovery) {
		t.Fatal("lost conflicting local file", err)
	}
}

func TestReadOnlyBoundariesAcrossEveryEntryPoint(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	notebookDir, content := notebook(t, backend)
	platform := Entry{Name: ".platform", Kind: DefinitionFile, Workspace: notebookDir.Workspace, Item: notebookDir.Item, Part: ".platform"}
	lake, files, tables := lakeRoots(t, backend)
	env := item(t, backend, "Environments", "Sample environment", testutil.EnvironmentID)
	envPlatform := Entry{Name: ".platform", Kind: DefinitionFile, Workspace: env.Workspace, Item: env.Item, Part: ".platform"}
	ctx := context.Background()
	for _, parent := range []Entry{notebookDir, env} {
		if _, err := backend.Lookup(ctx, parent, ".platform"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("hidden platform remained lookupable", err)
		}
	}
	for _, e := range []Entry{platform, envPlatform} {
		if _, err := backend.Open(ctx, e, os.O_RDONLY); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("forged hidden platform entry remained readable", err)
		}
	}
	original := remote.Counts()
	for _, e := range []Entry{platform, envPlatform} {
		if _, err := backend.Open(ctx, e, os.O_WRONLY); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Errorf("write-open accepted %s: %v", e.Name, err)
		}
		if err := backend.Truncate(ctx, e, 0); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Errorf("truncate accepted %s: %v", e.Name, err)
		}
	}
	for _, dir := range []Entry{backend.Root(), notebookDir, lake, env, tables} {
		if _, _, err := backend.Create(ctx, dir, "new", os.O_RDWR); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Errorf("create accepted %v: %v", dir.Name, err)
		}
		if _, err := backend.Mkdir(ctx, dir, "new"); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Errorf("mkdir accepted %v: %v", dir.Name, err)
		}
	}
	for _, e := range []Entry{files, tables, platform, envPlatform, content} {
		if err := backend.Remove(ctx, e, e.Directory); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Errorf("remove accepted %s: %v", e.Name, err)
		}
		if _, err := backend.Rename(ctx, e, files, "moved", false); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Errorf("rename accepted %s: %v", e.Name, err)
		}
	}
	if remote.Counts() != original {
		t.Fatal("read-only rejection made a remote write")
	}
	file := lookup(t, backend, files, "demo.txt")
	for _, target := range []Entry{tables, env, notebookDir, lake} {
		if _, err := backend.Rename(ctx, file, target, "overwritten", false); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Errorf("rename target boundary not enforced %s: %v", target.Name, err)
		}
	}
}

func TestFilesAccessDoesNotProbeUnauthorizedTables(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	remote.DenyTables(true)
	lake := item(t, backend, "Lakehouses", "Sample lakehouse", testutil.LakehouseID)
	entries, err := backend.ReadDir(context.Background(), lake)
	if err != nil || len(entries) != 3 {
		t.Fatalf("known protected root list failed: %v", err)
	}
	files := lookup(t, backend, lake, "Files")
	if _, err := backend.ReadDir(context.Background(), files); err != nil {
		t.Fatal(err)
	}
	file := lookup(t, backend, files, "demo.txt")
	handle, err := backend.Open(context.Background(), file, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, handle) != "0123456789" {
		t.Fatal("Files read unexpectedly failed")
	}
	save(t, handle, "authorized Files write")
	_ = handle.Close()
	if remote.Counts().TablesRequests != 0 {
		t.Fatal("accessing Files probed unauthorized Tables")
	}
	_, err = backend.Lookup(context.Background(), lake, "Tables")
	var response *transport.HTTPError
	if !errors.As(err, &response) || response.StatusCode != 403 {
		t.Fatalf("actual Tables permission error was swallowed: %v", err)
	}
}

func TestGlobalReadonlyLimitsAndConcurrentWriters(t *testing.T) {
	ro, _ := newTestFS(t, func(opts *Options) { opts.ReadOnly = true })
	_, e := notebook(t, ro)
	if _, err := ro.Open(context.Background(), e, os.O_WRONLY); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("global readonly bypass", err)
	}
	backend, _ := newTestFS(t, func(opts *Options) { opts.MaxFileSize = 10 })
	_, files, _ := lakeRoots(t, backend)
	demo := lookup(t, backend, files, "demo.txt")
	ctx := context.Background()
	handle, err := backend.Open(ctx, demo, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.WriteAt(ctx, []byte("overflow"), 8); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal("file limit not enforced", err)
	}
	if err := handle.Truncate(ctx, 11); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal("truncate limit not enforced", err)
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if second, err := backend.Open(ctx, demo, os.O_RDWR); !errors.Is(err, fserrors.ErrBusy) {
				if second != nil {
					_ = second.Close()
				}
				t.Errorf("simultaneous writer: %v", err)
			}
		})
	}
	wg.Wait()
	_ = handle.Close()
	other := files
	other.Item.ID = testutil.EnvironmentID
	if _, err := backend.Rename(ctx, demo, other, "destination", false); !errors.Is(err, fserrors.ErrCrossDevice) {
		t.Fatal("cross-item rename not rejected", err)
	}
	for _, unsafe := range []string{"..", "%2E%2E", "%2f", `a\b`, "a/b"} {
		if _, _, err := backend.Create(ctx, files, unsafe, os.O_RDWR); err == nil {
			t.Error("unsafe path accepted", unsafe)
		}
	}
}

func TestLiteralEscapesAreFilesNotTraversal(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	_, files, _ := lakeRoots(t, backend)
	for _, raw := range []string{"%2e%2e", "%5C", "%00", "50% complete", "\u6570\u636e.csv"} {
		name, err := namespace.FileName(raw)
		if err != nil {
			t.Fatal(err)
		}
		e, handle, err := backend.Create(context.Background(), files, name, os.O_RDWR|os.O_EXCL)
		if err != nil {
			t.Fatalf("create literal %q as %q: %v", raw, name, err)
		}
		save(t, handle, raw)
		_ = handle.Close()
		looked := lookup(t, backend, files, name)
		if looked.Remote != "Files/"+raw || e.Remote != looked.Remote {
			t.Fatal("literal encoded name became a control path", e.Remote)
		}
		if data, found := remote.File("Files/" + raw); !found || string(data) != raw {
			t.Fatalf("literal remote file not round-tripped: %q %v", data, found)
		}
	}
	entries, err := backend.ReadDir(context.Background(), files)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(entries); i++ {
		if strings.Compare(entries[i-1].Name, entries[i].Name) >= 0 {
			t.Fatal("unstable or duplicate readdir entries")
		}
	}
}
