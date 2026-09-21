//go:build linux

package fusefs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"
)

func requireSize(t *testing.T, path string, size int64, handles ...*os.File) {
	t.Helper()
	if info, err := os.Stat(path); err != nil || info.Size() != size {
		t.Fatalf("stat %s: %v %v; want size %d", path, info, err, size)
	}
	for _, handle := range handles {
		if info, err := handle.Stat(); err != nil || info.Size() != size {
			t.Fatalf("fstat %s: %v %v; want size %d", handle.Name(), info, err, size)
		}
	}
}

func requireMissing(t *testing.T, path string) {
	t.Helper()
	for range 3 {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected negative lookup for %s: %v", path, err)
		}
	}
}

func requirePinned(t *testing.T, handle *os.File, want string) {
	t.Helper()
	data := make([]byte, len(want)+1)
	n, err := handle.ReadAt(data, 0)
	if string(data[:n]) != want || (err != nil && !errors.Is(err, io.EOF)) {
		t.Fatalf("pinned reader changed: %q %v; want %q", data[:n], err, want)
	}
}

func TestMountedKernelCacheNotebookListingAndFirstSize(t *testing.T) {
	m := mountFixture(t, false)
	before := m.service.Counts().DefinitionReads
	entries, err := os.ReadDir(m.notebookDir)
	if err != nil || len(entries) != 3 || entries[0].Name() != ".fabric.json" ||
		entries[1].Name() != "builtin" || entries[2].Name() != "Sample notebook.ipynb" {
		t.Fatalf("fixed notebook roots: %v %v", entries, err)
	}
	if info, err := os.Stat(filepath.Join(m.notebookDir, "builtin")); err != nil || !info.IsDir() {
		t.Fatal("builtin root requires a provider", info, err)
	}
	if _, err := os.ReadDir(filepath.Join(m.notebookDir, "builtin")); !errors.Is(err, syscall.ENOTSUP) {
		t.Fatal("entering disabled builtin must report unsupported, not an empty directory", err)
	}
	requireMissing(t, filepath.Join(m.notebookDir, "env"))
	requireMissing(t, filepath.Join(m.notebookDir, ".platform"))
	plain, err := exec.Command("ls", m.notebookDir).CombinedOutput()
	if err != nil || !strings.Contains(string(plain), filepath.Base(m.notebook)) || !strings.Contains(string(plain), "builtin") {
		t.Fatalf("plain notebook ls: %s %v", plain, err)
	}
	if after := m.service.Counts().DefinitionReads; after != before {
		t.Fatalf("ordinary readdir/plain ls/fixed-root lookup exported a notebook: %d -> %d", before, after)
	}
	long, err := exec.Command("ls", "-la", m.notebookDir).CombinedOutput()
	if err != nil {
		t.Fatalf("notebook ls -la: %s %v", long, err)
	}
	found := false
	for _, line := range strings.Split(string(long), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 9 && fields[len(fields)-1] == filepath.Base(m.notebook) {
			size, err := strconv.ParseInt(fields[4], 10, 64)
			if err != nil || size != int64(len(testutil.InitialNotebook)) {
				t.Fatalf("first ls -la used an unknown or stale regular-file size: %q %v", line, err)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("notebook content missing from ls -la: %s", long)
	}
	requireSize(t, m.notebook, int64(len(testutil.InitialNotebook)))
	if after := m.service.Counts().DefinitionReads; after != before+1 {
		t.Fatalf("accurate ls -la/stat should share one export: %d -> %d", before, after)
	}
}

func TestMountedKernelCacheNotebookSavesAndLiveAttributes(t *testing.T) {
	m := mountFixture(t, false)
	requireSize(t, m.notebook, int64(len(testutil.InitialNotebook)))
	reader, err := os.Open(m.notebook)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	requirePinned(t, reader, testutil.InitialNotebook)
	writer, err := os.OpenFile(m.notebook, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	for _, edited := range []string{
		`{"nbformat":4,"cells":[]}`,
		`{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"larger":"second save on the same open writer"}}`,
	} {
		if _, err := writer.WriteAt([]byte(edited), 0); err != nil {
			t.Fatal(err)
		}
		if err := writer.Truncate(int64(len(edited))); err != nil {
			t.Fatal(err)
		}
		requireSize(t, m.notebook, int64(len(edited)), writer, reader)
		requirePinned(t, reader, testutil.InitialNotebook)
		if err := writer.Sync(); err != nil {
			t.Fatal(err)
		}
		commits := m.service.Counts().NotebookUpdates
		for range 3 {
			if err := writer.Sync(); err != nil {
				t.Fatal(err)
			}
			requireSize(t, m.notebook, int64(len(edited)), writer, reader)
		}
		if m.service.Counts().NotebookUpdates != commits {
			t.Fatal("clean fsync repeated an upload")
		}
		readFile(t, m.notebook, edited)
	}
	closeFile(t, writer)
	requirePinned(t, reader, testutil.InitialNotebook)
	writer, err = os.OpenFile(m.notebook, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal("cached notebook O_TRUNC open", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	requireSize(t, m.notebook, 0, writer, reader)
	edited := `{"nbformat":4,"cells":[],"metadata":{"save":"after O_TRUNC"}}`
	if _, err := writer.Write([]byte(edited)); err != nil {
		t.Fatal(err)
	}
	requireSize(t, m.notebook, int64(len(edited)), writer, reader)
	closeFile(t, writer)
	requireSize(t, m.notebook, int64(len(edited)), reader)
	requirePinned(t, reader, testutil.InitialNotebook)
	readFile(t, m.notebook, edited)
	if m.service.Counts().NotebookUpdates != 3 {
		t.Fatal("save count includes an intermediate truncation or duplicate flush", m.service.Counts())
	}
}

func TestMountedKernelCacheCreateMkdirRenameRemove(t *testing.T) {
	m := mountFixture(t, false)
	directory := filepath.Join(m.files, "cached-directory")
	requireMissing(t, directory)
	if err := os.Mkdir(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(directory); err != nil || !info.IsDir() {
		t.Fatal("mkdir left a negative dentry", info, err)
	}
	source := filepath.Join(directory, "source")
	requireMissing(t, source)
	writer, err := os.OpenFile(source, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	requireSize(t, source, 0, writer)
	if _, err := writer.WriteAt([]byte("saved-content"), 0); err != nil {
		t.Fatal(err)
	}
	requireSize(t, source, 13, writer)
	if err := writer.Truncate(5); err != nil {
		t.Fatal(err)
	}
	requireSize(t, source, 5, writer)
	closeFile(t, writer)
	readFile(t, source, "saved")
	destination := filepath.Join(directory, "negative-destination")
	requireMissing(t, destination)
	if err := os.Rename(source, destination); err != nil {
		t.Fatal(err)
	}
	requireMissing(t, source)
	requireSize(t, destination, 5)
	readFile(t, destination, "saved")
	if err := os.WriteFile(source, []byte("replacement-is-longer"), 0644); err != nil {
		t.Fatal("create at cached old rename source", err)
	}
	readFile(t, destination, "saved")
	if err := os.Rename(source, destination); err != nil {
		t.Fatal("rename over positive cached destination", err)
	}
	requireMissing(t, source)
	requireSize(t, destination, int64(len("replacement-is-longer")))
	readFile(t, destination, "replacement-is-longer")
	renamedDirectory := filepath.Join(m.files, "renamed-cached-directory")
	requireMissing(t, renamedDirectory)
	if err := os.Rename(directory, renamedDirectory); err != nil {
		t.Fatal("rename populated cached directory", err)
	}
	requireMissing(t, directory)
	destination = filepath.Join(renamedDirectory, "negative-destination")
	readFile(t, destination, "replacement-is-longer")
	if err := os.Remove(destination); err != nil {
		t.Fatal(err)
	}
	requireMissing(t, destination)
	if err := os.WriteFile(destination, []byte("recreated"), 0644); err != nil {
		t.Fatal("recreate at cached negative dentry", err)
	}
	readFile(t, destination, "recreated")
	if err := os.Remove(destination); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(renamedDirectory); err != nil {
		t.Fatal(err)
	}
	requireMissing(t, renamedDirectory)
	if err := os.Mkdir(renamedDirectory, 0755); err != nil {
		t.Fatal("mkdir at cached rmdir negative dentry", err)
	}
	if info, err := os.Stat(renamedDirectory); err != nil || !info.IsDir() {
		t.Fatal("recreated directory is not immediately visible", info, err)
	}
}

func TestMountedKernelCacheConflictPreservesDirtySpool(t *testing.T) {
	m := mountFixture(t, false)
	readFile(t, m.notebook, testutil.InitialNotebook)
	writer, err := os.OpenFile(m.notebook, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	local := `{"nbformat":4,"cells":[],"metadata":{"local":"do not lose this"}}`
	if _, err := writer.WriteAt([]byte(local), 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.Truncate(int64(len(local))); err != nil {
		t.Fatal(err)
	}
	remote := `{"nbformat":4,"cells":[],"metadata":{"remote":"a different editor won"}}`
	definition := m.service.Definition()
	for index := range definition.Parts {
		if definition.Parts[index].Path == "notebook-content.ipynb" {
			definition.Parts[index].Payload = base64.StdEncoding.EncodeToString([]byte(remote))
		}
	}
	m.service.SetDefinition(definition)
	if err := writer.Sync(); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("conflict not surfaced by fsync: %v", err)
	}
	requireSize(t, m.notebook, int64(len(local)), writer)
	requirePinned(t, writer, local)
	if err := writer.Close(); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("conflict not surfaced by close/flush: %v", err)
	}
	waitInvalidations(t, m.server.(*mountedServer).notifications)
	files, err := os.ReadDir(m.spool)
	if err != nil || len(files) != 1 {
		t.Fatal("conflicted writer did not retain exactly one recovery spool", files, err)
	}
	readFile(t, filepath.Join(m.spool, files[0].Name()), local)
	readFile(t, m.notebook, remote)
	if m.service.Counts().NotebookAttempts != 0 {
		t.Fatal("conflict overwrote the remote notebook", m.service.Counts())
	}
}

type ambiguousLake struct {
	workspacefs.LakeAPI
	mu   sync.Mutex
	next string
}

func (l *ambiguousLake) arm(operation string) {
	l.mu.Lock()
	l.next = operation
	l.mu.Unlock()
}

func (l *ambiguousLake) after(operation string, err error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil && l.next == operation {
		l.next = ""
		return syscall.EIO
	}
	return err
}

func (l *ambiguousLake) Mkdir(ctx context.Context, path onelake.Path) error {
	return l.after("mkdir", l.LakeAPI.Mkdir(ctx, path))
}

func (l *ambiguousLake) Put(ctx context.Context, path onelake.Path, source io.ReaderAt, size int64, etag string) (onelake.Info, error) {
	info, err := l.LakeAPI.Put(ctx, path, source, size, etag)
	return info, l.after("put", err)
}

func (l *ambiguousLake) Remove(ctx context.Context, path onelake.Path, directory bool, etag string) error {
	return l.after("remove", l.LakeAPI.Remove(ctx, path, directory, etag))
}

func (l *ambiguousLake) Rename(ctx context.Context, source, destination onelake.Path, etag, destinationETag string, noReplace bool) (onelake.Info, error) {
	info, err := l.LakeAPI.Rename(ctx, source, destination, etag, destinationETag, noReplace)
	return info, l.after("rename", err)
}

func TestMountedKernelCacheAmbiguousMutationErrorsInvalidate(t *testing.T) {
	var lake *ambiguousLake
	m := mountFixtureClients(t, false, nil, func(fab workspacefs.FabricAPI, base workspacefs.LakeAPI) (workspacefs.FabricAPI, workspacefs.LakeAPI) {
		lake = &ambiguousLake{LakeAPI: base}
		return fab, lake
	})
	await := func() { waitInvalidations(t, m.server.(*mountedServer).notifications) }
	directory := filepath.Join(m.files, "ambiguous-directory")
	requireMissing(t, directory)
	lake.arm("mkdir")
	if err := os.Mkdir(directory, 0755); !errors.Is(err, syscall.EIO) {
		t.Fatal("ambiguous mkdir was success-shaped", err)
	}
	// Notifications cannot be awaited inside their originating FUSE request.
	// Await the actual worker acknowledgement here, never the two-minute TTL.
	await()
	if info, err := os.Stat(directory); err != nil || !info.IsDir() {
		t.Fatal("ambiguous mkdir retained a negative dentry", info, err)
	}
	source := filepath.Join(directory, "ambiguous-create")
	requireMissing(t, source)
	lake.arm("put")
	if err := os.WriteFile(source, []byte("not sent"), 0644); !errors.Is(err, syscall.EIO) {
		t.Fatal("ambiguous create was success-shaped", err)
	}
	await()
	requireSize(t, source, 0)
	if err := os.WriteFile(source, []byte("remote bytes"), 0644); err != nil {
		t.Fatal(err)
	}
	await()
	destination := filepath.Join(directory, "ambiguous-destination")
	requireMissing(t, destination)
	lake.arm("rename")
	if err := os.Rename(source, destination); !errors.Is(err, syscall.EIO) {
		t.Fatal("ambiguous rename was success-shaped", err)
	}
	await()
	requireMissing(t, source)
	readFile(t, destination, "remote bytes")
	lake.arm("remove")
	// os.Remove retries a failed unlink as rmdir and may replace the first
	// EIO with ENOENT after notification. Test each wire operation directly.
	if err := syscall.Unlink(destination); !errors.Is(err, syscall.EIO) {
		t.Fatal("ambiguous remove was success-shaped", err)
	}
	await()
	requireMissing(t, destination)
	lake.arm("remove")
	if err := syscall.Rmdir(directory); !errors.Is(err, syscall.EIO) {
		t.Fatal("ambiguous rmdir was success-shaped", err)
	}
	await()
	requireMissing(t, directory)
}

func TestMountedKernelCachePathTruncateInvalidatesClosedFile(t *testing.T) {
	m := mountFixture(t, false)
	path := filepath.Join(m.files, "demo.txt")
	requireSize(t, path, 10)
	if err := os.Truncate(path, 3); err != nil {
		t.Fatal(err)
	}
	requireSize(t, path, 3)
	readFile(t, path, "012")
	if err := os.Truncate(path, 12); err != nil {
		t.Fatal(err)
	}
	requireSize(t, path, 12)
	readFile(t, path, "012"+string(make([]byte, 9)))
}

func TestMountedKernelCacheLifetimeIsNonzero(t *testing.T) {
	m := mountFixtureOptions(t, false, func(o *workspacefs.Options) { o.CacheTTL = 2 * time.Minute })
	path := filepath.Join(m.files, "demo.txt")
	requireSize(t, path, 10)
	before := m.service.Counts()
	for range 10 {
		requireSize(t, path, 10)
	}
	if after := m.service.Counts(); after != before {
		t.Fatalf("unchanged stats refetched remote state inside the cache lifetime: %+v -> %+v", before, after)
	}
}

type missingProofLake struct {
	workspacefs.LakeAPI
	deadline time.Time
	misses   atomic.Int32
}

func (l *missingProofLake) Stat(ctx context.Context, path onelake.Path) (onelake.Info, error) {
	info, err := l.LakeAPI.Stat(ctx, path)
	if workspacefs.IsNotExist(err) {
		l.misses.Add(1)
		err = fmt.Errorf("wrapped missing object: %w", cacheDeadlineError{deadline: l.deadline, cause: err})
	}
	return info, err
}

func TestMountedKernelCacheUnknownNegativeProofIsNotRetained(t *testing.T) {
	for _, name := range []string{"expired", "unknown"} {
		t.Run(name, func(t *testing.T) {
			var lake *missingProofLake
			m := mountFixtureClients(t, false, nil, func(fab workspacefs.FabricAPI, base workspacefs.LakeAPI) (workspacefs.FabricAPI, workspacefs.LakeAPI) {
				lake = &missingProofLake{LakeAPI: base}
				if name == "expired" {
					lake.deadline = time.Now().Add(-time.Second)
				}
				return fab, lake
			})
			path := filepath.Join(m.files, "source-proof-missing")
			requireMissing(t, path)
			if got := lake.misses.Load(); got != 3 {
				t.Fatalf("fresh parent attributes renewed an unproven negative dentry: %d source lookups, want 3", got)
			}
		})
	}
}
