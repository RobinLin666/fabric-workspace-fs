//go:build linux

package fusefs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"
)

type mounted struct {
	root        string
	workspace   string
	spool       string
	notebook    string
	notebookDir string
	files       string
	tables      string
	environment string
	service     *testutil.Service
	server      Server
}

func mountFixture(t *testing.T, readonly bool) *mounted {
	return mountFixtureOptions(t, readonly, nil)
}

func mountFixtureOptions(t *testing.T, readonly bool, configure func(*workspacefs.Options)) *mounted {
	return mountFixtureClients(t, readonly, configure, nil)
}

func mountFixtureClients(t *testing.T, readonly bool, configure func(*workspacefs.Options), wrap func(workspacefs.FabricAPI, workspacefs.LakeAPI) (workspacefs.FabricAPI, workspacefs.LakeAPI)) *mounted {
	t.Helper()
	if os.Getenv("FABRICFS_FUSE_TEST") != "1" {
		t.Skip("set FABRICFS_FUSE_TEST=1 to run real /dev/fuse tests against offline HTTP mocks")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Fatalf("explicit FUSE test requested but /dev/fuse is unavailable: %v", err)
	}
	service := testutil.New(t)
	fab, lake := service.Clients()
	temporary, err := os.MkdirTemp("/tmp", "fabric-workspace-fs-test-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(os.Stderr, "fabric-workspace-fs test runtime: %s\n", temporary); err != nil {
		t.Fatal(err)
	}
	root, spool := filepath.Join(temporary, "mount"), filepath.Join(temporary, "spool")
	var server Server
	var backend *workspacefs.FS
	var once sync.Once
	unmount := func() {
		once.Do(func() {
			if server != nil {
				if err := server.Unmount(); err != nil {
					t.Errorf("clean unmount failed: %v", err)
					reportOpenTestFiles(t, temporary)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					command := exec.CommandContext(ctx, "fusermount3", "-uz", root)
					if output, fallbackErr := command.CombinedOutput(); fallbackErr != nil {
						t.Errorf("emergency unmount failed; retaining %s: %s: %v", temporary, output, fallbackErr)
						return
					}
				}
				stopped := make(chan struct{})
				go func() { server.Wait(); close(stopped) }()
				select {
				case <-stopped:
				case <-time.After(5 * time.Second):
					t.Errorf("FUSE server did not exit; retaining %s", temporary)
					return
				}
			}
			if backend != nil {
				if err := backend.Close(); err != nil {
					t.Errorf("close overlay; retaining %s: %v", temporary, err)
					return
				}
			}
			// This exact private directory was created above. Never recurse
			// through a still-mounted FUSE tree or remove any other /tmp data.
			mounts, err := os.Open("/proc/self/mountinfo")
			if err != nil {
				t.Errorf("cannot verify unmount; retaining %s: %v", temporary, err)
				return
			}
			stillMounted, checkErr := hasMountedTree(mounts, temporary)
			_ = mounts.Close()
			if checkErr != nil || stillMounted {
				t.Errorf("mount cleanup not verified; retaining %s: mounted=%v err=%v", temporary, stillMounted, checkErr)
				return
			}
			if err := os.RemoveAll(temporary); err != nil {
				t.Errorf("cleanup %s: %v", temporary, err)
			}
		})
	}
	t.Cleanup(unmount)
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(spool, 0700); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(spool); err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("native Linux test spool is not private: %v %v", info, err)
	}
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs = []string{testutil.WorkspaceID}
	opts.SpoolDirectory = spool
	opts.ReadOnly = readonly
	if configure != nil {
		configure(&opts)
	}
	var fabricClient workspacefs.FabricAPI = fab
	var lakeClient workspacefs.LakeAPI = lake
	if wrap != nil {
		fabricClient, lakeClient = wrap(fabricClient, lakeClient)
	}
	backend, err = workspacefs.New(fabricClient, lakeClient, opts)
	if err != nil {
		t.Fatal(err)
	}
	server, err = Mount(root, backend, Options{ReadOnly: readonly, Logger: log.New(testLog{t: t}, "", 0)})
	if err != nil {
		t.Fatal("mount failed", err)
	}
	if err := server.WaitMount(); err != nil {
		t.Fatal("mount never became ready", err)
	}
	ws, _ := namespace.CatalogName("Sample workspace", testutil.WorkspaceID)
	nb, _ := namespace.CatalogName("Sample notebook", testutil.NotebookID)
	lh, _ := namespace.CatalogName("Sample lakehouse", testutil.LakehouseID)
	env, _ := namespace.CatalogName("Sample environment", testutil.EnvironmentID)
	workspace := filepath.Join(root, ws)
	notebookDir := filepath.Join(workspace, nb+".Notebook")
	return &mounted{
		root: root, workspace: workspace, spool: opts.SpoolDirectory, notebook: filepath.Join(notebookDir, namespace.NotebookContentName),
		notebookDir: notebookDir, files: filepath.Join(workspace, lh+".Lakehouse", "Files"),
		tables:      filepath.Join(workspace, lh+".Lakehouse", "Tables"),
		environment: filepath.Join(workspace, env+".Environment"), service: service, server: server,
	}
}

func hasMountedTree(reader io.Reader, directory string) (bool, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			return false, errors.New("malformed mountinfo entry")
		}
		point := unescape.Replace(fields[4])
		if point == directory || strings.HasPrefix(point, directory+"/") {
			return true, nil
		}
	}
	return false, scanner.Err()
}

func reportOpenTestFiles(t *testing.T, temporary string) {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Logf("inspect test process descriptors: %v", err)
		return
	}
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err == nil && (strings.Contains(target, temporary) || target == "/dev/fuse") {
			t.Logf("retained test descriptor %s -> %s", fd.Name(), target)
		}
	}
}

type testLog struct{ t *testing.T }

func (l testLog) Write(p []byte) (int, error) {
	l.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func readFile(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != want {
		t.Fatalf("read %s = %q, %v; want %q", path, data, err, want)
	}
}

func closeFile(t *testing.T, file *os.File) {
	t.Helper()
	if err := file.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestMountedNotebookOpenTruncate(t *testing.T) {
	m := mountFixture(t, false)
	readFile(t, m.notebook, testutil.InitialNotebook)
	edited := `{"cells":[],"nbformat":4,"nbformat_minor":5,"metadata":{"via":"O_TRUNC"}}`
	if err := os.WriteFile(m.notebook, []byte(edited), 0644); err != nil {
		t.Fatalf("in-place O_TRUNC notebook save: %v", err)
	}
	readFile(t, m.notebook, edited)
	if m.service.Counts().NotebookUpdates != 1 {
		t.Fatal("O_TRUNC committed intermediate empty content or duplicate update", m.service.Counts())
	}
}

func TestMountedNotebookFsyncOffsetTruncateAndFlushFailure(t *testing.T) {
	m := mountFixture(t, false)
	entries, err := os.ReadDir(m.notebookDir)
	if err != nil || len(entries) != 3 || entries[0].Name() != ".fabric.json" || entries[1].Name() != "builtin" || entries[2].Name() != "content.ipynb" {
		t.Fatalf("notebook readdir: %v %v", entries, err)
	}
	h, err := os.OpenFile(m.notebook, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	edited := `{"cells":[],"nbformat":4,"metadata":{"local":true}}`
	if _, err := h.WriteAt([]byte(edited), 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(int64(len(edited))); err != nil {
		t.Fatal(err)
	}
	stat, err := h.Stat()
	if err != nil || stat.Size() != int64(len(edited)) {
		t.Fatalf("dirty inode size not visible via fstat: %v %v", stat, err)
	}
	got := make([]byte, len(edited))
	if _, err := h.ReadAt(got, 0); err != nil || string(got) != edited {
		t.Fatalf("read own notebook writes: %q %v", got, err)
	}
	m.service.FailNotebookUpdates(1)
	if err := h.Sync(); err == nil {
		t.Fatal("fsync hid failed remote update")
	}
	if stat, err := h.Stat(); err != nil || stat.Size() != int64(len(edited)) {
		t.Fatalf("failed flush lost dirty fstat: %v %v", stat, err)
	}
	if stat, err := os.Stat(m.notebook); err != nil || stat.Size() != int64(len(edited)) {
		t.Fatalf("failed flush lost dirty path size: %v %v", stat, err)
	}
	if _, err := h.ReadAt(got, 0); err != nil || string(got) != edited {
		t.Fatalf("failed flush lost dirty bytes: %q %v", got, err)
	}
	if m.service.Counts().NotebookAttempts != 1 {
		t.Fatal("write response was automatically retried")
	}
	if err := h.Sync(); err != nil {
		t.Fatal("explicit fsync retry failed", err)
	}
	dup, err := syscall.Dup(int(h.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Close(dup); err != nil {
		t.Fatal("duplicate-fd flush", err)
	}
	second, err := os.OpenFile(m.notebook, os.O_WRONLY, 0)
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("dup flush released a still-open writer lease: %v", err)
	}
	for range 3 {
		if err := h.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	closeFile(t, h)
	readFile(t, m.notebook, edited)
	if m.service.Counts().NotebookUpdates != 1 || m.service.Counts().NotebookAttempts != 2 {
		t.Fatal("dup/close/fsync duplicated updates", m.service.Counts())
	}
}

func TestMountedLakehouseCRUDAndConditionalCommit(t *testing.T) {
	m := mountFixture(t, false)
	readFile(t, filepath.Join(m.files, "demo.txt"), "0123456789")
	dir := filepath.Join(m.files, "created-dir")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(dir, "editor.tmp")
	h, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.WriteAt([]byte("abcdef"), 3); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(7); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(temp); err != nil || info.Size() != 7 {
		t.Fatalf("path getattr missing dirty lake size: %v %v", info, err)
	}
	m.service.FailStorageCommits(1)
	if err := h.Sync(); err == nil {
		t.Fatal("lake fsync hid injected commit failure")
	}
	if err := h.Sync(); err != nil {
		t.Fatal("lake fsync retry failed", err)
	}
	renames := m.service.Counts().Renames
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	closeFile(t, h)
	if m.service.Counts().Renames != renames {
		t.Fatal("repeated clean flush reuploaded the lake file")
	}
	target := filepath.Join(dir, "saved.txt")
	if err := os.WriteFile(target, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temp, target); err != nil {
		t.Fatal("editor-style conditional replace", err)
	}
	readFile(t, target, "\x00\x00\x00abcd")
	if err := os.WriteFile(target, []byte("O_TRUNC replacement"), 0644); err != nil {
		t.Fatal("lake O_TRUNC save", err)
	}
	readFile(t, target, "O_TRUNC replacement")
	if err := os.Remove(dir); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("rmdir nonempty = %v", err)
	}
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rmdir not persisted: %v", err)
	}
}

func TestMountedReadonlyAndUnsupportedOperations(t *testing.T) {
	m := mountFixture(t, false)
	readFile(t, filepath.Join(m.environment, "Libraries", "PublicLibraries", "environment.yml"), "dependencies: []\n")
	readFile(t, filepath.Join(m.tables, "table", "part.parquet"), "read-only table bytes")
	mustFail := func(operation string, fn func() error) {
		t.Helper()
		if err := fn(); err == nil {
			t.Fatalf("%s silently succeeded", operation)
		}
	}
	for _, path := range []string{
		filepath.Join(m.notebookDir, ".platform"),
		filepath.Join(m.environment, ".platform"),
		filepath.Join(m.environment, "Setting", "Sparkcompute.yml"),
		filepath.Join(m.tables, "table", "part.parquet"),
	} {
		mustFail("readonly write", func() error { return os.WriteFile(path, []byte("must not persist"), 0644) })
		mustFail("readonly truncate", func() error { return os.Truncate(path, 0) })
		mustFail("readonly unlink", func() error { return os.Remove(path) })
		mustFail("readonly rename", func() error { return os.Rename(path, filepath.Join(m.files, "bypass")) })
		mustFail("chmod", func() error { return os.Chmod(path, 0777) })
	}
	for _, root := range []string{m.root, m.notebookDir, m.environment, m.tables} {
		mustFail("protected create", func() error { return os.WriteFile(filepath.Join(root, "new"), nil, 0644) })
		mustFail("protected mkdir", func() error { return os.Mkdir(filepath.Join(root, "new-dir"), 0755) })
	}
	for _, root := range []string{m.notebookDir, m.environment, m.tables, m.files} {
		mustFail("protected root remove", func() error { return os.Remove(root) })
		mustFail("protected root rename", func() error { return os.Rename(root, root+"-renamed") })
	}
	regular := filepath.Join(m.files, "demo.txt")
	mustFail("symlink", func() error { return os.Symlink(regular, filepath.Join(m.files, "link")) })
	mustFail("hardlink", func() error { return os.Link(regular, filepath.Join(m.files, "hardlink")) })
	mustFail("chmod", func() error { return os.Chmod(regular, 0600) })
	mustFail("chown", func() error { return os.Chown(regular, os.Getuid(), os.Getgid()) })
	mustFail("utimes", func() error { return os.Chtimes(regular, time.Now(), time.Now()) })
	mustFail("xattr", func() error { return syscall.Setxattr(regular, "user.example", []byte("value"), 0) })
	mustFail("notebook atomic save", func() error {
		return os.Rename(regular, m.notebook)
	})
}

func TestMountedReadOnlyMountAndTablesIsolation(t *testing.T) {
	m := mountFixture(t, true)
	m.service.DenyTables(true)
	readFile(t, filepath.Join(m.files, "demo.txt"), "0123456789")
	if m.service.Counts().TablesRequests != 0 {
		t.Fatal("Files lookup probed Tables")
	}
	if _, err := os.Stat(m.tables); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("Tables 403 mapped to %v", err)
	}
	if err := os.WriteFile(m.notebook, []byte(testutil.InitialNotebook), 0644); !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) {
		t.Fatal("read-only mount is writable", err)
	}
	if err := os.WriteFile(filepath.Join(m.files, "new"), nil, 0644); !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) {
		t.Fatal("read-only mount allowed lake create", err)
	}
}

func TestMountedCloseErrorPreservesRecovery(t *testing.T) {
	m := mountFixture(t, false)
	m.service.FailNotebookUpdates(100)
	h, err := os.OpenFile(m.notebook, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	data := `{"nbformat":4,"cells":[],"metadata":{"recovery":"only-copy"}}`
	if _, err := h.WriteAt([]byte(data), 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err == nil {
		t.Fatal("close/flush hid failed save")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		files, err := os.ReadDir(m.spool)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == 1 {
			recovered, err := os.ReadFile(filepath.Join(m.spool, files[0].Name()))
			if err == nil && string(recovered) == data {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("unsaved bytes not preserved in the spool directory")
		}
		time.Sleep(10 * time.Millisecond)
	}
	readFile(t, m.notebook, testutil.InitialNotebook)
}

func TestMountRejectsNonemptyDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "existing"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Mount(dir, nil, Options{}); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("nonempty mountpoint accepted: %v", err)
	}
}

func TestErrnoMapping(t *testing.T) {
	for _, code := range []syscall.Errno{syscall.ENOSPC, syscall.EIO, syscall.EACCES} {
		if got := errno(fmt.Errorf("wrapped: %w", code)); got != code {
			t.Fatalf("errno(%v)=%v", code, got)
		}
	}
	if errno(io.ErrUnexpectedEOF) != syscall.EIO {
		t.Fatal("unexpected EOF must fail IO")
	}
}
