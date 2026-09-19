//go:build windows || (darwin && cgo)

package fusefs

import (
	"encoding/base64"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"

	"github.com/winfsp/cgofuse/fuse"
)

func portableFixture(t *testing.T, readOnly bool) (*portableFS, *testutil.Service) {
	t.Helper()
	service := testutil.New(t)
	fab, lake := service.Clients()
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs = []string{testutil.WorkspaceID}
	opts.SpoolDirectory = t.TempDir()
	opts.CacheTTL = 0
	opts.ReadOnly = readOnly
	backend, err := workspacefs.New(fab, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close backend: %v", err)
		}
	})
	return newPortableFS(backend, log.New(io.Discard, "", 0)), service
}

func portablePaths(t *testing.T) (workspace, notebook, files, tables, environment string) {
	t.Helper()
	workspace, _ = namespace.CatalogName("Sample workspace", testutil.WorkspaceID)
	notebookName, _ := namespace.CatalogName("Sample notebook", testutil.NotebookID)
	lakehouseName, _ := namespace.CatalogName("Sample lakehouse", testutil.LakehouseID)
	environmentName, _ := namespace.CatalogName("Sample environment", testutil.EnvironmentID)
	notebook = "/" + workspace + "/" + notebookName + ".Notebook"
	files = "/" + workspace + "/" + lakehouseName + ".Lakehouse/Files"
	tables = "/" + workspace + "/" + lakehouseName + ".Lakehouse/Tables"
	environment = "/" + workspace + "/" + environmentName + ".Environment"
	return
}

func portableNames(t *testing.T, adapter *portableFS, name string) []string {
	t.Helper()
	var names []string
	status := adapter.Readdir(name, func(name string, _ *fuse.Stat_t, _ int64) bool {
		names = append(names, name)
		return true
	}, 0, 0)
	if status != 0 {
		t.Fatalf("readdir %s: %d", name, status)
	}
	return names
}

func TestPortableAdapterNamespaceAndReadOnlyBoundaries(t *testing.T) {
	adapter, _ := portableFixture(t, false)
	workspace, notebook, _, tables, environment := portablePaths(t)
	if got := portableNames(t, adapter, notebook); strings.Join(got, ",") != ".,..,.fabric.json,builtin,content.ipynb" {
		t.Fatalf("notebook entries = %v", got)
	}
	if got := portableNames(t, adapter, "/"+workspace); len(got) != 6 {
		t.Fatalf("workspace entries = %v", got)
	}
	if status := adapter.Getattr(notebook+"/.platform", &fuse.Stat_t{}, ^uint64(0)); status != -fuse.ENOENT {
		t.Fatalf("hidden .platform getattr = %d", status)
	}
	if status := adapter.Getattr(notebook+"/env", &fuse.Stat_t{}, ^uint64(0)); status != -fuse.ENOENT {
		t.Fatalf("hidden notebook env getattr = %d", status)
	}
	if status := adapter.Getattr(notebook+"/../content.ipynb", &fuse.Stat_t{}, ^uint64(0)); status != -fuse.EINVAL {
		t.Fatalf("parent traversal getattr = %d", status)
	}
	for _, path := range []string{notebook + "/.fabric.json", tables, environment + "/.fabric.json"} {
		if status := adapter.Access(path, 2); status != -fuse.EROFS {
			t.Fatalf("write access %s = %d", path, status)
		}
	}
}

func TestWindowsDriveMountpointNormalization(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows drive syntax")
	}
	got, err := NormalizeMountpoint("m:")
	if err != nil || got != "M:" {
		t.Fatalf("NormalizeMountpoint = %q, %v", got, err)
	}
}

func TestPortableRuntimeErrorNamesMissingDependency(t *testing.T) {
	message := portableRuntimeError("cgofuse: cannot find winfsp").Error()
	if runtime.GOOS == "windows" && !strings.Contains(message, "install WinFsp 2.1") {
		t.Fatalf("missing WinFsp guidance: %s", message)
	}
}

func TestPortableAdapterNotebookReadAndLakehouseCRUD(t *testing.T) {
	adapter, _ := portableFixture(t, false)
	_, notebook, files, _, _ := portablePaths(t)

	info := &fuse.FileInfo_t{Flags: fuse.O_RDONLY}
	if status := adapter.OpenEx(notebook+"/content.ipynb", info); status != 0 {
		t.Fatalf("open notebook: %d", status)
	}
	buffer := make([]byte, len(testutil.InitialNotebook)+1)
	n := adapter.Read(notebook+"/content.ipynb", buffer, 0, info.Fh)
	if n != len(testutil.InitialNotebook) || string(buffer[:n]) != testutil.InitialNotebook {
		t.Fatalf("notebook read = %q (%d)", buffer[:max(n, 0)], n)
	}
	if status := adapter.Release(notebook+"/content.ipynb", info.Fh); status != 0 {
		t.Fatalf("release notebook: %d", status)
	}

	dir := files + "/portable"
	if status := adapter.Mkdir(dir, 0755); status != 0 {
		t.Fatalf("mkdir: %d", status)
	}
	created := &fuse.FileInfo_t{Flags: fuse.O_RDWR | fuse.O_CREAT | fuse.O_EXCL}
	path := dir + "/hello.txt"
	if status := adapter.CreateEx(path, 0644, created); status != 0 {
		t.Fatalf("create: %d", status)
	}
	if n := adapter.Write(path, []byte("hello world"), 0, created.Fh); n != 11 {
		t.Fatalf("write = %d", n)
	}
	if status := adapter.Truncate(path, 5, created.Fh); status != 0 {
		t.Fatalf("truncate: %d", status)
	}
	if status := adapter.Fsync(path, false, created.Fh); status != 0 {
		t.Fatalf("fsync: %d", status)
	}
	if status := adapter.Release(path, created.Fh); status != 0 {
		t.Fatalf("release: %d", status)
	}
	renamed := dir + "/renamed.txt"
	if status := adapter.Rename3(path, renamed, fuse.RENAME_NOREPLACE); status != 0 {
		t.Fatalf("rename: %d", status)
	}
	read := &fuse.FileInfo_t{Flags: fuse.O_RDONLY}
	if status := adapter.OpenEx(renamed, read); status != 0 {
		t.Fatalf("open renamed: %d", status)
	}
	buffer = make([]byte, 8)
	n = adapter.Read(renamed, buffer, 0, read.Fh)
	if n != 5 || string(buffer[:n]) != "hello" {
		t.Fatalf("renamed read = %q (%d)", buffer[:max(n, 0)], n)
	}
	if status := adapter.Release(renamed, read.Fh); status != 0 {
		t.Fatalf("release renamed: %d", status)
	}
	if status := adapter.Unlink(renamed); status != 0 {
		t.Fatalf("unlink: %d", status)
	}
	if status := adapter.Rmdir(dir); status != 0 {
		t.Fatalf("rmdir: %d", status)
	}
	if status := adapter.Getattr(renamed, &fuse.Stat_t{}, ^uint64(0)); status != -fuse.ENOENT {
		t.Fatalf("removed file getattr = %d", status)
	}
}

func TestPortableAdapterCloseBeforeRename(t *testing.T) {
	adapter, _ := portableFixture(t, false)
	_, _, files, _, _ := portablePaths(t)
	path := files + "/close.txt"
	info := &fuse.FileInfo_t{Flags: fuse.O_RDWR | fuse.O_CREAT | fuse.O_EXCL}
	if status := adapter.CreateEx(path, 0644, info); status != 0 {
		t.Fatal(status)
	}
	if n := adapter.Write(path, []byte("done"), 0, info.Fh); n != 4 {
		t.Fatal(n)
	}
	if status := adapter.Flush(path, info.Fh); status != 0 {
		t.Fatalf("flush: %d", status)
	}
	if status := adapter.Release(path, info.Fh); status != 0 {
		t.Fatalf("release: %d", status)
	}
	if status := adapter.Rename(path, files+"/closed.txt"); status != 0 {
		t.Fatalf("rename after close: %d", status)
	}
}

func TestPortableAdapterNotebookSaveAndConflict(t *testing.T) {
	t.Run("save", func(t *testing.T) {
		adapter, service := portableFixture(t, false)
		_, notebook, _, _, _ := portablePaths(t)
		path := notebook + "/content.ipynb"
		info := &fuse.FileInfo_t{Flags: fuse.O_RDWR}
		if status := adapter.OpenEx(path, info); status != 0 {
			t.Fatal(status)
		}
		edited := []byte(`{"cells":[],"nbformat":4,"nbformat_minor":5,"metadata":{"portable":true}}`)
		if status := adapter.Truncate(path, int64(len(edited)), info.Fh); status != 0 {
			t.Fatal(status)
		}
		if n := adapter.Write(path, edited, 0, info.Fh); n != len(edited) {
			t.Fatal(n)
		}
		if status := adapter.Flush(path, info.Fh); status != 0 {
			t.Fatalf("flush: %d", status)
		}
		if status := adapter.Release(path, info.Fh); status != 0 {
			t.Fatalf("release: %d", status)
		}
		if service.Counts().NotebookUpdates != 1 {
			t.Fatalf("Notebook updates = %+v", service.Counts())
		}
	})

	t.Run("conflict", func(t *testing.T) {
		adapter, service := portableFixture(t, false)
		_, notebook, _, _, _ := portablePaths(t)
		path := notebook + "/content.ipynb"
		info := &fuse.FileInfo_t{Flags: fuse.O_RDWR}
		if status := adapter.OpenEx(path, info); status != 0 {
			t.Fatal(status)
		}
		if n := adapter.Write(path, []byte(" "), int64(len(testutil.InitialNotebook)), info.Fh); n != 1 {
			t.Fatal(n)
		}
		external := service.Definition()
		external.Parts[0].Payload = base64.StdEncoding.EncodeToString([]byte(`{"external":"metadata"}`))
		service.SetDefinition(external)
		if status := adapter.Flush(path, info.Fh); status != -fuse.EBUSY {
			t.Fatalf("conflicting flush = %d", status)
		}
		if status := adapter.Release(path, info.Fh); status != -fuse.EBUSY {
			t.Fatalf("conflicting release = %d", status)
		}
		if service.Counts().NotebookUpdates != 0 {
			t.Fatal("conflict overwrote the notebook")
		}
	})
}

func TestWinFspIntegration(t *testing.T) {
	if runtime.GOOS != "windows" || os.Getenv("FABRICFS_WINFSP_TEST") != "1" {
		t.Skip("set FABRICFS_WINFSP_TEST=1 on Windows with WinFsp installed")
	}
	service := testutil.New(t)
	fab, lake := service.Clients()
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs = []string{testutil.WorkspaceID}
	opts.SpoolDirectory = t.TempDir()
	backend, err := workspacefs.New(fab, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	mountpoint := availableWindowsDrive(t)
	server, err := Mount(mountpoint, backend, Options{Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatalf("WinFsp mount: %v", err)
	}
	defer func() {
		if err := server.Unmount(); err != nil {
			t.Errorf("WinFsp unmount: %v", err)
		}
	}()
	workspace, _, _, _, _ := portablePaths(t)
	if _, err := os.Stat(filepath.Join(mountpoint+`\`, workspace)); err != nil {
		t.Fatalf("mounted workspace: %v", err)
	}
	_, notebook, _, _, _ := portablePaths(t)
	notebookPath := filepath.Join(mountpoint+`\`, filepath.FromSlash(strings.TrimPrefix(notebook, "/")), "content.ipynb")
	updated := []byte(`{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"winfsp":true}}`)
	if err := os.WriteFile(notebookPath, updated, 0644); err != nil {
		t.Fatalf("WinFsp notebook save: %v", err)
	}
	if got, err := os.ReadFile(notebookPath); err != nil || string(got) != string(updated) {
		t.Fatalf("WinFsp notebook readback = %q, %v", got, err)
	}
	_, _, files, _, _ := portablePaths(t)
	directory := filepath.Join(mountpoint+`\`, filepath.FromSlash(strings.TrimPrefix(files, "/")), "e2e")
	if err := os.Mkdir(directory, 0755); err != nil {
		t.Fatalf("WinFsp Lakehouse mkdir: %v", err)
	}
	source, target := filepath.Join(directory, "payload.txt"), filepath.Join(directory, "renamed.txt")
	if err := os.WriteFile(source, []byte("payload"), 0644); err != nil {
		t.Fatalf("WinFsp Lakehouse write: %v", err)
	}
	if err := os.Rename(source, target); err != nil {
		t.Fatalf("WinFsp Lakehouse rename: %v", err)
	}
	if err := os.Remove(target); err != nil {
		t.Fatalf("WinFsp Lakehouse unlink: %v", err)
	}
	if err := os.Remove(directory); err != nil {
		t.Fatalf("WinFsp Lakehouse rmdir: %v", err)
	}
}

func availableWindowsDrive(t *testing.T) string {
	t.Helper()
	for letter := 'Z'; letter >= 'D'; letter-- {
		drive := string(letter) + ":"
		if _, err := os.Stat(drive + `\`); os.IsNotExist(err) {
			return drive
		}
	}
	t.Fatal("no unused drive letter is available for WinFsp integration")
	return ""
}
