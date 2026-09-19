//go:build unix

package overlay

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestUnixPrivateStoragePermissions(t *testing.T) {
	store, root := newTestStore(t, Options{})
	mkdir(t, store, ".agents")
	mkdir(t, store, ".agents/sub")
	put(t, store, ".agents/sub/file", "private")
	for _, path := range []string{root, filepath.Join(root, testWorkspace), filepath.Join(root, testWorkspace, "root"), physical(root, ".agents"), physical(root, ".agents/sub")} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("directory mode %q = %v, %v", path, info, err)
		}
	}
	info, err := os.Lstat(physical(root, ".agents/sub/file"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v, %v", info, err)
	}
}

func TestUnixRejectsNonprivateExistingEntries(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Chmod(root, 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := New(root, Options{})
		requireIs(t, err, fs.ErrPermission)
	})
	for _, tc := range []struct {
		path string
		mode fs.FileMode
	}{{testWorkspace, 0o755}, {testWorkspace + "/root", 0o777}, {testWorkspace + "/root/.agents", 0o750}, {testWorkspace + "/root/.agents/file", 0o644}} {
		t.Run(tc.path, func(t *testing.T) {
			root := t.TempDir()
			if err := os.MkdirAll(physical(root, ".agents"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(physical(root, ".agents/file"), []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(root, filepath.FromSlash(tc.path)), tc.mode); err != nil {
				t.Fatal(err)
			}
			_, err := New(root, Options{})
			requireIs(t, err, fs.ErrPermission)
		})
	}
	t.Run("changed while open", func(t *testing.T) {
		store, root := newTestStore(t, Options{})
		mkdir(t, store, ".agents")
		put(t, store, ".agents/file", "data")
		if err := os.Chmod(physical(root, ".agents/file"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := store.List(context.Background(), at(".agents"))
		requireIs(t, err, fs.ErrPermission)
		_, err = store.Open(context.Background(), at(".agents/file"), os.O_RDWR|os.O_TRUNC)
		requireIs(t, err, fs.ErrPermission)
		if data, err := os.ReadFile(physical(root, ".agents/file")); err != nil || string(data) != "data" {
			t.Fatalf("insecure file was mutated: %q, %v", data, err)
		}
	})
}

type foreignOwnerInfo struct {
	fs.FileInfo
	stat syscall.Stat_t
}

func (i foreignOwnerInfo) Sys() any { return &i.stat }

func TestUnixOwnershipIsRequired(t *testing.T) {
	root := t.TempDir()
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	stat := *info.Sys().(*syscall.Stat_t)
	stat.Uid++
	requireIs(t, checkPrivate(root, foreignOwnerInfo{FileInfo: info, stat: stat}), fs.ErrPermission)
}
