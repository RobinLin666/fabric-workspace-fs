package overlay

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
)

func symlink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		if errors.Is(err, fs.ErrPermission) || errors.Is(err, syscall.ENOSYS) ||
			errors.Is(err, syscall.EOPNOTSUPP) ||
			runtime.GOOS == "windows" && (errors.Is(err, syscall.Errno(1314)) || errors.Is(err, syscall.Errno(50))) {
			t.Skipf("OS cannot create test symlinks: %v", err)
		}
		t.Fatal(err)
	}
}

func TestSymlinkStorageRootAndAncestorsAreRejected(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(base, "outside")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	symlink(t, outside, link)
	_, err := New(link, Options{})
	requireIs(t, err, fs.ErrPermission)
	_, err = New(filepath.Join(link, "would-escape"), Options{})
	requireIs(t, err, fs.ErrPermission)
	_, err = os.Stat(filepath.Join(outside, "would-escape"))
	requireIs(t, err, fs.ErrNotExist)
}

func TestExistingSymlinksRejectMountWithoutOutsideAccess(t *testing.T) {
	for _, linkAt := range []string{
		testWorkspace,
		testWorkspace + "/root",
		testWorkspace + "/root/.agents",
		testWorkspace + "/root/.agents/file",
	} {
		t.Run(linkAt, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "storage")
			path := filepath.Join(root, filepath.FromSlash(linkAt))
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(base, "outside")
			if err := os.Mkdir(outside, 0o700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(outside, "marker")
			if err := os.WriteFile(marker, []byte("unchanged"), 0o600); err != nil {
				t.Fatal(err)
			}
			symlink(t, outside, path)
			_, err := New(root, Options{})
			requireIs(t, err, fs.ErrPermission)
			if data, err := os.ReadFile(marker); err != nil || string(data) != "unchanged" {
				t.Fatalf("outside marker changed: %q, %v", data, err)
			}
		})
	}
}

func TestSymlinksRejectedAtEveryStoreEntrance(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	put(t, store, ".agents/good", "safe")
	outside := filepath.Join(filepath.Dir(root), "outside")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(outside, "marker")
	if err := os.WriteFile(marker, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink(t, outside, physical(root, ".agents/escape"))
	symlink(t, marker, physical(root, ".agents/bad"))
	for _, relative := range []string{".agents/escape", ".agents/escape/marker", ".agents/bad"} {
		_, err := store.Stat(ctx, at(relative))
		requireIs(t, err, fs.ErrPermission)
		_, err = store.List(ctx, at(relative))
		requireIs(t, err, fs.ErrPermission)
		_, err = store.Open(ctx, at(relative), os.O_WRONLY|os.O_TRUNC)
		requireIs(t, err, fs.ErrPermission)
		_, err = store.Create(ctx, at(relative), os.O_RDWR|os.O_TRUNC)
		requireIs(t, err, fs.ErrPermission)
		requireIs(t, store.Mkdir(ctx, at(relative)), fs.ErrPermission)
		requireIs(t, store.Remove(ctx, at(relative), false), fs.ErrPermission)
		requireIs(t, store.Remove(ctx, at(relative), true), fs.ErrPermission)
		requireIs(t, store.Rename(ctx, at(relative), at(".agents/new"), false), fs.ErrPermission)
		requireIs(t, store.Rename(ctx, at(".agents/good"), at(relative), false), fs.ErrPermission)
	}
	_, err := store.List(ctx, at(".agents"))
	requireIs(t, err, fs.ErrPermission)
	_, err = store.Create(ctx, at(".agents/escape/new"), os.O_RDWR)
	requireIs(t, err, fs.ErrPermission)
	requireIs(t, store.Mkdir(ctx, at(".agents/escape/new")), fs.ErrPermission)
	requireIs(t, store.Rename(ctx, at(".agents/good"), at(".agents/escape/new"), false), fs.ErrPermission)
	if data, err := os.ReadFile(marker); err != nil || string(data) != "unchanged" {
		t.Fatalf("outside write through a symlink: %q, %v", data, err)
	}
	outsideEntries, err := os.ReadDir(outside)
	if err != nil || len(outsideEntries) != 1 || outsideEntries[0].Name() != "marker" {
		t.Fatalf("outside entries changed: %v, %v", outsideEntries, err)
	}
}

func TestInternalRelativeSymlinksAreAlsoRejected(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	put(t, store, ".agents/file", "safe")
	symlink(t, "file", physical(root, ".agents/alias"))
	_, err := store.Open(ctx, at(".agents/alias"), os.O_RDWR|os.O_TRUNC)
	requireIs(t, err, fs.ErrPermission)
	if read(t, store, ".agents/file") != "safe" {
		t.Fatal("internal symlink modified its target")
	}
}

func TestHardLinkedFilesAreRejected(t *testing.T) {
	ctx := context.Background()
	t.Run("scan", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "storage")
		if err := os.MkdirAll(physical(root, ".agents"), 0o700); err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(base, "outside")
		if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(outside, physical(root, ".agents/file")); err != nil {
			t.Fatal(err)
		}
		_, err := New(root, Options{})
		requireIs(t, err, fs.ErrPermission)
		data, err := os.ReadFile(outside)
		if err != nil || string(data) != "unchanged" {
			t.Fatalf("outside data = %q, %v", data, err)
		}
	})
	t.Run("already open handle", func(t *testing.T) {
		store, root := newTestStore(t, Options{})
		mkdir(t, store, ".agents")
		file := create(t, store, ".agents/file")
		if _, err := file.WriteAt(ctx, []byte("safe"), 0); err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(filepath.Dir(root), "outside")
		if err := os.Link(physical(root, ".agents/file"), outside); err != nil {
			t.Fatal(err)
		}
		_, err := file.WriteAt(ctx, []byte("bad"), 0)
		requireIs(t, err, fs.ErrPermission)
		requireIs(t, file.Truncate(ctx, 0), fs.ErrPermission)
		_, err = store.Stat(ctx, at(".agents/file"))
		requireIs(t, err, fs.ErrPermission)
		_, err = store.List(ctx, at(".agents"))
		requireIs(t, err, fs.ErrPermission)
		requireIs(t, store.Remove(ctx, at(".agents/file"), false), fs.ErrPermission)
		requireIs(t, store.Rename(ctx, at(".agents/file"), at(".agents/new"), false), fs.ErrPermission)
		if data, err := os.ReadFile(outside); err != nil || string(data) != "safe" {
			t.Fatalf("hard-link write escaped: %q, %v", data, err)
		}
	})
}

func TestSymlinkSwapCannotEscapeSandbox(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	put(t, store, ".agents/node", "inside")
	path := physical(root, ".agents/node")
	parked := physical(root, ".agents/parked")
	outside := filepath.Join(filepath.Dir(root), "outside")
	if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	raceSymlink(t, path, parked, outside, func(i int) {
		file, err := store.Open(ctx, at(".agents/node"), os.O_RDWR|os.O_TRUNC)
		if err != nil {
			checkRaceError(t, err)
			return
		}
		if _, err := file.WriteAt(ctx, []byte("local"), 0); err != nil {
			t.Error(err)
		}
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	if data, err := os.ReadFile(outside); err != nil || string(data) != "unchanged" {
		t.Fatalf("symlink race escaped the root: %q, %v", data, err)
	}
}

func TestDirectorySymlinkRaceCannotEscapeCreateMkdirOrRename(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	mkdir(t, store, ".agents/node")
	outside := filepath.Join(filepath.Dir(root), "outside")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	path := physical(root, ".agents/node")
	parked := physical(root, ".agents/parked")
	raceSymlink(t, path, parked, outside, func(i int) {
		relative := fmt.Sprintf(".agents/node/child-%d", i)
		switch i % 3 {
		case 0:
			file, err := store.Create(ctx, at(relative), os.O_RDWR)
			if err != nil {
				checkRaceError(t, err)
				return
			}
			if _, err := file.WriteAt(ctx, []byte("local"), 0); err != nil {
				t.Error(err)
			}
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		case 1:
			if err := store.Mkdir(ctx, at(relative)); err != nil {
				checkRaceError(t, err)
			}
		case 2:
			source := fmt.Sprintf(".agents/source-%d", i)
			file, err := store.Create(ctx, at(source), os.O_RDWR)
			if err != nil {
				checkRaceError(t, err)
				return
			}
			if err := file.Close(); err != nil {
				t.Error(err)
				return
			}
			if err := store.Rename(ctx, at(source), at(relative), false); err != nil {
				checkRaceError(t, err)
			}
		}
	})
	if names, err := os.ReadDir(outside); err != nil || len(names) != 0 {
		t.Fatalf("directory symlink race wrote outside the store: %v, %v", names, err)
	}
}

func checkRaceError(t *testing.T, err error) {
	t.Helper()
	var pathErr *fs.PathError
	var linkErr *os.LinkError
	if !errors.Is(err, fs.ErrPermission) && !errors.Is(err, fs.ErrNotExist) &&
		!errors.Is(err, fserrors.ErrConflict) && !errors.As(err, &pathErr) && !errors.As(err, &linkErr) {
		t.Errorf("unexpected race error: %v", err)
	}
}

func raceSymlink(t *testing.T, path, parked, outside string, attempt func(int)) {
	t.Helper()
	probe := filepath.Join(filepath.Dir(outside), "symlink-probe")
	symlink(t, outside, probe)
	if err := os.Remove(probe); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	ready := make(chan struct{})
	done := make(chan error, 1)
	var swaps atomic.Int32
	rename := func(old, next string) error {
		for i := 0; i < 1000; i++ {
			err := os.Rename(old, next)
			if err == nil {
				return nil
			}
			if runtime.GOOS != "windows" || !(errors.Is(err, syscall.Errno(32)) || errors.Is(err, fs.ErrPermission)) {
				return err
			}
			time.Sleep(time.Millisecond)
		}
		return errors.New("directory entry remained busy during test rename")
	}
	go func() {
		signalled := false
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			if err := rename(path, parked); err != nil {
				if !signalled {
					close(ready)
				}
				done <- err
				return
			}
			if err := os.Symlink(outside, path); err != nil {
				if !signalled {
					close(ready)
				}
				done <- errors.Join(err, rename(parked, path))
				return
			}
			swaps.Add(1)
			if !signalled {
				close(ready)
				signalled = true
			}
			runtime.Gosched()
			if err := os.Remove(path); err != nil {
				done <- err
				return
			}
			if err := rename(parked, path); err != nil {
				done <- err
				return
			}
		}
	}()
	defer func() {
		close(stop)
		if err := <-done; err != nil {
			t.Error(err)
		}
		if swaps.Load() == 0 {
			t.Error("test did not exercise a symlink swap")
		}
	}()
	<-ready
	for i := 0; i < 150; i++ {
		attempt(i)
	}
}
