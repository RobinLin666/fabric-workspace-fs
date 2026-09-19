package overlay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
)

const (
	testWorkspace = "12345678-1234-1234-1234-123456789abc"
	testParent    = "abcdefab-abcd-abcd-abcd-abcdefabcdef"
	otherID       = "22222222-2222-2222-2222-222222222222"
)

func at(relative string) Location {
	return Location{Workspace: testWorkspace, Parent: "root", Relative: relative}
}

func newTestStore(t *testing.T, opts Options) (*Store, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "storage")
	store, err := New(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return store, root
}

func requireIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want errors.Is(_, %v)", got, want)
	}
}

func mkdir(t *testing.T, store *Store, relative string) {
	t.Helper()
	if err := store.Mkdir(context.Background(), at(relative)); err != nil {
		t.Fatal(err)
	}
}

func create(t *testing.T, store *Store, relative string) *File {
	t.Helper()
	file, err := store.Create(context.Background(), at(relative), os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close file: %v", err)
		}
	})
	return file
}

func put(t *testing.T, store *Store, relative, data string) {
	t.Helper()
	file := create(t, store, relative)
	if n, err := file.WriteAt(context.Background(), []byte(data), 0); err != nil || n != len(data) {
		t.Fatalf("write = %d, %v", n, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, store *Store, relative string) string {
	t.Helper()
	file, err := store.Open(context.Background(), at(relative), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()
	buffer := make([]byte, file.Size())
	if n, err := file.ReadAt(context.Background(), buffer, 0); err != nil || n != len(buffer) {
		t.Fatalf("read = %d, %v", n, err)
	}
	return string(buffer)
}

func physical(root, relative string) string {
	return filepath.Join(root, testWorkspace, "root", filepath.FromSlash(relative))
}

func TestNewRootAndOptions(t *testing.T) {
	ctx := context.Background()
	t.Run("defaults and lazy namespace", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "private", "storage")
		store, err := New(root, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if store.opts != (Options{1 << 30, 4 << 30, 10000}) {
			t.Fatalf("defaults = %+v", store.opts)
		}
		children, err := os.ReadDir(root)
		if err != nil || len(children) != 0 {
			t.Fatalf("new namespace = %v, %v", children, err)
		}
		for i := 0; i < 2; i++ {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		}
		_, err = store.List(ctx, at(""))
		requireIs(t, err, fs.ErrClosed)
	})
	t.Run("invalid roots", func(t *testing.T) {
		volumeRoot := filepath.VolumeName(t.TempDir()) + string(os.PathSeparator)
		for _, root := range []string{"", ".", "..", "relative", volumeRoot, "bad\x00root"} {
			if store, err := New(root, Options{}); store != nil || !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("New(%q) = %v, %v", root, store, err)
			}
		}
		base := t.TempDir()
		traversal := base + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "escape"
		_, err := New(traversal, Options{})
		requireIs(t, err, fs.ErrInvalid)
	})
	t.Run("file root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(root, []byte("unchanged"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := New(root, Options{})
		requireIs(t, err, fserrors.ErrNotDir)
		data, err := os.ReadFile(root)
		if err != nil || string(data) != "unchanged" {
			t.Fatalf("root file changed: %q, %v", data, err)
		}
	})
	t.Run("negative options have no side effects", func(t *testing.T) {
		for _, opts := range []Options{{MaxFileSize: -1}, {MaxBytes: -1}, {MaxEntries: -1}} {
			root := filepath.Join(t.TempDir(), "absent")
			_, err := New(root, opts)
			requireIs(t, err, fs.ErrInvalid)
			_, err = os.Stat(root)
			requireIs(t, err, fs.ErrNotExist)
		}
	})
}

func TestRawLocationValidationAtEveryEntrance(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	invalidPaths := []string{
		".", "..", "...", ". ", "agents", "/.agents", ".agents/", ".agents//file",
		".agents/../escape", ".agents/./file", ".agents\\escape", ".agents/a\\b",
		".agents/a\x00b", ".agents/a\nb", ".agents/\xff", ".fabric.json", ".platform",
		".FABRIC.JSON", ".PLATFORM", ".agents/.fabric.json", ".agents/.platform",
		".agents/.fabrıc.json", ".agents/.fabric.jſon",
		".agents/child/.FaBrIc.JsOn", ".agents/child/.PLATFORM", ".agents/file.",
		".agents/file ", ".agents/stream:payload", ".agents/NUL", ".agents/nul.txt",
		".agents/CON", ".agents/AUX", ".agents/PRN", ".agents/COM1", ".agents/LPT9.txt",
		".agents/COM¹", ".agents/LPT²", ".agents/CONIN$", ".agents/CONOUT$",
		".agents/a*b", ".agents/a?b", ".agents/a<b", ".agents/a>b", ".agents/a|b",
		".agents/a\"b", "." + strings.Repeat("a", 255),
		".agents/" + strings.Repeat("x", 256), ".agents/" + strings.Repeat("child/", maxDepth) + "file",
	}
	cases := make([]Location, 0, len(invalidPaths)+10)
	for _, relative := range invalidPaths {
		cases = append(cases, at(relative))
	}
	for _, id := range []string{"", "root", "../escape", "..\\escape", strings.Repeat("a", 36), "g2345678-1234-1234-1234-123456789abc", testWorkspace + "\x00"} {
		cases = append(cases, Location{id, "root", ".agents"})
		if id != "root" {
			cases = append(cases, Location{testWorkspace, id, ".agents"})
		}
	}
	for _, loc := range cases {
		t.Run(fmt.Sprintf("%q-%q-%q", loc.Workspace, loc.Parent, loc.Relative), func(t *testing.T) {
			_, err := store.List(ctx, loc)
			requireIs(t, err, fs.ErrInvalid)
			_, err = store.Stat(ctx, loc)
			requireIs(t, err, fs.ErrInvalid)
			_, err = store.Open(ctx, loc, os.O_RDWR|os.O_TRUNC)
			requireIs(t, err, fs.ErrInvalid)
			_, err = store.Create(ctx, loc, os.O_RDWR)
			requireIs(t, err, fs.ErrInvalid)
			requireIs(t, store.Mkdir(ctx, loc), fs.ErrInvalid)
			requireIs(t, store.Remove(ctx, loc, false), fs.ErrInvalid)
			requireIs(t, store.Remove(ctx, loc, true), fs.ErrInvalid)
			requireIs(t, store.Rename(ctx, loc, at(".valid"), false), fs.ErrInvalid)
			requireIs(t, store.Rename(ctx, at(".valid"), loc, false), fs.ErrInvalid)
		})
	}
	children, err := os.ReadDir(root)
	if err != nil || len(children) != 0 {
		t.Fatalf("invalid locations changed storage: %v, %v", children, err)
	}
}

func TestScopeListingAndParentRules(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	list, err := store.List(ctx, at(""))
	if err != nil || len(list) != 0 {
		t.Fatalf("missing scope = %v, %v", list, err)
	}
	_, err = store.Stat(ctx, at(""))
	requireIs(t, err, fs.ErrNotExist)
	_, err = store.List(ctx, at(".agents"))
	requireIs(t, err, fs.ErrNotExist)
	_, err = store.Create(ctx, at(".agents/file"), os.O_RDWR)
	requireIs(t, err, fs.ErrNotExist)
	requireIs(t, store.Mkdir(ctx, at(".agents/child")), fs.ErrNotExist)
	_, err = store.Create(ctx, at(".agents"), os.O_RDWR)
	requireIs(t, err, fs.ErrInvalid)
	_, err = store.Open(ctx, at(""), os.O_RDONLY)
	requireIs(t, err, fs.ErrInvalid)
	_, err = store.Create(ctx, at(""), os.O_RDWR)
	requireIs(t, err, fs.ErrInvalid)
	requireIs(t, store.Mkdir(ctx, at("")), fs.ErrInvalid)
	requireIs(t, store.Remove(ctx, at(""), true), fs.ErrInvalid)
	requireIs(t, store.Rename(ctx, at(""), at(".new"), false), fs.ErrInvalid)
	requireIs(t, store.Rename(ctx, at(".new"), at(""), false), fs.ErrInvalid)
	children, err := os.ReadDir(root)
	if err != nil || len(children) != 0 {
		t.Fatalf("missing user parent created containers: %v, %v", children, err)
	}

	mkdir(t, store, ".z")
	mkdir(t, store, ".agents")
	mkdir(t, store, ".agents/sub")
	put(t, store, ".agents/z", "z")
	put(t, store, ".agents/a", "aaa")
	_, err = store.Create(ctx, at(".agents/missing/file"), os.O_RDWR)
	requireIs(t, err, fs.ErrNotExist)
	requireIs(t, store.Mkdir(ctx, at(".agents/missing/child")), fs.ErrNotExist)
	list, err = store.List(ctx, at(""))
	if err != nil || len(list) != 2 || list[0].Path != ".agents" || list[1].Path != ".z" {
		t.Fatalf("scope listing = %+v, %v", list, err)
	}
	list, err = store.List(ctx, at(".agents"))
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{}
	for _, info := range list {
		paths = append(paths, info.Path)
		if info.ModTime.IsZero() {
			t.Fatal("missing modification time")
		}
	}
	if !reflect.DeepEqual(paths, []string{".agents/a", ".agents/sub", ".agents/z"}) ||
		list[0].IsDir || list[0].Size != 3 || !list[1].IsDir {
		t.Fatalf("child listing = %+v", list)
	}
	_, err = store.List(ctx, at(".agents/a"))
	requireIs(t, err, fserrors.ErrNotDir)
	_, err = store.List(ctx, at(".agents/missing"))
	requireIs(t, err, fs.ErrNotExist)
	missingScope := Location{testWorkspace, testParent, ""}
	if list, err := store.List(ctx, missingScope); err != nil || len(list) != 0 {
		t.Fatalf("missing parent scope = %v, %v", list, err)
	}
	container, err := store.Stat(ctx, at(""))
	if err != nil || !container.IsDir || container.Path != "" {
		t.Fatalf("container stat = %+v, %v", container, err)
	}
	_, err = store.Open(ctx, at(".agents"), os.O_RDONLY)
	requireIs(t, err, fserrors.ErrIsDir)
	requireIs(t, store.Mkdir(ctx, at(".agents/a/child")), fserrors.ErrNotDir)
	_, err = store.Create(ctx, at(".agents/a/file"), os.O_RDWR)
	requireIs(t, err, fserrors.ErrNotDir)
}

func TestImmutableIDNamespacesAndNormalNames(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	first := Location{strings.ToUpper(testWorkspace), strings.ToUpper(testParent), ".agents"}
	if err := store.Mkdir(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.Relative = ".agents/config"
	file, err := store.Create(ctx, first, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(ctx, []byte("folder"), 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	mkdir(t, store, ".agents")
	put(t, store, ".agents/config", "root")
	mkdir(t, store, ".agents/普通 folder")
	put(t, store, ".agents/普通 folder/.nested", "nested")
	put(t, store, ".agents/普通 folder/file name", "spaces")
	put(t, store, ".agents/"+strings.Repeat("a", 255), "max name")
	if data, err := os.ReadFile(filepath.Join(root, testWorkspace, testParent, ".agents", "config")); err != nil || string(data) != "folder" {
		t.Fatalf("immutable ID storage = %q, %v", data, err)
	}
	if got := read(t, store, ".agents/config"); got != "root" {
		t.Fatalf("root scope = %q", got)
	}
	if got := read(t, store, ".agents/普通 folder/.nested"); got != "nested" {
		t.Fatalf("normal names = %q", got)
	}
}

func TestFileCRUDOffsetsTruncateSyncAndReopen(t *testing.T) {
	store, root := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	file := create(t, store, ".agents/config")
	for _, write := range []struct {
		data   string
		offset int64
	}{{"hello", 0}, {"XY", 8}, {"!", 1}} {
		if n, err := file.WriteAt(ctx, []byte(write.data), write.offset); err != nil || n != len(write.data) {
			t.Fatalf("WriteAt = %d, %v", n, err)
		}
	}
	want := []byte{'h', '!', 'l', 'l', 'o', 0, 0, 0, 'X', 'Y'}
	got := make([]byte, 10)
	if n, err := file.ReadAt(ctx, got, 0); err != nil || n != 10 || !bytes.Equal(got, want) {
		t.Fatalf("own writes = %v (%d, %v)", got, n, err)
	}
	if err := file.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if disk, err := os.ReadFile(physical(root, ".agents/config")); err != nil || !bytes.Equal(disk, want) {
		t.Fatalf("disk data = %v, %v", disk, err)
	}
	info, err := store.Stat(ctx, at(".agents/config"))
	diskInfo, diskErr := os.Stat(physical(root, ".agents/config"))
	if err != nil || diskErr != nil || info.Path != ".agents/config" || info.IsDir ||
		info.Size != 10 || !info.ModTime.Equal(diskInfo.ModTime()) || file.Size() != 10 {
		t.Fatalf("real metadata = %+v, %v, %v", info, err, diskErr)
	}
	if err := file.Truncate(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if n, err := file.ReadAt(ctx, got, 0); n != 3 || !errors.Is(err, io.EOF) || string(got[:3]) != "h!l" {
		t.Fatalf("partial read = %v, %d, %v", got, n, err)
	}
	if err := file.Truncate(ctx, 7); err != nil {
		t.Fatal(err)
	}
	got = make([]byte, 7)
	if n, err := file.ReadAt(ctx, got, 0); n != 7 || err != nil || !bytes.Equal(got, []byte{'h', '!', 'l', 0, 0, 0, 0}) {
		t.Fatalf("truncate growth = %v, %d, %v", got, n, err)
	}
	if _, err := file.WriteAt(ctx, []byte("Z"), 6); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if file.Size() != 7 || store.quota.TotalBytes != 7 {
		t.Fatalf("closed size/quota = %d / %+v", file.Size(), store.quota)
	}
	if got := read(t, store, ".agents/config"); got != "h!l\x00\x00\x00Z" {
		t.Fatalf("reopen = %q", got)
	}
	_, err = store.Create(ctx, at(".agents/config"), os.O_RDWR|os.O_TRUNC|os.O_CREATE)
	requireIs(t, err, fs.ErrExist)
	if got := read(t, store, ".agents/config"); got != "h!l\x00\x00\x00Z" {
		t.Fatal("Create truncated an existing file")
	}
	truncated, err := store.Open(ctx, at(".agents/config"), os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		t.Fatal(err)
	}
	if truncated.Size() != 0 || store.quota.TotalBytes != 0 {
		t.Fatal("O_TRUNC did not release quota")
	}
	if err := truncated.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendFlagsAndHandlePermissions(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	file, err := store.Create(ctx, at(".agents/log"), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_SYNC)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{500, 0, math.MaxInt64} {
		if n, err := file.WriteAt(ctx, []byte("x"), offset); err != nil || n != 1 {
			t.Fatalf("append = %d, %v", n, err)
		}
	}
	buffer := make([]byte, 3)
	if n, err := file.ReadAt(ctx, buffer, 0); err != nil || n != 3 || string(buffer) != "xxx" {
		t.Fatalf("append content = %q, %d, %v", buffer, n, err)
	}
	if err := file.Truncate(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(ctx, []byte("y"), 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if got := read(t, store, ".agents/log"); got != "xy" {
		t.Fatalf("append after truncate = %q", got)
	}
	readonly, err := store.Open(ctx, at(".agents/log"), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	_, err = readonly.WriteAt(ctx, []byte("bad"), 0)
	requireIs(t, err, fserrors.ErrReadOnly)
	requireIs(t, readonly.Truncate(ctx, 0), fserrors.ErrReadOnly)
	if err := readonly.Close(); err != nil {
		t.Fatal(err)
	}
	writeonly, err := store.Open(ctx, at(".agents/log"), os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	_, err = writeonly.ReadAt(ctx, buffer, 0)
	requireIs(t, err, fs.ErrPermission)
	if err := writeonly.Close(); err != nil {
		t.Fatal(err)
	}
	readonly, err = store.Create(ctx, at(".agents/empty"), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	_, err = readonly.WriteAt(ctx, nil, 0)
	requireIs(t, err, fserrors.ErrReadOnly)
	if n, err := readonly.ReadAt(ctx, nil, 0); n != 0 || err != nil {
		t.Fatalf("empty read = %d, %v", n, err)
	}
	if err := readonly.Close(); err != nil {
		t.Fatal(err)
	}
	for _, flags := range []int{-1, 3, 1 << 27, os.O_TRUNC, os.O_APPEND} {
		_, err := store.Open(ctx, at(".agents/log"), flags)
		requireIs(t, err, fs.ErrInvalid)
		_, err = store.Create(ctx, at(".agents/new"), flags)
		requireIs(t, err, fs.ErrInvalid)
	}
	for _, flags := range []int{os.O_CREATE, os.O_EXCL, os.O_RDWR | os.O_CREATE, os.O_RDWR | os.O_EXCL} {
		_, err := store.Open(ctx, at(".agents/log"), flags)
		requireIs(t, err, fs.ErrInvalid)
		_, err = store.Open(ctx, at(".agents/new"), flags)
		requireIs(t, err, fs.ErrInvalid)
	}
}

func TestFileInvalidInputsCancellationAndClosedHandles(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	file := create(t, store, ".agents/file")
	for _, offset := range []int64{-1, math.MinInt64, math.MaxInt64} {
		_, err := file.ReadAt(ctx, []byte{0}, offset)
		requireIs(t, err, fs.ErrInvalid)
		_, err = file.WriteAt(ctx, []byte{0}, offset)
		requireIs(t, err, fs.ErrInvalid)
	}
	requireIs(t, file.Truncate(ctx, -1), fs.ErrInvalid)
	if n, err := file.WriteAt(ctx, nil, math.MaxInt64); n != 0 || err != nil {
		t.Fatalf("empty positional write = %d, %v", n, err)
	}
	if file.Size() != 0 || store.quota.TotalBytes != 0 {
		t.Fatal("empty write grew a file")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	expired, release := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer release()
	for _, tc := range []struct {
		ctx  context.Context
		want error
	}{{nil, fs.ErrInvalid}, {cancelled, context.Canceled}, {expired, context.DeadlineExceeded}} {
		_, err := file.ReadAt(tc.ctx, []byte{0}, 0)
		requireIs(t, err, tc.want)
		_, err = file.WriteAt(tc.ctx, []byte("x"), 0)
		requireIs(t, err, tc.want)
		requireIs(t, file.Truncate(tc.ctx, 9), tc.want)
		requireIs(t, file.Flush(tc.ctx), tc.want)
		_, err = store.List(tc.ctx, at(""))
		requireIs(t, err, tc.want)
		_, err = store.Stat(tc.ctx, at(".agents/file"))
		requireIs(t, err, tc.want)
		_, err = store.Open(tc.ctx, at(".agents/file"), os.O_RDWR|os.O_TRUNC)
		requireIs(t, err, tc.want)
		_, err = store.Create(tc.ctx, at(".agents/new"), os.O_RDWR)
		requireIs(t, err, tc.want)
		requireIs(t, store.Mkdir(tc.ctx, at(".other")), tc.want)
		requireIs(t, store.Remove(tc.ctx, at(".agents/file"), false), tc.want)
		requireIs(t, store.Rename(tc.ctx, at(".agents/file"), at(".agents/new"), false), tc.want)
	}
	if file.Size() != 0 || store.quota.Entries != 2 {
		t.Fatal("cancelled operation had side effects")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := file.ReadAt(ctx, nil, 0)
	requireIs(t, err, fs.ErrClosed)
	requireIs(t, err, fserrors.ErrClosed)
	_, err = file.WriteAt(ctx, nil, 0)
	requireIs(t, err, fs.ErrClosed)
	requireIs(t, file.Truncate(ctx, 0), fs.ErrClosed)
	requireIs(t, file.Flush(ctx), fs.ErrClosed)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := store.Open(ctx, at(".agents/file"), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = reader.ReadAt(ctx, nil, 0)
	requireIs(t, err, fs.ErrClosed)
	requireIs(t, store.Mkdir(ctx, at(".new")), fs.ErrClosed)
	_, err = store.Create(ctx, at(".new/file"), os.O_RDWR)
	requireIs(t, err, fs.ErrClosed)
	_, err = store.Open(ctx, at(".agents/file"), os.O_RDONLY)
	requireIs(t, err, fs.ErrClosed)
	_, err = store.Stat(ctx, at(".agents/file"))
	requireIs(t, err, fs.ErrClosed)
	requireIs(t, store.Remove(ctx, at(".agents/file"), false), fs.ErrClosed)
	requireIs(t, store.Rename(ctx, at(".agents/file"), at(".agents/new"), false), fs.ErrClosed)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPersistentLogicalQuotas(t *testing.T) {
	store, root := newTestStore(t, Options{MaxFileSize: 8, MaxBytes: 12, MaxEntries: 10})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	a := create(t, store, ".agents/a")
	b := create(t, store, ".agents/b")
	_, err := a.WriteAt(ctx, []byte("too large"), 0)
	requireIs(t, err, fserrors.ErrTooLarge)
	requireIs(t, a.Truncate(ctx, 9), fserrors.ErrTooLarge)
	_, err = a.WriteAt(ctx, []byte("x"), 8)
	requireIs(t, err, fserrors.ErrTooLarge)
	if _, err := a.WriteAt(ctx, []byte("x"), 7); err != nil {
		t.Fatal(err)
	}
	if store.quota.TotalBytes != 8 || a.Size() != 8 {
		t.Fatal("sparse holes were not charged")
	}
	if _, err := b.WriteAt(ctx, []byte("bbbb"), 0); err != nil {
		t.Fatal(err)
	}
	_, err = b.WriteAt(ctx, []byte("x"), 4)
	requireIs(t, err, syscall.ENOSPC)
	requireIs(t, b.Truncate(ctx, 5), syscall.ENOSPC)
	if _, err := a.WriteAt(ctx, []byte("overwrite"), 0); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatalf("per-file bound = %v", err)
	}
	if _, err := a.WriteAt(ctx, []byte("12345678"), 0); err != nil {
		t.Fatal(err)
	}
	if err := a.Truncate(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if err := b.Truncate(ctx, 8); err != nil {
		t.Fatal(err)
	}
	if store.quota.TotalBytes != 12 {
		t.Fatalf("quota after resize = %+v", store.quota)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	appender, err := store.Open(ctx, at(".agents/b"), os.O_WRONLY|os.O_APPEND)
	if err != nil {
		t.Fatal(err)
	}
	_, err = appender.WriteAt(ctx, []byte("x"), 0)
	requireIs(t, err, fserrors.ErrTooLarge)
	if err := appender.Close(); err != nil {
		t.Fatal(err)
	}
	requireIs(t, store.Remove(ctx, at(".agents"), true), fserrors.ErrNotEmpty)
	if err := store.Remove(ctx, at(".agents/a"), false); err != nil {
		t.Fatal(err)
	}
	put(t, store, ".agents/c", "cccc")
	if err := store.Rename(ctx, at(".agents/b"), at(".agents/c"), false); err != nil {
		t.Fatal(err)
	}
	if store.quota.TotalBytes != 8 || store.quota.Entries != 2 {
		t.Fatalf("overwrite accounting = %+v", store.quota)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, Options{MaxFileSize: 8, MaxBytes: 8, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.quota != (storeQuota{TotalBytes: 8, Entries: 2}) {
		t.Fatalf("persistent quota = %+v", reopened.quota)
	}
	if got := read(t, reopened, ".agents/c"); got != "bbbb\x00\x00\x00\x00" {
		t.Fatalf("persisted contents = %q", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(physical(root, ".agents/c"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err = New(root, Options{MaxFileSize: 3, MaxBytes: 3, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.quota.TotalBytes != 3 {
		t.Fatal("changes between mounts were not rescanned")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEntryQuotaAndContainerReclamation(t *testing.T) {
	store, _ := newTestStore(t, Options{MaxFileSize: 10, MaxBytes: 10, MaxEntries: 2})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	put(t, store, ".agents/a", "a")
	_, err := store.Create(ctx, at(".agents/b"), os.O_RDWR)
	requireIs(t, err, syscall.ENOSPC)
	requireIs(t, store.Mkdir(ctx, at(".agents/b")), syscall.ENOSPC)
	requireIs(t, store.Mkdir(ctx, at(".other")), syscall.ENOSPC)
	requireIs(t, store.Mkdir(ctx, Location{otherID, "root", ".agents"}), syscall.ENOSPC)
	_, err = store.Create(ctx, at(".agents/a"), os.O_RDWR|os.O_TRUNC)
	requireIs(t, err, fs.ErrExist)
	requireIs(t, store.Mkdir(ctx, at(".agents")), fs.ErrExist)
	_, err = store.Open(ctx, at(".agents/b"), os.O_RDWR|os.O_CREATE)
	requireIs(t, err, fs.ErrInvalid)
	reader, err := store.Open(ctx, at(".agents/a"), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(ctx, at(".agents/a"), false); err != nil {
		t.Fatal(err)
	}
	mkdir(t, store, ".agents/child")
	if store.quota.Entries != 2 {
		t.Fatal("directories do not consume entry quota")
	}
	if err := store.Remove(ctx, at(".agents/child"), true); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(ctx, at(".agents"), true); err != nil {
		t.Fatal(err)
	}
	if store.quota != (storeQuota{}) || store.scopes != 0 || store.workspaces != 0 {
		t.Fatalf("empty scope not reclaimed: %+v, %d, %d", store.quota, store.scopes, store.workspaces)
	}
	for i := 0; i < 4; i++ {
		loc := Location{fmt.Sprintf("%08x-1234-1234-1234-123456789abc", i), "root", ".agents"}
		if err := store.Mkdir(ctx, loc); err != nil {
			t.Fatal(err)
		}
		if err := store.Remove(ctx, loc, true); err != nil {
			t.Fatal(err)
		}
	}
	if list, err := store.List(ctx, at("")); err != nil || len(list) != 0 {
		t.Fatalf("removed scope = %v, %v", list, err)
	}
}

func TestNewScansAndBoundsExistingNamespace(t *testing.T) {
	privateRoot := func(t *testing.T) string {
		t.Helper()
		root := t.TempDir()
		if err := os.Chmod(root, 0o700); err != nil {
			t.Fatal(err)
		}
		return root
	}
	for _, tc := range []struct {
		name string
		opts Options
		want error
	}{{"per file", Options{MaxFileSize: 3}, fserrors.ErrTooLarge},
		{"bytes", Options{MaxBytes: 7}, syscall.ENOSPC},
		{"entries", Options{MaxEntries: 2}, syscall.ENOSPC}} {
		t.Run(tc.name, func(t *testing.T) {
			root := privateRoot(t)
			if err := os.MkdirAll(physical(root, ".agents"), 0o700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a", "b"} {
				if err := os.WriteFile(physical(root, ".agents/"+name), []byte("1234"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := New(root, tc.opts)
			requireIs(t, err, tc.want)
		})
	}
	t.Run("bounded empty workspaces", func(t *testing.T) {
		root := privateRoot(t)
		for _, id := range []string{testWorkspace, otherID} {
			if err := os.Mkdir(filepath.Join(root, id), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		_, err := New(root, Options{MaxEntries: 1})
		requireIs(t, err, syscall.ENOSPC)
	})
	t.Run("bounded empty scopes", func(t *testing.T) {
		root := privateRoot(t)
		for _, id := range []string{"root", testParent} {
			if err := os.MkdirAll(filepath.Join(root, testWorkspace, id), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		_, err := New(root, Options{MaxEntries: 1})
		requireIs(t, err, syscall.ENOSPC)
	})
	for _, tc := range []struct {
		path string
		file bool
		want error
	}{
		{"display name", false, fs.ErrInvalid},
		{strings.ToUpper(testWorkspace), false, fs.ErrInvalid},
		{testWorkspace, true, fserrors.ErrNotDir},
		{testWorkspace + "/folder-name", false, fs.ErrInvalid},
		{testWorkspace + "/root", true, fserrors.ErrNotDir},
		{testWorkspace + "/root/plain", false, fs.ErrInvalid},
		{testWorkspace + "/root/.agents", true, fs.ErrInvalid},
		{testWorkspace + "/root/.fabric.json", true, fs.ErrInvalid},
		{testWorkspace + "/root/.agents/.platform", false, fs.ErrInvalid},
		{testWorkspace + "/root/.agents/child/.fabric.json", true, fs.ErrInvalid},
		{testWorkspace + "/root/.agents/" + strings.Repeat("a/", maxDepth) + "last", false, fs.ErrInvalid},
	} {
		t.Run(tc.path, func(t *testing.T) {
			root := privateRoot(t)
			path := filepath.Join(root, filepath.FromSlash(tc.path))
			if tc.file {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
			_, err := New(root, Options{})
			requireIs(t, err, tc.want)
		})
	}
}

func TestConcurrentGrowthCannotBypassQuota(t *testing.T) {
	store, _ := newTestStore(t, Options{MaxFileSize: 8, MaxBytes: 64, MaxEntries: 100})
	mkdir(t, store, ".agents")
	const count = 24
	start := make(chan struct{})
	failures := make(chan error, count*2)
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			file, err := store.Create(context.Background(), at(fmt.Sprintf(".agents/file-%d", i)), os.O_RDWR)
			if err != nil {
				failures <- err
				return
			}
			if i%2 == 0 {
				_, err = file.WriteAt(context.Background(), []byte("12345678"), 0)
			} else {
				err = file.Truncate(context.Background(), 8)
			}
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, syscall.ENOSPC) {
				failures <- err
			}
			if err := file.Close(); err != nil {
				failures <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if successes.Load() != 8 || store.quota.TotalBytes != 64 || store.quota.Entries != count+1 {
		t.Fatalf("concurrent quota = %+v, successes = %d", store.quota, successes.Load())
	}
	list, err := store.List(context.Background(), at(".agents"))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, info := range list {
		total += info.Size
	}
	if total != 64 {
		t.Fatalf("actual disk size = %d", total)
	}
}

func TestConcurrentCreateIsExclusive(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	mkdir(t, store, ".agents")
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			file, err := store.Create(context.Background(), at(".agents/shared"), os.O_RDWR|os.O_TRUNC)
			if err == nil {
				successes.Add(1)
				if err := file.Close(); err != nil {
					t.Error(err)
				}
			} else if !errors.Is(err, fs.ErrExist) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || store.quota.Entries != 2 {
		t.Fatalf("exclusive creation: successes=%d quota=%+v", successes.Load(), store.quota)
	}
}

func TestOpenHandlesMakeAffectedPathsBusy(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	mkdir(t, store, ".agents/sub")
	file := create(t, store, ".agents/sub/file")
	if _, err := file.WriteAt(ctx, []byte("original"), 0); err != nil {
		t.Fatal(err)
	}
	for _, flags := range []int{os.O_WRONLY, os.O_RDWR, os.O_WRONLY | os.O_TRUNC, os.O_WRONLY | os.O_APPEND} {
		_, err := store.Open(ctx, at(".agents/sub/file"), flags)
		requireIs(t, err, fserrors.ErrBusy)
	}
	reader, err := store.Open(ctx, at(".agents/sub/file"), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{".agents", ".agents/sub", ".agents/sub/file"} {
		requireIs(t, store.Rename(ctx, at(relative), at(".agents/new"), false), fserrors.ErrBusy)
		requireIs(t, store.Remove(ctx, at(relative), relative != ".agents/sub/file"), fserrors.ErrBusy)
	}
	put(t, store, ".agents/source", "source")
	requireIs(t, store.Rename(ctx, at(".agents/source"), at(".agents/sub/file"), false), fserrors.ErrBusy)
	put(t, store, ".agents/sub/file-other", "not in busy subtree")
	if err := store.Remove(ctx, at(".agents/sub/file-other"), false); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 8)
	if _, err := reader.ReadAt(ctx, buffer, 0); err != nil || string(buffer) != "original" {
		t.Fatalf("blocked O_TRUNC changed data: %q, %v", buffer, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(ctx, at(".agents/sub"), at(".agents/moved"), false); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(ctx, at(".agents/moved/file"), false); err != nil {
		t.Fatal(err)
	}
}

func TestRenameAndNonrecursiveRemoval(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	mkdir(t, store, ".agents/tree")
	mkdir(t, store, ".agents/tree/leaf")
	put(t, store, ".agents/tree/leaf/config", "config")
	put(t, store, ".agents/a", "a")
	put(t, store, ".agents/b", "bbbb")
	requireIs(t, store.Rename(ctx, at(".agents/a"), at(".agents/b"), true), fs.ErrExist)
	if read(t, store, ".agents/a") != "a" || read(t, store, ".agents/b") != "bbbb" {
		t.Fatal("noReplace overwrote data")
	}
	requireIs(t, store.Rename(ctx, at(".agents/a"), at(".agents/a"), true), fs.ErrExist)
	if err := store.Rename(ctx, at(".agents/a"), at(".agents/a"), false); err != nil {
		t.Fatal(err)
	}
	requireIs(t, store.Rename(ctx, at(".agents/a"), at(".agents/missing/b"), false), fs.ErrNotExist)
	requireIs(t, store.Rename(ctx, at(".agents/missing"), at(".agents/new"), false), fs.ErrNotExist)
	requireIs(t, store.Rename(ctx, at(".agents/a"), at(".file"), false), fs.ErrInvalid)
	if err := store.Rename(ctx, at(".agents/a"), at(".agents/b"), false); err != nil {
		t.Fatal(err)
	}
	if read(t, store, ".agents/b") != "a" || store.quota.TotalBytes != 7 {
		t.Fatalf("file overwrite quota = %+v", store.quota)
	}
	mkdir(t, store, ".agents/target")
	requireIs(t, store.Rename(ctx, at(".agents/tree"), at(".agents/target"), false), fserrors.ErrUnsupported)
	requireIs(t, store.Rename(ctx, at(".agents/tree"), at(".agents/target"), true), fs.ErrExist)
	requireIs(t, store.Rename(ctx, at(".agents/tree"), at(".agents/b"), false), fserrors.ErrUnsupported)
	requireIs(t, store.Rename(ctx, at(".agents/b"), at(".agents/target"), false), fserrors.ErrUnsupported)
	requireIs(t, store.Rename(ctx, at(".agents/tree"), at(".agents/tree/leaf/inside"), false), fs.ErrInvalid)
	requireIs(t, store.Remove(ctx, at(".agents/b"), true), fserrors.ErrNotDir)
	requireIs(t, store.Remove(ctx, at(".agents/tree"), false), fserrors.ErrIsDir)
	requireIs(t, store.Remove(ctx, at(".agents/tree"), true), fserrors.ErrNotEmpty)
	_, err := store.Stat(ctx, at(".agents/tree/leaf/config"))
	if err != nil {
		t.Fatal("rmdir removed a descendant:", err)
	}
	for _, dst := range []Location{{otherID, "root", ".agents/new"}, {testWorkspace, testParent, ".agents/new"}} {
		requireIs(t, store.Rename(ctx, at(".agents/b"), dst, false), fserrors.ErrCrossDevice)
	}
	if err := store.Rename(ctx, at(".agents/tree"), at(".agents/moved"), true); err != nil {
		t.Fatal(err)
	}
	if read(t, store, ".agents/moved/leaf/config") != "config" {
		t.Fatal("subtree rename lost data")
	}
	_, err = store.Stat(ctx, at(".agents/tree"))
	requireIs(t, err, fs.ErrNotExist)
	if err := store.Rename(ctx, at(".agents/moved"), at(".newroot"), false); err != nil {
		t.Fatal(err)
	}
	if read(t, store, ".newroot/leaf/config") != "config" {
		t.Fatal("new dot-root rename lost data")
	}
	if err := store.Rename(ctx, at(".newroot"), at(".renamed"), true); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(ctx, at(".renamed/leaf/config"), false); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(ctx, at(".renamed/leaf"), true); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(ctx, at(".renamed"), true); err != nil {
		t.Fatal(err)
	}
	requireIs(t, store.Remove(ctx, at(".renamed"), true), fs.ErrNotExist)
	if err := store.Remove(ctx, at(".agents/target"), true); err != nil {
		t.Fatal(err)
	}
	if store.quota.TotalBytes != 1 || store.quota.Entries != 2 {
		t.Fatalf("final rename/remove quota = %+v", store.quota)
	}
}

func TestConcurrentRenameNoReplace(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	mkdir(t, store, ".agents")
	const count = 8
	for i := 0; i < count; i++ {
		put(t, store, fmt.Sprintf(".agents/file-%d", i), "x")
	}
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := store.Rename(context.Background(), at(fmt.Sprintf(".agents/file-%d", i)), at(".agents/target"), true)
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, fs.ErrExist) {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 1 || store.quota.Entries != count+1 || store.quota.TotalBytes != count {
		t.Fatalf("rename noReplace successes=%d quota=%+v", successes.Load(), store.quota)
	}
}

func TestErrorsAreNotSuccessfulFallbacks(t *testing.T) {
	ctx := context.Background()
	t.Run("closed root errors survive missing scope list", func(t *testing.T) {
		store, err := New(filepath.Join(t.TempDir(), "store"), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.root.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = store.List(ctx, at(""))
		requireIs(t, err, fs.ErrClosed)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Sync and close expose underlying failures", func(t *testing.T) {
		store, err := New(filepath.Join(t.TempDir(), "store"), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Mkdir(ctx, at(".agents")); err != nil {
			t.Fatal(err)
		}
		file, err := store.Create(ctx, at(".agents/file"), os.O_RDWR)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.file.Close(); err != nil {
			t.Fatal(err)
		}
		underlyingSync := file.file.Sync()
		if underlyingSync == nil {
			t.Fatal("Sync on a closed descriptor unexpectedly succeeded")
		}
		requireIs(t, file.Flush(ctx), errors.Unwrap(underlyingSync))
		_, underlyingStat := file.file.Stat()
		var pathErr *fs.PathError
		if !errors.As(underlyingStat, &pathErr) {
			t.Fatalf("unexpected underlying stat error: %v", underlyingStat)
		}
		_, err = file.WriteAt(ctx, []byte("x"), 0)
		requireIs(t, err, pathErr.Err)
		_, err = file.ReadAt(ctx, []byte{0}, 0)
		requireIs(t, err, pathErr.Err)
		requireIs(t, file.Truncate(ctx, 0), pathErr.Err)
		requireIs(t, file.Close(), fs.ErrClosed)
		requireIs(t, file.Close(), fs.ErrClosed)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong scope container type is not empty", func(t *testing.T) {
		store, root := newTestStore(t, Options{})
		if err := os.WriteFile(filepath.Join(root, testWorkspace), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := store.List(ctx, at(""))
		requireIs(t, err, fserrors.ErrNotDir)
	})
	t.Run("external replacement is not silently adopted", func(t *testing.T) {
		store, root := newTestStore(t, Options{})
		mkdir(t, store, ".agents")
		put(t, store, ".agents/file", "first")
		if err := os.Rename(physical(root, ".agents/file"), filepath.Join(filepath.Dir(root), "old-file")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(physical(root, ".agents/file"), []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := store.Stat(ctx, at(".agents/file"))
		requireIs(t, err, fserrors.ErrConflict)
		_, err = store.List(ctx, at(".agents"))
		requireIs(t, err, fserrors.ErrConflict)
		_, err = store.Open(ctx, at(".agents/file"), os.O_RDWR|os.O_TRUNC)
		requireIs(t, err, fserrors.ErrConflict)
		data, err := os.ReadFile(physical(root, ".agents/file"))
		if err != nil || string(data) != "replacement" {
			t.Fatalf("replacement changed: %q, %v", data, err)
		}
	})
}

func TestWindowsCaseAliasesShareAccountingAndBusyState(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows case-insensitive namespace")
	}
	store, _ := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	file := create(t, store, ".agents/config")
	if _, err := file.WriteAt(ctx, []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	_, err := store.Open(ctx, at(".AGENTS/CONFIG"), os.O_RDWR|os.O_TRUNC)
	requireIs(t, err, fserrors.ErrBusy)
	requireIs(t, store.Remove(ctx, at(".AGENTS/CONFIG"), false), fserrors.ErrBusy)
	_, err = store.Create(ctx, at(".AGENTS/CONFIG"), os.O_RDWR)
	requireIs(t, err, fs.ErrExist)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	mkdir(t, store, ".AGENTS/child")
	put(t, store, ".AGENTS/CHILD/file", "xx")
	if err := store.Rename(ctx, at(".AGENTS/CHILD"), at(".AGENTS/new"), false); err != nil {
		t.Fatal(err)
	}
	if read(t, store, ".agents/new/file") != "xx" || store.quota.TotalBytes != 7 {
		t.Fatalf("case alias accounting = %+v", store.quota)
	}
}

func TestHandleStructuralContract(t *testing.T) {
	var _ interface {
		ReadAt(context.Context, []byte, int64) (int, error)
		WriteAt(context.Context, []byte, int64) (int, error)
		Truncate(context.Context, int64) error
		Flush(context.Context) error
		Close() error
		Size() int64
	} = (*File)(nil)
}

func TestAppendUsesSharedQuotaAndReadersSeeWrites(t *testing.T) {
	store, _ := newTestStore(t, Options{MaxFileSize: 8, MaxBytes: 8})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	put(t, store, ".agents/other", "1234")
	put(t, store, ".agents/log", "12")
	reader, err := store.Open(ctx, at(".agents/log"), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := store.Open(ctx, at(".agents/log"), os.O_RDWR|os.O_APPEND)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAt(ctx, []byte("34"), 0); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if n, err := reader.ReadAt(ctx, got, 0); n != 4 || err != nil || string(got) != "1234" || reader.Size() != 4 {
		t.Fatalf("shared reader = %q, %d, %v", got, n, err)
	}
	_, err = writer.WriteAt(ctx, []byte("5"), 0)
	requireIs(t, err, syscall.ENOSPC)
	if err := writer.Truncate(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAt(ctx, []byte("56"), 999); err != nil {
		t.Fatal(err)
	}
	if n, err := reader.ReadAt(ctx, got, 0); n != 4 || err != nil || string(got) != "1256" {
		t.Fatalf("append after shrink = %q, %d, %v", got, n, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentEntriesCannotBypassQuota(t *testing.T) {
	store, _ := newTestStore(t, Options{MaxEntries: 9})
	mkdir(t, store, ".agents")
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			if i%2 == 0 {
				err = store.Mkdir(context.Background(), at(fmt.Sprintf(".agents/dir-%d", i)))
			} else {
				var file *File
				file, err = store.Create(context.Background(), at(fmt.Sprintf(".agents/file-%d", i)), os.O_RDWR)
				if err == nil {
					err = file.Close()
				}
			}
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, syscall.ENOSPC) {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 8 || store.quota.Entries != 9 {
		t.Fatalf("concurrent entries successes=%d quota=%+v", successes.Load(), store.quota)
	}
}

func TestDepthLimitAlsoAppliesToRenamedDescendants(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	parent := ".agents"
	for i := 1; i < maxDepth-1; i++ {
		parent += "/x"
		mkdir(t, store, parent)
	}
	put(t, store, parent+"/file", "boundary")
	mkdir(t, store, ".agents/sub")
	put(t, store, ".agents/sub/file", "data")
	requireIs(t, store.Rename(ctx, at(".agents/sub"), at(parent+"/moved"), false), fs.ErrInvalid)
	if read(t, store, ".agents/sub/file") != "data" {
		t.Fatal("rejected deep rename moved a file")
	}
	mkdir(t, store, parent+"/empty")
	_, err := store.Create(ctx, at(parent+"/empty/file"), os.O_RDWR)
	requireIs(t, err, fs.ErrInvalid)
}

func TestUnderlyingWriteAndTruncateErrorsKeepAccounting(t *testing.T) {
	store, _ := newTestStore(t, Options{})
	ctx := context.Background()
	mkdir(t, store, ".agents")
	file := create(t, store, ".agents/file")
	if _, err := file.WriteAt(ctx, []byte("original"), 0); err != nil {
		t.Fatal(err)
	}
	if err := file.file.Close(); err != nil {
		t.Fatal(err)
	}
	readonly, err := store.root.OpenFile(diskPath(file.entry.path), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	file.file = readonly
	_, rawWrite := readonly.WriteAt([]byte("bad"), 0)
	var writeErr *fs.PathError
	if !errors.As(rawWrite, &writeErr) {
		t.Fatalf("expected native read-only write error, got %v", rawWrite)
	}
	n, err := file.WriteAt(ctx, []byte("bad"), 0)
	requireIs(t, err, writeErr.Err)
	if n != 0 || store.quota.TotalBytes != 8 {
		t.Fatalf("failed write = %d, quota=%+v", n, store.quota)
	}
	rawTruncate := readonly.Truncate(0)
	var truncateErr *fs.PathError
	if !errors.As(rawTruncate, &truncateErr) {
		t.Fatalf("expected native read-only truncate error, got %v", rawTruncate)
	}
	requireIs(t, file.Truncate(ctx, 0), truncateErr.Err)
	if file.Size() != 8 || store.quota.TotalBytes != 8 {
		t.Fatalf("failed truncate changed size: %+v", store.quota)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if read(t, store, ".agents/file") != "original" {
		t.Fatal("failed I/O changed data")
	}
}

func TestDisappearedEntriesAreNotRecreatedWithStaleQuota(t *testing.T) {
	ctx := context.Background()
	t.Run("file", func(t *testing.T) {
		store, root := newTestStore(t, Options{})
		mkdir(t, store, ".agents")
		put(t, store, ".agents/file", "data")
		if err := os.Remove(physical(root, ".agents/file")); err != nil {
			t.Fatal(err)
		}
		_, err := store.Create(ctx, at(".agents/file"), os.O_RDWR)
		requireIs(t, err, fserrors.ErrConflict)
		if store.quota.TotalBytes != 4 || store.quota.Entries != 2 {
			t.Fatalf("stale quota changed: %+v", store.quota)
		}
	})
	t.Run("directory", func(t *testing.T) {
		store, root := newTestStore(t, Options{})
		mkdir(t, store, ".agents")
		mkdir(t, store, ".agents/child")
		if err := os.Remove(physical(root, ".agents/child")); err != nil {
			t.Fatal(err)
		}
		requireIs(t, store.Mkdir(ctx, at(".agents/child")), fserrors.ErrConflict)
	})
	t.Run("container", func(t *testing.T) {
		store, root := newTestStore(t, Options{})
		mkdir(t, store, ".agents")
		if err := os.Remove(physical(root, ".agents")); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(root, testWorkspace, "root")); err != nil {
			t.Fatal(err)
		}
		requireIs(t, store.Mkdir(ctx, at(".new")), fserrors.ErrConflict)
	})
}
