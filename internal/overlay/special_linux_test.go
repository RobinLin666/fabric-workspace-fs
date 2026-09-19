package overlay

import (
	"context"
	"io/fs"
	"syscall"
	"testing"
)

func TestLinuxSpecialFilesAreRejectedWithoutOpening(t *testing.T) {
	store, root := newTestStore(t, Options{})
	mkdir(t, store, ".agents")
	path := physical(root, ".agents/pipe")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := store.Stat(context.Background(), at(".agents/pipe"))
	requireIs(t, err, fs.ErrPermission)
	_, err = store.List(context.Background(), at(".agents"))
	requireIs(t, err, fs.ErrPermission)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = New(root, Options{})
	requireIs(t, err, fs.ErrPermission)
}
