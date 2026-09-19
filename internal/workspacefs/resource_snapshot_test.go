package workspacefs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/writeback"
)

func TestResourceSnapshotIOIsConstantWhenTTLIsZero(t *testing.T) {
	s, _, store := resourceTestFS(t)
	owner := resources.Target{WorkspaceID: testutil.WorkspaceID, ItemID: testutil.NotebookID, Kind: "Notebook"}
	path := resources.Path{Target: owner, Relative: "large.bin"}
	data := bytes.Repeat([]byte{0, 1, 2, 0xff}, 128<<10)
	store.Seed(path, data)
	e := Entry{Name: "large.bin", Kind: ResourceFile, Workspace: owner.WorkspaceID, Resource: path}
	ctx := context.Background()
	before, _ := store.Counts()
	h, err := s.Open(ctx, e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := store.Counts()
	if after-before != 2 {
		t.Fatalf("spool load downloaded per chunk instead of one stat+snapshot: %d", after-before)
	}
	if _, err := h.WriteAt(ctx, []byte("edit"), 131073); err != nil {
		t.Fatal(err)
	}
	copy(data[131073:], []byte("edit"))
	data = data[:len(data)-97]
	if err := h.Truncate(ctx, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, writes := store.Counts()
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, current := store.Counts()
	if current != writes {
		t.Fatal("clean resource fsync repeated upload")
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ = store.Counts()
	reader, err := s.Open(ctx, e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	opened, _ := store.Counts()
	if opened-before != 2 {
		t.Fatal("read open must pin a single snapshot", opened-before)
	}
	read := make([]byte, len(data))
	for offset := 0; offset < len(read); offset += 4096 {
		size := min(4096, len(read)-offset)
		if _, err := reader.ReadAt(ctx, read[offset:offset+size], int64(offset)); err != nil {
			t.Fatal(err)
		}
	}
	after, _ = store.Counts()
	if after != opened || !bytes.Equal(read, data) {
		t.Fatal("chunked read refetched backend or changed bytes", opened, after)
	}
	store.Seed(path, []byte("external replacement"))
	first := make([]byte, 4)
	if _, err := reader.ReadAt(ctx, first, 0); err != nil || !bytes.Equal(first, data[:4]) {
		t.Fatal("existing read handle lost its pinned snapshot", first, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestResourcePartialWriteConflictRetainsDirtySnapshot(t *testing.T) {
	s, _, store := resourceTestFS(t)
	path := resources.Path{Target: resources.Target{WorkspaceID: testutil.WorkspaceID, ItemID: testutil.NotebookID, Kind: "Notebook"}, Relative: "large.bin"}
	store.Seed(path, bytes.Repeat([]byte("source"), 64<<10))
	e := Entry{Name: "large.bin", Kind: ResourceFile, Workspace: path.Target.WorkspaceID, Resource: path}
	h, err := s.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt(context.Background(), []byte("local-edit"), 65540); err != nil {
		t.Fatal(err)
	}
	store.Seed(path, []byte("outside"))
	if err := h.Flush(context.Background()); !errors.Is(err, fserrors.ErrConflict) {
		t.Fatal("stale snapshot overwrote resource", err)
	}
	var recovery *writeback.RecoveryError
	if err := h.Close(); !errors.As(err, &recovery) {
		t.Fatal("failed resource save discarded dirty bytes", err)
	}
	data, err := os.ReadFile(recovery.Path)
	if err != nil || string(data[65540:65550]) != "local-edit" {
		t.Fatal("resource recovery is incomplete", err)
	}
}
