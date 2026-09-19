package resources

import (
	"errors"
	"io"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
)

func TestSnapshotIsImmutableVersionedAndClosable(t *testing.T) {
	data := []byte("snapshot")
	snapshot := NewSnapshot(data, "content-sha")
	data[0] = 'X'
	result := make([]byte, 10)
	n, err := snapshot.ReadAt(result, 0)
	if n != 8 || !errors.Is(err, io.EOF) || string(result[:n]) != "snapshot" || snapshot.Size() != 8 || snapshot.Version() != "content-sha" {
		t.Fatal(n, err, string(result), snapshot.Version())
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.ReadAt(result, 0); !errors.Is(err, fserrors.ErrClosed) {
		t.Fatal("closed snapshot readable", err)
	}
}
