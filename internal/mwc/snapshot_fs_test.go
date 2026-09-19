package mwc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"
	"fabric-workspace-fs/internal/writeback"
)

func newSnapshotFS(t *testing.T, backend *Client) (*workspacefs.FS, string) {
	t.Helper()
	var id [12]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	// Keep the private test spool in the package directory, never the system
	// temporary directory. Remove only this test's newly created directory.
	spool := filepath.Join(".", "snapshot-fs-spool-"+hex.EncodeToString(id[:]))
	if err := os.Mkdir(spool, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(spool); err != nil {
			t.Errorf("remove owned snapshot test spool: %v", err)
		}
	})
	public := testutil.New(t)
	fab, lake := public.Clients()
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs = []string{testWorkspace}
	opts.CacheTTL = 0
	opts.SpoolDirectory = spool
	opts.ResourceBackend = backend
	s, err := workspacefs.New(fab, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close snapshot filesystem: %v", err)
		}
		if counts := public.Counts(); counts != (testutil.Counts{}) {
			t.Errorf("direct resource handles accessed Fabric definitions/catalog or OneLake: %+v", counts)
		}
	})
	return s, spool
}

func snapshotFSEntry() workspacefs.Entry {
	return workspacefs.Entry{
		Name: "large.bin", Kind: workspacefs.ResourceFile, Workspace: testWorkspace,
		Resource: notebookPath("large.bin"),
	}
}

func snapshotFileGETs(w *fakeWire) int {
	var count int
	for _, request := range w.recorded() {
		if request.method == http.MethodGet && request.query == "" &&
			strings.HasSuffix(request.path, "/filesystem/workdir/large.bin") {
			count++
		}
	}
	return count
}

func assertSnapshotOpenGETs(t *testing.T, w *fakeWire, before int) {
	t.Helper()
	downloads := snapshotFileGETs(w) - before
	if downloads < 1 || downloads > 2 {
		t.Fatalf("resource Open downloaded the whole file %d times; want at most one Stat plus one Snapshot", downloads)
	}
	t.Logf("resource Open full-content HTTP GETs: %d", downloads)
}

func readSnapshotFSHandle(t *testing.T, handle workspacefs.Handle, chunkSize int) []byte {
	t.Helper()
	result := make([]byte, handle.Size())
	for offset := 0; offset < len(result); {
		size := min(chunkSize, len(result)-offset)
		n, err := handle.ReadAt(context.Background(), result[offset:offset+size], int64(offset))
		if err != nil || n != size {
			t.Fatalf("resource ReadAt offset=%d returned n=%d, err=%v", offset, n, err)
		}
		offset += n
	}
	return result
}

func TestResourceFSSnapshotHTTPCountsAndPartialWritesTTLZero(t *testing.T) {
	for _, size := range []int{320 << 10, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			w := newWire(t)
			initial := bytes.Repeat([]byte{0, 0xff, 'a', '\n'}, size/4)
			w.files["large.bin"] = initial
			backend := w.client(func(opts *Options) { opts.CacheTTL = 0 })
			s, spool := newSnapshotFS(t, backend)
			ctx := context.Background()
			entry := snapshotFSEntry()
			before := snapshotFileGETs(w)
			writer, err := s.Open(ctx, entry, os.O_RDWR)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			assertSnapshotOpenGETs(t, w, before)
			openedRequests := len(w.recorded())
			if loaded := readSnapshotFSHandle(t, writer, 64<<10); !bytes.Equal(loaded, initial) {
				t.Fatal("spool loader lost initial file bytes")
			}
			if len(w.recorded()) != openedRequests {
				t.Fatal("reading the populated spool performed additional HTTP requests")
			}
			expected := append([]byte(nil), initial...)
			edit := []byte{0xff, 0, 'e', 'd', 'i', 't', '\n'}
			offset := int64(64<<10 + 7)
			if n, err := writer.WriteAt(ctx, edit, offset); err != nil || n != len(edit) {
				t.Fatalf("partial resource write: n=%d, err=%v", n, err)
			}
			copy(expected[offset:], edit)
			expected = expected[:len(expected)-97]
			if err := writer.Truncate(ctx, int64(len(expected))); err != nil {
				t.Fatal(err)
			}
			if writer.Size() != int64(len(expected)) {
				t.Fatal("resource spool size did not follow truncate")
			}
			if len(w.recorded()) != openedRequests {
				t.Fatal("offset writes or truncate performed an early upload")
			}
			if err := writer.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if uploads := w.count(http.MethodPut, "/filesystem/workdir/large.bin"); uploads != 1 {
				t.Fatalf("dirty resource fsync uploaded %d times; want one", uploads)
			}
			w.mu.Lock()
			saved := append([]byte(nil), w.files["large.bin"]...)
			w.mu.Unlock()
			if !bytes.Equal(saved, expected) {
				t.Fatal("HTTP PUT did not preserve offset edits, unchanged bytes, and truncation")
			}
			flushedRequests := len(w.recorded())
			if err := writer.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if len(w.recorded()) != flushedRequests {
				t.Fatal("second clean fsync performed another HTTP request")
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if len(w.recorded()) != flushedRequests {
				t.Fatal("clean close repeated the upload")
			}
			if files, err := os.ReadDir(spool); err != nil || len(files) != 0 {
				t.Fatalf("clean resource close retained a spool: entries=%d, err=%v", len(files), err)
			}

			before = snapshotFileGETs(w)
			reader, err := s.Open(ctx, entry, os.O_RDONLY)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			assertSnapshotOpenGETs(t, w, before)
			openedRequests = len(w.recorded())
			openedDownloads := snapshotFileGETs(w)
			tokenCalls := len(w.credentials.scopes)
			backend.contents.Clear()
			backend.listings.Clear()
			w.advance(2 * time.Hour)
			w.credentials.set("rotated-fake-resource-identity")
			w.mu.Lock()
			w.files["large.bin"] = []byte("external replacement after read open")
			w.mu.Unlock()
			if got := readSnapshotFSHandle(t, reader, 4093); !bytes.Equal(got, expected) {
				t.Fatal("ordinary chunked read lost its pinned snapshot")
			}
			if len(w.recorded()) != openedRequests || snapshotFileGETs(w) != openedDownloads ||
				len(w.credentials.scopes) != tokenCalls {
				t.Fatal("ordinary chunked reads refetched content or credentials after cache clear/expiry")
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResourceFSSnapshotHTTPConflictPreservesDirtyBytes(t *testing.T) {
	w := newWire(t)
	initial := bytes.Repeat([]byte{0, 'a', 0xff, '\n'}, 96<<10)
	w.files["large.bin"] = initial
	backend := w.client(func(opts *Options) { opts.CacheTTL = 0 })
	s, spool := newSnapshotFS(t, backend)
	ctx := context.Background()
	before := snapshotFileGETs(w)
	writer, err := s.Open(ctx, snapshotFSEntry(), os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	assertSnapshotOpenGETs(t, w, before)
	expected := append([]byte(nil), initial...)
	edit := []byte("local-edit")
	const offset = 131071
	if n, err := writer.WriteAt(ctx, edit, offset); err != nil || n != len(edit) {
		t.Fatalf("offset edit: n=%d, err=%v", n, err)
	}
	copy(expected[offset:], edit)
	expected = expected[:len(expected)-19]
	if err := writer.Truncate(ctx, int64(len(expected))); err != nil {
		t.Fatal(err)
	}
	external := append([]byte(nil), initial...)
	external[0] ^= 0xff
	w.mu.Lock()
	w.files["large.bin"] = external
	w.mu.Unlock()
	if err := writer.Flush(ctx); !errors.Is(err, fserrors.ErrConflict) {
		t.Fatalf("changed remote content did not produce an explicit conflict: %v", err)
	}
	if uploads := w.count(http.MethodPut, "/filesystem/workdir/large.bin"); uploads != 0 {
		t.Fatalf("known conflict uploaded %d times", uploads)
	}
	failedRequests := len(w.recorded())
	if dirty := readSnapshotFSHandle(t, writer, 4093); !bytes.Equal(dirty, expected) {
		t.Fatal("failed fsync discarded local edits or truncate state")
	}
	var recovery *writeback.RecoveryError
	if err := writer.Close(); !errors.As(err, &recovery) || !errors.Is(err, fserrors.ErrConflict) {
		t.Fatalf("dirty close did not preserve the conflict and recovery file: %v", err)
	}
	if len(w.recorded()) != failedRequests {
		t.Fatal("reading or closing a conflicted spool caused an implicit HTTP retry")
	}
	spoolPath, err := filepath.Abs(spool)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPath, err := filepath.Abs(recovery.Path)
	if err != nil || filepath.Dir(recoveryPath) != spoolPath {
		t.Fatal("recovery file escaped the test-owned spool directory")
	}
	recovered, err := os.ReadFile(recoveryPath)
	if err != nil || !bytes.Equal(recovered, expected) {
		t.Fatalf("dirty resource bytes were not retained intact: %v", err)
	}
	w.mu.Lock()
	remote := append([]byte(nil), w.files["large.bin"]...)
	w.mu.Unlock()
	if !bytes.Equal(remote, external) {
		t.Fatal("conflict handling overwrote the external resource change")
	}
}
