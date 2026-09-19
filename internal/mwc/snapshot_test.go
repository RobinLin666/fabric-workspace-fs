package mwc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
)

func TestSnapshotPinsOneDownloadAcrossChunksAndCacheEviction(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Minute} {
		t.Run(ttl.String(), func(t *testing.T) {
			w := newWire(t)
			payload := bytes.Repeat([]byte{0, 0xff, '%', '\n', 'x'}, 64<<10)
			w.files["file"] = payload
			w.contentType = "application/json"
			c := w.client(func(o *Options) { o.CacheTTL = ttl })
			ctx := context.Background()
			version := versionFor(payload)
			snapshot, err := c.Snapshot(ctx, notebookPath("file"), version)
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			if snapshot.Size() != int64(len(payload)) || snapshot.Version() != version {
				t.Fatal("snapshot did not preserve its exact size and content hash")
			}
			if ttl == 0 && c.contents.Stats().Entries != 0 {
				t.Fatal("TTL zero unexpectedly retained shared content")
			}
			if ttl > 0 {
				// Evict the downloaded entry without making any HTTP requests.
				for i := range 129 {
					_, err := c.contents.Get(ctx, fmt.Sprintf("other-entry-%d", i), func(context.Context) (fileContent, error) {
						return fileContent{bytes: []byte("other")}, nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if c.contents.Stats().Entries != 128 {
					t.Fatal("content cache did not exercise bounded LRU eviction")
				}
			}
			w.advance(2 * time.Hour)
			w.credentials.set("rotated-fake-identity")
			w.mu.Lock()
			w.files["file"] = []byte("changed after snapshot")
			w.mu.Unlock()
			requests, tokenCalls := len(w.recorded()), len(w.credentials.scopes)
			chunkSize := len(payload) / 4
			for i := range 4 {
				offset := i * chunkSize
				chunk := make([]byte, chunkSize)
				n, err := snapshot.ReadAt(chunk, int64(offset))
				if err != nil || n != len(chunk) || !bytes.Equal(chunk, payload[offset:offset+chunkSize]) {
					t.Fatalf("snapshot chunk %d changed or failed: n=%d err=%v", i, n, err)
				}
			}
			if w.count(http.MethodGet, "/filesystem/") != 1 || w.count(http.MethodGet, "/workdir/file") != 1 ||
				len(w.recorded()) != requests || len(w.credentials.scopes) != tokenCalls {
				t.Fatal("snapshot read performed repeated downloads, metadata preflights, or authentication")
			}
			for _, req := range w.recorded() {
				if strings.Contains(req.path, "/filesystem/") && (req.query != "" || req.headers.Get("Range") != "") {
					t.Fatal("snapshot used a listing or unverified range protocol")
				}
			}
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := snapshot.ReadAt(make([]byte, 1), 0); !errors.Is(err, fserrors.ErrClosed) {
				t.Fatal("closed snapshot remained readable")
			}
		})
	}
}

func TestSnapshotVersionChecksAndReadonlyEnvironment(t *testing.T) {
	w := newWire(t)
	w.files["file"] = []byte("environment file")
	c := w.client(func(o *Options) { o.CacheTTL = 0 })
	path := resources.Path{Target: testEnv, Relative: "file"}
	ctx := context.Background()
	for _, version := range []string{"", versionFor(w.files["file"])} {
		snapshot, err := c.Snapshot(ctx, path, version)
		if err != nil {
			t.Fatal(err)
		}
		data := make([]byte, snapshot.Size())
		if n, err := snapshot.ReadAt(data, 0); err != nil || int64(n) != snapshot.Size() || !bytes.Equal(data, w.files["file"]) {
			t.Fatalf("read-only Environment snapshot: n=%d err=%v", n, err)
		}
		snapshot.Close()
	}
	if snapshot, err := c.Snapshot(ctx, path, versionFor([]byte("different"))); snapshot != nil ||
		!errors.Is(err, fserrors.ErrConflict) {
		t.Fatalf("snapshot ignored content-version mismatch: %v", err)
	}
	for _, req := range w.recorded() {
		if strings.Contains(req.path, "/filesystem/") {
			if req.method != http.MethodGet || req.query != "" ||
				!strings.Contains(req.path, "/capacities/"+envCapacity+"/workloads/SparkCore/") ||
				!strings.HasSuffix(req.path, "/workspaces/"+envWorkspace+"/artifacts/"+envItem+"/filesystem/workdir/file") {
				t.Fatal("Environment snapshot used a write or another target's resource identity")
			}
		}
	}
	if w.count(http.MethodGet, "/workdir/file") != 3 {
		t.Fatal("TTL-zero snapshots did not fetch exactly once per snapshot request")
	}
}

func TestSnapshotPathAndContextGuardsBeforeCredentials(t *testing.T) {
	w := newWire(t)
	c := w.client()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	invalidTarget := notebookPath("file")
	invalidTarget.Target.ItemID = "invalid"
	for _, tc := range []struct {
		ctx  context.Context
		path resources.Path
		want error
	}{
		{context.Background(), notebookPath(""), fserrors.ErrIsDir},
		{context.Background(), notebookPath("../escape"), fs.ErrInvalid},
		{context.Background(), notebookPath("bad\n"), fs.ErrInvalid},
		{context.Background(), invalidTarget, fs.ErrInvalid},
		{nil, notebookPath("file"), fs.ErrInvalid},
		{canceled, notebookPath("file"), context.Canceled},
	} {
		if snapshot, err := c.Snapshot(tc.ctx, tc.path, ""); snapshot != nil || !errors.Is(err, tc.want) {
			t.Fatalf("snapshot guard: got %v, want %v", err, tc.want)
		}
	}
	if len(w.recorded()) != 0 || len(w.credentials.scopes) != 0 {
		t.Fatal("invalid snapshot request reached authentication or HTTP")
	}
}

type snapshotCancelReader struct {
	reader io.Reader
	cancel context.CancelFunc
}

func (r snapshotCancelReader) Read(dest []byte) (int, error) {
	n, err := r.reader.Read(dest)
	r.cancel()
	return n, err
}

func TestSnapshotBoundedBodyAndCancellation(t *testing.T) {
	for _, kind := range []string{"known oversized", "chunked oversized", "canceled during body", "forbidden"} {
		t.Run(kind, func(t *testing.T) {
			w := newWire(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &observedBody{reader: strings.NewReader(strings.Repeat("x", 100))}
			if kind == "canceled during body" {
				body.reader = snapshotCancelReader{reader: strings.NewReader("file"), cancel: cancel}
			}
			w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
				if !strings.HasSuffix(r.URL.Path, "/workdir/file") {
					return nil, nil, false
				}
				resp := response(http.StatusOK, nil)
				resp.Body, resp.ContentLength = body, -1
				switch kind {
				case "known oversized":
					resp.ContentLength = 100
				case "forbidden":
					resp.StatusCode = http.StatusForbidden
				}
				return resp, nil, true
			}
			c := w.client(func(o *Options) { o.MaxFileSize = 8 })
			want := fserrors.ErrTooLarge
			if kind == "canceled during body" {
				want = context.Canceled
			} else if kind == "forbidden" {
				want = fs.ErrPermission
			}
			if snapshot, err := c.Snapshot(ctx, notebookPath("file"), ""); snapshot != nil || !errors.Is(err, want) {
				t.Fatalf("snapshot failure: got %v, want %v", err, want)
			}
			if !body.closed.Load() || c.contents.Stats().Entries != 0 {
				t.Fatal("failed snapshot leaked its body or retained content")
			}
			if kind == "known oversized" && body.read.Load() != 0 ||
				kind == "chunked oversized" && body.read.Load() > 9 {
				t.Fatal("snapshot content exceeded its bounded read")
			}
			if w.count(http.MethodGet, "/filesystem/") != 1 {
				t.Fatal("snapshot failure caused an additional metadata request")
			}
		})
	}
}

func TestSnapshotRejectsExpiredGrantBeforeFileGET(t *testing.T) {
	w := newWire(t)
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "generatemwctoken") {
			return jsonResponse(http.StatusOK, map[string]any{
				"Token": testMWC, "TargetUriHost": "resources.mwc.test",
				"CapacityObjectId": testCapacity, "Expiry": testNow.Unix(),
			}), nil, true
		}
		return nil, nil, false
	}
	if snapshot, err := w.client().Snapshot(context.Background(), notebookPath("file"), ""); snapshot != nil || err == nil {
		t.Fatal("expired grant produced a snapshot")
	}
	if w.count(http.MethodGet, "/filesystem/") != 0 {
		t.Fatal("expired grant reached the file endpoint")
	}
}
