package mwc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

func TestListingSchemasAndStrictChildNames(t *testing.T) {
	child := `{"name":"sp ace%雪","fileSystemEntryType":"file","contentLength":"12","lastModified":"Fri, 18 Sep 2026 12:00:00 GMT"}`
	for _, wire := range []string{
		`{"children":[` + child + `]}`, `{"entries":[` + child + `]}`,
		`{"value":[` + child + `]}`, `[` + child + `]`,
	} {
		entries, err := parseListing([]byte(wire), "parent")
		if err != nil || len(entries) != 1 || entries[0].info.Path != "parent/sp ace%雪" ||
			entries[0].info.Size != 12 || !entries[0].info.Modified.Equal(testNow) || !entries[0].sizeKnown {
			t.Fatalf("listing = %+v, %v", entries, err)
		}
		if entries[0].info.Version != "" {
			t.Fatal("metadata-only listing fabricated a content version")
		}
	}
	for _, wire := range []string{`{"children":[]}`, `{"entries":[]}`, `{"value":[]}`, `[]`,
		`{"children":[],"value":[{"ignored":true}]}`} {
		entries, err := parseListing([]byte(wire), "")
		if err != nil || len(entries) != 0 {
			t.Fatalf("explicit empty array rejected: %v", err)
		}
	}
	for _, wire := range []string{
		``, `null`, `{}`, `{"error":"denied"}`, `{"children":null}`, `{"children":{},"entries":[]}`,
		`{"children":[],"nextLink":"https://foreign.test/"}`, `{"children":[],"hasMore":true}`,
		`{"children":[null]}`, `{"children":[{}]}`,
		`{"children":[{"name":"file","fileSystemEntryType":"File"}]}`,
		`{"children":[{"name":"file","fileSystemEntryType":"link"}]}`,
		`{"children":[{"name":"../escape","fileSystemEntryType":"file"}]}`,
		`{"children":[{"name":"parent/file","fileSystemEntryType":"file"}]}`,
		`{"children":[{"name":"/absolute","fileSystemEntryType":"file"}]}`,
		`{"children":[{"name":"..","fileSystemEntryType":"file"}]}`,
		`{"children":[{"name":"back\\slash","fileSystemEntryType":"file"}]}`,
		`{"children":[{"name":"bad\u0000","fileSystemEntryType":"file"}]}`,
		`{"children":[{"name":"bad\n","fileSystemEntryType":"file"}]}`,
		`{"children":[{"name":"file","fileSystemEntryType":"file","contentLength":-1}]}`,
		`{"children":[{"name":"file","fileSystemEntryType":"file","contentLength":null}]}`,
		`{"children":[{"name":"file","fileSystemEntryType":"file","contentLength":1.5}]}`,
		`{"children":[{"name":"file","fileSystemEntryType":"file","contentLength":"overflow999999999999999999999"}]}`,
		`{"children":[{"name":"file","fileSystemEntryType":"file","lastModified":"yesterday"}]}`,
		`{"children":[` + child + `,` + child + `]}`,
		"{\"children\":[{\"name\":\"bad\xff\",\"fileSystemEntryType\":\"file\"}]}",
	} {
		if _, err := parseListing([]byte(wire), "parent"); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("invalid schema accepted: %q (%v)", wire, err)
		}
	}
	tooMany := `{"children":[` + strings.Repeat(`{"name":"x","fileSystemEntryType":"folder"},`, maxListingEntries) +
		`{"name":"last","fileSystemEntryType":"folder"}]}`
	if _, err := parseListing([]byte(tooMany), ""); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal("listing entry bound not applied")
	}
}

func TestBinaryReadsAreBoundedCachedAndVersioned(t *testing.T) {
	w := newWire(t)
	payload := []byte{0, 0xff, '{', '}', '\r', '\n', 0, 'x'}
	w.files["file"] = payload
	w.contentType = "application/json"
	c := w.client()
	ctx := context.Background()
	info, err := c.Stat(ctx, notebookPath("file"))
	if err != nil || info.Size != int64(len(payload)) || info.Version != "" {
		t.Fatalf("Stat: %+v, %v", info, err)
	}
	if w.count(http.MethodGet, "/workdir/file") != 0 {
		t.Fatal("stat downloaded content")
	}
	info.Version = versionFor(payload)
	dest := make([]byte, 3)
	n, err := c.Read(ctx, notebookPath("file"), 2, dest, info.Version)
	if err != nil || n != 3 || !bytes.Equal(dest, payload[2:5]) {
		t.Fatalf("offset read = %d, %v, %v", n, dest, err)
	}
	dest[0] = 42
	whole := make([]byte, len(payload)+5)
	n, err = c.Read(ctx, notebookPath("file"), 0, whole, info.Version)
	if !errors.Is(err, io.EOF) || n != len(payload) || !bytes.Equal(whole[:n], payload) {
		t.Fatal("partial EOF or immutable cached bytes incorrect")
	}
	if n, err := c.Read(ctx, notebookPath("file"), int64(len(payload)), dest, info.Version); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatal("EOF offset incorrect")
	}
	if n, err := c.Read(ctx, notebookPath("file"), 1<<62, dest, info.Version); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatal("large offset overflowed")
	}
	if _, err := c.Read(ctx, notebookPath("file"), -1, dest, info.Version); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal("negative offset accepted")
	}
	if n, err := c.Read(ctx, notebookPath("file"), 0, dest, "wrong-version"); n != 0 || !errors.Is(err, fserrors.ErrConflict) {
		t.Fatal("version mismatch did not fail")
	}
	if w.count(http.MethodGet, "/workdir/file") != 1 {
		t.Fatal("cached offset reads repeatedly downloaded full bytes")
	}
	if n, err := c.Read(ctx, notebookPath("file"), 0, nil, info.Version); n != 0 || err != nil {
		t.Fatal("empty read failed")
	}
}

func TestUnknownSizeIsResolvedAndListingResultsAreSnapshots(t *testing.T) {
	w := newWire(t)
	w.omitSizes = true
	w.files["file"] = []byte("exact length")
	w.files["empty"] = []byte{}
	w.directories["directory"] = true
	c := w.client()
	infos, err := c.List(context.Background(), notebookPath(""))
	if err != nil || len(infos) != 3 {
		t.Fatalf("List: %+v, %v", infos, err)
	}
	byName := make(map[string]int64)
	for _, info := range infos {
		byName[info.Path] = info.Size
		if !info.IsDir && info.Version != "" {
			t.Fatal("metadata stat should not invent a content hash")
		}
	}
	if byName["file"] != int64(len("exact length")) || byName["empty"] != 0 {
		t.Fatal("unknown file size was fabricated as zero")
	}
	infos[0].Path = "mutated"
	again, err := c.List(context.Background(), notebookPath(""))
	if err != nil || again[0].Path == "mutated" || w.count(http.MethodGet, "/workdir/file") != 1 {
		t.Fatal("listing snapshot mutated cache or hydration was repeated")
	}
	if _, err := c.List(context.Background(), notebookPath("file")); !errors.Is(err, fserrors.ErrNotDir) {
		t.Fatal("file listed as a directory")
	}
	if _, err := c.Read(context.Background(), notebookPath("directory"), 0, make([]byte, 4), ""); !errors.Is(err, fserrors.ErrIsDir) {
		t.Fatal("directory listing read as file bytes")
	}
	if _, err := c.Stat(context.Background(), notebookPath("missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("missing child was not reported")
	}
}

func TestSizeCapsAndAllSuccessfulBodiesClosed(t *testing.T) {
	for _, declaredLength := range []int64{100, -1} {
		w := newWire(t)
		w.files["file"] = []byte("small")
		body := &observedBody{reader: strings.NewReader(strings.Repeat("x", 100))}
		w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
			if strings.HasSuffix(r.URL.Path, "/file") && r.URL.RawQuery == "" {
				resp := response(200, nil)
				resp.Body, resp.ContentLength = body, declaredLength
				return resp, nil, true
			}
			return nil, nil, false
		}
		c := w.client(func(o *Options) { o.MaxFileSize = 8 })
		if _, err := c.Snapshot(context.Background(), notebookPath("file"), ""); !errors.Is(err, fserrors.ErrTooLarge) {
			t.Fatalf("oversize body accepted: %v", err)
		}
		if !body.closed.Load() || body.read.Load() > 9 || (declaredLength > 0 && body.read.Load() != 0) {
			t.Fatal("oversize body was not bounded and closed")
		}
	}
	w := newWire(t)
	w.files["large"] = []byte("larger than limit")
	c := w.client(func(o *Options) { o.MaxFileSize = 8 })
	if info, err := c.Stat(context.Background(), notebookPath("large")); err != nil || info.Size != int64(len("larger than limit")) ||
		w.count(http.MethodGet, "/workdir/large") != 0 {
		t.Fatal("known oversized file metadata was rejected or downloaded")
	}
	body := &observedBody{reader: strings.NewReader(strings.Repeat(" ", maxListingBytes+8))}
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if r.URL.Query().Get("recursive") == "false" {
			resp := response(200, nil)
			resp.Body, resp.ContentLength = body, -1
			return resp, nil, true
		}
		return nil, nil, false
	}
	c.listings.Clear()
	if _, err := c.List(context.Background(), notebookPath("")); !errors.Is(err, fserrors.ErrTooLarge) ||
		!body.closed.Load() || body.read.Load() > maxListingBytes+1 {
		t.Fatal("directory response bound/closure incorrect")
	}
}

func TestCacheTTLZeroExpiryAndCredentialIsolation(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Minute} {
		t.Run(ttl.String(), func(t *testing.T) {
			w := newWire(t)
			w.files["file"] = []byte("old")
			c := w.client(func(o *Options) { o.CacheTTL = ttl })
			ctx := context.Background()
			for range 2 {
				if snapshot, err := c.Snapshot(ctx, notebookPath("file"), ""); err != nil {
					t.Fatal(err)
				} else {
					_ = snapshot.Close()
				}
			}
			want := 1
			if ttl == 0 {
				want = 2
			}
			if got := w.count(http.MethodGet, "/workdir/file"); got != want {
				t.Fatalf("content downloads=%d want=%d", got, want)
			}
			w.mu.Lock()
			w.files["file"] = []byte("new")
			w.mu.Unlock()
			w.advance(time.Minute)
			snapshot, err := c.Snapshot(ctx, notebookPath("file"), "")
			if err != nil || snapshot.Version() != versionFor([]byte("new")) {
				t.Fatal("expired file cache returned stale bytes")
			}
			_ = snapshot.Close()
			w.credentials.set("rotated-fake-principal")
			w.mu.Lock()
			w.files["file"] = []byte("new principal")
			w.mu.Unlock()
			snapshot, err = c.Snapshot(ctx, notebookPath("file"), "")
			if err != nil || snapshot.Version() != versionFor([]byte("new principal")) {
				t.Fatal("cached content crossed credential identities")
			}
			_ = snapshot.Close()
		})
	}
}

func TestResourceFailuresNeverBecomeEmptyRoots(t *testing.T) {
	for _, status := range []int{401, 403, 404, 302} {
		w := newWire(t)
		w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
			if strings.Contains(r.URL.Path, "filesystem") {
				return jsonResponse(status, map[string]string{"errorCode": "Denied", "requestId": "safe-id"}), nil, true
			}
			return nil, nil, false
		}
		c := w.client()
		for _, operation := range []func() error{
			func() error { _, err := c.List(context.Background(), notebookPath("")); return err },
			func() error { _, err := c.Stat(context.Background(), notebookPath("")); return err },
		} {
			err := operation()
			var remote *transport.HTTPError
			if !errors.As(err, &remote) || remote.StatusCode != status || remote.RequestID != "safe-id" {
				t.Fatalf("HTTP %d hidden as success or lost: %v", status, err)
			}
		}
		if c.listings.Stats().Entries != 0 {
			t.Fatal("error retained in listing cache")
		}
	}
}

func TestLiteralPercentAndNestedUnicodeEncoding(t *testing.T) {
	w := newWire(t)
	directory, name := "d%2e雪", "%2e%2e%2F.txt"
	w.directories[directory] = true
	w.files[directory+"/"+name] = []byte("safe literal percent")
	c := w.client()
	if _, err := c.Stat(context.Background(), notebookPath(directory+"/"+name)); err != nil {
		t.Fatal(err)
	}
	for _, r := range w.recorded() {
		if strings.HasSuffix(r.path, "/"+directory+"/"+name) &&
			!strings.HasSuffix(r.escapedPath, "/"+url.PathEscape(directory)+"/"+url.PathEscape(name)) {
			t.Fatal("literal percent name was decoded twice or slash was encoded as a separator")
		}
	}
}
