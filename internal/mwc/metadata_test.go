package mwc

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
)

const largeMetadataSize = 24 << 20

func TestLargeResourceMetadataNeverDownloadsKnownContent(t *testing.T) {
	w := newWire(t)
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if r.URL.Query().Get("recursive") == "false" {
			return jsonResponse(200, map[string]any{"children": []any{map[string]any{
				"name": "large.bin", "fileSystemEntryType": "file", "contentLength": largeMetadataSize,
				"lastModified": testNow.Format(http.TimeFormat), "etag": `"metadata-only-etag"`,
			}}}), nil, true
		}
		if strings.HasSuffix(r.URL.Path, "/large.bin") {
			t.Error("listing/stat downloaded file content")
			return response(500, nil), nil, true
		}
		return nil, nil, false
	}
	c := w.client(func(o *Options) { o.MaxFileSize = 16 << 20; o.CacheTTL = 0 })
	for range 3 {
		info, err := c.Stat(context.Background(), notebookPath("large.bin"))
		if err != nil || info.Size != largeMetadataSize || info.IsDir || !info.Modified.Equal(testNow) ||
			info.Version != "" || info.MetadataVersion != `"metadata-only-etag"` {
			t.Fatal("large stat metadata", info, err)
		}
		entries, err := c.List(context.Background(), notebookPath(""))
		if err != nil || len(entries) != 1 || entries[0].Size != largeMetadataSize {
			t.Fatal("large readdir metadata", entries, err)
		}
	}
	if w.count(http.MethodGet, "/workdir/large.bin") != 0 {
		t.Fatal("metadata requested whole object")
	}
}

func TestMissingListingSizeUsesHeadersWithoutReadingBody(t *testing.T) {
	w := newWire(t)
	w.omitSizes = true
	w.files["large.bin"] = nil
	body := &observedBody{reader: strings.NewReader("body must not be read")}
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "/large.bin") && r.URL.RawQuery == "" {
			if r.Header.Get("Range") != "" || r.Method == http.MethodHead {
				t.Error("invented HEAD/Range support")
			}
			resp := response(200, nil)
			resp.ContentLength = largeMetadataSize
			resp.Header.Set("ETag", `"header-etag"`)
			resp.Header.Set("Last-Modified", testNow.Format(http.TimeFormat))
			resp.Body = body
			return resp, nil, true
		}
		return nil, nil, false
	}
	c := w.client(func(o *Options) { o.MaxFileSize = 16 << 20 })
	for range 3 {
		entries, err := c.List(context.Background(), notebookPath(""))
		if err != nil || len(entries) != 1 || entries[0].Size != largeMetadataSize {
			t.Fatal("header metadata", entries, err)
		}
		info, err := c.Stat(context.Background(), notebookPath("large.bin"))
		if err != nil || info.Size != largeMetadataSize || info.Version != "" {
			t.Fatal(info, err)
		}
	}
	if body.read.Load() != 0 || !body.closed.Load() {
		t.Fatal("metadata GET buffered the large body", body.read.Load())
	}
	if w.count(http.MethodGet, "/workdir/large.bin") != 1 {
		t.Fatal("header metadata was not bounded/cached")
	}
}

func TestMissingMetadataLengthIsNotFabricatedAsZero(t *testing.T) {
	w := newWire(t)
	w.omitSizes = true
	w.files["file"] = nil
	body := &observedBody{reader: strings.NewReader("unbounded content")}
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "/file") && r.URL.RawQuery == "" {
			resp := response(200, nil)
			resp.ContentLength = -1
			resp.Body = body
			return resp, nil, true
		}
		return nil, nil, false
	}
	if _, err := w.client().Stat(context.Background(), notebookPath("file")); err == nil {
		t.Fatal("unknown size fabricated as zero")
	}
	if body.read.Load() != 0 || !body.closed.Load() {
		t.Fatal("unknown metadata size caused body download")
	}
}

func TestHeaderMetadataPreservesKnownListingModifiedTime(t *testing.T) {
	w := newWire(t)
	w.omitSizes = true
	w.files["file"] = nil
	body := &observedBody{reader: strings.NewReader("body must not be read")}
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "/workdir/file") && r.URL.RawQuery == "" {
			resp := response(200, nil)
			resp.ContentLength, resp.Body = largeMetadataSize, body
			resp.Header.Set("Last-Modified", "unneeded-invalid-header")
			return resp, nil, true
		}
		return nil, nil, false
	}
	c := policyClient(t, w, `{}`)
	for range 2 {
		info, err := c.Stat(context.Background(), notebookPath("file"))
		if err != nil || info.Size != largeMetadataSize || !info.Modified.Equal(testNow) {
			t.Fatal("header lookup replaced a known listing attribute", info, err)
		}
	}
	if w.count(http.MethodGet, "/workdir/file") != 1 || body.read.Load() != 0 || !body.closed.Load() {
		t.Fatal("metadata lookup bypassed retention or consumed the body")
	}
}

func TestLargeContentSnapshotRemainsBoundedAfterMetadataFix(t *testing.T) {
	w := newWire(t)
	body := &observedBody{reader: strings.NewReader("too big")}
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "/large.bin") {
			resp := response(200, nil)
			resp.ContentLength = largeMetadataSize
			resp.Body = body
			return resp, nil, true
		}
		return nil, nil, false
	}
	if _, err := w.client(func(o *Options) { o.MaxFileSize = 16 << 20 }).Snapshot(context.Background(), notebookPath("large.bin"), ""); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal("snapshot limit was removed", err)
	}
	if body.read.Load() != 0 || !body.closed.Load() {
		t.Fatal("oversized content body was consumed")
	}
}
