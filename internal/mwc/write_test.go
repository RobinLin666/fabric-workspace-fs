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

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/transport"
)

func TestPutExactBytesCreateAndCompareThenWrite(t *testing.T) {
	w := newWire(t)
	w.directories["dir"] = true
	c := w.client()
	ctx := context.Background()
	path := notebookPath("dir/sp ace%雪.bin")
	data := []byte{0, 0xff, 'x', '\r', '\n', 0}
	info, err := c.Put(ctx, path, bytes.NewReader(data), int64(len(data)), "")
	if err != nil || info.Path != path.Relative || info.Size != int64(len(data)) || info.Version != versionFor(data) {
		t.Fatalf("Put: %+v, %v", info, err)
	}
	if !bytes.Equal(w.files[path.Relative], data) {
		t.Fatal("file bytes changed on upload")
	}
	for _, r := range w.recorded() {
		if r.method == http.MethodPut {
			if r.headers.Get("ms-filesystem-entry-type") != "file" ||
				r.headers.Get("Content-Type") != "application/octet-stream" || !bytes.Equal(r.body, data) ||
				!strings.HasSuffix(r.escapedPath, "/dir/"+url.PathEscape("sp ace%雪.bin")) {
				t.Fatal("wrong PUT resource wire contract")
			}
			if r.headers.Get("If-Match") != "" || r.headers.Get("If-None-Match") != "" {
				t.Fatal("invented server-side CAS support")
			}
		}
	}
	if _, err := c.Put(ctx, path, bytes.NewReader(data), int64(len(data)), ""); !errors.Is(err, fserrors.ErrConflict) ||
		!errors.Is(err, fs.ErrExist) {
		t.Fatal("create-new overwrote existing file")
	}
	replacement := []byte("replacement")
	next, err := c.Put(ctx, path, bytes.NewReader(replacement), int64(len(replacement)), info.Version)
	if err != nil || next.Version != versionFor(replacement) {
		t.Fatalf("matching replacement failed: %v", err)
	}
	if _, err := c.Put(ctx, notebookPath("dir/missing"), bytes.NewReader(data), int64(len(data)), info.Version); !errors.Is(err, fserrors.ErrConflict) {
		t.Fatal("expected existing version silently became creation")
	}
}

func TestWritePreflightBypassesStaleCaches(t *testing.T) {
	w := newWire(t)
	w.files["file"] = []byte("old")
	c := w.client()
	ctx := context.Background()
	snapshot, err := c.Snapshot(ctx, notebookPath("file"), "")
	if err != nil {
		t.Fatal(err)
	}
	version := snapshot.Version()
	_ = snapshot.Close()
	w.mu.Lock()
	w.files["file"] = []byte("changed remotely")
	w.mu.Unlock()
	for _, mutate := range []func() error{
		func() error {
			_, err := c.Put(ctx, notebookPath("file"), strings.NewReader("replacement"), 11, version)
			return err
		},
		func() error { return c.Remove(ctx, notebookPath("file"), false, version) },
		func() error {
			_, err := c.Rename(ctx, notebookPath("file"), notebookPath("other"), version, "", true)
			return err
		},
	} {
		if err := mutate(); !errors.Is(err, fserrors.ErrConflict) {
			t.Fatalf("stale snapshot allowed mutation: %v", err)
		}
	}
	if w.count(http.MethodPut, "filesystem") != 0 || w.count(http.MethodDelete, "filesystem") != 0 ||
		w.count(http.MethodPost, "filesystem") != 0 {
		t.Fatal("known conflict reached a mutating request")
	}
	current, err := c.Snapshot(ctx, notebookPath("file"), "")
	if err != nil || current.Version() != versionFor([]byte("changed remotely")) {
		t.Fatal("conflict did not invalidate stale content")
	}
	_ = current.Close()
}

type errorReaderAt struct{ err error }

func (r errorReaderAt) ReadAt([]byte, int64) (int, error) { return 0, r.err }

func TestPutSourceBoundsAndErrorsBeforeNetwork(t *testing.T) {
	w := newWire(t)
	c := w.client(func(o *Options) { o.MaxFileSize = 8 })
	ctx := context.Background()
	for _, tc := range []struct {
		source io.ReaderAt
		size   int64
		want   error
	}{
		{strings.NewReader("123456789"), 9, fserrors.ErrTooLarge},
		{strings.NewReader(""), -1, fs.ErrInvalid},
		{nil, 1, fs.ErrInvalid},
		{strings.NewReader("short"), 8, io.ErrUnexpectedEOF},
	} {
		if _, err := c.Put(ctx, notebookPath("file"), tc.source, tc.size, ""); !errors.Is(err, tc.want) {
			t.Fatalf("bad source accepted: %v", err)
		}
	}
	secret := errors.New("source error containing " + testPBI)
	if _, err := c.Put(ctx, notebookPath("file"), errorReaderAt{secret}, 2, ""); !errors.Is(err, secret) ||
		strings.Contains(err.Error(), testPBI) {
		t.Fatal("source error not returned safely")
	}
	if len(w.recorded()) != 0 || len(w.credentials.scopes) != 0 {
		t.Fatal("invalid sources caused network/auth")
	}
}

func TestMkdirRemoveEmptyAndNonEmptyDirectories(t *testing.T) {
	w := newWire(t)
	c := w.client()
	ctx := context.Background()
	if err := c.Mkdir(ctx, notebookPath("folder")); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir(ctx, notebookPath("folder")); !errors.Is(err, fs.ErrExist) {
		t.Fatal("duplicate mkdir succeeded")
	}
	if err := c.Remove(ctx, notebookPath("folder"), true, ""); err != nil {
		t.Fatal(err)
	}
	var created, deleted bool
	for _, r := range w.recorded() {
		if r.method == http.MethodPut {
			created = r.headers.Get("ms-filesystem-entry-type") == "folder" && len(r.body) == 0
		}
		if r.method == http.MethodDelete {
			deleted = r.headers.Get("ms-filesystem-entry-type") == "folder" && r.query == "recursive=false"
		}
	}
	if !created || !deleted {
		t.Fatal("mkdir/nonrecursive rmdir wire contract incorrect")
	}
	w.mu.Lock()
	w.directories["folder"] = true
	w.files["folder/file"] = []byte("nonempty")
	w.mu.Unlock()
	before := w.count(http.MethodDelete, "filesystem")
	if err := c.Remove(ctx, notebookPath("folder"), true, ""); !errors.Is(err, fserrors.ErrNotEmpty) {
		t.Fatal("nonempty folder removed")
	}
	if w.count(http.MethodDelete, "filesystem") != before {
		t.Fatal("nonempty folder reached DELETE")
	}
	if err := c.Remove(ctx, notebookPath("folder/file"), true, ""); !errors.Is(err, fserrors.ErrNotDir) {
		t.Fatal("rmdir accepted a file")
	}
	if err := c.Remove(ctx, notebookPath("folder"), false, "version"); !errors.Is(err, fserrors.ErrIsDir) {
		t.Fatal("file removal accepted a directory")
	}
	if err := c.Remove(ctx, notebookPath("folder/file"), false, ""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal("file removal allowed missing comparison version")
	}
	if err := c.Remove(ctx, notebookPath("folder/file"), false, versionFor([]byte("nonempty"))); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(ctx, notebookPath("folder/file")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("deleted file remained in cache")
	}
}

func TestParentErrorsDoNotPermitCreation(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden} {
		w := newWire(t)
		w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "filesystem") {
				return response(status, nil), nil, true
			}
			return nil, nil, false
		}
		c := w.client()
		ctx := context.Background()
		if _, err := c.Put(ctx, notebookPath("file"), strings.NewReader("x"), 1, ""); err == nil {
			t.Fatal("missing/denied root treated as empty for creation")
		}
		if err := c.Mkdir(ctx, notebookPath("folder")); err == nil {
			t.Fatal("missing/denied root treated as empty for mkdir")
		}
		if w.count(http.MethodPut, "filesystem") != 0 {
			t.Fatal("write after failed parent listing")
		}
	}
}

func TestEnvironmentAliasAndCrossTargetWriteDenials(t *testing.T) {
	w := newWire(t)
	c := w.client()
	ctx := context.Background()
	env := resources.Path{Target: testEnv, Relative: "file"}
	notebook := notebookPath("file")
	for _, mutate := range []func() error{
		func() error { _, err := c.Put(ctx, env, strings.NewReader("x"), 1, ""); return err },
		func() error { return c.Mkdir(ctx, env) },
		func() error { return c.Remove(ctx, env, false, "version") },
		func() error { return c.Remove(ctx, env, true, "") },
		func() error { _, err := c.Rename(ctx, notebook, env, "version", "", false); return err },
		func() error { _, err := c.Rename(ctx, env, notebook, "version", "", false); return err },
	} {
		if err := mutate(); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Fatalf("Environment target write allowed: %v", err)
		}
	}
	for _, target := range []resources.Target{
		{WorkspaceID: testWorkspace, ItemID: envItem, Kind: "Notebook"},
		{WorkspaceID: envWorkspace, ItemID: testItem, Kind: "Notebook"},
	} {
		if _, err := c.Rename(ctx, notebook, resources.Path{Target: target, Relative: "other"}, "version", "", false); !errors.Is(err, fserrors.ErrCrossDevice) {
			t.Fatal("cross-item/workspace move allowed")
		}
	}
	if len(w.recorded()) != 0 || len(w.credentials.scopes) != 0 {
		t.Fatal("denied writes caused auth/network")
	}
}

func TestNativeFileMoveWireAndVerification(t *testing.T) {
	w := newWire(t)
	w.directories["dir"] = true
	source, destination := notebookPath("dir/source %雪"), notebookPath("dir/%2e target")
	data := []byte("move exactly")
	w.files[source.Relative] = data
	c := w.client()
	version := versionFor(data)
	info, err := c.Rename(context.Background(), source, destination, version, "", true)
	if err != nil || info.Path != destination.Relative || info.Version != version {
		t.Fatalf("Rename: %+v, %v", info, err)
	}
	if _, exists := w.files[source.Relative]; exists || !bytes.Equal(w.files[destination.Relative], data) {
		t.Fatal("move did not preserve bytes or remove the source")
	}
	if w.count(http.MethodPost, "filesystem") != 1 {
		t.Fatal("move was not exactly one native action")
	}
	for _, r := range w.recorded() {
		if r.method == http.MethodPost && strings.Contains(r.path, "filesystem") {
			if r.query != "action=move" || !strings.HasSuffix(r.escapedPath, "/dir/"+url.PathEscape("source %雪")) ||
				r.headers.Get("ms-filesystem-location") != "dir/"+url.PathEscape("%2e target") ||
				r.headers.Get("ms-filesystem-entry-type") != "file" ||
				r.headers.Get("If-Match") != "" || r.headers.Get("If-None-Match") != "" {
				t.Fatal("native move wire contract incorrect")
			}
		}
	}
}

func TestRenameDestinationConflictsAndUnsupportedFolders(t *testing.T) {
	for _, tc := range []struct {
		name, sourceVersion, destVersion string
		noReplace                        bool
		want                             error
	}{
		{"no replace", versionFor([]byte("source")), "", true, fs.ErrExist},
		{"unknown destination version", versionFor([]byte("source")), "", false, fserrors.ErrConflict},
		{"wrong source", "wrong", versionFor([]byte("dest")), false, fserrors.ErrConflict},
		{"wrong destination", versionFor([]byte("source")), "wrong", false, fserrors.ErrConflict},
		{"missing source version", "", "", true, fs.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWire(t)
			w.files["source"], w.files["dest"] = []byte("source"), []byte("dest")
			c := w.client()
			if _, err := c.Rename(context.Background(), notebookPath("source"), notebookPath("dest"),
				tc.sourceVersion, tc.destVersion, tc.noReplace); !errors.Is(err, tc.want) {
				t.Fatalf("conflict = %v", err)
			}
			if w.count(http.MethodPost, "filesystem") != 0 {
				t.Fatal("conflict caused a move request")
			}
		})
	}
	w := newWire(t)
	w.directories["folder"] = true
	c := w.client()
	if _, err := c.Rename(context.Background(), notebookPath("folder"), notebookPath("other"), "", "", false); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatalf("folder rename not explicitly unsupported: %v", err)
	}
	w.files["source"], w.files["dest"] = []byte("source"), []byte("dest")
	if _, err := c.Rename(context.Background(), notebookPath("source"), notebookPath("dest"),
		versionFor([]byte("source")), versionFor([]byte("dest")), false); err != nil {
		t.Fatalf("version-checked destination replacement failed: %v", err)
	}
}

func TestMoveSuccessStatusMustActuallyMove(t *testing.T) {
	for _, action := range []string{"no-op", "copy-only", "different-bytes"} {
		t.Run(action, func(t *testing.T) {
			w := newWire(t)
			w.files["source"] = []byte("source")
			w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
				if r.Method != http.MethodPost || !strings.Contains(r.URL.Path, "filesystem") {
					return nil, nil, false
				}
				w.mu.Lock()
				defer w.mu.Unlock()
				switch action {
				case "copy-only":
					w.files["dest"] = []byte("source")
				case "different-bytes":
					w.files["dest"] = []byte("wrong")
					delete(w.files, "source")
				}
				return response(http.StatusNoContent, nil), nil, true
			}
			if _, err := w.client().Rename(context.Background(), notebookPath("source"), notebookPath("dest"),
				versionFor([]byte("source")), "", true); err == nil {
				t.Fatal("unverified move reported success")
			}
		})
	}
}

func TestAmbiguousMutationIsNotRetriedAndInvalidatesCaches(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			w := newWire(t)
			w.files["source"] = []byte("old")
			c := w.client()
			ctx := context.Background()
			snapshot, err := c.Snapshot(ctx, notebookPath("source"), "")
			if err != nil {
				t.Fatal(err)
			}
			version := snapshot.Version()
			_ = snapshot.Close()
			w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
				if r.Method == method && strings.Contains(r.URL.Path, "filesystem") {
					w.mu.Lock()
					w.files["source"] = []byte("server changed before failure")
					w.mu.Unlock()
					resp := jsonResponse(http.StatusServiceUnavailable, map[string]string{
						"requestId": "mutation-request", "message": testMWC,
					})
					return resp, nil, true
				}
				return nil, nil, false
			}
			switch method {
			case http.MethodPut:
				_, err = c.Put(ctx, notebookPath("source"), strings.NewReader("new"), 3, version)
			case http.MethodDelete:
				err = c.Remove(ctx, notebookPath("source"), false, version)
			case http.MethodPost:
				_, err = c.Rename(ctx, notebookPath("source"), notebookPath("dest"), version, "", true)
			}
			var remote *transport.HTTPError
			if !errors.As(err, &remote) || remote.StatusCode != http.StatusServiceUnavailable ||
				remote.RequestID != "mutation-request" || strings.Contains(err.Error(), testMWC) {
				t.Fatalf("mutation error lost or unsafe: %v", err)
			}
			if w.count(method, "filesystem") != 1 {
				t.Fatal("non-idempotent mutation retried")
			}
			current, err := c.Snapshot(ctx, notebookPath("source"), "")
			if err != nil || current.Version() == version {
				t.Fatal("ambiguous response left stale content in cache")
			}
			_ = current.Close()
		})
	}
}

func TestMutationNetworkFailureAndUnknownSuccessAreErrors(t *testing.T) {
	for _, kind := range []string{"network", "accepted"} {
		w := newWire(t)
		cause := errors.New("network failure containing " + testMWC)
		body := &observedBody{reader: strings.NewReader("pending")}
		w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
			if r.Method != http.MethodPut {
				return nil, nil, false
			}
			if kind == "network" {
				return nil, cause, true
			}
			resp := response(http.StatusAccepted, nil)
			resp.Body = body
			return resp, nil, true
		}
		c := w.client()
		_, err := c.Put(context.Background(), notebookPath("file"), strings.NewReader("x"), 1, "")
		if err == nil || strings.Contains(err.Error(), testMWC) {
			t.Fatal("uncertain mutation reported success or exposed network error")
		}
		if kind == "network" && !errors.Is(err, cause) {
			t.Fatal("network failure cause lost")
		}
		if kind == "accepted" && !body.closed.Load() {
			t.Fatal("unexpected successful-status body not closed")
		}
		if w.count(http.MethodPut, "filesystem") != 1 {
			t.Fatal("uncertain mutation retried")
		}
	}
}
