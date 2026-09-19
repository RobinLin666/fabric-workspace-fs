package mwc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/resources"
)

const (
	testWorkspace = "11111111-1111-4111-8111-111111111111"
	testItem      = "22222222-2222-4222-8222-222222222222"
	testCapacity  = "33333333-3333-4333-8333-333333333333"
	envWorkspace  = "44444444-4444-4444-8444-444444444444"
	envItem       = "55555555-5555-4555-8555-555555555555"
	envCapacity   = "66666666-6666-4666-8666-666666666666"
	testPBI       = "fake-pbi-sensitive-token"
	testMWC       = "fake-mwc-sensitive-token"
)

var (
	testNotebook = resources.Target{WorkspaceID: testWorkspace, ItemID: testItem, Kind: "Notebook"}
	testEnv      = resources.Target{WorkspaceID: envWorkspace, ItemID: envItem, Kind: "Environment"}
	testNow      = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
)

func notebookPath(relative string) resources.Path {
	return resources.Path{Target: testNotebook, Relative: relative}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type workspaceFunc func(context.Context, string) (fabric.Workspace, error)

func (f workspaceFunc) GetWorkspace(ctx context.Context, id string) (fabric.Workspace, error) {
	return f(ctx, id)
}

type fakeCredentials struct {
	mu     sync.Mutex
	value  string
	err    error
	scopes []string
}

func (f *fakeCredentials) Token(ctx context.Context, scope string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scopes = append(f.scopes, scope)
	return f.value, f.err
}

func (f *fakeCredentials) set(value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.value = value
}

type recordedRequest struct {
	method, host, path, escapedPath, query string
	headers                                http.Header
	body                                   []byte
}

type fakeWire struct {
	t            *testing.T
	mu           sync.Mutex
	now          time.Time
	credentials  *fakeCredentials
	requests     []recordedRequest
	workspaces   map[string]fabric.Workspace
	files        map[string][]byte
	directories  map[string]bool
	omitSizes    bool
	contentType  string
	workspaceIDs []string
	hook         func(*http.Request, []byte) (*http.Response, error, bool)
}

func newWire(t *testing.T) *fakeWire {
	t.Helper()
	return &fakeWire{
		t: t, now: testNow, credentials: &fakeCredentials{value: testPBI},
		workspaces: map[string]fabric.Workspace{
			testWorkspace: {ID: testWorkspace, CapacityID: testCapacity},
			envWorkspace:  {ID: envWorkspace, CapacityID: envCapacity},
		},
		files: make(map[string][]byte), directories: map[string]bool{"": true},
		contentType: "application/octet-stream",
	}
}

func (w *fakeWire) clock() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}

func (w *fakeWire) advance(delta time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(delta)
}

func (w *fakeWire) client(edit ...func(*Options)) *Client {
	w.t.Helper()
	opts := Options{
		FabricOrigin: "https://fabric.mwc.test", Tokens: w.credentials,
		HTTPClient: &http.Client{Transport: roundTripFunc(w.roundTrip)},
		Workspaces: workspaceFunc(func(_ context.Context, id string) (fabric.Workspace, error) {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.workspaceIDs = append(w.workspaceIDs, id)
			ws, ok := w.workspaces[id]
			if !ok {
				return fabric.Workspace{}, fs.ErrNotExist
			}
			return ws, nil
		}),
		CacheTTL: time.Minute,
	}
	for _, apply := range edit {
		apply(&opts)
	}
	c, err := New(opts)
	if err != nil {
		w.t.Fatal(err)
	}
	c.now = w.clock
	return c
}

func response(status int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: status, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(string(body))), ContentLength: int64(len(body)),
	}
}

func jsonResponse(status int, value any) *http.Response {
	body, _ := json.Marshal(value)
	resp := response(status, body)
	resp.Header.Set("Content-Type", "application/json")
	return resp
}

func (w *fakeWire) roundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}
	w.mu.Lock()
	w.requests = append(w.requests, recordedRequest{
		method: r.Method, host: r.URL.Host, path: r.URL.Path, escapedPath: r.URL.EscapedPath(),
		query: r.URL.RawQuery, headers: r.Header.Clone(), body: body,
	})
	w.mu.Unlock()
	if w.hook != nil {
		if resp, err, handled := w.hook(r, body); handled {
			return resp, err
		}
	}
	if r.URL.Path == "/metadata/cluster" {
		return jsonResponse(http.StatusOK, map[string]string{"backendUrl": "cluster.mwc.test"}), nil
	}
	if r.URL.Path == "/metadata/v201606/generatemwctoken" {
		var request struct {
			Capacity string `json:"capacityObjectId"`
		}
		_ = json.Unmarshal(body, &request)
		return jsonResponse(http.StatusOK, map[string]any{
			"Token": testMWC, "TargetUriHost": "resources.mwc.test",
			"CapacityObjectId": request.Capacity, "Expiry": w.clock().Add(time.Hour).Format(time.RFC3339),
		}), nil
	}
	_, relative, ok := strings.Cut(r.URL.Path, "/filesystem/workdir/")
	if !ok {
		return nil, errors.New("fake transport refuses unknown URL")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("recursive") == "false" {
			if !w.directories[relative] {
				return response(http.StatusNotFound, nil), nil
			}
			var names []string
			prefix := relative
			if prefix != "" {
				prefix += "/"
			}
			for name := range w.files {
				if child, ok := strings.CutPrefix(name, prefix); ok && !strings.Contains(child, "/") {
					names = append(names, child)
				}
			}
			for name := range w.directories {
				if name == relative {
					continue
				}
				if child, ok := strings.CutPrefix(name, prefix); ok && !strings.Contains(child, "/") {
					names = append(names, child)
				}
			}
			sort.Strings(names)
			children := make([]map[string]any, 0, len(names))
			for _, name := range names {
				entry := map[string]any{
					"name": name, "fileSystemEntryType": "file",
					"lastModified": w.now.Format(time.RFC3339), "etag": `"unverified-etag"`,
					"_remoteContentHash": "not-a-client-version",
				}
				if w.directories[prefix+name] {
					entry["fileSystemEntryType"] = "folder"
				} else if !w.omitSizes {
					entry["contentLength"] = len(w.files[prefix+name])
				}
				children = append(children, entry)
			}
			return jsonResponse(http.StatusOK, map[string]any{"children": children}), nil
		}
		if data, ok := w.files[relative]; ok {
			resp := response(http.StatusOK, data)
			resp.Header.Set("Content-Type", w.contentType)
			resp.Header.Set("Last-Modified", w.now.Format(http.TimeFormat))
			return resp, nil
		}
		return response(http.StatusNotFound, nil), nil
	case http.MethodPut:
		if r.Header.Get("ms-filesystem-entry-type") == "folder" {
			w.directories[relative] = true
		} else {
			w.files[relative] = append([]byte(nil), body...)
		}
		return response(http.StatusCreated, nil), nil
	case http.MethodDelete:
		if r.Header.Get("ms-filesystem-entry-type") == "folder" {
			if r.URL.Query().Get("recursive") != "false" {
				return response(http.StatusBadRequest, nil), nil
			}
			delete(w.directories, relative)
		} else {
			delete(w.files, relative)
		}
		return response(http.StatusNoContent, nil), nil
	case http.MethodPost:
		if r.URL.Query().Get("action") != "move" {
			return response(http.StatusBadRequest, nil), nil
		}
		dest, err := url.PathUnescape(r.Header.Get("ms-filesystem-location"))
		if err != nil {
			return response(http.StatusBadRequest, nil), nil
		}
		data, ok := w.files[relative]
		if !ok {
			return response(http.StatusNotFound, nil), nil
		}
		w.files[dest] = data
		delete(w.files, relative)
		return response(http.StatusNoContent, nil), nil
	default:
		return response(http.StatusMethodNotAllowed, nil), nil
	}
}

func (w *fakeWire) recorded() []recordedRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]recordedRequest(nil), w.requests...)
}

func (w *fakeWire) count(method, containsPath string) int {
	count := 0
	for _, r := range w.recorded() {
		if r.method == method && strings.Contains(r.path, containsPath) {
			count++
		}
	}
	return count
}

type observedBody struct {
	reader io.Reader
	closed atomic.Bool
	read   atomic.Int64
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read.Add(int64(n))
	return n, err
}

func (b *observedBody) Close() error { b.closed.Store(true); return nil }
