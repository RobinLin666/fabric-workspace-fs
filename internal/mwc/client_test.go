package mwc

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
)

func TestNewAndStrictOrigins(t *testing.T) {
	w := newWire(t)
	base := Options{Tokens: w.credentials, Workspaces: workspaceFunc(nil)}
	c, err := New(base)
	if err != nil || c.origin != "https://api.fabric.microsoft.com" || c.maxFileSize != DefaultMaxFileSize {
		t.Fatalf("defaults: client=%v error=%v", c != nil, err)
	}
	for _, raw := range []string{
		"http://example.test", "//example.test", "example.test", "https://example.test/path",
		"https://user:secret@example.test", "https://127.0.0.1", "https://[::1]",
		"https://example.test:8443", "https://example.test:", "https://example.test:0443",
		"https://example.test?token=secret", "https://example.test?", "https://example.test/#",
		"https://example.test#frag", "https://example.test/%2f", "https://example.test\\foreign",
		"https://example.test./", "https://invalid_host.test", "https://-bad.test",
		"https://bad-.test", "https://example..test", "https://2130706433", "https://127.1",
		"https://localhost", " https://example.test", "https://example.test\r\n",
		"https://雪.test", "https://" + strings.Repeat("a", 64) + ".test",
	} {
		t.Run(raw, func(t *testing.T) {
			opts := base
			opts.FabricOrigin = raw
			if _, err := New(opts); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("accepted unsafe origin: %v", err)
			}
		})
	}
	for _, raw := range []string{"https://EXAMPLE.test:443/", "https://example.test"} {
		origin, err := strictOrigin(raw, false)
		if err != nil || origin != "https://example.test" {
			t.Fatalf("normalization failed: %v", err)
		}
	}
	for _, edit := range []func(*Options){
		func(o *Options) { o.Tokens = nil },
		func(o *Options) { o.Workspaces = nil },
		func(o *Options) { o.CacheTTL = -time.Second },
		func(o *Options) { o.MaxFileSize = -1 },
		func(o *Options) { o.MaxFileSize = 1<<63 - 1 },
	} {
		opts := base
		edit(&opts)
		if _, err := New(opts); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("invalid options accepted: %v", err)
		}
	}
	if len(w.credentials.scopes) != 0 {
		t.Fatal("constructor obtained credentials")
	}
}

func TestRootsAndEnvironmentAliases(t *testing.T) {
	w := newWire(t)
	c := w.client()
	for _, tc := range []struct {
		target resources.Target
		name   string
	}{{testNotebook, "builtin"}, {testEnv, "resources"}} {
		roots, err := c.Roots(context.Background(), tc.target)
		if err != nil || len(roots) != 1 || roots[0].Name != tc.name || roots[0].Target != tc.target {
			t.Fatalf("roots = %+v, %v", roots, err)
		}
	}
	resolved := []resources.Root{{Name: "builtin", Target: testNotebook}, {Name: "env", Target: testEnv}}
	c = w.client(func(o *Options) {
		o.ResolveRoots = func(context.Context, resources.Target) ([]resources.Root, error) { return resolved, nil }
	})
	roots, err := c.Roots(context.Background(), testNotebook)
	if err != nil || roots[1].Target != testEnv {
		t.Fatalf("binding was not preserved: %+v, %v", roots, err)
	}
	roots[1].Name = "modified"
	if resolved[1].Name != "env" {
		t.Fatal("root result aliases resolver memory")
	}
	for _, bad := range [][]resources.Root{
		{{Name: "builtin", Target: testEnv}},
		{{Name: "env", Target: testNotebook}},
		{{Name: "resources", Target: testEnv}},
		{{Name: "builtin", Target: testNotebook}, {Name: "builtin", Target: testNotebook}},
		{{Name: "../escape", Target: testNotebook}},
	} {
		resolved = bad
		if _, err := c.Roots(context.Background(), testNotebook); err == nil {
			t.Fatal("invalid resolver roots accepted")
		}
	}
	if len(w.recorded()) != 0 || len(w.credentials.scopes) != 0 {
		t.Fatal("default roots caused authentication or HTTP")
	}
	if c.WriteGuarantee() != resources.CompareThenWrite {
		t.Fatal("backend must not claim atomic conditional writes")
	}
}

func TestDefaultRootsRemainAvailableWhenResourcesAreForbidden(t *testing.T) {
	w := newWire(t)
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.Contains(r.URL.Path, "filesystem") {
			return response(http.StatusForbidden, nil), nil, true
		}
		return nil, nil, false
	}
	c := w.client()
	ctx := context.Background()
	for _, target := range []resources.Target{testNotebook, testEnv} {
		if roots, err := c.Roots(ctx, target); err != nil || len(roots) != 1 || roots[0].Target != target {
			t.Fatalf("default roots unavailable: %+v, %v", roots, err)
		}
	}
	if len(w.recorded()) != 0 || len(w.credentials.scopes) != 0 || len(w.workspaceIDs) != 0 {
		t.Fatal("describing default roots performed authentication or workspace discovery")
	}
	for _, target := range []resources.Target{testNotebook, testEnv} {
		path := resources.Path{Target: target}
		if _, err := c.List(ctx, path); !errors.Is(err, fs.ErrPermission) {
			t.Fatalf("List hid the resource denial: %v", err)
		}
		if _, err := c.Stat(ctx, path); !errors.Is(err, fs.ErrPermission) {
			t.Fatalf("Stat hid the resource denial: %v", err)
		}
	}
	requests, credentials, workspaces := len(w.recorded()), len(w.credentials.scopes), len(w.workspaceIDs)
	w.credentials.err = errors.New("credential unavailable")
	for _, target := range []resources.Target{testNotebook, testEnv} {
		if roots, err := c.Roots(ctx, target); err != nil || len(roots) != 1 || roots[0].Target != target {
			t.Fatalf("resource failure blocked default root descriptors: %+v, %v", roots, err)
		}
	}
	if len(w.recorded()) != requests || len(w.credentials.scopes) != credentials || len(w.workspaceIDs) != workspaces {
		t.Fatal("default roots retried forbidden resources")
	}
}

func TestNotebookAndEnvironmentURLsAndAudiences(t *testing.T) {
	for _, target := range []resources.Target{testNotebook, testEnv} {
		t.Run(target.Kind, func(t *testing.T) {
			w := newWire(t)
			w.files["sp ace+#%雪.txt"] = []byte("file")
			c := w.client()
			path := resources.Path{Target: target, Relative: "sp ace+#%雪.txt"}
			info, err := c.Stat(context.Background(), path)
			if err != nil || info.Size != 4 || info.Version != "" || info.MetadataVersion != `"unverified-etag"` {
				t.Fatalf("Stat: %+v, %v", info, err)
			}
			snapshot, err := c.Snapshot(context.Background(), path, "")
			if err != nil || snapshot.Version() != versionFor([]byte("file")) {
				t.Fatal("content snapshot", err)
			}
			_ = snapshot.Close()
			capacity := testCapacity
			base := "/webapi/capacities/" + capacity + "/workloads/Notebook/Data/Direct/api/workspaces/" +
				testWorkspace + "/artifacts/" + testItem + "/filesystem/workdir/"
			if target.Kind == "Environment" {
				capacity = envCapacity
				base = "/webapi/capacities/" + capacity + "/workloads/SparkCore/SparkCoreService/Automatic/v1/Environments/workspaces/" +
					envWorkspace + "/artifacts/" + envItem + "/filesystem/workdir/"
			}
			requests := w.recorded()
			if len(requests) != 4 {
				t.Fatalf("request count = %d", len(requests))
			}
			if requests[0].host != "fabric.mwc.test" || requests[0].path != "/metadata/cluster" ||
				requests[0].headers.Get("Authorization") != "Bearer "+testPBI {
				t.Fatal("discovery not bound to configured origin and Power BI credential")
			}
			if requests[1].host != "cluster.mwc.test" || requests[1].method != http.MethodPost ||
				requests[1].headers.Get("Authorization") != "Bearer "+testPBI {
				t.Fatal("exchange not bound to the discovered cluster")
			}
			if requests[2].escapedPath != base || requests[2].query != "recursive=false" ||
				requests[2].headers.Get("Cache-Control") != "no-cache" {
				t.Fatalf("wrong listing URL: %s?%s", requests[2].escapedPath, requests[2].query)
			}
			if requests[3].escapedPath != strings.TrimSuffix(base, "/")+"/"+url.PathEscape(path.Relative) ||
				requests[3].query != "" {
				t.Fatalf("wrong file URL: %s?%s", requests[3].escapedPath, requests[3].query)
			}
			for _, req := range requests[2:] {
				if req.host != "resources.mwc.test" || req.headers.Get("Authorization") != "MwcToken "+testMWC {
					t.Fatal("resource credential scheme or exact host incorrect")
				}
				if req.headers.Get("Range") != "" {
					t.Fatal("backend used an unverified range API")
				}
			}
			for _, scope := range w.credentials.scopes {
				if scope != powerBIScope {
					t.Fatal("resource backend requested a Fabric or Storage audience")
				}
			}
		})
	}
}

func TestNotebookBuiltinIsOnlyALocalRootAlias(t *testing.T) {
	w := newWire(t)
	c := w.client()
	ctx := context.Background()
	roots, err := c.Roots(ctx, testNotebook)
	if err != nil || len(roots) != 1 || roots[0].Name != "builtin" {
		t.Fatalf("local root descriptor changed: %+v, %v", roots, err)
	}
	if entries, err := c.List(ctx, notebookPath("")); err != nil || len(entries) != 0 {
		t.Fatalf("actual workdir root unavailable: %+v, %v", entries, err)
	}
	r, err := c.route(ctx, testNotebook)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.list(ctx, r, "builtin", true); !errors.Is(err, fs.ErrNotExist) || !hasHTTPError(err) {
		t.Fatalf("missing remote builtin directory was synthesized or treated as empty: %v", err)
	}
	if err := c.Mkdir(ctx, notebookPath("probe")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(ctx, notebookPath("probe/file"), strings.NewReader("bytes"), 5, ""); err != nil {
		t.Fatal(err)
	}
	if w.directories["builtin"] || !w.directories["probe"] || string(w.files["probe/file"]) != "bytes" {
		t.Fatal("local root alias was materialized as a remote directory")
	}
	for _, request := range w.recorded() {
		if request.method == http.MethodPut &&
			!strings.HasSuffix(request.path, "/filesystem/workdir/probe") &&
			!strings.HasSuffix(request.path, "/filesystem/workdir/probe/file") {
			t.Fatalf("mutation included an invented root component: %s", request.path)
		}
	}
}

func TestProtectedPathsFailBeforeAuthentication(t *testing.T) {
	w := newWire(t)
	c := w.client()
	ctx := context.Background()
	for _, relative := range []string{".", "..", "../escape", "a/../b", "/absolute", "a//b", "a\\b", "bad\x00", "bad\n"} {
		path := notebookPath(relative)
		if _, err := c.Stat(ctx, path); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("Stat(%q): %v", relative, err)
		}
	}
	invalid := notebookPath("file")
	invalid.Target.ItemID = "not-a-uuid"
	if _, err := c.Stat(ctx, invalid); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal(err)
	}
	invalid.Target = testNotebook
	invalid.Target.Kind = "Lakehouse"
	if _, err := c.Stat(ctx, invalid); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal(err)
	}
	if err := c.Mkdir(ctx, notebookPath("")); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal(err)
	}
	if _, err := c.Stat(nil, notebookPath("file")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.Stat(canceled, notebookPath("file")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(w.recorded()) != 0 || len(w.credentials.scopes) != 0 {
		t.Fatal("invalid input obtained credentials or issued a request")
	}
}
