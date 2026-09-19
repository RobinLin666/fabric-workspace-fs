package mwc

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/resources"
)

func policyClient(t *testing.T, w *fakeWire, config string) *Client {
	t.Helper()
	policy, err := cachepolicy.Parse([]byte(config))
	if err != nil {
		t.Fatal(err)
	}
	return w.client(func(opts *Options) {
		opts.CachePolicy = policy
		opts.CacheTTL = time.Hour // An explicit policy takes precedence.
	})
}

func policySize(t *testing.T, c *Client, target resources.Target, layer string) int64 {
	t.Helper()
	ctx := context.Background()
	path := resources.Path{Target: target, Relative: "file"}
	switch layer {
	case "attr":
		info, err := c.Stat(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		return info.Size
	case "directory":
		path.Relative = ""
		infos, err := c.List(ctx, path)
		if err != nil || len(infos) != 1 {
			t.Fatalf("listing: %+v, %v", infos, err)
		}
		return infos[0].Size
	case "content":
		snapshot, err := c.Snapshot(ctx, path, "")
		if err != nil {
			t.Fatal(err)
		}
		defer snapshot.Close()
		return snapshot.Size()
	default:
		t.Fatalf("unknown test cache layer %q", layer)
		return 0
	}
}

func policyGETs(w *fakeWire, target resources.Target, listing bool) int {
	var count int
	for _, request := range w.recorded() {
		if request.method == http.MethodGet &&
			strings.Contains(request.path, "/artifacts/"+target.ItemID+"/filesystem/workdir/") &&
			(request.query == "recursive=false") == listing {
			count++
		}
	}
	return count
}

func TestCachePolicyDefaultTwoMinuteLifetimes(t *testing.T) {
	w := newWire(t)
	w.files["file"] = []byte("old")
	c := policyClient(t, w, `{}`)
	ctx := context.Background()
	for _, layer := range []string{"directory", "attr", "content"} {
		if got := policySize(t, c, testNotebook, layer); got != 3 {
			t.Fatalf("%s size=%d", layer, got)
		}
	}
	w.advance(cachepolicy.DefaultTTL - time.Second)
	w.mu.Lock()
	w.files["file"] = []byte("replacement")
	w.mu.Unlock()
	info, err := c.Stat(ctx, notebookPath("file"))
	if err != nil || info.Size != 3 || !info.ObservedAt.Equal(testNow) ||
		!info.ValidUntil.Equal(testNow.Add(cachepolicy.DefaultTTL)) ||
		info.ValidUntil.Sub(w.clock()) != time.Second {
		t.Fatal("stat reset its listing's source deadline", info, err)
	}
	for _, layer := range []string{"directory", "content"} {
		if got := policySize(t, c, testNotebook, layer); got != 3 {
			t.Fatalf("%s expired before the default two minutes", layer)
		}
	}
	if policyGETs(w, testNotebook, true) != 1 || policyGETs(w, testNotebook, false) != 1 ||
		len(w.workspaceIDs) != 1 {
		t.Fatal("default cache lifetimes did not reuse listing, content, and workspace metadata")
	}
	w.advance(time.Second)
	for _, layer := range []string{"attr", "directory", "content"} {
		if got := policySize(t, c, testNotebook, layer); got != int64(len("replacement")) {
			t.Fatalf("%s did not expire at two minutes", layer)
		}
	}
	if policyGETs(w, testNotebook, true) != 2 || policyGETs(w, testNotebook, false) != 2 ||
		len(w.workspaceIDs) != 2 {
		t.Fatal("cache policy did not replace the compatibility CacheTTL")
	}
	if w.count(http.MethodPost, "generatemwctoken") != 1 {
		t.Fatal("data cache expiration changed the actual grant lifetime")
	}
}

func TestCachePolicyTypeAndSurfaceLifetimesAreIndependent(t *testing.T) {
	const config = `{
		"defaults":{"attr":"1h","directory":"1h","content":"1h"},
		"types":{
			"Notebook":{"attr":"9s","directory":"9s","content":"9s","surfaces":{
				"builtin":{"attr":"1s","directory":"2s","content":"3s"},
				"content":{"attr":"0s","directory":"0s","content":"0s"}
			}},
			"Environment":{"attr":"9s","directory":"9s","content":"9s","surfaces":{
				"resources":{"attr":"4s","directory":"5s","content":"6s"},
				"definition":{"attr":"0s","directory":"0s","content":"0s"}
			}}
		}
	}`
	for _, tc := range []struct {
		layer         string
		notebook, env time.Duration
	}{
		{"attr", time.Second, 4 * time.Second},
		{"directory", 2 * time.Second, 5 * time.Second},
		{"content", 3 * time.Second, 6 * time.Second},
	} {
		t.Run(tc.layer, func(t *testing.T) {
			w := newWire(t)
			w.files["file"] = []byte("old")
			c := policyClient(t, w, config)
			for _, target := range []resources.Target{testNotebook, testEnv} {
				for range 2 {
					if got := policySize(t, c, target, tc.layer); got != 3 {
						t.Fatal("initial value", got)
					}
				}
				if got := policyGETs(w, target, tc.layer != "content"); got != 1 {
					t.Fatalf("wrong surface disabled %s cache: GETs=%d", target.Kind, got)
				}
			}
			w.mu.Lock()
			w.files["file"] = []byte("replacement")
			w.mu.Unlock()
			w.advance(tc.notebook)
			if got := policySize(t, c, testNotebook, tc.layer); got != int64(len("replacement")) {
				t.Fatal("Notebook builtin override was not applied")
			}
			if got := policySize(t, c, testEnv, tc.layer); got != 3 {
				t.Fatal("Notebook expiry affected Environment resources")
			}
			w.advance(tc.env - tc.notebook)
			if got := policySize(t, c, testEnv, tc.layer); got != int64(len("replacement")) {
				t.Fatal("Environment resources override was not applied")
			}
			if policyGETs(w, testNotebook, tc.layer != "content") != 2 ||
				policyGETs(w, testEnv, tc.layer != "content") != 2 {
				t.Fatal("resource type/surface TTLs made unexpected requests")
			}
		})
	}
}

func TestCachePolicyUsesActualWorkspaceAndItemIdentity(t *testing.T) {
	w := newWire(t)
	c := policyClient(t, w, fmt.Sprintf(`{
		"types":{"Notebook":{"attr":"7m","surfaces":{"builtin":{"directory":"8m"}}}},
		"workspaces":{
			%q:{"items":{%q:{"type":"Notebook","surfaces":{
				"builtin":{"attr":"3s","directory":"4s","content":"0s"}
			}}}},
			%q:{"types":{"Environment":{"surfaces":{"resources":{"attr":"5s","content":"6s"}}}}}
		}
	}`, testWorkspace, testItem, envWorkspace))
	otherItem := testNotebook
	otherItem.ItemID = envItem
	otherWorkspace := testNotebook
	otherWorkspace.WorkspaceID = envWorkspace
	for _, tc := range []struct {
		target                   resources.Target
		attr, directory, content time.Duration
	}{
		{testNotebook, 3 * time.Second, 4 * time.Second, 0},
		{otherItem, 7 * time.Minute, 8 * time.Minute, cachepolicy.DefaultTTL},
		{otherWorkspace, 7 * time.Minute, 8 * time.Minute, cachepolicy.DefaultTTL},
		{testEnv, 5 * time.Second, cachepolicy.DefaultTTL, 6 * time.Second},
	} {
		r, err := c.route(context.Background(), tc.target)
		if err != nil {
			t.Fatal(err)
		}
		if r.policy.Attr != tc.attr || r.policy.Directory != tc.directory || r.policy.Content != tc.content {
			t.Fatalf("selector for %+v resolved to %+v", tc.target, r.policy)
		}
	}
}

func TestCachePolicyAttributeAndDirectoryAgesAreIndependent(t *testing.T) {
	for _, first := range []string{"attr", "directory"} {
		t.Run(first, func(t *testing.T) {
			w := newWire(t)
			w.files["file"] = []byte("old")
			attr, directory := "30s", "5s"
			second := "directory"
			if first == "directory" {
				attr, directory, second = "5s", "30s", "attr"
			}
			c := policyClient(t, w, fmt.Sprintf(`{"defaults":{"attr":%q,"directory":%q}}`, attr, directory))
			if got := policySize(t, c, testNotebook, first); got != 3 {
				t.Fatal(got)
			}
			w.advance(6 * time.Second)
			w.mu.Lock()
			w.files["file"] = []byte("replacement")
			w.mu.Unlock()
			if got := policySize(t, c, testNotebook, first); got != 3 {
				t.Fatalf("shorter %s TTL discarded the still-fresh %s listing", second, first)
			}
			if policyGETs(w, testNotebook, true) != 1 {
				t.Fatal("listing was not retained for the longer operation lifetime")
			}
			if first == "directory" {
				infos, err := c.List(context.Background(), notebookPath(""))
				if err != nil || len(infos) != 1 || !infos[0].ObservedAt.Equal(testNow) ||
					!infos[0].ValidUntil.Equal(testNow.Add(5*time.Second)) {
					t.Fatal("directory cache extended expired listing attributes", infos, err)
				}
			} else {
				info, err := c.Stat(context.Background(), notebookPath("file"))
				if err != nil || !info.ValidUntil.Equal(testNow.Add(30*time.Second)) {
					t.Fatal("stat used directory rather than attribute freshness", info, err)
				}
			}
			if got := policySize(t, c, testNotebook, second); got != int64(len("replacement")) {
				t.Fatalf("%s incorrectly used the longer %s age limit", second, first)
			}
			if got := policySize(t, c, testNotebook, first); got != int64(len("replacement")) ||
				policyGETs(w, testNotebook, true) != 2 {
				t.Fatal("refreshed listing was not shared between metadata operations")
			}
			if policyGETs(w, testNotebook, false) != 0 {
				t.Fatal("complete listing metadata caused a file GET")
			}
		})
	}
}

func TestCachePolicyZeroOperationRetainsOnlyForOtherLayer(t *testing.T) {
	for _, disabled := range []string{"attr", "directory"} {
		t.Run(disabled, func(t *testing.T) {
			w := newWire(t)
			w.files["file"] = []byte("old")
			c := policyClient(t, w, fmt.Sprintf(`{"defaults":{%q:"0s"}}`, disabled))
			retained := "attr"
			if disabled == "attr" {
				retained = "directory"
			}
			policySize(t, c, testNotebook, retained)
			for range 2 {
				policySize(t, c, testNotebook, disabled)
			}
			policySize(t, c, testNotebook, retained)
			if policyGETs(w, testNotebook, true) != 3 || c.listings.Stats().Entries != 1 {
				t.Fatal("zero max age either hit retained data or disabled the other layer")
			}
		})
	}
}

func TestCachePolicyZeroDoesNotUnpinOpenSnapshot(t *testing.T) {
	w := newWire(t)
	payload := bytes.Repeat([]byte{0, 0xff, 'x', '\n'}, 32<<10)
	w.files["file"] = payload
	policy, err := cachepolicy.Uniform(0)
	if err != nil {
		t.Fatal(err)
	}
	c := w.client(func(opts *Options) { opts.CachePolicy = policy })
	ctx := context.Background()
	var pinned *resources.Snapshot
	for range 2 {
		policySize(t, c, testNotebook, "attr")
		policySize(t, c, testNotebook, "directory")
		snapshot, err := c.Snapshot(ctx, notebookPath("file"), "")
		if err != nil {
			t.Fatal(err)
		}
		if pinned == nil {
			pinned = snapshot
			defer pinned.Close()
		} else {
			snapshot.Close()
		}
	}
	if policyGETs(w, testNotebook, true) != 4 || policyGETs(w, testNotebook, false) != 2 ||
		c.listings.Stats().Entries != 0 || c.contents.Stats().Entries != 0 ||
		c.metadata.Stats().Entries != 0 || c.capacities.Stats().Entries != 0 || c.clusters.Stats().Entries != 0 {
		t.Fatal("explicit zero retained resource data")
	}
	w.advance(2 * time.Hour)
	w.credentials.set("rotated-fake-identity")
	w.mu.Lock()
	w.files["file"] = []byte("changed")
	w.mu.Unlock()
	requests, tokens := len(w.recorded()), len(w.credentials.scopes)
	for offset := 0; offset < len(payload); offset += 4096 {
		chunk := make([]byte, 4096)
		n, err := pinned.ReadAt(chunk, int64(offset))
		if err != nil || n != len(chunk) || !bytes.Equal(chunk, payload[offset:offset+len(chunk)]) {
			t.Fatalf("pinned chunk changed: n=%d err=%v", n, err)
		}
	}
	if len(w.recorded()) != requests || len(w.credentials.scopes) != tokens {
		t.Fatal("zero content TTL caused per-chunk downloads or authentication")
	}
}

func TestCachePolicyHeaderMetadataPreservesEachSourceDeadline(t *testing.T) {
	for _, missing := range []string{"size", "modified"} {
		t.Run(missing, func(t *testing.T) {
			w := newWire(t)
			w.files["file"] = []byte("old")
			var bodies []*observedBody
			w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
				if !strings.Contains(r.URL.Path, "/filesystem/") {
					return nil, nil, false
				}
				w.mu.Lock()
				size := len(w.files["file"])
				now := w.now
				w.mu.Unlock()
				if r.URL.Query().Get("recursive") == "false" {
					child := map[string]any{"name": "file", "fileSystemEntryType": "file"}
					if missing != "size" {
						child["contentLength"] = size
					}
					if missing != "modified" {
						child["lastModified"] = now.Format(http.TimeFormat)
					}
					return jsonResponse(200, map[string]any{"children": []any{child}}), nil, true
				}
				body := &observedBody{reader: strings.NewReader("metadata must never consume this body")}
				bodies = append(bodies, body)
				resp := response(200, nil)
				resp.ContentLength, resp.Body = int64(size), body
				resp.Header.Set("Last-Modified", now.Format(http.TimeFormat))
				return resp, nil, true
			}
			c := policyClient(t, w, `{}`)
			ctx := context.Background()
			if _, err := c.Stat(ctx, notebookPath("")); err != nil {
				t.Fatal(err)
			}
			w.advance(119 * time.Second)
			info, err := c.Stat(ctx, notebookPath("file"))
			if err != nil || !info.ObservedAt.Equal(testNow) ||
				!info.ValidUntil.Equal(testNow.Add(2*time.Minute)) ||
				info.ValidUntil.Sub(w.clock()) != time.Second {
				t.Fatal("header lookup reset a nearly expired listing", info, err)
			}
			w.advance(time.Second)
			w.mu.Lock()
			w.files["file"] = []byte("replacement")
			w.mu.Unlock()
			info, err = c.Stat(ctx, notebookPath("file"))
			if err != nil || !info.ObservedAt.Equal(testNow.Add(119*time.Second)) ||
				!info.ValidUntil.Equal(testNow.Add(239*time.Second)) {
				t.Fatal("refreshed listing extended cached header freshness", info, err)
			}
			if missing == "size" && !info.Modified.Equal(w.clock()) ||
				missing == "modified" && info.Size != int64(len("replacement")) {
				t.Fatal("header cache copied stale listing fields into a new listing", info)
			}
			if policyGETs(w, testNotebook, true) != 2 || policyGETs(w, testNotebook, false) != 1 {
				t.Fatal("fresh headers were not reused separately from listing attributes")
			}
			w.advance(119 * time.Second)
			info, err = c.Stat(ctx, notebookPath("file"))
			if err != nil || info.Size != int64(len("replacement")) ||
				info.ValidUntil.Sub(w.clock()) != time.Second {
				t.Fatal("renewed headers extended their listing source deadline", info, err)
			}
			if policyGETs(w, testNotebook, true) != 2 || policyGETs(w, testNotebook, false) != 2 {
				t.Fatal("header expiry did not obey its original observation time")
			}
			for _, body := range bodies {
				if body.read.Load() != 0 || !body.closed.Load() {
					t.Fatal("metadata policy read or leaked a file body")
				}
			}
		})
	}
}

func TestCachePolicyZeroAttributeMetadataNeverRetainsHeaders(t *testing.T) {
	w := newWire(t)
	w.omitSizes = true
	w.files["file"] = nil
	var bodies []*observedBody
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if !strings.HasSuffix(r.URL.Path, "/workdir/file") || r.URL.RawQuery != "" {
			return nil, nil, false
		}
		body := &observedBody{reader: strings.NewReader("must not be read")}
		bodies = append(bodies, body)
		resp := response(200, nil)
		resp.ContentLength, resp.Body = largeMetadataSize, body
		resp.Header.Set("Last-Modified", w.clock().Format(http.TimeFormat))
		return resp, nil, true
	}
	c := policyClient(t, w, `{"defaults":{"attr":"0s"}}`)
	for range 2 {
		info, err := c.Stat(context.Background(), notebookPath("file"))
		if err != nil || info.Size != largeMetadataSize || info.Version != "" ||
			!info.ValidUntil.Equal(w.clock()) {
			t.Fatal("TTL-zero large metadata", info, err)
		}
	}
	if len(bodies) != 2 || c.metadata.Stats().Entries != 0 || c.contents.Stats().Entries != 0 ||
		c.listings.Stats().Entries != 1 {
		t.Fatal("attribute zero disabled the wrong layer or retained metadata")
	}
	for _, body := range bodies {
		if body.read.Load() != 0 || !body.closed.Load() {
			t.Fatal("large metadata GET read or leaked its body")
		}
	}
}

func TestCachePolicyEmptyDirectoryKeepsOriginalObservation(t *testing.T) {
	w := newWire(t)
	c := policyClient(t, w, `{"defaults":{"attr":"3s","directory":"10s"}}`)
	ctx := context.Background()
	if entries, err := c.List(ctx, notebookPath("")); err != nil || len(entries) != 0 {
		t.Fatal("empty listing", entries, err)
	}
	w.advance(2 * time.Second)
	info, err := c.Stat(ctx, notebookPath(""))
	if err != nil || !info.IsDir || !info.ObservedAt.Equal(testNow) ||
		!info.ValidUntil.Equal(testNow.Add(3*time.Second)) {
		t.Fatal("empty root fabricated fresh timestamps", info, err)
	}
	w.advance(time.Second)
	info, err = c.Stat(ctx, notebookPath(""))
	if err != nil || !info.ObservedAt.Equal(w.clock()) || policyGETs(w, testNotebook, true) != 2 {
		t.Fatal("empty root stat ignored attribute age", info, err)
	}
}

func TestCachePolicyCatalogExpiryDoesNotAlterGrantLifetime(t *testing.T) {
	w := newWire(t)
	c := policyClient(t, w, fmt.Sprintf(`{
		"defaults":{"catalog":"1h"},
		"workspaces":{%q:{"catalog":"1s"}}
	}`, testWorkspace))
	ctx := context.Background()
	first, err := c.route(ctx, testNotebook)
	if err != nil {
		t.Fatal(err)
	}
	w.advance(time.Second)
	again, err := c.route(ctx, testNotebook)
	if err != nil || again.grant != first.grant || !again.grant.expires.Equal(testNow.Add(time.Hour)) {
		t.Fatal("catalog expiration changed the authenticated grant", err)
	}
	other := testNotebook
	other.ItemID = envItem
	if _, err := c.route(ctx, other); err != nil {
		t.Fatal(err)
	}
	if len(w.workspaceIDs) != 2 || w.count(http.MethodGet, "/metadata/cluster") != 2 ||
		w.count(http.MethodPost, "generatemwctoken") != 2 {
		t.Fatal("workspace catalog policy did not apply to capacity and cluster discovery")
	}
	if _, err := c.route(ctx, testEnv); err != nil {
		t.Fatal(err)
	}
	w.advance(time.Second)
	if _, err := c.route(ctx, testEnv); err != nil {
		t.Fatal(err)
	}
	if len(w.workspaceIDs) != 3 {
		t.Fatal("workspace catalog override affected another workspace")
	}
}
