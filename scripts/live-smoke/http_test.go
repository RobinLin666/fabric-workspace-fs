package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
)

func request(t *testing.T, method, target string, headers map[string]string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, target, nil)
	if err != nil {
		t.Fatal("cannot construct offline request")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return req
}

func ownedGuard(t *testing.T) (*scopeGuard, string) {
	t.Helper()
	plan := testPlan(t)
	g := newScopeGuard(testOptions(), plan)
	g.setNotebook(testNotebook)
	root := onelake.Endpoint + "/" + testWorkspace + "/" + testLakehouse + "/" + plan.LakeDirectory
	create := request(t, "PUT", root+"?resource=directory", map[string]string{"If-None-Match": "*"})
	if _, err := g.authorize(create); err != nil {
		t.Fatal("conditional new directory was rejected")
	}
	g.observe(create, response(201, "", nil))
	return g, root
}

func TestScopeGuardRestrictsWritesAndRequiresConditions(t *testing.T) {
	plan := testPlan(t)
	base := onelake.Endpoint + "/" + testWorkspace + "/" + testLakehouse + "/"
	root := base + plan.LakeDirectory
	fabricBase := "https://api.fabric.microsoft.com/v1/workspaces/" + testWorkspace
	tests := []struct {
		name, method, target string
		headers              map[string]string
		allowed              bool
	}{
		{"own new file", "PUT", root + "/payload.bin?resource=file", map[string]string{"If-None-Match": "*"}, true},
		{"unconditional file", "PUT", root + "/payload.bin?resource=file", nil, false},
		{"existing sibling", "PUT", root + "-other/payload.bin?resource=file", map[string]string{"If-None-Match": "*"}, false},
		{"Files root", "DELETE", base + "Files", map[string]string{"If-Match": `"tag"`}, false},
		{"Tables child", "PUT", base + "Tables/probe?resource=file", map[string]string{"If-None-Match": "*"}, false},
		{"existing file content", "GET", base + "Files/existing-user-file", nil, false},
		{"existing Tables content", "GET", base + "Tables/existing-user-file", nil, false},
		{"existing nested listing", "GET", onelake.Endpoint + "/" + testWorkspace + "?resource=filesystem&recursive=false&directory=" + testLakehouse + "/Files/existing-user-directory", nil, false},
		{"recursive delete", "DELETE", root + "?recursive=true", map[string]string{"If-Match": `"tag"`}, false},
		{"conditional delete", "DELETE", root + "/payload.bin", map[string]string{"If-Match": `"tag"`}, true},
		{"wildcard delete", "DELETE", root + "/payload.bin", map[string]string{"If-Match": "*"}, false},
		{"own append", "PATCH", root + "/payload.bin?action=append", nil, true},
		{"unknown action", "PATCH", root + "/payload.bin?action=untrusted-value", nil, false},
		{"own notebook update", "POST", fabricBase + "/notebooks/" + testNotebook + "/updateDefinition", nil, true},
		{"existing notebook update", "POST", fabricBase + "/notebooks/" + testLakehouse + "/updateDefinition", nil, false},
		{"existing environment definition", "POST", fabricBase + "/environments/" + testLakehouse + "/getDefinition", nil, false},
		{"hard delete", "DELETE", fabricBase + "/notebooks/" + testNotebook + "?hardDelete=true", nil, false},
		{"own SDK soft delete", "DELETE", fabricBase + "/notebooks/" + testNotebook, nil, true},
		{"legacy raw item delete", "DELETE", fabricBase + "/items/" + testNotebook, nil, false},
		{"selected folder catalog", "GET", fabricBase + "/folders?recursive=true", nil, true},
		{"foreign origin", "POST", "https://region-redirect.analysis.windows.net/v1/operations/" + testOperation, nil, false},
		{"insecure origin", "HEAD", strings.Replace(root, "https:", "http:", 1), nil, false},
		{"foreign workspace", "HEAD", onelake.Endpoint + "/" + testNotebook + "/" + testLakehouse + "/Files/a", nil, false},
		{"unowned operation", "GET", "https://api.fabric.microsoft.com/v1/operations/" + testOperation, nil, false},
		{"selected shallow list", "GET", onelake.Endpoint + "/" + testWorkspace + "?resource=filesystem&recursive=false&directory=" + testLakehouse + "/Files", nil, true},
		{"recursive scan", "GET", onelake.Endpoint + "/" + testWorkspace + "?resource=filesystem&recursive=true&directory=" + testLakehouse + "/Files", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, _ := ownedGuard(t)
			_, err := g.authorize(request(t, tt.method, tt.target, tt.headers))
			if (err == nil) != tt.allowed {
				t.Fatalf("scope guard allowed=%v, want %v", err == nil, tt.allowed)
			}
		})
	}
}

func TestRenameScopeGuardsBothSourceAndDestination(t *testing.T) {
	plan := testPlan(t)
	base := "/" + testWorkspace + "/" + testLakehouse + "/"
	root := base + plan.LakeDirectory
	for _, source := range []string{root + "/payload.bin", root + "-other/file", root + "/../existing", "/other/workspace/file"} {
		g, destination := ownedGuard(t)
		headers := map[string]string{
			"x-ms-rename-source": source, "x-ms-source-if-match": `"source"`, "If-Match": `"destination"`,
		}
		_, err := g.authorize(request(t, "PUT", destination+"/replace.bin", headers))
		if (err == nil) != (source == root+"/payload.bin") {
			t.Fatal("rename source escaped its exact owned directory or valid source was rejected")
		}
	}
	g, destination := ownedGuard(t)
	_, err := g.authorize(request(t, "PUT", destination+"/replace.bin", map[string]string{"x-ms-rename-source": root + "/payload.bin"}))
	if err == nil {
		t.Fatal("unconditional replacement was accepted")
	}
}

func TestReadonlyGuardAndSingleCreate(t *testing.T) {
	g, root := ownedGuard(t)
	g.setReadOnly(true)
	if _, err := g.authorize(request(t, "PATCH", root+"/payload.bin?action=append", nil)); err == nil {
		t.Fatal("readonly phase allowed an outgoing mutation")
	}
	if _, err := g.authorize(request(t, "HEAD", root+"/payload.bin", nil)); err != nil {
		t.Fatal("readonly phase rejected a read-only metadata request")
	}
	g.setReadOnly(false)
	body, err := fixtureBody(testPlan(t))
	if err != nil {
		t.Fatal("cannot encode scoped fixture body")
	}
	create, err := http.NewRequest("POST", testPlan(t).NotebookCreateURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal("cannot construct scoped fixture request")
	}
	if _, err := g.authorize(create); err != nil {
		t.Fatal("first fixture create was rejected")
	}
	if _, err := g.authorize(create); err == nil {
		t.Fatal("multiple notebook creates were allowed")
	}
}

func TestHTTPCountsContainNoHeadersPathsOrQueryValues(t *testing.T) {
	g, root := ownedGuard(t)
	counter := &requestCounter{}
	forwards := 0
	rt := &countedTransport{
		guard: g, counter: counter,
		base: roundTripFunc(func(*http.Request) (*http.Response, error) {
			forwards++
			return response(200, "", nil), nil
		}),
	}
	req := request(t, "GET", root+"/payload.bin?action=untrusted-query-value", map[string]string{
		"Authorization": "Bearer untrusted-header-value", "If-Match": `"untrusted-etag-value"`,
	})
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal("allowed read was rejected")
	}
	resp.Body.Close()
	if _, err := rt.RoundTrip(request(t, "DELETE", root+"-other/file", nil)); err == nil {
		t.Fatal("unauthorized deletion reached the base roundtripper")
	}
	snapshot := counter.snapshot()
	if forwards != 1 || totalCounts(snapshot) != 1 || snapshot["onelake|GET|read"] != 1 {
		t.Fatal("counter did not count exactly actual forwarded attempts")
	}
	data, _ := json.Marshal(snapshot)
	for _, forbidden := range []string{"untrusted", testWorkspace, testLakehouse, testPlan(t).Stem, "Authorization", "payload.bin"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatal("counter serialized request data instead of fixed categories")
		}
	}
	if totalCounts(countDelta(snapshot, snapshot)) != 0 {
		t.Fatal("no-op counter delta was not empty")
	}
}

func TestRealOneLakeClientWireShapesPassOwnedScopeGuardOffline(t *testing.T) {
	plan := testPlan(t)
	g := newScopeGuard(testOptions(), plan)
	counter := &requestCounter{}
	root := "/" + testWorkspace + "/" + testLakehouse + "/" + plan.LakeDirectory
	stageETag := `"stage"`
	committed := false
	route := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		query := req.URL.Query()
		switch {
		case req.Method == "GET" && query.Get("resource") == "filesystem":
			return response(200, `{"paths":[]}`, nil), nil
		case req.Method == "HEAD":
			if req.URL.Path == root+"/payload.bin" && !committed {
				return response(404, `{}`, nil), nil
			}
			etag := stageETag
			if req.URL.Path == root+"/payload.bin" && committed {
				etag = `"committed"`
			}
			return response(200, "", http.Header{
				"Etag": {etag}, "Content-Length": {"4"}, "X-Ms-Resource-Type": {"file"},
				"Last-Modified": {"Fri, 18 Sep 2026 10:20:30 GMT"},
			}), nil
		case req.Method == "PUT" && req.Header.Get("x-ms-rename-source") != "":
			committed = true
			return response(201, "", http.Header{"Etag": {`"committed"`}}), nil
		case req.Method == "PUT":
			return response(201, "", http.Header{"Etag": {stageETag}}), nil
		case req.Method == "PATCH" && query.Get("action") == "append":
			return response(202, "", nil), nil
		case req.Method == "PATCH" && query.Get("action") == "flush":
			return response(200, "", http.Header{"Etag": {stageETag}}), nil
		case req.Method == "DELETE":
			return response(200, "", nil), nil
		default:
			t.Fatal("unexpected real-client offline wire method")
			return nil, fail("unexpected offline method")
		}
	})
	wire, err := transport.New(transport.Options{
		BaseURL: onelake.Endpoint, Scope: auth.OneLakeScope, Tokens: offlineTokens{},
		HTTPClient: &http.Client{Transport: &countedTransport{base: route, counter: counter, guard: g}},
	})
	if err != nil {
		t.Fatal("offline OneLake transport setup failed")
	}
	client := onelake.New(wire, onelake.Options{ChunkSize: 4 << 20, MaxPages: 1000})
	p := onelake.Path{Workspace: testWorkspace, Item: testLakehouse, Relative: plan.LakeDirectory}
	if err := client.Mkdir(context.Background(), p); err != nil {
		t.Fatalf("conditional Mkdir was rejected: %s", safeError(err))
	}
	if _, err := client.List(context.Background(), p); err != nil {
		t.Fatalf("shallow List was rejected: %s", safeError(err))
	}
	p.Relative += "/payload.bin"
	if _, err := client.Put(context.Background(), p, bytes.NewReader([]byte("test")), 4, ""); err != nil {
		_, rejected := g.state()
		t.Fatalf("conditional staged Put was rejected: %s; type=%T denied=%d attempts=%v", safeError(err), err, rejected, counter.snapshot())
	}
	_, rejected := g.state()
	if !committed || rejected != 0 || counter.snapshot()["onelake|PUT|rename"] == 0 {
		t.Fatal("real client staged upload did not complete within owned scope")
	}
}
