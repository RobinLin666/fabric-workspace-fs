package mwc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/transport"
)

func TestGrantBindingAndIndependentInstances(t *testing.T) {
	w := newWire(t)
	c := w.client()
	ctx := context.Background()
	for range 2 {
		if _, err := c.Stat(ctx, notebookPath("")); err != nil {
			t.Fatal(err)
		}
	}
	secondNotebook := testNotebook
	secondNotebook.ItemID = envItem
	for _, target := range []resources.Target{secondNotebook, testEnv} {
		if _, err := c.Stat(ctx, resources.Path{Target: target}); err != nil {
			t.Fatal(err)
		}
	}
	if w.count(http.MethodPost, "generatemwctoken") != 3 {
		t.Fatal("artifact/workspace/workload bindings did not get independent grants")
	}
	var exchanges []map[string]any
	for _, r := range w.recorded() {
		if strings.HasSuffix(r.path, "generatemwctoken") {
			var request map[string]any
			if err := json.Unmarshal(r.body, &request); err != nil {
				t.Fatal(err)
			}
			exchanges = append(exchanges, request)
		}
	}
	for index, target := range []resources.Target{testNotebook, secondNotebook, testEnv} {
		capacity, workload := testCapacity, "Notebook"
		if target.Kind == "Environment" {
			capacity, workload = envCapacity, "SparkCore"
		}
		r := exchanges[index]
		artifacts, ok := r["artifactObjectIds"].([]any)
		if len(r) != 4 || r["capacityObjectId"] != capacity || r["workspaceObjectId"] != target.WorkspaceID ||
			r["workloadType"] != workload || !ok || len(artifacts) != 1 || artifacts[0] != target.ItemID {
			t.Fatal("exchange body was not bound to the actual resource target")
		}
	}
	w.mu.Lock()
	w.workspaces[testWorkspace] = fabric.Workspace{ID: testWorkspace, CapacityID: envCapacity}
	w.mu.Unlock()
	c.capacities.Clear()
	if _, err := c.Stat(ctx, notebookPath("")); err != nil {
		t.Fatal(err)
	}
	if w.count(http.MethodPost, "generatemwctoken") != 4 {
		t.Fatal("capacity change reused the old grant")
	}
	w.credentials.set("another-fake-principal")
	if _, err := c.Stat(ctx, notebookPath("")); err != nil {
		t.Fatal(err)
	}
	if w.count(http.MethodPost, "generatemwctoken") != 5 || w.count(http.MethodGet, "/metadata/cluster") != 2 {
		t.Fatal("credential rotation reused discovery/grants from the previous token identity")
	}
	other := w.client(func(o *Options) { o.FabricOrigin = "https://another.fabric.test" })
	if _, err := other.Stat(ctx, notebookPath("")); err != nil {
		t.Fatal(err)
	}
	if w.count(http.MethodPost, "generatemwctoken") != 6 || w.count(http.MethodGet, "/metadata/cluster") != 3 {
		t.Fatal("instances/environments shared a credential grant")
	}
}

func TestGrantRefreshCapturesTokenHostPair(t *testing.T) {
	w := newWire(t)
	var exchanges atomic.Int32
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if !strings.HasSuffix(r.URL.Path, "generatemwctoken") {
			return nil, nil, false
		}
		n := exchanges.Add(1)
		return jsonResponse(200, map[string]any{
			"token": fmt.Sprintf("fake-grant-%d", n), "targetUriHost": fmt.Sprintf("target-%d.mwc.test", n),
			"capacityObjectId": testCapacity, "expiry": w.clock().Add(3 * time.Minute).Unix(),
		}), nil, true
	}
	c := w.client()
	old, err := c.route(context.Background(), testNotebook)
	if err != nil {
		t.Fatal(err)
	}
	w.advance(2 * time.Minute)
	current, err := c.route(context.Background(), testNotebook)
	if err != nil || exchanges.Load() != 2 {
		t.Fatalf("refresh did not happen before expiry: %v", err)
	}
	for _, r := range []route{old, current} {
		resp, err := r.grant.http.Request(context.Background(), http.MethodGet,
			r.url("", url.Values{"recursive": {"false"}}), nil, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	for _, r := range w.recorded() {
		if strings.HasPrefix(r.host, "target-1.") && r.headers.Get("Authorization") != "MwcToken fake-grant-1" {
			t.Fatal("refreshed credential leaked to the old resource host")
		}
		if strings.HasPrefix(r.host, "target-2.") && r.headers.Get("Authorization") != "MwcToken fake-grant-2" {
			t.Fatal("new host did not use its own captured grant")
		}
	}
	w.advance(time.Minute)
	before := len(w.recorded())
	if _, err := old.grant.http.Request(context.Background(), http.MethodGet, old.url("", nil), nil, nil, true); err == nil {
		t.Fatal("expired captured credential was returned")
	}
	if len(w.recorded()) != before {
		t.Fatal("expired token reached the wire")
	}
}

func TestGrantSingleflightAndWaiterCancellation(t *testing.T) {
	w := newWire(t)
	started, release := make(chan struct{}), make(chan struct{})
	var exchanges atomic.Int32
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if !strings.HasSuffix(r.URL.Path, "generatemwctoken") {
			return nil, nil, false
		}
		if exchanges.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return nil, nil, false
		case <-r.Context().Done():
			return nil, r.Context().Err(), true
		}
	}
	c := w.client()
	results := make(chan error, 33)
	go func() { _, err := c.route(context.Background(), testNotebook); results <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("exchange did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.route(ctx, testNotebook); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting caller did not cancel: %v", err)
	}
	var ready sync.WaitGroup
	ready.Add(32)
	for range 32 {
		go func() {
			ready.Done()
			_, err := c.route(context.Background(), testNotebook)
			results <- err
		}()
	}
	ready.Wait()
	close(release)
	for range 33 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("grant waiter stranded")
		}
	}
	if exchanges.Load() != 1 || len(c.grants.flights) != 0 {
		t.Fatal("duplicate exchange or retained flight")
	}
}

func jwtWithExpiry(expiry any) string {
	claims, _ := json.Marshal(map[string]any{"exp": expiry})
	return base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`)) + "." +
		base64.RawURLEncoding.EncodeToString(claims) + "." +
		base64.RawURLEncoding.EncodeToString([]byte("fake-signature"))
}

func TestExplicitAndJWTFallbackExpiry(t *testing.T) {
	expiry := testNow.Add(time.Hour)
	for _, raw := range []json.RawMessage{
		json.RawMessage(`"` + expiry.Format(time.RFC3339) + `"`),
		json.RawMessage(fmt.Sprint(expiry.Unix())),
		json.RawMessage(`"` + fmt.Sprint(expiry.Unix()) + `"`),
	} {
		got, err := grantExpiry(raw, "opaque")
		if err != nil || !got.Equal(expiry) {
			t.Fatalf("expiry = %v, %v", got, err)
		}
	}
	if got, err := grantExpiry(nil, jwtWithExpiry(expiry.Unix())); err != nil || !got.Equal(expiry) {
		t.Fatalf("JWT expiry = %v, %v", got, err)
	}
	for _, tc := range []struct {
		raw   json.RawMessage
		token string
	}{
		{nil, "opaque"},
		{nil, "a.b.c"},
		{nil, jwtWithExpiry("not-an-epoch")},
		{nil, jwtWithExpiry(fmt.Sprint(expiry.Unix()))},
		{nil, strings.TrimSuffix(jwtWithExpiry(expiry.Unix()), base64.RawURLEncoding.EncodeToString([]byte("fake-signature")))},
		{json.RawMessage(`"not-a-date"`), jwtWithExpiry(expiry.Unix())},
		{json.RawMessage(`null`), jwtWithExpiry(expiry.Unix())},
		{json.RawMessage(`-1`), "opaque"},
		{json.RawMessage(`true`), "opaque"},
		{json.RawMessage(`1.5`), "opaque"},
		{json.RawMessage(`999999999999999999999999`), "opaque"},
	} {
		if _, err := grantExpiry(tc.raw, tc.token); err == nil {
			t.Fatal("accepted invalid expiry or malformed fallback JWT")
		}
	}
}

func TestInvalidDiscoveryAndGrantOriginsNeverReceiveCredentials(t *testing.T) {
	for _, stage := range []string{"discovery", "exchange"} {
		for _, origin := range []string{
			"http://foreign.mwc.test", "https://user@foreign.mwc.test", "https://127.0.0.1",
			"https://[::1]", "foreign.mwc.test:8080", "foreign.mwc.test/path",
			"foreign.mwc.test?secret=query", "foreign.mwc.test#fragment", "//foreign.mwc.test",
			"foreign.mwc.test\\other", "https://bad_host.test",
		} {
			t.Run(stage+"/"+origin, func(t *testing.T) {
				w := newWire(t)
				w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
					if stage == "discovery" && r.URL.Path == "/metadata/cluster" {
						return jsonResponse(200, map[string]string{"backendUrl": origin}), nil, true
					}
					if stage == "exchange" && strings.HasSuffix(r.URL.Path, "generatemwctoken") {
						return jsonResponse(200, map[string]any{
							"Token": testMWC, "TargetUriHost": origin, "CapacityObjectId": testCapacity,
							"Expiry": testNow.Add(time.Hour).Unix(),
						}), nil, true
					}
					return nil, nil, false
				}
				if _, err := w.client().Stat(context.Background(), notebookPath("")); !errors.Is(err, fs.ErrInvalid) {
					t.Fatalf("unsafe origin not rejected: %v", err)
				}
				for _, r := range w.recorded() {
					if r.host != "fabric.mwc.test" && r.host != "cluster.mwc.test" {
						t.Fatal("unsafe host received credentials")
					}
				}
			})
		}
	}
}

func TestRedirectsBlockedAtEveryGrantBoundary(t *testing.T) {
	for _, stage := range []string{"/metadata/cluster", "generatemwctoken", "filesystem/workdir"} {
		t.Run(stage, func(t *testing.T) {
			w := newWire(t)
			w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
				if strings.Contains(r.URL.Path, stage) {
					resp := response(http.StatusFound, nil)
					resp.Header.Set("Location", "https://foreign.mwc.test/?secret="+testMWC)
					return resp, nil, true
				}
				return nil, nil, false
			}
			c := w.client()
			_, err := c.Stat(context.Background(), notebookPath(""))
			var remote *transport.HTTPError
			if !errors.As(err, &remote) || remote.StatusCode != http.StatusFound {
				t.Fatalf("redirect accepted: %v", err)
			}
			for _, r := range w.recorded() {
				if r.host == "foreign.mwc.test" {
					t.Fatal("redirect leaked credentials")
				}
			}
			if strings.Contains(err.Error(), testMWC) || strings.Contains(err.Error(), "secret=") {
				t.Fatal("error leaked redirect data")
			}
		})
	}
}

func TestGrantFailuresNotCachedOrRetriedAndAreBounded(t *testing.T) {
	w := newWire(t)
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "generatemwctoken") {
			return response(http.StatusServiceUnavailable, []byte(testMWC)), nil, true
		}
		return nil, nil, false
	}
	c := w.client()
	for range 2 {
		if _, err := c.Stat(context.Background(), notebookPath("")); err == nil || strings.Contains(err.Error(), testMWC) {
			t.Fatal("failed exchange succeeded or leaked response")
		}
		if len(c.grants.entries) != 0 || len(c.grants.flights) != 0 {
			t.Fatal("failure left a grant tombstone")
		}
	}
	if w.count(http.MethodPost, "generatemwctoken") != 2 {
		t.Fatal("token exchange was automatically retried or cached as failure")
	}
	for _, stage := range []string{"/metadata/cluster", "generatemwctoken"} {
		t.Run(stage, func(t *testing.T) {
			w := newWire(t)
			body := &observedBody{reader: strings.NewReader(strings.Repeat("x", maxControlBytes+100))}
			w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
				if strings.Contains(r.URL.Path, stage) {
					resp := response(200, nil)
					resp.Body, resp.ContentLength = body, -1
					return resp, nil, true
				}
				return nil, nil, false
			}
			if _, err := w.client().Stat(context.Background(), notebookPath("")); !errors.Is(err, fserrors.ErrTooLarge) {
				t.Fatalf("unbounded control response: %v", err)
			}
			if !body.closed.Load() || body.read.Load() > maxControlBytes+1 {
				t.Fatal("control body not bounded and closed")
			}
		})
	}
}

func TestMalformedGrantResponsesFailWithoutResourceRequests(t *testing.T) {
	for _, edit := range []func(map[string]any){
		func(m map[string]any) { m["Token"] = "" },
		func(m map[string]any) { m["Token"] = "fake\r\ninjection" },
		func(m map[string]any) { m["CapacityObjectId"] = envCapacity },
		func(m map[string]any) { delete(m, "CapacityObjectId") },
		func(m map[string]any) { m["Expiry"] = testNow.Unix() },
		func(m map[string]any) { m["Expiry"] = "bad" },
		func(m map[string]any) { delete(m, "Expiry") },
	} {
		w := newWire(t)
		w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
			if !strings.HasSuffix(r.URL.Path, "generatemwctoken") {
				return nil, nil, false
			}
			wire := map[string]any{
				"Token": testMWC, "TargetUriHost": "resources.mwc.test",
				"CapacityObjectId": testCapacity, "Expiry": testNow.Add(time.Hour).Unix(),
			}
			edit(wire)
			return jsonResponse(200, wire), nil, true
		}
		c := w.client()
		if _, err := c.Stat(context.Background(), notebookPath("")); err == nil ||
			strings.Contains(err.Error(), testMWC) || strings.Contains(err.Error(), testPBI) {
			t.Fatal("invalid grant accepted or leaked")
		}
		if w.count(http.MethodGet, "filesystem") != 0 {
			t.Fatal("invalid grant used for resources")
		}
	}
}

func TestWorkspaceIdentityAndCredentialErrors(t *testing.T) {
	for _, ws := range []fabric.Workspace{
		{ID: testWorkspace}, {ID: envWorkspace, CapacityID: envCapacity}, {ID: testWorkspace, CapacityID: "invalid"},
	} {
		w := newWire(t)
		w.workspaces[testWorkspace] = ws
		if _, err := w.client().Stat(context.Background(), notebookPath("")); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("invalid workspace/capacity accepted: %v", err)
		}
		if len(w.recorded()) != 0 {
			t.Fatal("invalid capacity triggered exchange")
		}
	}
	w := newWire(t)
	secret := errors.New("SDK error containing " + testPBI)
	w.credentials.err = secret
	if _, err := w.client().Stat(context.Background(), notebookPath("")); !errors.Is(err, secret) ||
		strings.Contains(err.Error(), testPBI) {
		t.Fatal("credential error not preserved safely")
	}
}

func TestGrantCacheLRUBoundAndExpiredReclamation(t *testing.T) {
	w := newWire(t)
	c := w.client()
	ctx := context.Background()
	for i := range maxGrants + 5 {
		target := testNotebook
		target.ItemID = fmt.Sprintf("22222222-2222-4222-8222-%012x", i)
		if _, err := c.route(ctx, target); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.grants.entries) != maxGrants || c.grants.lru.Len() != maxGrants || len(c.grants.flights) != 0 {
		t.Fatal("grant cache exceeded its bound")
	}
	before := w.count(http.MethodPost, "generatemwctoken")
	target := testNotebook
	target.ItemID = "22222222-2222-4222-8222-000000000000"
	if _, err := c.route(ctx, target); err != nil || w.count(http.MethodPost, "generatemwctoken") != before+1 {
		t.Fatal("oldest grant was not evicted")
	}
	w.advance(2 * time.Hour)
	if _, err := c.route(ctx, testNotebook); err != nil {
		t.Fatal(err)
	}
	if len(c.grants.entries) != 1 || c.grants.lru.Len() != 1 {
		t.Fatal("expired grants accumulated")
	}
}
