package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/transport"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type offlineTokens struct{}

func (offlineTokens) Token(context.Context, string) (string, error) { return "offline-test-token", nil }

func response(status int, body string, headers http.Header) *http.Response {
	if headers == nil {
		headers = make(http.Header)
	}
	length := int64(len(body))
	if header := headers.Get("Content-Length"); header != "" {
		if parsed, err := strconv.ParseInt(header, 10, 64); err == nil {
			length = parsed
		}
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), ContentLength: length}
}

func fixtureItemJSON(t *testing.T, plan fixturePlan) string {
	t.Helper()
	data, err := json.Marshal(fabric.Item{
		ID: testNotebook, Type: "Notebook", DisplayName: plan.NotebookDisplayName,
		Description: plan.NotebookDescription,
	})
	if err != nil {
		t.Fatal("cannot encode mock fixture item")
	}
	return string(data)
}

func newFixtureHarness(t *testing.T, route roundTripFunc) (*fixtureClient, *requestCounter, *scopeGuard) {
	t.Helper()
	plan := testPlan(t)
	counter := &requestCounter{}
	guard := newScopeGuard(testOptions(), plan)
	client, err := transport.New(transport.Options{
		BaseURL: fabric.BaseURL, Scope: auth.FabricScope, Tokens: offlineTokens{},
		MaxRetries: 3, RetryDelay: time.Second, MaxRetryDelay: 30 * time.Second,
		HTTPClient: &http.Client{Transport: &countedTransport{base: route, counter: counter, guard: guard}},
	})
	if err != nil {
		t.Fatal("cannot construct offline transport")
	}
	fixture := &fixtureClient{http: client, workspace: testWorkspace, plan: plan}
	fixture.onChange = func() error {
		if fixture.ownedID != "" {
			guard.setNotebook(fixture.ownedID)
		}
		return nil
	}
	return fixture, counter, guard
}

func TestDirectCreateValidatesIdentityBeforeOwnershipAndSoftDeletes(t *testing.T) {
	plan := testPlan(t)
	item := fixtureItemJSON(t, plan)
	deleted := false
	var trace []string
	c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
		trace = append(trace, req.Method+" "+req.URL.Path)
		base := "/v1/workspaces/" + testWorkspace
		switch {
		case req.Method == "POST" && req.URL.Path == base+"/notebooks":
			var body map[string]json.RawMessage
			if decodeSmall(req.Body, 1<<20, &body) != nil || body["definition"] == nil {
				t.Fatal("missing create definition")
			}
			return response(201, item, nil), nil
		case req.Method == "GET" && req.URL.Path == base+"/items/"+testNotebook:
			if deleted {
				return response(404, `{"message":"must never be logged"}`, nil), nil
			}
			return response(200, item, nil), nil
		case req.Method == "DELETE" && req.URL.Path == base+"/notebooks/"+testNotebook:
			if req.URL.RawQuery != "" {
				t.Fatal("delete used hardDelete or another query")
			}
			deleted = true
			return response(204, "", nil), nil
		default:
			t.Fatal("unexpected offline request")
			return nil, fail("unexpected offline request")
		}
	})
	if c.ownedID != "" {
		t.Fatal("ownership was claimed before create")
	}
	if err := c.create(context.Background()); err != nil {
		t.Fatalf("direct create failed: %s", safeError(err))
	}
	if c.returnedID != testNotebook || c.ownedID != testNotebook || !c.candidateValid {
		t.Fatal("direct create did not validate returned and fetched identities")
	}
	if err := c.remove(context.Background()); err != nil || !deleted {
		t.Fatalf("verified fixture cleanup failed: %s", safeError(err))
	}
	if len(trace) != 5 || trace[1] != "GET /v1/workspaces/"+testWorkspace+"/items/"+testNotebook {
		t.Fatal("ownership was not fetched before subsequent fixture operations")
	}
	if counter.snapshot()["fabric|DELETE|notebook-delete"] != 1 {
		t.Fatal("cleanup did not delete exactly one item")
	}
}

func TestCreateAndDeleteLROIgnoreRegionalLocations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		plan := testPlan(t)
		item := fixtureItemJSON(t, plan)
		deleted, deleting := false, false
		polls := 0
		var trace []string
		c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
			if req.URL.Host != "api.fabric.microsoft.com" {
				t.Fatal("bearer request escaped the public origin")
			}
			trace = append(trace, req.Method+" "+req.URL.Path)
			base := "/v1/workspaces/" + testWorkspace
			switch {
			case req.Method == "POST" && req.URL.Path == base+"/notebooks":
				return response(202, "", http.Header{
					"X-Ms-Operation-Id": {testOperation}, "Retry-After": {"2"},
					"Location": {"https://region-redirect.analysis.windows.net/v1/operations/" + testOperation},
				}), nil
			case req.URL.Path == "/v1/operations/"+testOperation:
				polls++
				status := "Running"
				if polls > 1 {
					status = "Succeeded"
				}
				return response(200, `{"status":"`+status+`"}`, http.Header{"Retry-After": {"1"}}), nil
			case req.URL.Path == "/v1/operations/"+testOperation+"/result":
				return response(200, item, nil), nil
			case req.Method == "GET" && req.URL.Path == base+"/items/"+testNotebook:
				if deleted {
					return response(404, `{}`, nil), nil
				}
				return response(200, item, nil), nil
			case req.Method == "DELETE" && req.URL.Path == base+"/notebooks/"+testNotebook:
				deleting = true
				return response(202, "", http.Header{
					"X-Ms-Operation-Id": {testDeleteOp}, "Retry-After": {"1"},
					"Location": {"https://region-redirect.analysis.windows.net/v1/operations/" + testDeleteOp},
				}), nil
			case req.URL.Path == "/v1/operations/"+testDeleteOp && deleting:
				deleted = true
				return response(200, `{"status":"Succeeded"}`, nil), nil
			default:
				t.Fatal("unexpected LRO endpoint, including forbidden deletion result")
				return nil, fail("unexpected request")
			}
		})
		start := time.Now()
		if err := c.create(context.Background()); err != nil {
			t.Fatalf("LRO creation failed: %s", safeError(err))
		}
		if time.Since(start) < 4*time.Second || c.operationID != testOperation || c.ownedID != testNotebook {
			t.Fatal("Retry-After or creation identity was not respected")
		}
		if err := c.remove(context.Background()); err != nil || !deleted {
			t.Fatalf("LRO deletion failed: %s", safeError(err))
		}
		if counter.snapshot()["fabric|GET|operation-result"] != 1 {
			t.Fatal("/result was not restricted to creation")
		}
		if len(trace) == 0 {
			t.Fatal("offline protocol was not exercised")
		}
	})
}

func TestLROFailureUnknownStatusInvalidIDAndDeadline(t *testing.T) {
	tests := []struct {
		name      string
		id        string
		retry     string
		status    string
		timeout   time.Duration
		wantPolls int
	}{
		{"failed", testOperation, "0", "Failed", time.Minute, 1},
		{"canceled", testOperation, "0", "Canceled", time.Minute, 1},
		{"unknown status", testOperation, "0", "untrusted-body-secret", time.Minute, 1},
		{"invalid operation ID", "../escape", "0", "", time.Minute, 0},
		{"missing operation ID", "", "0", "", time.Minute, 0},
		{"invalid retry", testOperation, "untrusted-header-secret", "", time.Minute, 0},
		{"retry exceeds deadline", testOperation, "3600", "Running", time.Second, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				polls := 0
				c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
					if req.Method == "POST" {
						return response(202, "", http.Header{"X-Ms-Operation-Id": {tt.id}, "Retry-After": {tt.retry}}), nil
					}
					polls++
					return response(200, `{"status":"`+tt.status+`","error":{"message":"untrusted-body-secret"}}`, nil), nil
				})
				ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
				defer cancel()
				err := c.create(ctx)
				if err == nil || c.ownedID != "" || polls != tt.wantPolls {
					t.Fatal("unsafe or failed LRO did not propagate its failure")
				}
				if strings.Contains(safeError(err), "secret") || counter.snapshot()["fabric|GET|operation-result"] != 0 {
					t.Fatal("failed LRO exposed service data or requested a result")
				}
			})
		})
	}
}

func TestIdentityMismatchNeverClaimsOrDeletesExistingNotebook(t *testing.T) {
	for _, mutation := range []string{"id", "type", "display name", "description"} {
		t.Run(mutation, func(t *testing.T) {
			plan := testPlan(t)
			item := fabric.Item{ID: testNotebook, Type: "Notebook", DisplayName: plan.Stem, Description: plan.NotebookDescription}
			requests := 0
			c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
				requests++
				current := item
				if req.Method == "GET" || mutation != "description" {
					switch mutation {
					case "id":
						current.ID = testLakehouse
					case "type":
						current.Type = "Lakehouse"
					case "display name":
						current.DisplayName = "existing-user-notebook"
					case "description":
						current.Description = "not-our-owner-marker"
					}
				}
				// For ID mismatch, creation reports the fixture ID but GET disagrees.
				if req.Method == "POST" && mutation == "id" {
					current = item
				}
				body, _ := json.Marshal(current)
				status := 200
				if req.Method == "POST" {
					status = 201
				}
				return response(status, string(body), nil), nil
			})
			if err := c.create(context.Background()); err == nil || c.ownedID != "" {
				t.Fatal("mismatched identity was accepted")
			}
			before := requests
			if err := c.remove(context.Background()); err == nil || requests != before {
				t.Fatal("unverified ownership reached DELETE or a further request")
			}
			if counter.snapshot()["fabric|DELETE|notebook-delete"] != 0 {
				t.Fatal("existing/unverified notebook was deleted")
			}
		})
	}
}

func TestOwnershipIsRecheckedBeforeCleanup(t *testing.T) {
	plan := testPlan(t)
	original := fixtureItemJSON(t, plan)
	changed := false
	c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" {
			return response(201, original, nil), nil
		}
		body := original
		if changed {
			body = strings.ReplaceAll(body, plan.NotebookDescription, "ownership-changed")
		}
		return response(200, body, nil), nil
	})
	if err := c.create(context.Background()); err != nil {
		t.Fatal("create setup failed")
	}
	changed = true
	if c.remove(context.Background()) == nil || counter.snapshot()["fabric|DELETE|notebook-delete"] != 0 {
		t.Fatal("cleanup ignored changed ownership metadata")
	}
}

func TestCleanupCanReconcileVerifiedCreateCandidateAfterJournalFailure(t *testing.T) {
	plan := testPlan(t)
	item := fixtureItemJSON(t, plan)
	c, _, guard := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
		status := 200
		if req.Method == "POST" {
			status = 201
		}
		return response(status, item, nil), nil
	})
	c.onChange = func() error {
		if c.returnedID != "" {
			return fail("offline evidence failure")
		}
		return nil
	}
	if err := c.create(context.Background()); err == nil || !c.candidateValid || c.returnedID != testNotebook {
		t.Fatal("journal failure discarded the already parsed ownership candidate")
	}
	c.onChange = func() error { guard.setNotebook(c.ownedID); return nil }
	if err := c.reconcile(context.Background()); err != nil || c.ownedID != testNotebook {
		t.Fatal("cleanup could not revalidate its exact returned item ID")
	}
}

func TestAmbiguousCreateDoesNotSearchByNameOrDelete(t *testing.T) {
	c, counter, _ := newFixtureHarness(t, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("simulated lost response")
	})
	if c.create(context.Background()) == nil {
		t.Fatal("lost create response was treated as success")
	}
	before := totalCounts(counter.snapshot())
	if c.reconcile(context.Background()) == nil || totalCounts(counter.snapshot()) != before {
		t.Fatal("ambiguous create expanded cleanup to a name search or new mutation")
	}
}

func TestCanceledRetryWaitAndResponseBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(waitRetryAfter(ctx, "60"), context.Canceled) {
		t.Fatal("retry wait did not honor cancellation")
	}
	c, _, _ := newFixtureHarness(t, func(*http.Request) (*http.Response, error) {
		return response(201, strings.Repeat("x", (1<<20)+1), nil), nil
	})
	if c.create(context.Background()) == nil || c.ownedID != "" {
		t.Fatal("oversized create response was accepted")
	}
}
