package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/transport"
)

func TestSDKLocationNormalizationAllowlist(t *testing.T) {
	op := "/v1/operations/" + testOperation
	for _, location := range []string{
		fabric.BaseURL + op,
		fabric.BaseURL + op + "/result",
		"https://region-redirect.analysis.windows.net" + op,
		"https://region-redirect.analysis.windows.net" + op + "/result",
		op,
		op + "/result",
	} {
		if validateOperationLocation(location, testOperation) != nil {
			t.Fatal("approved matching operation Location was rejected")
		}
	}
	bad := []string{
		"https://unrelated.invalid" + op,
		"https://analysis.windows.net" + op,
		"https://region.analysis.windows.net.unrelated.invalid" + op,
		"https://-bad.analysis.windows.net" + op,
		"https://bad..analysis.windows.net" + op,
		"https://user@region.analysis.windows.net" + op,
		"https://region.analysis.windows.net:443" + op,
		"https://api.fabric.microsoft.com:443" + op,
		"http://api.fabric.microsoft.com" + op,
		"http://region.analysis.windows.net" + op,
		"//region.analysis.windows.net" + op,
		fabric.BaseURL + op + "?token=untrusted-secret",
		fabric.BaseURL + op + "?",
		fabric.BaseURL + op + "#untrusted-secret",
		fabric.BaseURL + op + "#",
		fabric.BaseURL + "/v1/operations/" + testDeleteOp,
		fabric.BaseURL + "/v1/workspaces/" + testWorkspace,
		fabric.BaseURL + "/v1/operations/../workspaces/" + testOperation,
		fabric.BaseURL + "/v1/operations/%2e%2e/" + testOperation,
		fabric.BaseURL + "/v1/operations%2F" + testOperation,
		fabric.BaseURL + op + "/result/extra",
		fabric.BaseURL + op + "/result/",
		fabric.BaseURL + op + "/",
		fabric.BaseURL + op + "/result?untrusted-secret",
		" " + fabric.BaseURL + op,
		fabric.BaseURL + op + " ",
		"v1/operations/" + testOperation,
		"",
	}
	for _, location := range bad {
		err := validateOperationLocation(location, testOperation)
		if err == nil || strings.Contains(safeError(err), "untrusted-secret") {
			t.Fatal("unsafe operation Location was accepted or exposed")
		}
	}
	if validateOperationLocation(fabric.BaseURL+op, "") == nil {
		t.Fatal("Location was trusted without a captured operation UUID")
	}
}

func TestSDKRejectsUnsafeInitialAndSuccessLocations(t *testing.T) {
	unsafe := []string{
		"https://unrelated.invalid/v1/operations/" + testOperation,
		"http://region.analysis.windows.net/v1/operations/" + testOperation,
		"//region.analysis.windows.net/v1/operations/" + testOperation,
		"https://region.analysis.windows.net:443/v1/operations/" + testOperation,
		"https://user@region.analysis.windows.net/v1/operations/" + testOperation,
		"https://region.analysis.windows.net/v1/operations/" + testDeleteOp,
		"https://region.analysis.windows.net/v1/operations/" + testOperation + "?untrusted-secret",
		"https://region.analysis.windows.net/v1/operations/" + testOperation + "#untrusted-secret",
		"https://region.analysis.windows.net/not-an-operation",
	}
	for phase := range 2 {
		for _, location := range unsafe {
			synctest.Test(t, func(t *testing.T) {
				forwards := 0
				c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
					forwards++
					if req.URL.Host != "api.fabric.microsoft.com" || req.Header.Get("Authorization") != "Bearer offline-test-token" {
						t.Fatal("credentials were not restricted to the public authenticated transport")
					}
					headers := http.Header{"X-Ms-Operation-Id": {testOperation}, "Retry-After": {"0"}}
					if req.Method == http.MethodPost {
						if phase == 0 {
							headers.Set("Location", location)
						}
						return response(202, "", headers), nil
					}
					headers.Set("Location", location)
					return response(200, `{"status":"Succeeded"}`, headers), nil
				})
				err := c.create(context.Background())
				want := phase + 1
				if err == nil || c.ownedID != "" || forwards != want || counter.snapshot()["fabric|GET|operation-result"] != 0 {
					t.Fatal("unsafe Location reached a subsequent SDK request or claimed ownership")
				}
				if strings.Contains(safeError(err), "untrusted-secret") {
					t.Fatal("unsafe SDK Location leaked in an error")
				}
			})
		}
	}
}

func TestSDKReconstructsIDOnlyCreateAndValidatesSuccessID(t *testing.T) {
	for _, mismatched := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			item := fixtureItemJSON(t, testPlan(t))
			c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodPost {
					return response(202, "", http.Header{"X-Ms-Operation-Id": {testOperation}, "Retry-After": {"0"}}), nil
				}
				if req.URL.Path == "/v1/operations/"+testOperation {
					headers := make(http.Header)
					if mismatched {
						headers.Set("x-ms-operation-id", testDeleteOp)
					}
					return response(200, `{"status":"Succeeded"}`, headers), nil
				}
				return response(200, item, nil), nil
			})
			err := c.create(context.Background())
			if mismatched {
				if err == nil || c.ownedID != "" || counter.snapshot()["fabric|GET|operation-result"] != 0 {
					t.Fatal("SDK accepted a changed operation ID")
				}
			} else if err != nil || c.ownedID != testNotebook || counter.snapshot()["fabric|GET|operation-result"] != 1 {
				t.Fatal("SDK did not reconstruct the ID-only poll and result Locations")
			}
		})
	}
}

func TestSDKRequiresUnambiguousOperationIDHeader(t *testing.T) {
	for _, headers := range []http.Header{
		{"Location": {operationURL(testOperation, false)}},
		{"X-Ms-Operation-Id": {testOperation, testDeleteOp}},
		{"X-Ms-Operation-Id": {testOperation}, "Location": {operationURL(testOperation, false), operationURL(testDeleteOp, false)}},
		{"X-Ms-Operation-Id": {testOperation}, "x-ms-operation-id": {testDeleteOp}},
	} {
		c, counter, _ := newFixtureHarness(t, func(*http.Request) (*http.Response, error) {
			return response(202, "", headers), nil
		})
		if c.create(context.Background()) == nil || totalCounts(counter.snapshot()) != 1 || c.operationID != "" {
			t.Fatal("missing/ambiguous operation identity was trusted")
		}
	}
}

type observedTokens struct {
	calls int
}

func (s *observedTokens) Token(_ context.Context, scope string) (string, error) {
	if scope != auth.FabricScope {
		return "", fail("offline unexpected token audience")
	}
	s.calls++
	return "offline-adapter-token", nil
}

func directSDKAdapter(t *testing.T, create bool, route roundTripFunc) (*fixtureSDKTransport, *observedTokens) {
	t.Helper()
	tokens := &observedTokens{}
	client, err := transport.New(transport.Options{
		BaseURL: fabric.BaseURL, Scope: auth.FabricScope, Tokens: tokens,
		MaxRetries: 3, RetryDelay: time.Second, MaxRetryDelay: 30 * time.Second,
		HTTPClient: &http.Client{Transport: route},
	})
	if err != nil {
		t.Fatal("offline adapter setup failed")
	}
	return &fixtureSDKTransport{http: client, workspace: testWorkspace, notebook: testNotebook, create: create}, tokens
}

func TestSDKOutgoingRequestsAreGuardedBeforeTokenAcquisition(t *testing.T) {
	for _, candidate := range []struct {
		method, url string
		auth        bool
	}{
		{"GET", "https://region.analysis.windows.net/v1/operations/" + testOperation, false},
		{"GET", "http://api.fabric.microsoft.com/v1/operations/" + testOperation, false},
		{"GET", operationURL(testDeleteOp, false), false},
		{"GET", operationURL(testOperation, true), false},
		{"GET", operationURL(testOperation, false) + "?query=value", false},
		{"GET", fabric.BaseURL + "/v1/workspaces/" + testWorkspace + "/items/" + testNotebook, false},
		{"DELETE", fabric.BaseURL + "/v1/workspaces/" + testWorkspace + "/notebooks/" + testNotebook + "?hardDelete=false", false},
		{"GET", operationURL(testOperation, false), true},
	} {
		adapter, tokens := directSDKAdapter(t, false, func(*http.Request) (*http.Response, error) {
			t.Fatal("forbidden SDK request reached HTTP")
			return nil, fail("unexpected offline request")
		})
		adapter.operation = testOperation
		req := request(t, candidate.method, candidate.url, nil)
		if candidate.auth {
			req.Header["authorization"] = []string{"must-not-be-forwarded"}
		}
		if _, err := adapter.Do(req); err == nil || tokens.calls != 0 {
			t.Fatal("SDK validation happened after credential retrieval")
		}
	}
}

func TestSDKWritesNeverRetryAtEitherLayer(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		for _, networkFailure := range []bool{false, true} {
			plan := testPlan(t)
			item := fixtureItemJSON(t, plan)
			writes := 0
			c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("Authorization") != "Bearer offline-test-token" {
					t.Fatal("SDK did not delegate authentication to the existing token source")
				}
				if req.Header.Get("User-Agent") != "" {
					t.Fatal("SDK telemetry was forwarded through the fixture adapter")
				}
				failing := req.Method == http.MethodPost && !deleting || req.Method == http.MethodDelete && deleting
				if failing {
					writes++
					if networkFailure {
						return nil, errors.New("untrusted-sdk-error-secret")
					}
					return response(503, `{"errorCode":"untrusted-sdk-error-secret","message":"untrusted-sdk-error-secret"}`,
						http.Header{"Retry-After": {"0"}}), nil
				}
				status := 200
				if req.Method == http.MethodPost {
					status = 201
				}
				return response(status, item, nil), nil
			})
			var err error
			if deleting {
				if c.create(context.Background()) != nil {
					t.Fatal("offline fixture setup failed")
				}
				err = c.remove(context.Background())
			} else {
				err = c.create(context.Background())
			}
			if err == nil || writes != 1 || strings.Contains(safeError(err), "untrusted-sdk-error-secret") {
				t.Fatal("SDK/transport retried a write, swallowed failure, or exposed raw SDK details")
			}
			key := "fabric|POST|notebook-create"
			if deleting {
				key = "fabric|DELETE|notebook-delete"
			}
			if counter.snapshot()[key] != 1 {
				t.Fatal("more than one actual fixture mutation attempt was issued")
			}
			if !networkFailure {
				var status *transport.HTTPError
				if !errors.As(err, &status) || status.StatusCode != 503 {
					t.Fatal("guarded HTTPError was not preserved")
				}
			}
		}
	}
}

type closedBody struct {
	io.Reader
	closed bool
}

func (b *closedBody) Close() error {
	b.closed = true
	return nil
}

func TestSDKRequestAndResponseBytesAreBounded(t *testing.T) {
	for _, declared := range []bool{false, true} {
		adapter, tokens := directSDKAdapter(t, true, func(*http.Request) (*http.Response, error) {
			t.Fatal("oversized request reached HTTP")
			return nil, fail("unexpected offline request")
		})
		body := &closedBody{Reader: strings.NewReader(strings.Repeat("x", sdkFixtureLimit+1))}
		req := request(t, "POST", fabric.BaseURL+"/v1/workspaces/"+testWorkspace+"/notebooks", nil)
		req.Body = body
		req.ContentLength = -1
		if declared {
			req.ContentLength = sdkFixtureLimit + 1
		}
		if _, err := adapter.Do(req); err == nil || tokens.calls != 0 || !body.closed {
			t.Fatal("oversized SDK request was not rejected before authentication and closed")
		}
	}
	for _, polling := range []bool{false, true} {
		limit := sdkFixtureLimit
		status := 201
		if polling {
			limit, status = sdkPollLimit, 200
		}
		body := &closedBody{Reader: bytes.NewReader(bytes.Repeat([]byte{'x'}, limit+1))}
		adapter, tokens := directSDKAdapter(t, true, func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: body}, nil
		})
		method, target := "POST", fabric.BaseURL+"/v1/workspaces/"+testWorkspace+"/notebooks"
		if polling {
			adapter.operation = testOperation
			method, target = "GET", operationURL(testOperation, false)
		}
		if _, err := adapter.Do(request(t, method, target, nil)); err == nil || tokens.calls != 1 || !body.closed {
			t.Fatal("oversized SDK response was not bounded and closed")
		}
	}
}

func TestSDKDeleteLROFailureIsNotHiddenByAnAbsentItem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		item := fixtureItemJSON(t, testPlan(t))
		deleting := false
		c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost {
				return response(201, item, nil), nil
			}
			if req.Method == http.MethodDelete {
				deleting = true
				return response(202, "", http.Header{"X-Ms-Operation-Id": {testDeleteOp}}), nil
			}
			if req.URL.Path == "/v1/operations/"+testDeleteOp {
				return response(200, `{"status":"Failed","error":{"message":"untrusted-body-secret"}}`, nil), nil
			}
			if deleting {
				return response(404, `{}`, nil), nil
			}
			return response(200, item, nil), nil
		})
		if c.create(context.Background()) != nil {
			t.Fatal("fixture setup failed")
		}
		err := c.remove(context.Background())
		if err == nil || c.deleteOperationID != testDeleteOp ||
			counter.snapshot()["fabric|GET|operation-result"] != 0 || strings.Contains(safeError(err), "untrusted-body-secret") {
			t.Fatal("failed SDK deletion was hidden, queried a result, or exposed service details")
		}
	})
}

func TestSDKReconciliationAfterAsyncEvidenceFailureDoesNotRecreate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		item := fixtureItemJSON(t, testPlan(t))
		started := time.Now()
		c, counter, guard := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost {
				return response(202, "", http.Header{"X-Ms-Operation-Id": {testOperation}, "Retry-After": {"3"}}), nil
			}
			if req.URL.Path == "/v1/operations/"+testOperation {
				if time.Since(started) < 3*time.Second {
					t.Fatal("reconciliation ignored Retry-After after journal failure")
				}
				return response(200, `{"status":"Succeeded"}`, nil), nil
			}
			return response(200, item, nil), nil
		})
		c.onChange = func() error {
			if c.operationID != "" {
				return fail("offline journal failure")
			}
			return nil
		}
		if c.create(context.Background()) == nil || c.createPoller != nil || c.operationID != testOperation {
			t.Fatal("async journal failure did not preserve the verified operation")
		}
		c.onChange = func() error {
			if c.ownedID != "" {
				guard.setNotebook(c.ownedID)
			}
			return nil
		}
		if err := c.reconcile(context.Background()); err != nil || c.ownedID != testNotebook {
			t.Fatalf("safe SDK reconciliation failed: %s", safeError(err))
		}
		if counter.snapshot()["fabric|POST|notebook-create"] != 1 {
			t.Fatal("reconciliation issued another create")
		}
	})
}

func TestSDKRetryAfterDateAndCancellationGateRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		c, counter, _ := newFixtureHarness(t, func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodPost {
				t.Fatal("polling ignored a later Retry-After date and deadline")
			}
			return response(202, "", http.Header{
				"X-Ms-Operation-Id": {testOperation},
				"Retry-After":       {started.Add(time.Hour).UTC().Format(http.TimeFormat)},
			}), nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.create(ctx); !errors.Is(err, context.DeadlineExceeded) || totalCounts(counter.snapshot()) != 1 {
			t.Fatal("SDK retry-date/deadline handling permitted an early poll")
		}
	})
}

func TestRawSDKErrorSanitization(t *testing.T) {
	secret := errors.New("raw SDK body and URL containing untrusted-secret")
	if strings.Contains(sanitizeSDKError(secret).Error(), "untrusted-secret") {
		t.Fatal("SDK sanitizer exposed raw error text")
	}
	httpErr := &transport.HTTPError{StatusCode: 403, Method: "POST", Path: "/v1/notebooks", Message: "untrusted-secret"}
	if sanitizeSDKError(httpErr) != httpErr || strings.Contains(safeError(sanitizeSDKError(httpErr)), "untrusted-secret") {
		t.Fatal("SDK sanitizer did not preserve the already sanitized HTTP error")
	}
}

func TestSDKMalformedAndIncompleteModelsCannotClaimOwnership(t *testing.T) {
	for _, body := range []string{
		`{"id":{"untrusted-secret":"untrusted-secret"},"displayName":"untrusted-secret","type":"Notebook"}`,
		`{"id":"` + testNotebook + `","displayName":"untrusted-secret"}`,
		`{"id":"` + testNotebook + `","displayName":"untrusted-secret","type":"Notebook","workspaceId":"` + testLakehouse + `"}`,
	} {
		c, counter, _ := newFixtureHarness(t, func(*http.Request) (*http.Response, error) {
			return response(201, body, nil), nil
		})
		err := c.create(context.Background())
		if err == nil || c.ownedID != "" || strings.Contains(safeError(err), "untrusted-secret") || totalCounts(counter.snapshot()) != 1 {
			t.Fatal("malformed SDK result was trusted or exposed SDK model decoding details")
		}
		if strings.Contains(body, `"id":"`+testNotebook+`"`) && c.returnedID != testNotebook {
			t.Fatal("valid returned UUID was lost from failure evidence")
		}
	}
}
