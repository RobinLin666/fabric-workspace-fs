package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/transport"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	fabricsdk "github.com/microsoft/fabric-sdk-go/fabric"
	"github.com/microsoft/fabric-sdk-go/fabric/notebook"
)

const (
	sdkFixtureLimit = 1 << 20
	sdkPollLimit    = 64 << 10
)

// This adapter is only for disposable SDK notebook fixtures. Production
// definitions, discovery, credentials and token caching remain unchanged.
type fixtureSDKTransport struct {
	http      *transport.Client
	workspace string
	notebook  string
	create    bool
	onID      func(string) error

	mu           sync.Mutex
	mutationSent bool
	operation    string
	notBefore    time.Time
}

func (c *fixtureClient) newSDK(create bool) (*notebook.ItemsClient, *fixtureSDKTransport, error) {
	adapter := &fixtureSDKTransport{
		http: c.http, workspace: c.workspace, notebook: c.ownedID, create: create,
		onID: func(id string) error {
			if create {
				c.operationID = id
			} else {
				c.deleteOperationID = id
			}
			return c.changed()
		},
	}
	endpoint := fabric.BaseURL
	client, err := fabricsdk.NewClient(nil, &endpoint, &fabricsdk.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: adapter,
			Retry:     policy.RetryOptions{MaxRetries: -1},
			Telemetry: policy.TelemetryOptions{Disabled: true},
			Logging:   policy.LogOptions{IncludeBody: false},
		},
	})
	if err != nil {
		return nil, nil, sanitizeSDKError(err)
	}
	return notebook.NewClientFactoryWithClient(*client).NewItemsClient(), adapter, nil
}

func sanitizeSDKError(err error) error {
	if err == nil {
		return nil
	}
	var httpErr *transport.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr
	}
	var own *problem
	if errors.As(err, &own) {
		return own
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return fail("official notebook SDK operation failed; untrusted SDK error details omitted")
}

func operationURL(id string, result bool) string {
	value := fabric.BaseURL + "/v1/operations/" + id
	if result {
		value += "/result"
	}
	return value
}

func analysisHost(host string) bool {
	const suffix = ".analysis.windows.net"
	host = strings.ToLower(host)
	if !strings.HasSuffix(host, suffix) || len(host) <= len(suffix) || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range label {
			if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-') {
				return false
			}
		}
	}
	return true
}

// Validate the original header before the SDK can rewrite its host. Only a
// public-origin or known HTTPS regional operation URL with the same UUID is
// accepted. The caller selects poll versus result; no service-provided URL is
// ever used as an authenticated outgoing target.
func validateOperationLocation(raw, id string) error {
	if fabric.ValidateID(id) != nil || raw == "" || raw != strings.TrimSpace(raw) ||
		strings.ContainsAny(raw, "\\#\r\n\t") || strings.HasPrefix(raw, "//") {
		return fail("SDK operation Location was missing or unsafe")
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery ||
		u.Fragment != "" || u.RawFragment != "" || u.RawPath != "" || strings.Contains(u.Host, ":") {
		return fail("SDK operation Location contained forbidden URL components")
	}
	if u.IsAbs() {
		if u.Scheme != "https" || (!strings.EqualFold(u.Host, "api.fabric.microsoft.com") && !analysisHost(u.Host)) {
			return fail("SDK operation Location was outside the approved HTTPS origins")
		}
	} else if u.Host != "" || u.Scheme != "" || !strings.HasPrefix(raw, "/") {
		return fail("SDK operation Location was not origin-relative or approved HTTPS")
	}
	prefix := "/v1/operations/"
	if !strings.HasPrefix(u.Path, prefix) {
		return fail("SDK operation Location was not an operation endpoint")
	}
	tail := strings.TrimPrefix(u.Path, prefix)
	tail = strings.TrimSuffix(tail, "/result")
	if fabric.ValidateID(tail) != nil || !strings.EqualFold(tail, id) {
		return fail("SDK operation Location did not match the verified operation UUID")
	}
	return nil
}

func oneHeader(headers http.Header, name string) (string, error) {
	var values []string
	for key, value := range headers {
		if strings.EqualFold(key, name) {
			values = append(values, value...)
		}
	}
	if len(values) > 1 {
		return "", fail("SDK response had ambiguous operation headers")
	}
	if len(values) == 0 {
		return "", nil
	}
	return values[0], nil
}

func (a *fixtureSDKTransport) authorize(req *http.Request) (string, string, time.Time, error) {
	if req == nil || req.URL == nil {
		return "", "", time.Time{}, fail("SDK supplied no request URL")
	}
	u := req.URL
	if u.Scheme != "https" || u.Host != "api.fabric.microsoft.com" || u.User != nil ||
		u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		u.RawFragment != "" || u.RawPath != "" || req.Host != "" && req.Host != u.Host {
		return "", "", time.Time{}, fail("SDK request was not the exact configured public origin")
	}
	for name, values := range req.Header {
		if (strings.EqualFold(name, "Authorization") || strings.EqualFold(name, "Proxy-Authorization")) &&
			strings.Join(values, "") != "" {
			return "", "", time.Time{}, fail("SDK must not supply a second authentication policy")
		}
	}
	if fabric.ValidateID(a.workspace) != nil {
		return "", "", time.Time{}, fail("SDK adapter has no valid selected workspace")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	base := "/v1/workspaces/" + a.workspace + "/notebooks"
	switch {
	case a.create && req.Method == http.MethodPost && u.Path == base:
		if a.mutationSent {
			return "", "", time.Time{}, fail("SDK fixture creation must not be retried")
		}
		a.mutationSent = true
		return "create", "", time.Time{}, nil
	case !a.create && req.Method == http.MethodDelete && fabric.ValidateID(a.notebook) == nil && u.Path == base+"/"+a.notebook:
		if a.mutationSent {
			return "", "", time.Time{}, fail("SDK fixture deletion must not be retried")
		}
		a.mutationSent = true
		return "delete", "", time.Time{}, nil
	case req.Method == http.MethodGet && a.operation != "":
		target := "/v1/operations/" + a.operation
		if u.Path == target {
			return "poll", a.operation, a.notBefore, nil
		}
		if a.create && u.Path == target+"/result" {
			return "result", a.operation, a.notBefore, nil
		}
	}
	return "", "", time.Time{}, fail("SDK request escaped its current verified fixture operation")
}

func waitUntil(ctx context.Context, until time.Time) error {
	delay := time.Until(until)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func boundedSDKBytes(reader io.Reader, limit int64) ([]byte, error) {
	if reader == nil {
		return nil, nil
	}
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fail("bounded SDK body read failed")
	}
	if int64(len(data)) > limit {
		return nil, fail("SDK fixture request or response exceeded its small byte bound")
	}
	return data, nil
}

// Do implements azcore's Transporter, not an authenticated SDK pipeline.
// Origin/method/operation validation precedes the only token-acquiring call.
func (a *fixtureSDKTransport) Do(req *http.Request) (*http.Response, error) {
	if req != nil && req.Body != nil {
		defer req.Body.Close()
	}
	kind, id, until, err := a.authorize(req)
	if err != nil {
		return nil, err
	}
	if err := waitUntil(req.Context(), until); err != nil {
		return nil, err
	}
	if req.ContentLength > sdkFixtureLimit {
		return nil, fail("SDK fixture request exceeded its small byte bound")
	}
	body, err := boundedSDKBytes(req.Body, sdkFixtureLimit)
	if err != nil {
		return nil, err
	}
	if kind != "create" && len(body) != 0 {
		return nil, fail("SDK poll, result, or deletion unexpectedly contained a body")
	}
	headers := make(http.Header)
	headers.Set("Accept", "application/json")
	if kind == "create" {
		headers.Set("Content-Type", "application/json")
	}
	resp, err := a.http.Request(req.Context(), req.Method, req.URL.String(), headers, body, req.Method == http.MethodGet)
	if err != nil {
		return nil, sanitizeSDKError(err)
	}
	limit := int64(sdkFixtureLimit)
	if kind == "poll" || kind == "delete" {
		limit = sdkPollLimit
	}
	data, readErr := boundedSDKBytes(resp.Body, limit)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fail("SDK fixture response could not be closed")
	}
	resp, err = a.prepareResponse(req, resp, data, kind, id)
	if err != nil {
		return nil, err
	}
	if kind == "delete" && resp.StatusCode == http.StatusAccepted {
		resp.Body.Close()
		if err := a.finishDelete(req.Context()); err != nil {
			return nil, err
		}
		// v0.20.0 DeleteNotebook declares only HTTP 200. Adapt 202 only
		// after its verified operation succeeded, and still require a
		// separate same-item 404 in the fixture's cleanup procedure.
		resp.StatusCode = http.StatusOK
		resp.Status = "200 OK"
		resp.Header.Del("Location")
		resp.Body = io.NopCloser(bytes.NewReader(nil))
		resp.ContentLength = 0
		resp.Header.Set("Content-Length", "0")
	} else if kind == "delete" && resp.StatusCode == http.StatusNoContent {
		resp.StatusCode = http.StatusOK
		resp.Status = "200 OK"
	}
	return resp, nil
}

func (a *fixtureSDKTransport) prepareResponse(req *http.Request, original *http.Response, data []byte, kind, currentID string) (*http.Response, error) {
	location, err := oneHeader(original.Header, "Location")
	if err != nil {
		return nil, err
	}
	headerID, err := oneHeader(original.Header, "x-ms-operation-id")
	if err != nil {
		return nil, err
	}
	headerID = strings.ToLower(strings.TrimSpace(headerID))
	if headerID != "" && fabric.ValidateID(headerID) != nil {
		return nil, fail("SDK response contained an invalid operation UUID")
	}
	if currentID != "" && headerID != "" && currentID != headerID {
		return nil, fail("SDK response changed the current verified operation UUID")
	}
	id := currentID
	initialAsync := (kind == "create" || kind == "delete") && original.StatusCode == http.StatusAccepted
	if initialAsync {
		if headerID == "" {
			return nil, fail("SDK 202 response lacked a verified operation UUID")
		}
		id = headerID
	}
	if location != "" {
		locationID := id
		if locationID == "" {
			locationID = headerID
		}
		if err := validateOperationLocation(location, locationID); err != nil {
			return nil, err
		}
	}
	status := ""
	switch kind {
	case "create":
		if original.StatusCode != http.StatusCreated && !initialAsync {
			return nil, fail("SDK notebook creation returned an unexpected success status")
		}
	case "delete":
		if original.StatusCode != http.StatusOK && original.StatusCode != http.StatusNoContent && !initialAsync {
			return nil, fail("SDK notebook deletion returned an unexpected success status")
		}
	case "poll":
		if original.StatusCode != http.StatusOK && original.StatusCode != http.StatusAccepted {
			return nil, fail("SDK operation polling returned an unexpected success status")
		}
		var wire struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(data, &wire) != nil {
			return nil, fail("SDK operation polling returned invalid small JSON")
		}
		status = wire.Status
		switch status {
		case "NotStarted", "Running", "Succeeded":
		case "Failed", "Canceled", "Cancelled":
			return nil, fail("fixture operation reported a terminal failure")
		default:
			return nil, fail("SDK operation polling returned an unknown status")
		}
		// The SDK needs only status, never untrusted service error details.
		data, _ = json.Marshal(wire)
	case "result":
		if original.StatusCode != http.StatusOK && original.StatusCode != http.StatusCreated {
			return nil, fail("SDK create result returned an unexpected success status")
		}
	}
	retry, err := oneHeader(original.Header, "Retry-After")
	if err != nil {
		return nil, err
	}
	var delay time.Duration
	if retry != "" {
		delay, err = transport.RetryAfter(retry, time.Now())
		if err != nil {
			return nil, fail("SDK response contained an invalid Retry-After header")
		}
	} else if initialAsync || status == "NotStarted" || status == "Running" {
		delay = time.Second
	}
	clean := make(http.Header)
	clean.Set("Content-Type", "application/json")
	clean.Set("Content-Length", strconv.Itoa(len(data)))
	if retry != "" {
		seconds := int64(delay / time.Second)
		if delay%time.Second != 0 {
			seconds++
		}
		clean.Set("Retry-After", strconv.FormatInt(seconds, 10))
	}
	a.mu.Lock()
	if initialAsync {
		if a.operation != "" && a.operation != id {
			a.mu.Unlock()
			return nil, fail("SDK attempted to replace its current operation")
		}
		a.operation = id
	}
	a.notBefore = time.Now().Add(delay)
	a.mu.Unlock()
	if initialAsync {
		if a.onID != nil {
			if err := a.onID(id); err != nil {
				return nil, err
			}
		}
	}
	if id != "" {
		clean.Set("x-ms-operation-id", id)
	}
	switch {
	case initialAsync:
		clean.Set("Location", operationURL(id, false))
	case kind == "poll" && status == "Succeeded" && a.create:
		// Some responses provide only the operation ID. The SDK otherwise
		// tries to decode the status document as an empty Notebook result.
		clean.Set("Location", operationURL(id, true))
	case kind == "poll":
		clean.Set("Location", operationURL(id, false))
	}
	copy := *original
	copy.Header = clean
	copy.Request = req.Clone(req.Context())
	copy.Request.Header = http.Header{"Accept": {"application/json"}}
	copy.Body = io.NopCloser(bytes.NewReader(data))
	copy.ContentLength = int64(len(data))
	return &copy, nil
}

func (a *fixtureSDKTransport) operationRequest(ctx context.Context, result bool) (*http.Response, error) {
	a.mu.Lock()
	id := a.operation
	a.mu.Unlock()
	if fabric.ValidateID(id) != nil || (result && !a.create) {
		return nil, fail("SDK adapter has no verified operation for this request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, operationURL(id, result), nil)
	if err != nil {
		return nil, fail("cannot construct the verified SDK operation request")
	}
	return a.Do(req)
}

func (a *fixtureSDKTransport) finishDelete(ctx context.Context) error {
	for {
		resp, err := a.operationRequest(ctx, false)
		if err != nil {
			return err
		}
		var wire struct {
			Status string `json:"status"`
		}
		err = decodeSmall(resp.Body, sdkPollLimit, &wire)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if wire.Status == "Succeeded" {
			return nil
		}
	}
}

// Reconciliation uses only guarded GETs for an ID already captured by the
// SDK's create adapter. It never reissues a create, constructs an SDK resume
// token, trusts a Location URL, or searches existing items by display name.
func (a *fixtureSDKTransport) reconcileCreate(ctx context.Context) (notebook.Notebook, error) {
	if !a.create {
		return notebook.Notebook{}, fail("deletion cannot retrieve a creation result")
	}
	for {
		resp, err := a.operationRequest(ctx, false)
		if err != nil {
			return notebook.Notebook{}, err
		}
		var wire struct {
			Status string `json:"status"`
		}
		err = decodeSmall(resp.Body, sdkPollLimit, &wire)
		resp.Body.Close()
		if err != nil {
			return notebook.Notebook{}, err
		}
		if wire.Status != "Succeeded" {
			continue
		}
		result, err := a.operationRequest(ctx, true)
		if err != nil {
			return notebook.Notebook{}, err
		}
		defer result.Body.Close()
		var item notebook.Notebook
		if err := decodeSmall(result.Body, sdkFixtureLimit, &item); err != nil {
			return notebook.Notebook{}, err
		}
		return item, nil
	}
}
