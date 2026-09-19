package transport

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPErrorSchemasAndSecretRedaction(t *testing.T) {
	const requestID = "c743f6bd-7c61-4925-99b7-d7085662b6a2"
	tests := []struct {
		name    string
		body    string
		headers http.Header
		code    string
		id      string
	}{
		{"Fabric", `{"errorCode":"ItemNotFound","message":"Bearer ` + testToken + ` body-secret query-secret","requestId":"` + requestID + `"}`, nil, "ItemNotFound", requestID},
		{"nested Azure", `{"error":{"code":"PathNotFound","message":"body-secret","requestId":"` + requestID + `"}}`, nil, "PathNotFound", requestID},
		{"storage XML", `<Error><Code>PathNotFound</Code><Message>body-secret</Message></Error>`, http.Header{"X-Ms-Request-Id": {requestID}}, "PathNotFound", requestID},
		{"header precedence", `{"errorCode":"BodyCode","requestId":"body-id"}`, http.Header{"X-Ms-Error-Code": {"ConditionNotMet"}, "X-Ms-Request-Id": {requestID}}, "ConditionNotMet", requestID},
		{"HTML", `<html>Bearer ` + testToken + ` body-secret query-secret</html>`, nil, "", ""},
		{"bad JSON", `{"errorCode":`, nil, "", ""},
		{"reflected token", `{"errorCode":"` + testToken + `","requestId":"` + testToken + `"}`, nil, "", ""},
		{"reflected query", `{"errorCode":"query-secret","requestId":"query-secret"}`, nil, "", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for key, values := range test.headers {
					w.Header()[key] = values
				}
				w.WriteHeader(http.StatusForbidden)
				io.WriteString(w, test.body)
			}))
			defer server.Close()
			c := newClient(t, options(server.URL))
			_, err := c.Request(context.Background(), "GET", "/v1/workspaces?continuationToken=query-secret", nil, nil, false)
			var got *HTTPError
			if !errors.As(err, &got) || got.StatusCode != 403 || got.Code != test.code || got.RequestID != test.id ||
				got.Method != "GET" || got.Path != "/v1/workspaces" || got.Message != "Forbidden" {
				t.Fatalf("unexpected HTTP error: %#v", got)
			}
			for _, value := range []string{err.Error(), got.Message, got.Code, got.RequestID, got.Path} {
				for _, secret := range []string{testToken, "query-secret", "body-secret", "Bearer", "continuationToken"} {
					if strings.Contains(value, secret) {
						t.Errorf("error field leaked %q", secret)
					}
				}
			}
		})
	}
	err := &HTTPError{StatusCode: 500, Method: "GET", Path: "/path?token=secret", Code: "body-secret", Message: "token-secret", RequestID: "request-secret"}
	if got := err.Error(); strings.Contains(got, "secret") || got != "GET /path: HTTP 500 Internal Server Error" {
		t.Fatal(got)
	}
}

type trackedBody struct {
	data   io.Reader
	read   int
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.data.Read(p)
	b.read += n
	return n, err
}
func (b *trackedBody) Close() error { b.closed = true; return nil }

func TestErrorReadsAreBoundedAndBodiesClosed(t *testing.T) {
	body := &trackedBody{data: strings.NewReader(strings.Repeat("secret", maxErrorBytes))}
	opts := options("https://example.test")
	opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Header: make(http.Header), Body: body, Request: r}, nil
	})}
	c := newClient(t, opts)
	if _, err := c.Request(context.Background(), "GET", "/", nil, nil, false); err == nil {
		t.Fatal("expected error")
	}
	if body.read > maxErrorBytes || !body.closed {
		t.Fatalf("body read %d bytes, closed=%v", body.read, body.closed)
	}
	body = &trackedBody{data: strings.NewReader("success")}
	opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, Request: r}, nil
	})}
	c = newClient(t, opts)
	resp, err := c.Request(context.Background(), "GET", "/", nil, nil, false)
	if err != nil || body.read != 0 || body.closed {
		t.Fatalf("success body ownership lost: %v", err)
	}
	resp.Body.Close()
	if !body.closed {
		t.Fatal("caller could not close successful body")
	}
}

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		header string
		want   time.Duration
		valid  bool
	}{
		{"0", 0, true},
		{" 15 ", 15 * time.Second, true},
		{now.Add(45 * time.Second).Format(http.TimeFormat), 45 * time.Second, true},
		{now.Add(-time.Hour).Format(http.TimeFormat), 0, true},
		{"", 0, false},
		{"-1", 0, false},
		{"+1", 0, false},
		{"1.5", 0, false},
		{"tomorrow", 0, false},
		{"9223372036854775807", 0, false},
		{"999999999999999999999999999999999", 0, false},
	} {
		got, err := RetryAfter(test.header, now)
		if got != test.want || (err == nil) != test.valid {
			t.Errorf("RetryAfter(%q) = %s, %v", test.header, got, err)
		}
	}
	c := newClient(t, options("https://example.test"))
	c.retryDelay, c.maxRetryDelay = time.Second, 3*time.Second
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second} {
		if got, ok := c.retryWait(attempt, ""); !ok || got != want {
			t.Errorf("backoff %d = %v %v", attempt, got, ok)
		}
	}
}
