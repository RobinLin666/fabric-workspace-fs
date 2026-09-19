package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testScope = "https://api.fabric.microsoft.com/.default"
const testToken = "offline-sensitive-access-token"

type tokenFunc func(context.Context, string) (string, error)

func (f tokenFunc) Token(ctx context.Context, scope string) (string, error) {
	return f(ctx, scope)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func options(base string) Options {
	return Options{
		BaseURL: base, Scope: testScope, RetryDelay: time.Nanosecond,
		MaxRetryDelay: time.Second,
		Tokens:        tokenFunc(func(context.Context, string) (string, error) { return testToken, nil }),
	}
}

func newClient(t *testing.T, opts Options) *Client {
	t.Helper()
	client, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestNewOptions(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Options)
	}{
		{"relative origin", func(o *Options) { o.BaseURL = "/relative" }},
		{"missing host", func(o *Options) { o.BaseURL = "https:///path" }},
		{"unsupported scheme", func(o *Options) { o.BaseURL = "ftp://example.test" }},
		{"credentials", func(o *Options) { o.BaseURL = "https://user:secret@example.test" }},
		{"fragment", func(o *Options) { o.BaseURL = "https://example.test/#" }},
		{"query", func(o *Options) { o.BaseURL = "https://example.test/?secret=value" }},
		{"empty query", func(o *Options) { o.BaseURL = "https://example.test/?" }},
		{"traversal", func(o *Options) { o.BaseURL = "https://example.test/a/../b" }},
		{"missing tokens", func(o *Options) { o.Tokens = nil }},
		{"missing scope", func(o *Options) { o.Scope = "" }},
		{"negative retries", func(o *Options) { o.MaxRetries = -1 }},
		{"excessive retries", func(o *Options) { o.MaxRetries = 11 }},
		{"negative delay", func(o *Options) { o.RetryDelay = -1 }},
		{"negative ceiling", func(o *Options) { o.MaxRetryDelay = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := options("https://example.test")
			test.edit(&opts)
			if _, err := New(opts); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("New() error = %v", err)
			}
		})
	}
	opts := options("https://example.test/ignored-base-path")
	opts.RetryDelay, opts.MaxRetryDelay = 0, 0
	c := newClient(t, opts)
	if c.retryDelay != 250*time.Millisecond || c.maxRetryDelay != 30*time.Second || c.maxRetries != 0 {
		t.Fatalf("unexpected transport defaults")
	}
	if got := c.URL("/v1/workspaces", nil); got != "https://example.test/v1/workspaces" {
		t.Fatal(got)
	}
}

func TestURLPreservesEscapedSegmentsAndQuery(t *testing.T) {
	c := newClient(t, options("https://example.test:443/base"))
	path := "/workspace/item/Files/" + url.PathEscape("sp ace+#%雪.txt")
	query := url.Values{"continuationToken": {"a+/==&?secret#%"}, "recursive": {"true"}}
	got := c.URL(path, query)
	u, err := url.Parse(got)
	if err != nil || u.Path != "/workspace/item/Files/sp ace+#%雪.txt" || !reflect.DeepEqual(u.Query(), query) {
		t.Fatalf("incorrect escaped URL: %q (%v)", got, err)
	}
	if u.EscapedPath() != path {
		t.Fatalf("path double encoded: %q, expected %q", u.EscapedPath(), path)
	}
	if resolved, err := c.Resolve(got); err != nil || resolved != got {
		t.Fatalf("Resolve valid escaped URL: %q, %v", resolved, err)
	}
	for _, path := range []string{"/a?query=not-a-path", "/a#fragment", "/a/../b", "/a/%2e%2e/b", "//elsewhere/path", "/invalid%xx"} {
		if got := c.URL(path, nil); got != "" {
			t.Errorf("URL(%q) = %q", path, got)
		}
	}
	for _, target := range []string{"/ok", "ok", "https://EXAMPLE.test:443/ok", "//example.test:443/ok"} {
		got, err := c.Resolve(target)
		if err != nil || got != "https://example.test:443/ok" {
			t.Errorf("Resolve(%q) = %q, %v", target, got, err)
		}
	}
}

func TestRequestRejectsUnsafeTargetsBeforeCredentials(t *testing.T) {
	var tokens, requests atomic.Int64
	opts := options("https://example.test:443")
	opts.Tokens = tokenFunc(func(context.Context, string) (string, error) {
		tokens.Add(1)
		return testToken, nil
	})
	opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return nil, errors.New("unexpected network call")
	})}
	c := newClient(t, opts)
	for _, target := range []string{
		"", "https://evil.test/steal", "//evil.test/steal", "http://example.test:443/downgrade",
		"https://example.test:444/port", "https://example.test/no-explicit-port",
		"https://user:secret@example.test:443/path", "/path#fragment", "/path#",
		"/a/../b", "../b", "/%2e%2e/b", "/a%2f..%2fb",
		"/a%5cb", "/a\\b", "/%00", "/bad%xx", "/ok?token=%xx",
		" /path", "/path ", "/path\nsecret", "javascript:steal()",
	} {
		t.Run(target, func(t *testing.T) {
			if _, err := c.Resolve(target); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("Resolve() error = %v", err)
			}
			if _, err := c.Request(context.Background(), http.MethodGet, target, nil, nil, true); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("Request() error = %v", err)
			}
		})
	}
	if tokens.Load() != 0 || requests.Load() != 0 {
		t.Fatalf("unsafe request reached auth/network: %d/%d", tokens.Load(), requests.Load())
	}
}

func TestRequestReplayHeadersAndTokenRenewal(t *testing.T) {
	var mu sync.Mutex
	var requests, scopes []string
	var tokenCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, string(data))
		if r.Method != http.MethodPost || r.URL.RequestURI() != "/read?format=ipynb" {
			t.Errorf("unexpected method/target %s %s", r.Method, r.URL)
		}
		if got := r.Header.Values("Authorization"); len(got) != 1 || got[0] != fmt.Sprintf("Bearer token-%d", len(requests)) {
			t.Errorf("Authorization = %v", got)
		}
		if r.Header.Get("X-Test") != "keep-me" {
			t.Error("caller header lost")
		}
		if len(requests) < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"errorCode":"Unavailable"}`)
			return
		}
		fmt.Fprint(w, "caller-readable")
	}))
	defer server.Close()
	opts := options(server.URL)
	opts.MaxRetries = 2
	opts.Tokens = tokenFunc(func(_ context.Context, scope string) (string, error) {
		mu.Lock()
		scopes = append(scopes, scope)
		mu.Unlock()
		return fmt.Sprintf("token-%d", tokenCalls.Add(1)), nil
	})
	c := newClient(t, opts)
	headers := http.Header{"authorization": {"caller-secret"}, "X-Test": {"keep-me"}}
	resp, err := c.Request(context.Background(), http.MethodPost, "/read?format=ipynb", headers, []byte("unchanged request body"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "caller-readable" {
		t.Fatalf("successful body prematurely closed: %q, %v", body, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(requests, []string{"unchanged request body", "unchanged request body", "unchanged request body"}) {
		t.Fatal(requests)
	}
	if !reflect.DeepEqual(scopes, []string{testScope, testScope, testScope}) {
		t.Fatal(scopes)
	}
	if headers["authorization"][0] != "caller-secret" || len(headers) != 2 {
		t.Fatal("Request mutated caller headers")
	}
}

func TestRetryStatusPolicy(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 408, 409, 412, 416, 429, 500, 502, 503, 504} {
		for _, safe := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/safe=%v", status, safe), func(t *testing.T) {
				var calls atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(status)
				}))
				defer server.Close()
				opts := options(server.URL)
				opts.MaxRetries = 2
				c := newClient(t, opts)
				_, err := c.Request(context.Background(), http.MethodPut, "/write", nil, []byte("content"), safe)
				var httpErr *HTTPError
				if !errors.As(err, &httpErr) || httpErr.StatusCode != status {
					t.Fatalf("status not preserved: %v", err)
				}
				want := int64(1)
				if safe && (status == 408 || status == 429 || status >= 500) {
					want = 3
				}
				if got := calls.Load(); got != want {
					t.Fatalf("%d requests, expected %d", got, want)
				}
			})
		}
	}
}

func TestZeroRetriesMeansOneAttempt(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	c := newClient(t, options(server.URL))
	if _, err := c.Request(context.Background(), http.MethodGet, "/", nil, nil, true); err == nil || calls.Load() != 1 {
		t.Fatalf("zero retries: calls=%d, error=%v", calls.Load(), err)
	}
}

func TestRetryAfterCeilingNeverRetriesEarly(t *testing.T) {
	for _, header := range []string{"60", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat), "99999999999999999999", "bad", "-1"} {
		t.Run(header, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", header)
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer server.Close()
			opts := options(server.URL)
			opts.MaxRetries = 3
			opts.MaxRetryDelay = time.Millisecond
			c := newClient(t, opts)
			_, err := c.Request(context.Background(), http.MethodGet, "/", nil, nil, true)
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != 429 || calls.Load() != 1 {
				t.Fatalf("ceiling ignored: calls=%d, error=%v", calls.Load(), err)
			}
		})
	}
}

func TestRetryAfterSecondsIsObserved(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	opts := options(server.URL)
	opts.MaxRetries = 1
	opts.MaxRetryDelay = 2 * time.Second
	c := newClient(t, opts)
	start := time.Now()
	resp, err := c.Request(context.Background(), http.MethodGet, "/", nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if time.Since(start) < time.Second || calls.Load() != 2 {
		t.Fatal("retried before the service's Retry-After")
	}
}

func TestRetryDelayHonorsCancellation(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	opts := options(server.URL)
	opts.MaxRetries, opts.MaxRetryDelay = 3, time.Minute
	c := newClient(t, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.Request(ctx, http.MethodGet, "/", nil, nil, true)
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("delay not cancelled: calls=%d error=%v", calls.Load(), err)
	}
}

func TestRedirectsNeverFollowOrMutateHTTPClient(t *testing.T) {
	var foreignCalls, tokenCalls atomic.Int64
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		foreignCalls.Add(1)
	}))
	defer foreign.Close()
	for _, status := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", foreign.URL+"/steal?secret=redirect")
				w.WriteHeader(status)
			}))
			defer server.Close()
			sentinel := errors.New("original redirect handler")
			original := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return sentinel }}
			opts := options(server.URL)
			opts.HTTPClient, opts.MaxRetries = original, 3
			opts.Tokens = tokenFunc(func(context.Context, string) (string, error) {
				tokenCalls.Add(1)
				return testToken, nil
			})
			c := newClient(t, opts)
			before := tokenCalls.Load()
			_, err := c.Request(context.Background(), http.MethodGet, "/", nil, nil, true)
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != status || tokenCalls.Load() != before+1 {
				t.Fatalf("unexpected redirect behavior: %v", err)
			}
			if !errors.Is(original.CheckRedirect(nil, nil), sentinel) {
				t.Error("caller's HTTP client was mutated")
			}
		})
	}
	if foreignCalls.Load() != 0 {
		t.Fatal("foreign origin received a request")
	}
}

func TestRequestValidationAndAuthenticationFailures(t *testing.T) {
	var calls, tokens atomic.Int64
	opts := options("https://example.test")
	opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected call")
	})}
	opts.Tokens = tokenFunc(func(context.Context, string) (string, error) {
		tokens.Add(1)
		return testToken, nil
	})
	c := newClient(t, opts)
	for _, tc := range []struct {
		method  string
		headers http.Header
	}{
		{"bad\nmethod", nil},
		{"GET", http.Header{"Bad Name": {"value"}}},
		{"GET", http.Header{"X-Test": {"bad\r\nAuthorization: secret"}}},
		{"GET", http.Header{"X-Test": {"bad\x00value"}}},
	} {
		if _, err := c.Request(context.Background(), tc.method, "/", tc.headers, nil, false); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("invalid request error = %v", err)
		}
	}
	if _, err := c.Request(nil, "GET", "/", nil, nil, false); !errors.Is(err, fs.ErrInvalid) {
		t.Error(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Request(ctx, "GET", "/", nil, nil, true); !errors.Is(err, context.Canceled) {
		t.Error(err)
	}
	if tokens.Load() != 0 || calls.Load() != 0 {
		t.Fatal("invalid request reached authentication or network")
	}
	for _, token := range []string{"", "bad token", "bad\r\nheader", "bad\x00token"} {
		opts.Tokens = tokenFunc(func(context.Context, string) (string, error) { return token, nil })
		c = newClient(t, opts)
		_, err := c.Request(context.Background(), "GET", "/", nil, nil, false)
		if err == nil || (token != "" && strings.Contains(err.Error(), token)) {
			t.Errorf("unsafe token handling: %v", err)
		}
	}
	cause := errors.New("token backend leaked " + testToken)
	opts.Tokens = tokenFunc(func(context.Context, string) (string, error) { return "", cause })
	c = newClient(t, opts)
	_, err := c.Request(context.Background(), "GET", "/?secret=query-secret", nil, nil, true)
	if !errors.Is(err, cause) || strings.Contains(err.Error(), testToken) || calls.Load() != 0 {
		t.Fatalf("unsafe authentication error: %v", err)
	}
}

func TestNetworkRetriesAndSanitizedErrors(t *testing.T) {
	for _, safe := range []bool{false, true} {
		t.Run(fmt.Sprint(safe), func(t *testing.T) {
			var calls atomic.Int64
			cause := errors.New("network " + testToken + " body-secret")
			opts := options("https://example.test")
			opts.MaxRetries = 2
			opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, cause
			})}
			c := newClient(t, opts)
			_, err := c.Request(context.Background(), "PUT", "/file?sig=query-secret", nil, []byte("body-secret"), safe)
			want := int64(1)
			if safe {
				want = 3
			}
			if !errors.Is(err, cause) || calls.Load() != want {
				t.Fatalf("network retry: calls=%d error=%v", calls.Load(), err)
			}
			for _, secret := range []string{testToken, "body-secret", "query-secret", "sig="} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error leaked %q", secret)
				}
			}
		})
	}
}

func TestContextErrorsFromProvidersAreNotRetried(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, fromToken := range []bool{false, true} {
			var calls atomic.Int64
			opts := options("https://example.test")
			opts.MaxRetries = 3
			if fromToken {
				opts.Tokens = tokenFunc(func(context.Context, string) (string, error) {
					calls.Add(1)
					return "", fmt.Errorf("provider: %w", cause)
				})
			} else {
				opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls.Add(1)
					return nil, fmt.Errorf("network: %w", cause)
				})}
			}
			c := newClient(t, opts)
			_, err := c.Request(context.Background(), "GET", "/", nil, nil, true)
			if !errors.Is(err, cause) || calls.Load() != 1 {
				t.Fatalf("context error retried: %v (%d calls)", err, calls.Load())
			}
		}
	}
}
