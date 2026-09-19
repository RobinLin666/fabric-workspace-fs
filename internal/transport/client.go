// Package transport provides origin-bound, authenticated HTTP requests.
package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type TokenSource interface {
	Token(context.Context, string) (string, error)
}

type Options struct {
	BaseURL             string
	Scope               string
	Tokens              TokenSource
	HTTPClient          *http.Client
	MaxRetries          int
	RetryDelay          time.Duration
	MaxRetryDelay       time.Duration
	AuthorizationScheme string
}

type Client struct {
	base                *url.URL
	scope               string
	tokens              TokenSource
	http                *http.Client
	maxRetries          int
	retryDelay          time.Duration
	maxRetryDelay       time.Duration
	authorizationScheme string
}

// New binds credentials to a single origin, including its explicit port.
// Zero MaxRetries disables retries. Zero delays use 250ms and 30s respectively.
// The supplied HTTP client is copied; redirects are disabled on the copy.
func New(opts Options) (*Client, error) {
	base, err := url.Parse(opts.BaseURL)
	if err != nil || base == nil || !base.IsAbs() || base.Hostname() == "" ||
		(base.Scheme != "https" && base.Scheme != "http") || base.User != nil ||
		base.Opaque != "" || base.RawQuery != "" || base.ForceQuery ||
		strings.Contains(opts.BaseURL, "#") || unsafePath(base.Path) ||
		hasControl(opts.BaseURL) || strings.Contains(opts.BaseURL, "\\") {
		return nil, fmt.Errorf("invalid HTTP base URL: %w", fs.ErrInvalid)
	}
	if opts.Tokens == nil || strings.TrimSpace(opts.Scope) == "" || hasControl(opts.Scope) ||
		opts.MaxRetries < 0 || opts.MaxRetries > 10 || opts.RetryDelay < 0 || opts.MaxRetryDelay < 0 {
		return nil, fmt.Errorf("invalid HTTP transport options: %w", fs.ErrInvalid)
	}
	if opts.AuthorizationScheme == "" {
		opts.AuthorizationScheme = "Bearer"
	}
	if opts.AuthorizationScheme != "Bearer" && opts.AuthorizationScheme != "MwcToken" {
		return nil, fmt.Errorf("unsupported authorization scheme: %w", fs.ErrInvalid)
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = 250 * time.Millisecond
	}
	if opts.MaxRetryDelay == 0 {
		opts.MaxRetryDelay = 30 * time.Second
	}
	base.Path, base.RawPath = "/", ""
	client := http.Client{Timeout: time.Minute}
	if opts.HTTPClient != nil {
		client = *opts.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{
		base: base, scope: opts.Scope, tokens: opts.Tokens, http: &client,
		maxRetries: opts.MaxRetries, retryDelay: opts.RetryDelay, maxRetryDelay: opts.MaxRetryDelay,
		authorizationScheme: opts.AuthorizationScheme,
	}, nil
}

// URL builds a root-relative URL from an already escaped path. Callers must
// escape individual path segments, not a whole path containing separators.
// Invalid paths produce an empty target, which Request rejects before auth.
func (c *Client) URL(path string, query url.Values) string {
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u, err := url.Parse(path)
	if err != nil || u.Host != "" || u.Scheme != "" || u.RawQuery != "" || u.ForceQuery ||
		strings.Contains(path, "#") || strings.HasPrefix(path, "//") || unsafePath(u.Path) {
		return ""
	}
	u.Scheme, u.Host = c.base.Scheme, c.base.Host
	u.RawQuery = query.Encode()
	return u.String()
}

// Resolve accepts absolute or origin-root-relative URLs, never another origin,
// user information, fragments, opaque URLs, or path traversal (including escaped
// traversal). URL validation always happens before credential retrieval.
func (c *Client) Resolve(target string) (string, error) {
	invalid := func() (string, error) {
		return "", fmt.Errorf("invalid or foreign HTTP target: %w", fs.ErrInvalid)
	}
	if target == "" || target != strings.TrimSpace(target) || hasControl(target) ||
		strings.ContainsAny(target, "\\#") {
		return invalid()
	}
	ref, err := url.Parse(target)
	if err != nil || ref.User != nil || ref.Opaque != "" || unsafePath(ref.Path) {
		return invalid()
	}
	if _, err := url.QueryUnescape(ref.RawQuery); err != nil {
		return invalid()
	}
	u := c.base.ResolveReference(ref)
	if !strings.EqualFold(u.Scheme, c.base.Scheme) || !strings.EqualFold(u.Host, c.base.Host) {
		return invalid()
	}
	u.Scheme, u.Host = c.base.Scheme, c.base.Host
	return u.String(), nil
}

func unsafePath(path string) bool {
	// url.Parse already decodes URL.Path once. Decoding again would turn the
	// perfectly valid literal filename "%2e%2e" into a traversal component.
	if hasControl(path) || strings.Contains(path, "\\") || strings.HasPrefix(path, "//") {
		return true
	}
	for _, part := range strings.Split(path, "/") {
		if part == "." || part == ".." {
			return true
		}
	}
	return false
}

func hasControl(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// Request returns only 2xx responses. The caller owns successful response bodies.
// All other bodies are read with a fixed bound and closed. Retries require an
// explicit safeRetry and are never inferred from a method or an error code.
func (c *Client) Request(ctx context.Context, method, target string, headers http.Header, body []byte, safeRetry bool) (*http.Response, error) {
	resolved, err := c.Resolve(target)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, fmt.Errorf("missing request context: %w", fs.ErrInvalid)
	}
	cleanHeaders, err := requestHeaders(headers)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, resolved, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP request: %w", fs.ErrInvalid)
		}
		req.Header = cleanHeaders.Clone()
		token, err := c.tokens.Token(ctx, c.scope)
		if err != nil {
			return nil, contextOrSafeError(ctx, "access token acquisition failed", err)
		}
		if token == "" || strings.ContainsAny(token, " \t\r\n") || hasControl(token) {
			return nil, errors.New("identity provider returned an invalid access token")
		}
		req.Header.Set("Authorization", c.authorizationScheme+" "+token)
		resp, requestErr := c.http.Do(req)
		var resultErr error
		var retryAfter string
		var retryable bool
		if requestErr != nil {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			resultErr = contextOrSafeError(ctx, "HTTP request failed", requestErr)
			if errors.Is(resultErr, context.Canceled) || errors.Is(resultErr, context.DeadlineExceeded) {
				return nil, resultErr
			}
			retryable = true
		} else {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, nil
			}
			retryAfter = resp.Header.Get("Retry-After")
			retryable = resp.StatusCode == http.StatusTooManyRequests ||
				resp.StatusCode == http.StatusRequestTimeout ||
				(resp.StatusCode >= 500 && resp.StatusCode <= 599)
			resultErr = responseError(resp, req, token)
		}
		if !safeRetry || !retryable || attempt >= c.maxRetries {
			return nil, resultErr
		}
		delay, ok := c.retryWait(attempt, retryAfter)
		if !ok {
			return nil, resultErr
		}
		if err := wait(ctx, delay); err != nil {
			return nil, err
		}
	}
}

func requestHeaders(headers http.Header) (http.Header, error) {
	clean := make(http.Header, len(headers))
	for name, values := range headers {
		if !validHeaderName(name) {
			return nil, fmt.Errorf("invalid HTTP header: %w", fs.ErrInvalid)
		}
		if strings.EqualFold(name, "Authorization") || strings.EqualFold(name, "Proxy-Authorization") {
			continue
		}
		for _, value := range values {
			for _, r := range value {
				if r == 0x7f || (r < 0x20 && r != '\t') {
					return nil, fmt.Errorf("invalid HTTP header: %w", fs.ErrInvalid)
				}
			}
			clean.Add(name, value)
		}
	}
	return clean, nil
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", r)) {
			return false
		}
	}
	return true
}

type safeError struct {
	message string
	cause   error
}

func (e *safeError) Error() string { return e.message }
func (e *safeError) Unwrap() error { return e.cause }

func contextOrSafeError(ctx context.Context, message string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return &safeError{message: message, cause: err}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func boundedErrorBody(body io.ReadCloser) []byte {
	if body == nil {
		return nil
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxErrorBytes))
	if err != nil {
		return nil
	}
	return data
}
