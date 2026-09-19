// Package mwc implements the private Notebook and read-only Environment resource
// APIs. Its credentials and caches are instance-local and never persisted.
//
// Writes use a fresh read/compare followed by a single mutation request. The
// service has no verified conditional-write contract: another writer can change
// an object between the comparison and mutation. WriteGuarantee reports this
// limitation rather than treating an ETag or content hash as server-side CAS.
package mwc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fabric-workspace-fs/internal/cache"
	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/transport"
)

const (
	DefaultMaxFileSize = 16 << 20
	powerBIScope       = "https://analysis.windows.net/powerbi/api/.default"
	resourceScope      = "mwc-resource"
	maxControlBytes    = 256 << 10
	maxListingBytes    = 4 << 20
	maxListingEntries  = 10000
)

type Options struct {
	FabricOrigin string
	Tokens       transport.TokenSource
	Workspaces   interface {
		GetWorkspace(context.Context, string) (fabric.Workspace, error)
	}
	HTTPClient   *http.Client
	MaxFileSize  int64
	CacheTTL     time.Duration
	CachePolicy  *cachepolicy.Policy
	ResolveRoots func(context.Context, resources.Target) ([]resources.Root, error)
}

type Client struct {
	origin     string
	tokens     transport.TokenSource
	workspaces interface {
		GetWorkspace(context.Context, string) (fabric.Workspace, error)
	}
	httpClient   *http.Client
	maxFileSize  int64
	cachePolicy  *cachepolicy.Policy
	resolveRoots func(context.Context, resources.Target) ([]resources.Root, error)
	now          func() time.Time
	clusters     *cache.Cache[string]
	capacities   *cache.Cache[fabric.Workspace]
	listings     *cache.Cache[directoryListing]
	contents     *cache.Cache[fileContent]
	metadata     *cache.Cache[fileMetadata]
	grants       grantCache
}

var _ resources.Backend = (*Client)(nil)

// New performs no network or credential operations. Without CachePolicy,
// CacheTTL applies uniformly for compatibility; zero disables retention, not
// concurrent load sharing. MWC grants always use their actual expiry,
// independently of either cache option.
func New(opts Options) (*Client, error) {
	if opts.FabricOrigin == "" {
		opts.FabricOrigin = fabric.BaseURL
	}
	origin, err := strictOrigin(opts.FabricOrigin, false)
	if err != nil {
		return nil, err
	}
	if opts.Tokens == nil || opts.Workspaces == nil || opts.CacheTTL < 0 ||
		opts.MaxFileSize < 0 || opts.MaxFileSize == math.MaxInt64 {
		return nil, fmt.Errorf("invalid MWC options: %w", fs.ErrInvalid)
	}
	if opts.MaxFileSize == 0 {
		opts.MaxFileSize = DefaultMaxFileSize
	}
	policy := opts.CachePolicy
	if policy == nil {
		policy, err = cachepolicy.Uniform(opts.CacheTTL)
		if err != nil {
			return nil, err
		}
	}
	c := &Client{
		origin: origin, tokens: opts.Tokens, workspaces: opts.Workspaces,
		maxFileSize: opts.MaxFileSize, cachePolicy: policy,
		resolveRoots: opts.ResolveRoots, now: time.Now,
	}
	if opts.HTTPClient != nil {
		copied := *opts.HTTPClient
		c.httpClient = &copied
	}
	clock := func() time.Time { return c.now() }
	defaults := policy.Resolve(cachepolicy.Selector{})
	c.clusters = cache.New(cache.Options[string]{TTL: defaults.Catalog, MaxEntries: 128, Now: clock})
	c.capacities = cache.New(cache.Options[fabric.Workspace]{TTL: defaults.Catalog, MaxEntries: 128, Now: clock})
	c.listings = cache.New(cache.Options[directoryListing]{
		TTL: defaults.Directory, MaxEntries: 256, MaxBytes: 32 << 20, Now: clock,
		ObservedAt: func(listing directoryListing) time.Time { return listing.observedAt },
		Size: func(listing directoryListing) int64 {
			var size int64
			for _, e := range listing.entries {
				size += int64(192 + len(e.info.Path) + len(e.info.MetadataVersion))
			}
			return size + 32
		},
	})
	c.contents = cache.New(cache.Options[fileContent]{
		TTL: defaults.Content, MaxEntries: 128, MaxBytes: 64 << 20, Now: clock,
		ObservedAt: func(f fileContent) time.Time { return f.observedAt },
		Size:       func(f fileContent) int64 { return int64(len(f.bytes)) + 160 },
	})
	c.metadata = cache.New(cache.Options[fileMetadata]{
		TTL: defaults.Attr, MaxEntries: 256, MaxBytes: 1 << 20, Now: clock,
		ObservedAt: func(metadata fileMetadata) time.Time { return metadata.observedAt },
		Size: func(metadata fileMetadata) int64 {
			return int64(128 + len(metadata.lastModified) + len(metadata.version))
		},
	})
	return c, nil
}

func (c *Client) WriteGuarantee() resources.WriteGuarantee { return resources.CompareThenWrite }

// Roots returns local presentation names, not remote directory components.
// Both Notebook builtin and Environment resources map to their workdir root.
func (c *Client) Roots(ctx context.Context, target resources.Target) ([]resources.Root, error) {
	if err := ready(ctx); err != nil {
		return nil, err
	}
	if err := resources.ValidateTarget(target); err != nil {
		return nil, err
	}
	var roots []resources.Root
	if c.resolveRoots != nil {
		var err error
		roots, err = c.resolveRoots(ctx, target)
		if err != nil {
			return nil, safeError(ctx, "resource root discovery failed", err)
		}
	} else {
		name := "builtin"
		if target.Kind == "Environment" {
			name = "resources"
		}
		roots = []resources.Root{{Name: name, Target: target}}
	}
	if err := resources.ValidateRoots(target, roots); err != nil {
		return nil, err
	}
	return append([]resources.Root(nil), roots...), ctx.Err()
}

func ready(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("missing MWC context: %w", fs.ErrInvalid)
	}
	return ctx.Err()
}

func validatePath(ctx context.Context, path resources.Path, write bool) error {
	if err := ready(ctx); err != nil {
		return err
	}
	if err := resources.ValidatePath(path, write); err != nil {
		return err
	}
	// transport rejects controls too; fail before requesting any credentials.
	for _, r := range path.Relative {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("invalid resource path: %w", fs.ErrInvalid)
		}
	}
	return nil
}

// A discovered hostname is a grant from the preceding authenticated origin, not
// from a domain suffix allowlist. Accept no URL features that can change origin
// interpretation, nor IP literals or numeric IP aliases.
func strictOrigin(raw string, allowHostname bool) (string, error) {
	invalid := func() (string, error) {
		return "", fmt.Errorf("invalid MWC HTTPS origin: %w", fs.ErrInvalid)
	}
	if raw == "" || raw != strings.TrimSpace(raw) || strings.ContainsAny(raw, "\\#") {
		return invalid()
	}
	for _, r := range raw {
		if r < 0x21 || r > 0x7e {
			return invalid()
		}
	}
	if allowHostname && !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Opaque != "" ||
		u.Host == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" ||
		(u.Port() != "" && u.Port() != "443") {
		return invalid()
	}
	host := strings.ToLower(u.Hostname())
	if net.ParseIP(host) != nil || !dnsName(host) {
		return invalid()
	}
	if u.Host != u.Hostname() && u.Host != u.Hostname()+":443" {
		return invalid()
	}
	// The default HTTPS port is canonicalized so aliases cannot split bindings.
	return "https://" + host, nil
}

func dnsName(host string) bool {
	if len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '-' {
				return false
			}
		}
	}
	for _, r := range labels[len(labels)-1] {
		if r >= 'a' && r <= 'z' {
			return true
		}
	}
	return false
}

func (c *Client) boundHTTP(origin, scope, scheme, token string, expiry time.Time) (*transport.Client, error) {
	return transport.New(transport.Options{
		BaseURL: origin, Scope: scope, AuthorizationScheme: scheme,
		Tokens:     capturedToken{value: token, scope: scope, expiry: expiry, now: c.now},
		HTTPClient: c.httpClient, MaxRetries: 2,
	})
}

func readBody(ctx context.Context, resp *http.Response, limit int64) ([]byte, error) {
	defer resp.Body.Close()
	if resp.ContentLength > limit {
		return nil, fserrors.ErrTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, safeError(ctx, "MWC response could not be read", err)
	}
	if int64(len(body)) > limit {
		return nil, fserrors.ErrTooLarge
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return body, nil
}

func requireOK(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return invalidResponse("unexpected success status")
	}
	return nil
}

func invalidResponse(detail string) error {
	return fmt.Errorf("invalid MWC response (%s): %w", detail, fs.ErrInvalid)
}

type hiddenError struct {
	message string
	cause   error
}

func (e *hiddenError) Error() string { return e.message }
func (e *hiddenError) Unwrap() error { return e.cause }

type lookupError struct {
	cause      error
	validUntil time.Time
}

func (e *lookupError) Error() string            { return e.cause.Error() }
func (e *lookupError) Unwrap() error            { return e.cause }
func (e *lookupError) CacheDeadline() time.Time { return e.validUntil }

// Namespace callers can use errors.As with interface{ CacheDeadline() time.Time }
// to bound negative caching without depending on the private lookup error type.
// Missing proof uses a due deadline, never the zero "unspecified" timestamp.
func (c *Client) lookupFailure(err error, maxAge time.Duration, sourceDeadline time.Time) error {
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	until := c.now()
	var source interface{ CacheDeadline() time.Time }
	var remote *transport.HTTPError
	if errors.As(err, &source) {
		if deadline := source.CacheDeadline(); !deadline.IsZero() {
			until = deadline
		}
	} else if errors.As(err, &remote) && remote.StatusCode == http.StatusNotFound {
		until = until.Add(maxAge)
	}
	if !sourceDeadline.IsZero() && sourceDeadline.Before(until) {
		until = sourceDeadline
	}
	return &lookupError{cause: err, validUntil: until}
}

func safeError(ctx context.Context, message string, cause error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(cause, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return &hiddenError{message: message, cause: cause}
}

func resourceError(err error) error {
	var remote *transport.HTTPError
	if errors.As(err, &remote) {
		switch remote.StatusCode {
		case http.StatusNotFound:
			return errors.Join(fs.ErrNotExist, err)
		case http.StatusUnauthorized, http.StatusForbidden:
			return errors.Join(fs.ErrPermission, err)
		case http.StatusPreconditionFailed:
			return errors.Join(fserrors.ErrConflict, err)
		case http.StatusConflict:
			if strings.EqualFold(remote.Code, "DirectoryNotEmpty") {
				return errors.Join(fserrors.ErrNotEmpty, err)
			}
		}
	}
	return err
}

func hasHTTPError(err error) bool {
	var remote *transport.HTTPError
	return errors.As(err, &remote)
}

func versionFor(data []byte) string {
	hash := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(hash[:])
}

func targetPrefix(target resources.Target) string {
	return strings.ToLower(target.WorkspaceID) + "/" + strings.ToLower(target.ItemID) + "/" + target.Kind + "/"
}

func (c *Client) invalidate(target resources.Target) {
	prefix := targetPrefix(target)
	// Whole-target invalidation includes both parents and subtrees, across grant
	// refreshes and credential rotations. Detached loads cannot repopulate them.
	c.listings.InvalidatePrefix(prefix)
	c.contents.InvalidatePrefix(prefix)
	c.metadata.InvalidatePrefix(prefix)
}
