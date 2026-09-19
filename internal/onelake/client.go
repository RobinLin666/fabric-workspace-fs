// Package onelake implements the OneLake ADLS Gen2 path APIs. It never changes
// Fabric-managed roots, ACLs, workspaces, or items.
package onelake

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

const (
	// Endpoint and Scope are deliberately distinct from the Fabric control plane.
	Endpoint = "https://onelake.dfs.fabric.microsoft.com"
	Scope    = "https://storage.azure.com/.default"

	// This version is used by the official OneLake access API example.
	serviceVersion    = "2021-06-08"
	defaultChunkSize  = 4 << 20
	maxChunkSize      = 64 << 20
	defaultMaxPages   = 1000
	maxListBytes      = 8 << 20
	maxPageEntries    = 5000
	maxContinuation   = 16 << 10
	maxListingBytes   = 64 << 20
	maxListingEntries = 100000
)

// Info.Path is relative to the item, including its Files or Tables prefix.
type Info struct {
	Path    string
	IsDir   bool
	Size    int64
	ETag    string
	ModTime time.Time
	// Cache provenance is local metadata, not a remote modification timestamp.
	ObservedAt time.Time
	ValidUntil time.Time
}

type Options struct {
	// ChunkSize defaults to 4 MiB and must not exceed 64 MiB.
	ChunkSize int
	// MaxPages bounds both listing and directory-rename continuation requests.
	// Zero selects 1000 pages. Incomplete operations return an error.
	MaxPages int
}

type Client struct {
	http       *transport.Client
	chunkSize  int
	maxPages   int
	maxEntries int
	initErr    error
}

// New uses an authenticated storage-audience transport supplied by the caller.
// Invalid options are reported by operations, before any request is sent.
func New(client *transport.Client, opts Options) *Client {
	c := &Client{http: client, chunkSize: opts.ChunkSize, maxPages: opts.MaxPages, maxEntries: maxListingEntries}
	if c.chunkSize == 0 {
		c.chunkSize = defaultChunkSize
	}
	if c.maxPages == 0 {
		c.maxPages = defaultMaxPages
	}
	switch {
	case client == nil:
		c.initErr = fmt.Errorf("missing OneLake transport: %w", fs.ErrInvalid)
	case c.chunkSize < 0 || c.maxPages < 0:
		c.initErr = fmt.Errorf("invalid OneLake options: %w", fs.ErrInvalid)
	case c.chunkSize > maxChunkSize:
		c.initErr = fmt.Errorf("OneLake chunk size exceeds 64 MiB: %w", fserrors.ErrTooLarge)
	}
	return c
}

func (c *Client) ready() error {
	if c == nil {
		return fmt.Errorf("nil OneLake client: %w", fs.ErrInvalid)
	}
	return c.initErr
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values, headers http.Header, body []byte, statuses ...int) (*http.Response, error) {
	if err := c.ready(); err != nil {
		return nil, err
	}
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("x-ms-version", serviceVersion)
	target := c.http.URL(path, query)
	resp, err := c.http.Request(ctx, method, target, headers, body, method == http.MethodGet || method == http.MethodHead)
	if err != nil {
		return nil, mapError(err)
	}
	// OneLake can ignore unsupported headers while returning a 2xx response.
	// A rejected precondition or ownership marker is never a safe success.
	for _, rejected := range strings.FieldsFunc(resp.Header.Get("x-ms-rejected-headers"), func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t'
	}) {
		if _, sent := headers[http.CanonicalHeaderKey(rejected)]; sent {
			resp.Body.Close()
			return nil, fmt.Errorf("OneLake rejected a required request header; operation may be incomplete: %w", fserrors.ErrUnsupported)
		}
	}
	for _, status := range statuses {
		if resp.StatusCode == status {
			return resp, nil
		}
	}
	resp.Body.Close()
	return nil, &transport.HTTPError{
		StatusCode: resp.StatusCode,
		Code:       "UnexpectedStatus",
		Message:    "unexpected OneLake response status",
		RequestID:  resp.Header.Get("x-ms-request-id"),
		Method:     method,
		Path:       path,
	}
}

func mapError(err error) error {
	var he *transport.HTTPError
	if !errors.As(err, &he) {
		return err
	}
	switch {
	case he.StatusCode == http.StatusPreconditionFailed:
		return errors.Join(fserrors.ErrConflict, err)
	case he.StatusCode == http.StatusNotFound:
		return errors.Join(fs.ErrNotExist, err)
	case he.StatusCode == http.StatusForbidden || he.StatusCode == http.StatusUnauthorized:
		return errors.Join(fs.ErrPermission, err)
	case he.StatusCode == http.StatusConflict && he.Code == "DirectoryNotEmpty":
		return errors.Join(fserrors.ErrNotEmpty, err)
	case he.StatusCode == http.StatusConflict && he.Code == "PathAlreadyExists":
		return errors.Join(fs.ErrExist, err)
	case he.StatusCode == http.StatusRequestEntityTooLarge:
		return errors.Join(fserrors.ErrTooLarge, err)
	default:
		return err
	}
}

func (c *Client) Stat(ctx context.Context, p Path) (Info, error) {
	if err := ValidatePath(p, false); err != nil {
		return Info{}, err
	}
	info, _, err := c.stat(ctx, p, "")
	return info, err
}

func (c *Client) stat(ctx context.Context, p Path, etag string) (Info, http.Header, error) {
	headers := make(http.Header)
	if etag != "" {
		headers.Set("If-Match", etag)
	}
	resp, err := c.request(ctx, http.MethodHead, escapedPath(p), nil, headers, nil, http.StatusOK)
	if err != nil {
		return Info{}, nil, err
	}
	defer resp.Body.Close()
	info, err := infoFromHeaders(p.Relative, resp.Header)
	if err == nil && etag != "" && info.ETag != etag {
		err = fmt.Errorf("OneLake returned a different ETag for a conditional HEAD: %w", fserrors.ErrConflict)
	}
	return info, resp.Header, err
}

func infoFromHeaders(path string, h http.Header) (Info, error) {
	info := Info{Path: path}
	switch h.Get("x-ms-resource-type") {
	case "directory":
		info.IsDir = true
	case "file":
	default:
		return Info{}, fmt.Errorf("missing or invalid OneLake resource type")
	}
	size := h.Get("Content-Length")
	if size == "" && info.IsDir {
		size = "0"
	}
	var err error
	info.Size, err = nonnegativeInt(size)
	if err != nil {
		return Info{}, fmt.Errorf("invalid OneLake content length: %w", err)
	}
	if info.IsDir {
		info.Size = 0
	}
	info.ETag, err = entityTag(h.Get("ETag"), true)
	if err != nil {
		return Info{}, fmt.Errorf("invalid OneLake response ETag: %w", err)
	}
	info.ModTime, err = modificationTime(h.Get("Last-Modified"))
	if err != nil {
		return Info{}, err
	}
	return info, nil
}

func modificationTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := http.ParseTime(s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid OneLake modification time")
	}
	return t, nil
}

func nonnegativeInt(s string) (int64, error) {
	if s == "" {
		return 0, fs.ErrInvalid
	}
	for _, b := range []byte(s) {
		if b < '0' || b > '9' {
			return 0, fs.ErrInvalid
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errors.Join(fserrors.ErrTooLarge, err)
	}
	return n, nil
}

func nextContinuation(token string, seen map[string]bool) error {
	if len(token) > maxContinuation {
		return fmt.Errorf("OneLake continuation token too large: %w", fserrors.ErrTooLarge)
	}
	if seen[token] {
		return fmt.Errorf("OneLake repeated a continuation token")
	}
	seen[token] = true
	return nil
}
