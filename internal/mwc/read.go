package mwc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/resources"
)

type entry struct {
	info      resources.Info
	sizeKnown bool
}

type directoryListing struct {
	entries    []entry
	observedAt time.Time
}

type fileMetadata struct {
	size         int64
	lastModified string
	version      string
	observedAt   time.Time
}

type fileContent struct {
	bytes      []byte
	version    string
	modified   time.Time
	observedAt time.Time
}

func (r route) url(relative string, query url.Values) string {
	path := r.basePath
	if relative != "" {
		parts := strings.Split(relative, "/")
		for i := range parts {
			parts[i] = url.PathEscape(parts[i])
		}
		path = strings.TrimSuffix(path, "/") + "/" + strings.Join(parts, "/")
	}
	return r.grant.http.URL(path, query)
}

func (c *Client) Stat(ctx context.Context, path resources.Path) (resources.Info, error) {
	if err := validatePath(ctx, path, false); err != nil {
		return resources.Info{}, err
	}
	r, err := c.route(ctx, path.Target)
	if err != nil {
		return resources.Info{}, c.lookupFailure(err, 0, c.now())
	}
	return c.stat(ctx, r, path.Relative, false)
}

func (c *Client) stat(ctx context.Context, r route, relative string, fresh bool) (resources.Info, error) {
	e, err := c.lookup(ctx, r, relative, fresh)
	if err != nil {
		return resources.Info{}, err
	}
	if e.info.IsDir {
		return e.info, nil
	}
	return c.metadataInfo(ctx, r, e, fresh)
}

// Metadata operations must never download content merely to calculate a SHA.
// If a listing omits metadata, GET is used only for its real response headers;
// no HEAD/Range support is invented and the body is closed without buffering.
func (c *Client) metadataInfo(ctx context.Context, r route, e entry, fresh bool) (resources.Info, error) {
	info := e.info
	info.Version = ""
	if e.sizeKnown && !info.Modified.IsZero() {
		return info, nil
	}
	// Only headers are retained here. Caching their merged listing attributes
	// would give an older listing a new observation time and a new deadline.
	load := func(ctx context.Context) (result fileMetadata, err error) {
		defer func() { err = c.lookupFailure(err, r.policy.Attr, time.Time{}) }()
		resp, err := r.grant.http.Request(ctx, http.MethodGet, r.url(info.Path, nil),
			http.Header{"Accept": {"application/octet-stream"}, "Accept-Encoding": {"identity"}, "Cache-Control": {"no-cache"}}, nil, true)
		if err != nil {
			return fileMetadata{}, resourceError(err)
		}
		if err := requireOK(resp); err != nil {
			return fileMetadata{}, err
		}
		defer resp.Body.Close()
		if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
			return fileMetadata{}, invalidResponse("encoded metadata response")
		}
		if !e.sizeKnown && resp.ContentLength < 0 {
			return fileMetadata{}, invalidResponse("resource metadata omitted content length")
		}
		lastModified := resp.Header.Get("Last-Modified")
		if info.Modified.IsZero() {
			if _, err := parseModified(lastModified); err != nil {
				return fileMetadata{}, err
			}
		}
		if err := ctx.Err(); err != nil {
			return fileMetadata{}, err
		}
		return fileMetadata{
			size: resp.ContentLength, lastModified: lastModified, version: resp.Header.Get("ETag"), observedAt: c.now(),
		}, nil
	}
	var metadata fileMetadata
	var err error
	if fresh {
		metadata, err = load(ctx)
	} else {
		metadata, err = c.metadata.GetWithTTL(ctx, r.cachePrefix+info.Path, r.policy.Attr, load)
	}
	if err != nil {
		sourceDeadline := info.ValidUntil
		if !info.ObservedAt.IsZero() {
			until := info.ObservedAt.Add(min(r.policy.Attr, r.policy.Directory))
			if sourceDeadline.IsZero() || until.Before(sourceDeadline) {
				sourceDeadline = until
			}
		}
		return resources.Info{}, c.lookupFailure(err, 0, sourceDeadline)
	}
	if !e.sizeKnown {
		if metadata.size < 0 {
			return resources.Info{}, invalidResponse("resource metadata omitted content length")
		}
		info.Size = metadata.size
	}
	if info.Modified.IsZero() {
		info.Modified, err = parseModified(metadata.lastModified)
		if err != nil {
			return resources.Info{}, err
		}
	}
	if info.MetadataVersion == "" {
		info.MetadataVersion = metadata.version
	}
	return capObservation(info, metadata.observedAt, r.policy.Attr), nil
}

func (c *Client) fileInfo(ctx context.Context, r route, e entry, fresh bool) (resources.Info, error) {
	if e.sizeKnown && e.info.Size > c.maxFileSize {
		return resources.Info{}, fserrors.ErrTooLarge
	}
	data, err := c.content(ctx, r, e.info.Path, fresh)
	if err != nil {
		return resources.Info{}, err
	}
	info := e.info
	info.Size, info.Version = int64(len(data.bytes)), data.version
	if !data.modified.IsZero() {
		info.Modified = data.modified
	}
	return capObservation(info, data.observedAt, min(r.policy.Attr, r.policy.Content)), nil
}

func (c *Client) List(ctx context.Context, path resources.Path) ([]resources.Info, error) {
	if err := validatePath(ctx, path, false); err != nil {
		return nil, err
	}
	r, err := c.route(ctx, path.Target)
	if err != nil {
		return nil, c.lookupFailure(err, 0, c.now())
	}
	if path.Relative != "" {
		e, err := c.lookupWithin(ctx, r, path.Relative, r.policy.Directory, false)
		if err != nil {
			return nil, err
		}
		if !e.info.IsDir {
			return nil, fserrors.ErrNotDir
		}
	}
	entries, err := c.list(ctx, r, path.Relative, false)
	if err != nil {
		return nil, err
	}
	infos := make([]resources.Info, 0, len(entries))
	for _, e := range entries {
		info := e.info
		if !info.IsDir && (!e.sizeKnown || info.Modified.IsZero()) {
			info, err = c.metadataInfo(ctx, r, e, false)
			if err != nil {
				return nil, err
			}
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// Snapshot pins at most one bounded content download independently of cache TTL
// or eviction. File classification belongs to the caller's open/stat path; this
// method does not list the parent or infer file types from the response body.
func (c *Client) Snapshot(ctx context.Context, path resources.Path, expectedVersion string) (*resources.Snapshot, error) {
	if err := validatePath(ctx, path, false); err != nil {
		return nil, err
	}
	if path.Relative == "" {
		return nil, fserrors.ErrIsDir
	}
	r, err := c.route(ctx, path.Target)
	if err != nil {
		return nil, err
	}
	data, err := c.content(ctx, r, path.Relative, false)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if expectedVersion != "" && data.version != expectedVersion {
		return nil, fserrors.ErrConflict
	}
	snapshot := resources.NewSnapshot(data.bytes, data.version)
	if err := ctx.Err(); err != nil {
		snapshot.Close()
		return nil, err
	}
	return snapshot, nil
}

func (c *Client) Read(ctx context.Context, path resources.Path, offset int64, dest []byte, version string) (int, error) {
	if err := validatePath(ctx, path, false); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, fmt.Errorf("negative resource read offset: %w", fs.ErrInvalid)
	}
	if path.Relative == "" {
		return 0, fserrors.ErrIsDir
	}
	if len(dest) == 0 {
		return 0, nil
	}
	r, err := c.route(ctx, path.Target)
	if err != nil {
		return 0, err
	}
	e, err := c.lookup(ctx, r, path.Relative, false)
	if err != nil {
		return 0, err
	}
	if e.info.IsDir {
		return 0, fserrors.ErrIsDir
	}
	if e.sizeKnown && e.info.Size > c.maxFileSize {
		return 0, fserrors.ErrTooLarge
	}
	data, err := c.content(ctx, r, path.Relative, false)
	if err != nil {
		return 0, err
	}
	if version != "" && data.version != version {
		return 0, fserrors.ErrConflict
	}
	if offset >= int64(len(data.bytes)) {
		return 0, io.EOF
	}
	n := copy(dest, data.bytes[int(offset):])
	if n < len(dest) {
		return n, io.EOF
	}
	return n, nil
}

func (c *Client) lookup(ctx context.Context, r route, relative string, fresh bool) (entry, error) {
	return c.lookupWithin(ctx, r, relative, r.policy.Attr, fresh)
}

func (c *Client) lookupWithin(ctx context.Context, r route, relative string, maxAge time.Duration, fresh bool) (entry, error) {
	if relative == "" {
		listing, err := c.listing(ctx, r, "", maxAge, fresh)
		if err != nil {
			return entry{}, err
		}
		return entry{
			info: capObservation(resources.Info{IsDir: true}, listing.observedAt, maxAge), sizeKnown: true,
		}, nil
	}
	parent := ""
	if separator := strings.LastIndexByte(relative, '/'); separator >= 0 {
		parent = relative[:separator]
	}
	listing, err := c.listing(ctx, r, parent, maxAge, fresh)
	if err != nil {
		return entry{}, err
	}
	for _, e := range listing.entries {
		if e.info.Path == relative {
			e.info = capObservation(e.info, listing.observedAt, maxAge)
			return e, nil
		}
	}
	return entry{}, &lookupError{
		cause: fs.ErrNotExist, validUntil: listing.observedAt.Add(min(maxAge, r.policy.Attr, r.policy.Directory)),
	}
}

func (c *Client) list(ctx context.Context, r route, relative string, fresh bool) ([]entry, error) {
	listing, err := c.listing(ctx, r, relative, r.policy.Directory, fresh)
	if err != nil {
		return nil, err
	}
	entries := make([]entry, len(listing.entries))
	for i, e := range listing.entries {
		e.info = capObservation(e.info, listing.observedAt, min(r.policy.Directory, r.policy.Attr))
		entries[i] = e
	}
	return entries, nil
}

func (c *Client) listing(ctx context.Context, r route, relative string, maxAge time.Duration, fresh bool) (directoryListing, error) {
	load := func(ctx context.Context) (result directoryListing, err error) {
		defer func() { err = c.lookupFailure(err, min(maxAge, r.policy.Attr, r.policy.Directory), time.Time{}) }()
		resp, err := r.grant.http.Request(ctx, http.MethodGet, r.url(relative, url.Values{"recursive": {"false"}}),
			http.Header{"Accept": {"application/json"}, "Cache-Control": {"no-cache"}}, nil, true)
		if err != nil {
			return directoryListing{}, resourceError(err)
		}
		if err := requireOK(resp); err != nil {
			return directoryListing{}, err
		}
		body, err := readBody(ctx, resp, maxListingBytes)
		if err != nil {
			return directoryListing{}, err
		}
		entries, err := parseListing(body, relative)
		if err != nil {
			return directoryListing{}, err
		}
		return directoryListing{entries: entries, observedAt: c.now()}, nil
	}
	if fresh {
		return load(ctx)
	}
	return c.listings.GetWithin(ctx, r.cachePrefix+relative, maxAge, max(r.policy.Attr, r.policy.Directory), load)
}

func capObservation(info resources.Info, observedAt time.Time, ttl time.Duration) resources.Info {
	if info.ObservedAt.IsZero() || observedAt.Before(info.ObservedAt) {
		info.ObservedAt = observedAt
	}
	until := observedAt.Add(ttl)
	if info.ValidUntil.IsZero() || until.Before(info.ValidUntil) {
		info.ValidUntil = until
	}
	return info
}

func (c *Client) content(ctx context.Context, r route, relative string, fresh bool) (fileContent, error) {
	load := func(ctx context.Context) (fileContent, error) {
		resp, err := r.grant.http.Request(ctx, http.MethodGet, r.url(relative, nil),
			http.Header{"Accept": {"application/octet-stream"}, "Cache-Control": {"no-cache"}}, nil, true)
		if err != nil {
			return fileContent{}, resourceError(err)
		}
		if err := requireOK(resp); err != nil {
			return fileContent{}, err
		}
		body, err := readBody(ctx, resp, c.maxFileSize)
		if err != nil {
			return fileContent{}, err
		}
		modified, err := parseModified(resp.Header.Get("Last-Modified"))
		if err != nil {
			return fileContent{}, err
		}
		return fileContent{bytes: body, version: versionFor(body), modified: modified, observedAt: c.now()}, nil
	}
	if fresh {
		return load(ctx)
	}
	return c.contents.GetWithTTL(ctx, r.cachePrefix+relative, r.policy.Content, load)
}

// The wire contract uses children. The other envelopes are explicit compatibility
// forms, not an inference that a missing/malformed listing means an empty root.
// Names must be immediate children; full paths require a separately verified
// contract and must not be silently cleaned, decoded, or joined across roots.
func parseListing(body []byte, relative string) ([]entry, error) {
	if !utf8.Valid(body) {
		return nil, invalidResponse("directory listing encoding")
	}
	raw := bytes.TrimSpace(body)
	if len(raw) == 0 {
		return nil, invalidResponse("empty directory listing")
	}
	if raw[0] != '[' {
		var envelope map[string]json.RawMessage
		if json.Unmarshal(raw, &envelope) != nil || envelope == nil {
			return nil, invalidResponse("directory listing envelope")
		}
		for _, name := range []string{"continuationToken", "continuationUri", "nextLink", "@odata.nextLink", "hasMore", "isTruncated"} {
			if value, present := envelope[name]; present &&
				string(value) != "null" && string(value) != `""` && string(value) != "false" {
				return nil, invalidResponse("incomplete directory listing")
			}
		}
		raw = nil
		for _, name := range []string{"children", "entries", "value"} {
			if value, present := envelope[name]; present {
				raw = value
				break
			}
		}
	}
	var children []json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &children) != nil || children == nil {
		return nil, invalidResponse("missing directory children array")
	}
	if len(children) > maxListingEntries {
		return nil, fserrors.ErrTooLarge
	}
	result := make([]entry, 0, len(children))
	seen := make(map[string]bool, len(children))
	for _, child := range children {
		var wire struct {
			Name     string          `json:"name"`
			Type     string          `json:"fileSystemEntryType"`
			Modified string          `json:"lastModified"`
			Length   json.RawMessage `json:"contentLength"`
			ETag     string          `json:"etag"`
		}
		if json.Unmarshal(child, &wire) != nil || namespace.Component(wire.Name) != nil ||
			(wire.Type != "file" && wire.Type != "folder") {
			return nil, invalidResponse("directory child schema or name")
		}
		for _, r := range wire.Name {
			if r < 0x20 || r == 0x7f {
				return nil, invalidResponse("directory child control character")
			}
		}
		if seen[wire.Name] {
			return nil, invalidResponse("duplicate directory child")
		}
		seen[wire.Name] = true
		modified, err := parseModified(wire.Modified)
		if err != nil {
			return nil, err
		}
		path := wire.Name
		if relative != "" {
			path = relative + "/" + path
		}
		e := entry{info: resources.Info{Path: path, IsDir: wire.Type == "folder", Modified: modified, MetadataVersion: wire.ETag}}
		if e.info.IsDir {
			e.sizeKnown = true
		} else if len(wire.Length) != 0 {
			var err error
			e.info.Size, err = parseLength(wire.Length)
			if err != nil {
				return nil, err
			}
			e.sizeKnown = true
		}
		result = append(result, e)
	}
	return result, nil
}

func parseLength(raw json.RawMessage) (int64, error) {
	value := string(raw)
	if strings.HasPrefix(value, `"`) {
		if json.Unmarshal(raw, &value) != nil {
			return 0, invalidResponse("file content length")
		}
	}
	if value == "" {
		return 0, invalidResponse("file content length")
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, invalidResponse("file content length")
		}
	}
	size, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, invalidResponse("file content length")
	}
	return size, nil
}

func parseModified(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	if parsed, err := http.ParseTime(value); err == nil {
		return parsed, nil
	}
	return time.Time{}, invalidResponse("last modified timestamp")
}
