package onelake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

type listedPath struct {
	Name          string          `json:"name"`
	IsDirectory   json.RawMessage `json:"isDirectory"`
	ContentLength json.RawMessage `json:"contentLength"`
	ETag          string          `json:"etag"`
	LastModified  string          `json:"lastModified"`
}

// List returns only immediate children. Server-supplied paths are checked
// against the requested directory; they are never trusted as request targets.
func (c *Client) List(ctx context.Context, p Path) ([]Info, error) {
	if err := ValidatePath(p, false); err != nil {
		return nil, err
	}
	if err := c.ready(); err != nil {
		return nil, err
	}
	query := url.Values{
		"resource":   {"filesystem"},
		"directory":  {p.Item + "/" + p.Relative},
		"recursive":  {"false"},
		"maxResults": {strconv.Itoa(maxPageEntries)},
	}
	result := make([]Info, 0)
	seenTokens := make(map[string]bool)
	seenPaths := make(map[string]bool)
	remaining := int64(maxListingBytes)
	for page := 0; page < c.maxPages; page++ {
		headers := make(http.Header)
		headers.Set("Accept", "application/json")
		resp, err := c.request(ctx, http.MethodGet, "/"+url.PathEscape(p.Workspace), query, headers, nil, http.StatusOK)
		if err != nil {
			return nil, err
		}
		limited := &io.LimitedReader{R: resp.Body, N: remaining + 1}
		paths, decodeErr := decodePaths(limited)
		resp.Body.Close()
		remaining -= remaining + 1 - limited.N
		if remaining < 0 {
			return nil, fmt.Errorf("OneLake aggregate listing byte limit exceeded: %w", fserrors.ErrTooLarge)
		}
		if decodeErr != nil {
			return nil, decodeErr
		}
		if len(paths) > c.maxEntries-len(result) {
			return nil, fmt.Errorf("OneLake aggregate listing entry limit exceeded: %w", fserrors.ErrTooLarge)
		}
		for _, path := range paths {
			info, err := listedInfo(p, path)
			if err != nil {
				return nil, err
			}
			if seenPaths[info.Path] {
				return nil, fmt.Errorf("OneLake listing repeated a path: %w", fserrors.ErrConflict)
			}
			seenPaths[info.Path] = true
			result = append(result, info)
		}
		token := resp.Header.Get("x-ms-continuation")
		if token == "" {
			sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
			return result, nil
		}
		if err := nextContinuation(token, seenTokens); err != nil {
			return nil, err
		}
		query.Set("continuation", token)
	}
	return nil, fmt.Errorf("OneLake listing exceeded MaxPages: %w", fserrors.ErrTooLarge)
}

func decodePaths(body io.Reader) ([]listedPath, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxListBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxListBytes {
		return nil, fmt.Errorf("OneLake listing response too large: %w", fserrors.ErrTooLarge)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("OneLake listing contains invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("invalid OneLake listing envelope")
	}
	var paths []listedPath
	found := false
	for decoder.More() {
		name, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid OneLake listing property: %w", err)
		}
		if name != "paths" {
			var ignored json.RawMessage
			if err := decoder.Decode(&ignored); err != nil {
				return nil, fmt.Errorf("invalid OneLake listing property: %w", err)
			}
			continue
		}
		if found {
			return nil, fmt.Errorf("OneLake listing repeated its paths property")
		}
		found = true
		token, err := decoder.Token()
		if err != nil || token != json.Delim('[') {
			return nil, fmt.Errorf("invalid OneLake paths array")
		}
		for decoder.More() {
			// Enforce the count while decoding, not after allocating an
			// arbitrarily large server-supplied array of empty objects.
			if len(paths) == maxPageEntries {
				return nil, fmt.Errorf("OneLake listing exceeded maxResults: %w", fserrors.ErrTooLarge)
			}
			var path listedPath
			if err := decoder.Decode(&path); err != nil {
				return nil, fmt.Errorf("invalid OneLake listing entry: %w", err)
			}
			paths = append(paths, path)
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, fmt.Errorf("invalid OneLake paths array terminator")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("invalid OneLake listing terminator")
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return nil, fmt.Errorf("trailing data in OneLake listing")
	}
	if !found {
		return nil, fmt.Errorf("OneLake listing omitted paths")
	}
	return paths, nil
}

func listedInfo(parent Path, entry listedPath) (Info, error) {
	item, relative, ok := strings.Cut(entry.Name, "/")
	prefix := parent.Relative + "/"
	if !ok || !strings.EqualFold(item, parent.Item) || !strings.HasPrefix(relative, prefix) {
		return Info{}, fmt.Errorf("OneLake listing returned a path outside the requested directory: %w", fs.ErrInvalid)
	}
	if strings.ContainsRune(strings.TrimPrefix(relative, prefix), '/') {
		return Info{}, fmt.Errorf("OneLake nonrecursive listing returned a descendant: %w", fs.ErrInvalid)
	}
	p := parent
	p.Relative = relative
	if err := ValidatePath(p, false); err != nil {
		return Info{}, fmt.Errorf("unsafe OneLake listing entry: %w", err)
	}
	info := Info{Path: relative}
	if len(entry.IsDirectory) != 0 {
		v := scalarText(entry.IsDirectory)
		if v != "true" && v != "false" {
			return Info{}, fmt.Errorf("invalid OneLake directory flag")
		}
		info.IsDir = v == "true"
	}
	size := scalarText(entry.ContentLength)
	if size == "" && info.IsDir {
		size = "0"
	}
	var err error
	info.Size, err = nonnegativeInt(size)
	if err != nil {
		return Info{}, fmt.Errorf("invalid OneLake listed size: %w", err)
	}
	if info.IsDir {
		info.Size = 0
	}
	info.ETag, err = entityTag(entry.ETag, true)
	if err != nil {
		return Info{}, err
	}
	info.ModTime, err = modificationTime(entry.LastModified)
	return info, err
}

func scalarText(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) != 0 && raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return ""
		}
		return s
	}
	return string(raw)
}

// Read reads a byte range directly into dest. A short read at the actual end of
// the object returns io.EOF; a truncated HTTP response returns io.ErrUnexpectedEOF.
func (c *Client) Read(ctx context.Context, p Path, offset int64, dest []byte, etag string) (int, error) {
	if err := ValidatePath(p, false); err != nil {
		return 0, err
	}
	if err := c.ready(); err != nil {
		return 0, err
	}
	if offset < 0 || (len(dest) != 0 && int64(len(dest))-1 > math.MaxInt64-offset) {
		return 0, fmt.Errorf("invalid OneLake byte range: %w", fs.ErrInvalid)
	}
	tag, err := entityTag(etag, false)
	if err != nil {
		return 0, err
	}
	if len(dest) == 0 {
		return 0, nil
	}
	end := offset + (int64(len(dest)) - 1)
	headers := make(http.Header)
	headers.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
	headers.Set("Accept-Encoding", "identity")
	if tag != "" {
		headers.Set("If-Match", tag)
	}
	resp, err := c.request(ctx, http.MethodGet, escapedPath(p), nil, headers, nil, http.StatusOK, http.StatusPartialContent)
	if err != nil {
		var he *transport.HTTPError
		if errors.As(err, &he) && he.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			return 0, io.EOF
		}
		return 0, err
	}
	defer resp.Body.Close()
	if resp.Header.Get("x-ms-resource-type") == "directory" {
		return 0, fserrors.ErrIsDir
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return 0, fmt.Errorf("OneLake returned encoded byte-range content")
	}
	if tag != "" {
		got, err := entityTag(resp.Header.Get("ETag"), true)
		if err != nil {
			return 0, err
		}
		if got != tag {
			return 0, fserrors.ErrConflict
		}
	}
	if resp.StatusCode == http.StatusOK {
		if offset != 0 {
			return 0, fmt.Errorf("OneLake ignored a nonzero byte range")
		}
		n, err := io.ReadFull(resp.Body, dest)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if resp.ContentLength >= 0 && int64(n) < resp.ContentLength {
				return n, io.ErrUnexpectedEOF
			}
			return n, io.EOF
		}
		return n, err
	}
	start, last, total, err := parseContentRange(resp.Header.Get("Content-Range"))
	if err != nil || start != offset || last > end {
		return 0, fmt.Errorf("invalid OneLake Content-Range")
	}
	length := last - start + 1
	if resp.ContentLength >= 0 && resp.ContentLength != length {
		return 0, fmt.Errorf("OneLake Content-Length does not match Content-Range")
	}
	n, err := io.ReadFull(resp.Body, dest[:int(length)])
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return n, err
	}
	if n < len(dest) {
		if last == total-1 {
			return n, io.EOF
		}
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

func parseContentRange(value string) (start, end, total int64, err error) {
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, fs.ErrInvalid
	}
	bounds, size, ok := strings.Cut(strings.TrimPrefix(value, "bytes "), "/")
	if !ok {
		return 0, 0, 0, fs.ErrInvalid
	}
	first, last, ok := strings.Cut(bounds, "-")
	if !ok {
		return 0, 0, 0, fs.ErrInvalid
	}
	start, err = nonnegativeInt(first)
	if err != nil {
		return
	}
	end, err = nonnegativeInt(last)
	if err != nil {
		return
	}
	total, err = nonnegativeInt(size)
	if err == nil && (start > end || end >= total) {
		err = fs.ErrInvalid
	}
	return
}
