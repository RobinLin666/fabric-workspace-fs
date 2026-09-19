package mwc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
)

func (c *Client) Put(ctx context.Context, path resources.Path, source io.ReaderAt, size int64, expectedVersion string) (resources.Info, error) {
	if err := validatePath(ctx, path, true); err != nil {
		return resources.Info{}, err
	}
	if size < 0 || source == nil {
		return resources.Info{}, fmt.Errorf("invalid resource write source: %w", fs.ErrInvalid)
	}
	if size > c.maxFileSize {
		return resources.Info{}, fserrors.ErrTooLarge
	}
	body, err := io.ReadAll(io.NewSectionReader(source, 0, size))
	if err != nil {
		return resources.Info{}, safeError(ctx, "resource write source could not be read", err)
	}
	if int64(len(body)) != size {
		return resources.Info{}, io.ErrUnexpectedEOF
	}
	if err := ctx.Err(); err != nil {
		return resources.Info{}, err
	}
	r, err := c.route(ctx, path.Target)
	if err != nil {
		return resources.Info{}, err
	}
	defer c.invalidate(path.Target)
	if _, err := c.compareDestination(ctx, r, path.Relative, expectedVersion, false); err != nil {
		return resources.Info{}, err
	}
	modified, err := c.mutate(ctx, r, path, http.MethodPut, nil, fileHeaders("file"), body)
	if err != nil {
		return resources.Info{}, err
	}
	return capObservation(resources.Info{
		Path: path.Relative, Size: size, Version: versionFor(body), Modified: modified,
	}, c.now(), min(r.policy.Attr, r.policy.Content)), nil
}

func (c *Client) Mkdir(ctx context.Context, path resources.Path) error {
	if err := validatePath(ctx, path, true); err != nil {
		return err
	}
	r, err := c.route(ctx, path.Target)
	if err != nil {
		return err
	}
	defer c.invalidate(path.Target)
	_, err = c.lookup(ctx, r, path.Relative, true)
	if err == nil {
		return fs.ErrExist
	}
	if !errors.Is(err, fs.ErrNotExist) || hasHTTPError(err) {
		return err
	}
	_, err = c.mutate(ctx, r, path, http.MethodPut, nil, fileHeaders("folder"), nil)
	return err
}

func (c *Client) Remove(ctx context.Context, path resources.Path, directory bool, expectedVersion string) error {
	if err := validatePath(ctx, path, true); err != nil {
		return err
	}
	if !directory && expectedVersion == "" {
		return fmt.Errorf("file removal requires its content version: %w", fs.ErrInvalid)
	}
	r, err := c.route(ctx, path.Target)
	if err != nil {
		return err
	}
	defer c.invalidate(path.Target)
	current, err := c.lookup(ctx, r, path.Relative, true)
	if err != nil {
		return err
	}
	if current.info.IsDir != directory {
		if directory {
			return fserrors.ErrNotDir
		}
		return fserrors.ErrIsDir
	}
	entryType := "file"
	var query url.Values
	if directory {
		if expectedVersion != "" {
			return fserrors.ErrConflict
		}
		children, err := c.list(ctx, r, path.Relative, true)
		if err != nil {
			return err
		}
		if len(children) != 0 {
			return fserrors.ErrNotEmpty
		}
		entryType, query = "folder", url.Values{"recursive": {"false"}}
	} else {
		info, err := c.fileInfo(ctx, r, current, true)
		if err != nil {
			return err
		}
		if info.Version != expectedVersion {
			return fserrors.ErrConflict
		}
	}
	_, err = c.mutate(ctx, r, path, http.MethodDelete, query, fileHeaders(entryType), nil)
	return err
}

// Rename uses the private file move action. Folder moves are deliberately not
// emulated. The destination is workdir-relative; local root aliases are never
// sent as path components. Move syntax and listing schemas must be revalidated
// against the deployed private API when its contract changes.
func (c *Client) Rename(ctx context.Context, source, destination resources.Path, sourceVersion, destinationVersion string, noReplace bool) (resources.Info, error) {
	if err := validatePath(ctx, source, true); err != nil {
		return resources.Info{}, err
	}
	if err := validatePath(ctx, destination, true); err != nil {
		return resources.Info{}, err
	}
	if !sameTarget(source.Target, destination.Target) {
		return resources.Info{}, fserrors.ErrCrossDevice
	}
	r, err := c.route(ctx, source.Target)
	if err != nil {
		return resources.Info{}, err
	}
	defer c.invalidate(source.Target)
	current, err := c.lookup(ctx, r, source.Relative, true)
	if err != nil {
		return resources.Info{}, err
	}
	if current.info.IsDir {
		return resources.Info{}, fserrors.ErrUnsupported
	}
	if sourceVersion == "" {
		return resources.Info{}, fmt.Errorf("file rename requires its content version: %w", fs.ErrInvalid)
	}
	info, err := c.fileInfo(ctx, r, current, true)
	if err != nil {
		return resources.Info{}, err
	}
	if info.Version != sourceVersion {
		return resources.Info{}, fserrors.ErrConflict
	}
	if source.Relative == destination.Relative {
		if noReplace {
			return resources.Info{}, fs.ErrExist
		}
		return info, nil
	}
	if _, err := c.compareDestination(ctx, r, destination.Relative, destinationVersion, noReplace); err != nil {
		return resources.Info{}, err
	}
	parts := strings.Split(destination.Relative, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	headers := fileHeaders("file")
	headers.Set("ms-filesystem-location", strings.Join(parts, "/"))
	if _, err := c.mutate(ctx, r, source, http.MethodPost, url.Values{"action": {"move"}}, headers, nil); err != nil {
		return resources.Info{}, err
	}
	// A successful status without a completed move is not success. There are no
	// verified version headers, so verify bytes and source disappearance instead.
	destinationEntry, err := c.lookup(ctx, r, destination.Relative, true)
	if err != nil {
		return resources.Info{}, err
	}
	result, err := c.fileInfo(ctx, r, destinationEntry, true)
	if err != nil {
		return resources.Info{}, err
	}
	if result.IsDir || result.Version != sourceVersion {
		return resources.Info{}, fmt.Errorf("resource move destination did not match its source: %w", fserrors.ErrConflict)
	}
	if _, err := c.lookup(ctx, r, source.Relative, true); err == nil {
		return resources.Info{}, fmt.Errorf("resource move left its source in place: %w", fserrors.ErrConflict)
	} else if !errors.Is(err, fs.ErrNotExist) || hasHTTPError(err) {
		return resources.Info{}, err
	}
	return result, nil
}

func sameTarget(a, b resources.Target) bool {
	return a.Kind == b.Kind && strings.EqualFold(a.WorkspaceID, b.WorkspaceID) && strings.EqualFold(a.ItemID, b.ItemID)
}

// Empty expected means absent, not unconditional overwrite. A missing child in a
// successful parent listing is authoritative; a 404 for the parent/root is not.
func (c *Client) compareDestination(ctx context.Context, r route, relative, expected string, noReplace bool) (resources.Info, error) {
	current, err := c.lookup(ctx, r, relative, true)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) || hasHTTPError(err) {
			return resources.Info{}, err
		}
		if expected != "" {
			return resources.Info{}, errors.Join(fserrors.ErrConflict, fs.ErrNotExist)
		}
		return resources.Info{}, nil
	}
	if noReplace {
		return resources.Info{}, fs.ErrExist
	}
	if current.info.IsDir {
		return resources.Info{}, fserrors.ErrIsDir
	}
	if expected == "" {
		return resources.Info{}, errors.Join(fserrors.ErrConflict, fs.ErrExist)
	}
	info, err := c.fileInfo(ctx, r, current, true)
	if err != nil {
		return resources.Info{}, err
	}
	if info.Version != expected {
		return resources.Info{}, fserrors.ErrConflict
	}
	return info, nil
}

func fileHeaders(entryType string) http.Header {
	return http.Header{
		"Ms-Filesystem-Entry-Type": {entryType},
		"Content-Type":             {"application/octet-stream"},
	}
}

func (c *Client) mutate(ctx context.Context, r route, path resources.Path, method string, query url.Values, headers http.Header, body []byte) (time.Time, error) {
	c.invalidate(path.Target)
	resp, err := r.grant.http.Request(ctx, method, r.url(path.Relative, query), headers, body, false)
	if err != nil {
		return time.Time{}, resourceError(err)
	}
	if err := resp.Body.Close(); err != nil {
		return time.Time{}, safeError(ctx, "resource mutation response could not be closed", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return time.Time{}, invalidResponse("unconfirmed resource mutation status")
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	return parseModified(resp.Header.Get("Last-Modified"))
}
