package onelake

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

const stageProperty = "fabricfsupload"

// Put commits a stable ReaderAt spool through a private sibling file and a
// conditional rename. An empty expectedETag means that the target must be absent.
// No write request is automatically retried, including append and rename.
func (c *Client) Put(ctx context.Context, p Path, source io.ReaderAt, size int64, expectedETag string) (info Info, err error) {
	if err := ValidatePath(p, true); err != nil {
		return Info{}, err
	}
	if err := c.ready(); err != nil {
		return Info{}, err
	}
	if ctx == nil || size < 0 || (source == nil && size != 0) {
		return Info{}, fmt.Errorf("invalid OneLake upload source or size: %w", fs.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return Info{}, err
	}
	targetTag, err := entityTag(expectedETag, false)
	if err != nil {
		return Info{}, err
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return Info{}, fmt.Errorf("create OneLake staging name: %w", err)
	}
	stage := p
	stage.Relative = p.Relative[:strings.LastIndexByte(p.Relative, '/')+1] + ".fabric-fs-upload-" + hex.EncodeToString(entropy[:])
	// ADLS properties contain base64-encoded text, not arbitrary binary bytes.
	// OneLake rejects non-text decoded values as InvalidPropertyName.
	marker := base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(entropy[:])))
	cleanup := true
	defer func() {
		if !cleanup || err == nil {
			return
		}
		// Cancellation of the upload must not cancel its bounded cleanup attempt.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if cleanupErr := c.cleanupStage(cleanupCtx, stage, marker); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("cleanup OneLake staging file %q failed: %w", stage.Relative, cleanupErr))
		}
	}()
	headers := make(http.Header)
	headers.Set("If-None-Match", "*")
	headers.Set("x-ms-properties", stageProperty+"="+marker)
	resp, err := c.request(ctx, http.MethodPut, escapedPath(stage), url.Values{"resource": {"file"}}, headers, nil, http.StatusCreated)
	if err != nil {
		var he *transport.HTTPError
		if errors.As(err, &he) && he.StatusCode >= 400 && he.StatusCode < 500 && he.StatusCode != http.StatusRequestTimeout {
			cleanup = false // A rejected create never gives us ownership of an existing path.
		}
		return Info{}, err
	}
	createTag, tagErr := entityTag(resp.Header.Get("ETag"), true)
	resp.Body.Close()
	if tagErr != nil {
		return Info{}, tagErr
	}

	bufferSize := int64(c.chunkSize)
	if size < bufferSize {
		bufferSize = size
	}
	buffer := make([]byte, int(bufferSize))
	for position := int64(0); position < size; {
		if err := ctx.Err(); err != nil {
			return Info{}, err
		}
		length := int64(len(buffer))
		if remaining := size - position; remaining < length {
			length = remaining
		}
		chunk := buffer[:int(length)]
		n, readErr := source.ReadAt(chunk, position)
		if n != len(chunk) || (readErr != nil && readErr != io.EOF) {
			if readErr == nil || readErr == io.EOF {
				readErr = io.ErrUnexpectedEOF
			}
			return Info{}, fmt.Errorf("read OneLake upload spool: %w", readErr)
		}
		// ADLS explicitly forbids If-Match on append. The random staging name
		// isolates these writes; flush and commit carry the ETag conditions.
		resp, err := c.request(ctx, http.MethodPatch, escapedPath(stage), url.Values{
			"action": {"append"}, "position": {strconv.FormatInt(position, 10)},
		}, http.Header{"Content-Type": {"application/octet-stream"}}, chunk, http.StatusAccepted)
		if err != nil {
			return Info{}, err
		}
		resp.Body.Close()
		position += length
	}
	headers = make(http.Header)
	headers.Set("If-Match", createTag)
	resp, err = c.request(ctx, http.MethodPatch, escapedPath(stage), url.Values{
		"action": {"flush"}, "position": {strconv.FormatInt(size, 10)}, "close": {"true"},
	}, headers, nil, http.StatusOK)
	if err != nil {
		return Info{}, err
	}
	flushedTag, tagErr := entityTag(resp.Header.Get("ETag"), true)
	resp.Body.Close()
	if tagErr != nil {
		return Info{}, tagErr
	}
	stageInfo, _, err := c.stat(ctx, stage, flushedTag)
	if err != nil {
		return Info{}, err
	}
	if stageInfo.IsDir || stageInfo.Size != size {
		return Info{}, fmt.Errorf("OneLake staging file does not match the flushed upload: %w", fserrors.ErrConflict)
	}
	info, err = c.rename(ctx, stage, p, stageInfo, targetTag, false, true)
	if err == nil {
		cleanup = false
	}
	return info, err
}

func (c *Client) cleanupStage(ctx context.Context, stage Path, marker string) error {
	info, headers, err := c.stat(ctx, stage, "")
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir || !ownsStage(headers.Get("x-ms-properties"), marker) {
		return fmt.Errorf("staging ownership changed; refusing deletion: %w", fserrors.ErrConflict)
	}
	return c.delete(ctx, stage, false, info.ETag)
}

func ownsStage(properties, marker string) bool {
	found := false
	for _, part := range strings.Split(properties, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && name == stageProperty {
			if found || value != marker {
				return false
			}
			found = true
		}
	}
	return found
}

func (c *Client) Mkdir(ctx context.Context, p Path) error {
	if err := ValidatePath(p, true); err != nil {
		return err
	}
	headers := make(http.Header)
	headers.Set("If-None-Match", "*")
	resp, err := c.request(ctx, http.MethodPut, escapedPath(p), url.Values{"resource": {"directory"}}, headers, nil, http.StatusCreated)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.Header.Get("x-ms-continuation") != "" {
		return fmt.Errorf("unexpected OneLake create continuation")
	}
	return nil
}

// Remove never recursively deletes. The ETag is required even for empty
// directories, and the server rejects directories populated after the HEAD.
func (c *Client) Remove(ctx context.Context, p Path, directory bool, etag string) error {
	if err := ValidatePath(p, true); err != nil {
		return err
	}
	tag, err := entityTag(etag, true)
	if err != nil {
		return err
	}
	info, _, err := c.stat(ctx, p, tag)
	if err != nil {
		return err
	}
	if info.IsDir != directory {
		if info.IsDir {
			return fserrors.ErrIsDir
		}
		return fserrors.ErrNotDir
	}
	return c.delete(ctx, p, directory, tag)
}

func (c *Client) delete(ctx context.Context, p Path, directory bool, etag string) error {
	query := make(url.Values)
	if directory {
		query.Set("recursive", "false")
	}
	headers := make(http.Header)
	headers.Set("If-Match", etag)
	resp, err := c.request(ctx, http.MethodDelete, escapedPath(p), query, headers, nil, http.StatusOK, http.StatusAccepted)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.Header.Get("x-ms-continuation") != "" {
		return fmt.Errorf("refusing continuation of nonrecursive OneLake deletion: %w", fserrors.ErrUnsupported)
	}
	return nil
}

// Rename is conditional on both paths. A missing destinationETag or noReplace
// requires an absent destination. Directory replacement is deliberately not
// emulated by deleting the destination; it returns ErrUnsupported instead.
func (c *Client) Rename(ctx context.Context, source, destination Path, sourceETag, destinationETag string, noReplace bool) (Info, error) {
	if err := ValidatePath(source, true); err != nil {
		return Info{}, err
	}
	if err := ValidatePath(destination, true); err != nil {
		return Info{}, err
	}
	if !sameItem(source, destination) {
		return Info{}, fserrors.ErrCrossDevice
	}
	sourceTag, err := entityTag(sourceETag, true)
	if err != nil {
		return Info{}, err
	}
	destinationTag, err := entityTag(destinationETag, false)
	if err != nil {
		return Info{}, err
	}
	if strings.HasPrefix(destination.Relative, source.Relative+"/") {
		return Info{}, fmt.Errorf("a path cannot be renamed beneath itself: %w", fs.ErrInvalid)
	}
	if source.Relative == destination.Relative && noReplace {
		return Info{}, fs.ErrExist
	}
	info, _, err := c.stat(ctx, source, sourceTag)
	if err != nil {
		return Info{}, err
	}
	if source.Relative == destination.Relative {
		return info, nil
	}
	return c.rename(ctx, source, destination, info, destinationTag, noReplace, false)
}

func (c *Client) rename(ctx context.Context, source, destination Path, sourceInfo Info, destinationTag string, noReplace, clearProperties bool) (Info, error) {
	if destinationTag != "" && !noReplace {
		target, _, err := c.stat(ctx, destination, destinationTag)
		if err != nil {
			return Info{}, err
		}
		if target.IsDir && !sourceInfo.IsDir {
			return Info{}, fserrors.ErrIsDir
		}
		if sourceInfo.IsDir {
			if !target.IsDir {
				return Info{}, fserrors.ErrNotDir
			}
			return Info{}, fserrors.ErrUnsupported
		}
	}
	headers := make(http.Header)
	headers.Set("x-ms-rename-source", escapedPath(source))
	headers.Set("x-ms-source-if-match", sourceInfo.ETag)
	if destinationTag == "" || noReplace {
		headers.Set("If-None-Match", "*")
	} else {
		headers.Set("If-Match", destinationTag)
	}
	if clearProperties {
		headers.Set("x-ms-properties", "")
	}
	query := url.Values{"mode": {"posix"}}
	seen := make(map[string]bool)
	for page := 0; page < c.maxPages; page++ {
		resp, err := c.request(ctx, http.MethodPut, escapedPath(destination), query, headers, nil, http.StatusCreated)
		if err != nil {
			return Info{}, err
		}
		token := resp.Header.Get("x-ms-continuation")
		tag := resp.Header.Get("ETag")
		resp.Body.Close()
		if token != "" {
			if err := nextContinuation(token, seen); err != nil {
				return Info{}, err
			}
			query.Set("continuation", token)
			continue
		}
		tag, err = entityTag(tag, true)
		if err != nil {
			return Info{}, fmt.Errorf("OneLake rename did not return its committed ETag: %w", err)
		}
		info, _, err := c.stat(ctx, destination, tag)
		if err != nil {
			return Info{}, err
		}
		if info.IsDir != sourceInfo.IsDir || info.Size != sourceInfo.Size {
			return Info{}, fmt.Errorf("OneLake rename verification did not match its source: %w", fserrors.ErrConflict)
		}
		return info, nil
	}
	return Info{}, fmt.Errorf("OneLake rename exceeded MaxPages and may be incomplete: %w", fserrors.ErrTooLarge)
}
