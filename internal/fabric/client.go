// Package fabric implements the public Fabric REST v1 item APIs.
// Lists explicitly request recursive discovery and reject incomplete pagination.
package fabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

const (
	BaseURL                   = "https://api.fabric.microsoft.com"
	DefaultMaxDefinitionBytes = 64 << 20
	defaultMaxPages           = 1000
	maxPageBytes              = 16 << 20
	maxListingBytes           = 64 << 20
	maxListingEntries         = 100000
)

type Options struct {
	OperationTimeout   time.Duration
	PollInterval       time.Duration
	MaxDefinitionBytes int64
}

type Client struct {
	http               *transport.Client
	operationTimeout   time.Duration
	pollInterval       time.Duration
	maxDefinitionBytes int64
	maxPages           int
	configErr          error
}

// New uses five minutes for operations, one second between polls, and 64 MiB
// for definition envelopes when the corresponding options are zero.
func New(client *transport.Client, opts Options) *Client {
	c := &Client{http: client, maxPages: defaultMaxPages}
	if client == nil || opts.OperationTimeout < 0 || opts.PollInterval < 0 ||
		opts.MaxDefinitionBytes < 0 || opts.MaxDefinitionBytes == math.MaxInt64 {
		c.configErr = fmt.Errorf("invalid Fabric client options: %w", fs.ErrInvalid)
	}
	if opts.OperationTimeout == 0 {
		opts.OperationTimeout = 5 * time.Minute
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	if opts.MaxDefinitionBytes == 0 {
		opts.MaxDefinitionBytes = DefaultMaxDefinitionBytes
	}
	c.operationTimeout = opts.OperationTimeout
	c.pollInterval = opts.PollInterval
	c.maxDefinitionBytes = opts.MaxDefinitionBytes
	return c
}

type Workspace struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	CapacityID  string `json:"capacityId,omitempty"`
}

type Folder struct {
	ID             string `json:"id"`
	DisplayName    string `json:"displayName"`
	ParentFolderID string `json:"parentFolderId,omitempty"`
}

type Item struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Type        string `json:"type"`
	FolderID    string `json:"folderId,omitempty"`
	Description string `json:"description,omitempty"`
}

// ValidateID accepts the canonical, hyphenated UUID representation.
func ValidateID(id string) error {
	if len(id) != 36 {
		return fmt.Errorf("invalid Fabric UUID: %w", fs.ErrInvalid)
	}
	for i, r := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return fmt.Errorf("invalid Fabric UUID: %w", fs.ErrInvalid)
			}
		} else if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return fmt.Errorf("invalid Fabric UUID: %w", fs.ErrInvalid)
		}
	}
	return nil
}

func (c *Client) ready(ctx context.Context, ids ...string) error {
	if c.configErr != nil {
		return c.configErr
	}
	if ctx == nil {
		return fmt.Errorf("missing Fabric context: %w", fs.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := ValidateID(id); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) ListWorkspaces(ctx context.Context) ([]Workspace, error) {
	if err := c.ready(ctx); err != nil {
		return nil, err
	}
	return list(ctx, c, "/v1/workspaces", nil, validWorkspace)
}

func (c *Client) GetWorkspace(ctx context.Context, id string) (Workspace, error) {
	if err := c.ready(ctx, id); err != nil {
		return Workspace{}, err
	}
	resp, err := c.http.Request(ctx, http.MethodGet, c.http.URL("/v1/workspaces/"+id, nil), jsonHeaders(), nil, true)
	if err != nil {
		return Workspace{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Workspace{}, unexpectedResponse(resp)
	}
	var workspace Workspace
	if err := readJSON(resp, maxPageBytes, &workspace); err != nil {
		return Workspace{}, err
	}
	if err := validWorkspace(workspace); err != nil {
		return Workspace{}, err
	}
	if !strings.EqualFold(workspace.ID, id) {
		return Workspace{}, invalidResponse("workspace ID does not match the request")
	}
	return workspace, nil
}

func (c *Client) ListItems(ctx context.Context, workspace string) ([]Item, error) {
	if err := c.ready(ctx, workspace); err != nil {
		return nil, err
	}
	return list(ctx, c, "/v1/workspaces/"+workspace+"/items", url.Values{"recursive": {"true"}}, validItem)
}

func (c *Client) ListFolders(ctx context.Context, workspace string) ([]Folder, error) {
	if err := c.ready(ctx, workspace); err != nil {
		return nil, err
	}
	return list(ctx, c, "/v1/workspaces/"+workspace+"/folders", url.Values{"recursive": {"true"}}, validFolder)
}

func validWorkspace(w Workspace) error {
	if ValidateID(w.ID) != nil || w.DisplayName == "" {
		return invalidResponse("workspace is missing a valid ID or display name")
	}
	return nil
}

func validFolder(f Folder) error {
	if ValidateID(f.ID) != nil || f.DisplayName == "" || (f.ParentFolderID != "" && ValidateID(f.ParentFolderID) != nil) {
		return invalidResponse("folder is missing valid identity information")
	}
	return nil
}

func validItem(item Item) error {
	if ValidateID(item.ID) != nil || item.DisplayName == "" || item.Type == "" ||
		(item.FolderID != "" && ValidateID(item.FolderID) != nil) {
		return invalidResponse("item is missing valid identity information")
	}
	return nil
}

func list[T any](ctx context.Context, c *Client, path string, query url.Values, validate func(T) error) ([]T, error) {
	initial := c.http.URL(path, query)
	next := initial
	seenURLs, seenTokens := make(map[string]bool), make(map[string]bool)
	result := make([]T, 0)
	remaining := int64(maxListingBytes)
	for page := 0; page < c.maxPages; page++ {
		resolved, err := c.http.Resolve(next)
		if err != nil {
			return nil, err
		}
		key, err := paginationKey(resolved)
		if err != nil {
			return nil, err
		}
		if seenURLs[key] {
			return nil, invalidResponse("pagination cycle")
		}
		seenURLs[key] = true
		resp, err := c.http.Request(ctx, http.MethodGet, resolved, jsonHeaders(), nil, true)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, unexpectedResponse(resp)
		}
		body, err := readResponse(resp, min(int64(maxPageBytes), remaining))
		if err != nil {
			return nil, err
		}
		remaining -= int64(len(body))
		var envelope struct {
			Value             []T    `json:"value"`
			ContinuationToken string `json:"continuationToken"`
			ContinuationURI   string `json:"continuationUri"`
		}
		if json.Unmarshal(body, &envelope) != nil || envelope.Value == nil {
			return nil, invalidResponse("list body must contain a value array")
		}
		for _, value := range envelope.Value {
			if err := validate(value); err != nil {
				return nil, err
			}
			if len(envelope.Value) > maxListingEntries-len(result) {
				return nil, fserrors.ErrTooLarge
			}
		}
		result = append(result, envelope.Value...)
		if envelope.ContinuationURI == "" && envelope.ContinuationToken == "" {
			return result, nil
		}
		if envelope.ContinuationToken != "" {
			if seenTokens[envelope.ContinuationToken] {
				return nil, invalidResponse("pagination token cycle")
			}
			seenTokens[envelope.ContinuationToken] = true
		}
		next, err = c.nextPage(initial, envelope.ContinuationURI, envelope.ContinuationToken)
		if err != nil {
			return nil, err
		}
	}
	return nil, invalidResponse("pagination page limit exceeded")
}

func paginationKey(target string) (string, error) {
	u, err := url.Parse(target)
	if err != nil {
		return "", invalidResponse("invalid pagination URL")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", invalidResponse("invalid pagination query")
	}
	u.RawQuery = query.Encode()
	u.ForceQuery = false
	return u.String(), nil
}

func (c *Client) nextPage(initial, uri, token string) (string, error) {
	base, _ := url.Parse(initial)
	if uri == "" {
		query := base.Query()
		query.Set("continuationToken", token)
		base.RawQuery = query.Encode()
		return base.String(), nil
	}
	// Validate even when a token is also supplied; a bad URI is never silently
	// replaced with a token-based fallback.
	resolved, err := c.http.Resolve(uri)
	if err != nil {
		return "", err
	}
	next, _ := url.Parse(resolved)
	ref, _ := url.Parse(uri)
	if ref.Path == "" && ref.Host == "" && ref.Scheme == "" {
		next.Path, next.RawPath = base.Path, base.RawPath
	}
	if next.Path != base.Path {
		return "", invalidResponse("continuation changed the list endpoint")
	}
	query, err := url.ParseQuery(next.RawQuery)
	if err != nil {
		return "", invalidResponse("invalid continuation query")
	}
	for name, values := range base.Query() {
		if existing, ok := query[name]; ok {
			if len(existing) != len(values) || strings.Join(existing, "\x00") != strings.Join(values, "\x00") {
				return "", invalidResponse("continuation changed list options")
			}
		} else {
			query[name] = values
		}
	}
	next.RawQuery = query.Encode()
	return next.String(), nil
}

func jsonHeaders() http.Header {
	return http.Header{"Accept": {"application/json"}}
}

func readResponse(resp *http.Response, limit int64) ([]byte, error) {
	if resp.Body == nil {
		return nil, invalidResponse("missing response body")
	}
	defer resp.Body.Close()
	if resp.ContentLength > limit {
		return nil, fserrors.ErrTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}
		return nil, &responseReadError{cause: err}
	}
	if int64(len(body)) > limit {
		return nil, fserrors.ErrTooLarge
	}
	return body, nil
}

func readJSON(resp *http.Response, limit int64, target any) error {
	body, err := readResponse(resp, limit)
	if err != nil {
		return err
	}
	if json.Unmarshal(body, target) != nil {
		return invalidResponse("invalid JSON body")
	}
	return nil
}

func unexpectedResponse(resp *http.Response) error {
	if resp.Body != nil {
		resp.Body.Close()
	}
	err := &transport.HTTPError{
		StatusCode: resp.StatusCode, Code: "UnexpectedResponse", Message: "unexpected Fabric HTTP status",
		RequestID: resp.Header.Get("requestId"),
	}
	if err.RequestID == "" {
		err.RequestID = resp.Header.Get("x-ms-request-id")
	}
	if resp.Request != nil {
		err.Method = resp.Request.Method
		err.Path = resp.Request.URL.EscapedPath()
	}
	return err
}

func invalidResponse(reason string) error {
	return errors.New("invalid Fabric response: " + reason)
}

type responseReadError struct{ cause error }

func (e *responseReadError) Error() string { return "unable to read Fabric response" }
func (e *responseReadError) Unwrap() error { return e.cause }
