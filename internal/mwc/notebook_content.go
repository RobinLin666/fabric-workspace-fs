package mwc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strings"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
)

// GetNotebookContent reads the Notebook content endpoint used by fntk. The
// returned ETag is informational: fntk's endpoint contract has no verified
// conditional PUT support.
func (c *Client) GetNotebookContent(ctx context.Context, workspaceID, itemID string) ([]byte, string, error) {
	target, err := notebookTarget(workspaceID, itemID)
	if err != nil {
		return nil, "", err
	}
	r, err := c.route(ctx, target)
	if err != nil {
		return nil, "", err
	}
	resp, err := r.grant.http.Request(ctx, http.MethodGet, notebookContentURL(r, target), nil, nil, true)
	if err != nil {
		return nil, "", resourceError(err)
	}
	if err := requireOK(resp); err != nil {
		return nil, "", err
	}
	body, err := readBody(ctx, resp, c.maxFileSize)
	if err != nil {
		return nil, "", err
	}
	if len(body) == 0 {
		return nil, "", invalidResponse("Notebook content is empty")
	}
	// Some workload deployments serialize the ipynb document as a JSON string
	// rather than returning its object directly. Normalize that transport
	// envelope before handing the same notebook object fntk consumes to FUSE.
	var wrapped string
	if err := json.Unmarshal(body, &wrapped); err == nil {
		if !json.Valid([]byte(wrapped)) {
			return nil, "", invalidResponse("Notebook content string is not JSON")
		}
		body = []byte(wrapped)
	}
	return body, resp.Header.Get("ETag"), nil
}

// PutNotebookContent writes the Notebook content endpoint used by fntk. The
// endpoint's documented behavior is an unconditional PUT, so callers must not
// mistake the returned ETag for atomic compare-and-swap protection.
func (c *Client) PutNotebookContent(ctx context.Context, workspaceID, itemID string, data []byte) (string, error) {
	target, err := notebookTarget(workspaceID, itemID)
	if err != nil {
		return "", err
	}
	if int64(len(data)) > c.maxFileSize {
		return "", fserrors.ErrTooLarge
	}
	r, err := c.route(ctx, target)
	if err != nil {
		return "", err
	}
	resp, err := r.grant.http.Request(ctx, http.MethodPut, notebookContentURL(r, target), http.Header{
		"Content-Type": {"application/json"},
	}, data, true)
	if err != nil {
		return "", resourceError(err)
	}
	if err := requireOK(resp); err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return resp.Header.Get("ETag"), nil
}

func notebookTarget(workspaceID, itemID string) (resources.Target, error) {
	target := resources.Target{WorkspaceID: strings.ToLower(workspaceID), ItemID: strings.ToLower(itemID), Kind: "Notebook"}
	if err := resources.ValidateTarget(target); err != nil {
		return resources.Target{}, fmt.Errorf("invalid Notebook content target: %w", fs.ErrInvalid)
	}
	return target, nil
}

func notebookContentURL(r route, target resources.Target) string {
	return r.grant.http.URL("/webapi/capacities/"+r.grant.capacity+
		"/workloads/Notebook/Data/Automatic/api/workspaces/"+strings.ToLower(target.WorkspaceID)+
		"/artifacts/"+strings.ToLower(target.ItemID)+"/content", nil)
}
