package fabric

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"fabric-workspace-fs/internal/transport"
)

// Unlike operation's read-only definition POST, a create POST must never be
// retried. Reuse its target validation and delays without enabling safeRetry
// on the write or handing untrusted operation locations to an SDK poller.
// https://learn.microsoft.com/rest/api/fabric/articles/long-running-operation
func (c *Client) createOperation(ctx context.Context, target string, body []byte) (*http.Response, error) {
	resp, err := c.http.Request(ctx, http.MethodPost, target, operationHeaders(body), body, false)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return resp, nil
	}
	if resp.StatusCode != http.StatusAccepted {
		return nil, mutationUnexpectedResponse(resp)
	}
	headers := resp.Header.Clone()
	if _, err := readResponse(resp, min(c.maxDefinitionBytes, maxMutationResponseBytes)); err != nil {
		return nil, err
	}
	poll, err := c.operationTarget(headers.Get("Location"), headers.Get("x-ms-operation-id"))
	if err != nil {
		return nil, err
	}
	if poll.result {
		return nil, invalidResponse("accepted create points directly to a result")
	}
	for {
		if err := c.waitForPoll(ctx, headers.Get("Retry-After")); err != nil {
			return nil, err
		}
		resp, err = c.http.Request(ctx, http.MethodGet, poll.url, jsonHeaders(), nil, true)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
			return nil, mutationUnexpectedResponse(resp)
		}
		statusCode := resp.StatusCode
		headers = resp.Header.Clone()
		data, err := readResponse(resp, min(c.maxDefinitionBytes, maxMutationResponseBytes))
		if err != nil {
			return nil, err
		}
		next := poll
		if location, id := headers.Get("Location"), headers.Get("x-ms-operation-id"); location != "" || id != "" {
			if id != "" && !strings.EqualFold(id, poll.id) {
				return nil, invalidResponse("operation ID changed during creation")
			}
			next, err = c.operationTarget(location, poll.id)
			if err != nil {
				return nil, err
			}
		}
		var state struct {
			Status string `json:"status"`
			Error  struct {
				Code      string `json:"errorCode"`
				RequestID string `json:"requestId"`
			} `json:"error"`
		}
		if len(data) == 0 && statusCode == http.StatusAccepted {
			state.Status = "Running"
		} else if json.Unmarshal(data, &state) != nil || state.Status == "" {
			return nil, invalidResponse("missing create operation status")
		}
		switch state.Status {
		case "NotStarted", "Running":
			if next.result {
				return nil, invalidResponse("pending create points to a result")
			}
			poll = next
		case "Failed", "Cancelled", "Canceled":
			return nil, &OperationError{
				Status: state.Status, Code: mutationIdentifier(state.Error.Code, resp),
				RequestID: mutationIdentifier(state.Error.RequestID, resp),
			}
		case "Succeeded":
			if statusCode == http.StatusAccepted {
				return nil, invalidResponse("accepted create is not a final success")
			}
			resultURL := c.http.URL("/v1/operations/"+next.id+"/result", nil)
			resp, err := c.http.Request(ctx, http.MethodGet, resultURL, jsonHeaders(), nil, true)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
				return nil, mutationUnexpectedResponse(resp)
			}
			return resp, nil
		default:
			return nil, invalidResponse("unknown create operation status")
		}
	}
}

func mutationUnexpectedResponse(resp *http.Response) error {
	err := unexpectedResponse(resp)
	if status, ok := err.(*transport.HTTPError); ok {
		status.RequestID = mutationIdentifier(status.RequestID, resp)
	}
	return err
}

func mutationIdentifier(value string, resp *http.Response) string {
	if len(value) > 128 {
		return ""
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.') {
			return ""
		}
	}
	if resp.Request != nil {
		token := strings.TrimPrefix(resp.Request.Header.Get("Authorization"), "Bearer ")
		if token != "" && strings.Contains(value, token) {
			return ""
		}
	}
	return value
}
