package fabric

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fabric-workspace-fs/internal/transport"
)

type OperationError struct {
	Status    string
	Code      string
	RequestID string
}

func (e *OperationError) Error() string {
	switch e.Status {
	case "Cancelled", "Canceled":
		return "Fabric operation was cancelled"
	default:
		return "Fabric operation failed"
	}
}

type operationTarget struct {
	url    string
	id     string
	result bool
}

func (c *Client) operationTarget(location, id string) (operationTarget, error) {
	if strings.HasPrefix(location, "//") {
		return operationTarget{}, invalidResponse("scheme-relative operation URL")
	}
	if id != "" && ValidateID(id) != nil {
		return operationTarget{}, invalidResponse("invalid operation ID")
	}
	if location == "" {
		if id == "" {
			return operationTarget{}, invalidResponse("accepted operation has no polling target")
		}
		location = c.http.URL("/v1/operations/"+id, nil)
	}
	// Some Fabric deployments return a regional analysis.windows.net Location.
	// Never follow it with a bearer token. The documented operation-ID flow
	// instead reconstructs the request on the configured public API origin.
	resolved, resolveErr := c.http.Resolve(location)
	if resolveErr != nil {
		regional, err := url.Parse(location)
		if err != nil || id == "" || regional.Scheme != "https" || regional.User != nil ||
			regional.Opaque != "" || strings.Contains(location, "#") || regional.Port() != "" ||
			regional.RawQuery != "" || regional.ForceQuery ||
			!strings.HasSuffix(strings.ToLower(regional.Hostname()), ".analysis.windows.net") {
			return operationTarget{}, resolveErr
		}
		expected := "/v1/operations/" + id
		if regional.EscapedPath() != expected && regional.EscapedPath() != expected+"/result" {
			return operationTarget{}, invalidResponse("regional operation path does not match its ID")
		}
		canonical := c.http.URL(regional.EscapedPath(), nil)
		resolved, err = c.http.Resolve(canonical)
		if err != nil {
			return operationTarget{}, err
		}
	}
	u, _ := url.Parse(resolved)
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) > 0 && parts[0] == "v1" {
		parts = parts[1:]
	}
	if (len(parts) != 2 && len(parts) != 3) || parts[0] != "operations" || ValidateID(parts[1]) != nil ||
		(len(parts) == 3 && parts[2] != "result") {
		return operationTarget{}, invalidResponse("invalid operation URL")
	}
	if id != "" && !strings.EqualFold(parts[1], id) {
		return operationTarget{}, invalidResponse("operation ID changed")
	}
	result := len(parts) == 3
	path := "/v1/operations/" + parts[1]
	if result {
		path += "/result"
	}
	return operationTarget{url: c.http.URL(path, nil), id: parts[1], result: result}, nil
}

// operation follows the state/result protocol documented at:
// https://learn.microsoft.com/rest/api/fabric/articles/long-running-operation
// Successful writes do not require (and may not have) a result resource.
func (c *Client) operation(ctx context.Context, target string, body []byte, readResult bool) (*http.Response, error) {
	resp, err := c.http.Request(ctx, http.MethodPost, target, operationHeaders(body), body, readResult)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return resp, nil
	}
	if resp.StatusCode != http.StatusAccepted {
		return nil, unexpectedResponse(resp)
	}
	headers := resp.Header.Clone()
	if _, err := readResponse(resp, c.maxDefinitionBytes); err != nil {
		return nil, err
	}
	poll, err := c.operationTarget(headers.Get("Location"), headers.Get("x-ms-operation-id"))
	if err != nil {
		return nil, err
	}
	if poll.result {
		return nil, invalidResponse("accepted operation points directly to a result")
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
			return nil, unexpectedResponse(resp)
		}
		statusCode := resp.StatusCode
		headers = resp.Header.Clone()
		data, err := readResponse(resp, min(c.maxDefinitionBytes, 1<<20))
		if err != nil {
			return nil, err
		}
		next := poll
		if location, id := headers.Get("Location"), headers.Get("x-ms-operation-id"); location != "" || id != "" {
			if id != "" && !strings.EqualFold(id, poll.id) {
				return nil, invalidResponse("operation ID changed during polling")
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
			return nil, invalidResponse("missing operation status")
		}
		switch state.Status {
		case "NotStarted", "Running":
			if next.result {
				return nil, invalidResponse("pending operation points to a result")
			}
			poll = next
		case "Failed", "Cancelled", "Canceled":
			return nil, &OperationError{Status: state.Status, Code: state.Error.Code, RequestID: state.Error.RequestID}
		case "Succeeded":
			if statusCode == http.StatusAccepted {
				return nil, invalidResponse("accepted operation is not a final success")
			}
			if !readResult {
				return nil, nil
			}
			if !next.result {
				u, _ := url.Parse(next.url)
				u.Path += "/result"
				u.RawPath = ""
				next.url = u.String()
			}
			resp, err := c.http.Request(ctx, http.MethodGet, next.url, jsonHeaders(), nil, true)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
				return nil, unexpectedResponse(resp)
			}
			return resp, nil
		default:
			return nil, invalidResponse("unknown operation status")
		}
	}
}

func (c *Client) waitForPoll(ctx context.Context, header string) error {
	delay := c.pollInterval
	if header != "" {
		serverDelay, err := transport.RetryAfter(header, time.Now())
		if err != nil {
			return err
		}
		delay = max(delay, serverDelay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
