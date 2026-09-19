package transport

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const maxErrorBytes = 64 << 10

type HTTPError struct {
	StatusCode int
	Code       string
	Message    string
	RequestID  string
	Method     string
	Path       string
}

// Error deliberately excludes service messages, codes, and IDs. Those are
// untrusted response data and can contain reflected credentials or query values.
func (e *HTTPError) Error() string {
	method := e.Method
	if !validHeaderName(method) || len(method) > 16 {
		method = "HTTP"
	}
	path := "/"
	if parsed, err := url.Parse(e.Path); err == nil && !hasControl(parsed.Path) {
		path = parsed.EscapedPath()
	}
	status := http.StatusText(e.StatusCode)
	if status == "" {
		status = "HTTP error"
	}
	return fmt.Sprintf("%s %s: HTTP %d %s", method, path, e.StatusCode, status)
}

func responseError(resp *http.Response, req *http.Request, token string) *HTTPError {
	body := boundedErrorBody(resp.Body)
	var wire struct {
		ErrorCode string `json:"errorCode"`
		Code      string `json:"code"`
		RequestID string `json:"requestId"`
		Error     struct {
			Code      string `json:"code"`
			ErrorCode string `json:"errorCode"`
			RequestID string `json:"requestId"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &wire)
	code := firstNonempty(resp.Header.Get("x-ms-error-code"), wire.ErrorCode, wire.Code, wire.Error.ErrorCode, wire.Error.Code)
	if code == "" {
		var storage struct {
			Code string `xml:"Code"`
		}
		if xml.Unmarshal(body, &storage) == nil {
			code = storage.Code
		}
	}
	requestID := firstNonempty(
		resp.Header.Get("x-ms-request-id"), resp.Header.Get("request-id"),
		resp.Header.Get("requestid"), wire.RequestID, wire.Error.RequestID,
		resp.Header.Get("x-ms-correlation-request-id"),
	)
	path := req.URL.EscapedPath()
	if token != "" {
		path = strings.ReplaceAll(path, token, "[redacted]")
	}
	return &HTTPError{
		StatusCode: resp.StatusCode,
		Code:       safeIdentifier(code, token, req.URL.Query()),
		Message:    http.StatusText(resp.StatusCode),
		RequestID:  safeIdentifier(requestID, token, req.URL.Query()),
		Method:     req.Method,
		Path:       path,
	}
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func safeIdentifier(value, token string, query url.Values) string {
	if len(value) > 128 || (token != "" && strings.Contains(value, token)) {
		return ""
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.') {
			return ""
		}
	}
	for _, values := range query {
		for _, secret := range values {
			if len(secret) >= 4 && strings.Contains(value, secret) {
				return ""
			}
		}
	}
	return value
}
