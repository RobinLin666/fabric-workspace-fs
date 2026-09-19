package fabric

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strings"

	"fabric-workspace-fs/internal/fserrors"
)

type Definition struct {
	Format string
	Parts  []Part
	Extra  map[string]json.RawMessage
}

type Part struct {
	Path        string
	Payload     string
	PayloadType string
	Extra       map[string]json.RawMessage
}

func (d Definition) MarshalJSON() ([]byte, error) {
	fields := copyExtra(d.Extra, "format", "parts")
	if d.Format != "" {
		value, _ := json.Marshal(d.Format)
		fields["format"] = value
	}
	parts, err := json.Marshal(d.Parts)
	if err != nil {
		return nil, fmt.Errorf("invalid definition parts: %w", fs.ErrInvalid)
	}
	fields["parts"] = parts
	body, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("invalid definition metadata: %w", fs.ErrInvalid)
	}
	return body, nil
}

func (d *Definition) UnmarshalJSON(body []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return invalidResponse("definition must be an object")
	}
	var decoded Definition
	if value, ok := fields["format"]; ok {
		if string(value) == "null" || json.Unmarshal(value, &decoded.Format) != nil {
			return invalidResponse("invalid definition format")
		}
	}
	if json.Unmarshal(fields["parts"], &decoded.Parts) != nil || decoded.Parts == nil {
		return invalidResponse("definition must contain a parts array")
	}
	decoded.Extra = copyExtra(fields, "format", "parts")
	if err := validateDefinition(decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

func (p Part) MarshalJSON() ([]byte, error) {
	fields := copyExtra(p.Extra, "path", "payload", "payloadType")
	for key, value := range map[string]string{"path": p.Path, "payload": p.Payload, "payloadType": p.PayloadType} {
		encoded, _ := json.Marshal(value)
		fields[key] = encoded
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("invalid definition part metadata: %w", fs.ErrInvalid)
	}
	return body, nil
}

func (p *Part) UnmarshalJSON(body []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return invalidResponse("definition part must be an object")
	}
	var decoded Part
	for key, target := range map[string]*string{"path": &decoded.Path, "payload": &decoded.Payload, "payloadType": &decoded.PayloadType} {
		value, ok := fields[key]
		if !ok || string(value) == "null" || json.Unmarshal(value, target) != nil {
			return invalidResponse("definition part is missing a string field")
		}
	}
	decoded.Extra = copyExtra(fields, "path", "payload", "payloadType")
	if err := validatePart(decoded); err != nil {
		return err
	}
	*p = decoded
	return nil
}

func copyExtra(fields map[string]json.RawMessage, known ...string) map[string]json.RawMessage {
	extra := make(map[string]json.RawMessage, len(fields))
	for key, value := range fields {
		skip := false
		for _, name := range known {
			if key == name {
				skip = true
				break
			}
		}
		if !skip {
			extra[key] = value
		}
	}
	return extra
}

func validatePart(part Part) error {
	if !fs.ValidPath(part.Path) || part.Path == "." || strings.ContainsAny(part.Path, "\\\x00") || part.PayloadType == "" {
		return fmt.Errorf("invalid definition part: %w", fs.ErrInvalid)
	}
	return nil
}

func validateDefinition(definition Definition) error {
	seen := make(map[string]bool, len(definition.Parts))
	for _, part := range definition.Parts {
		if err := validatePart(part); err != nil {
			return err
		}
		if seen[part.Path] {
			return fmt.Errorf("duplicate definition part path: %w", fs.ErrInvalid)
		}
		seen[part.Path] = true
	}
	return nil
}

func (p Part) Decode() ([]byte, error) {
	if p.PayloadType != "InlineBase64" {
		return nil, fserrors.ErrUnsupported
	}
	data, err := base64.StdEncoding.Strict().DecodeString(p.Payload)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 definition payload: %w", fs.ErrInvalid)
	}
	return data, nil
}

// SetData replaces only the payload and its encoding; path and metadata survive.
func (p *Part) SetData(data []byte) {
	p.Payload = base64.StdEncoding.EncodeToString(data)
	p.PayloadType = "InlineBase64"
}

func notebookDefinition(definition *Definition) error {
	if definition.Format != "" && definition.Format != "ipynb" {
		return fserrors.ErrUnsupported
	}
	if err := validateDefinition(*definition); err != nil {
		return err
	}
	for _, part := range definition.Parts {
		if strings.HasSuffix(part.Path, ".ipynb") {
			// GetDefinition examples omit format; updateDefinition defaults to
			// fabricGitSource unless ipynb is explicit in the definition body.
			definition.Format = "ipynb"
			return nil
		}
	}
	return fmt.Errorf("notebook definition has no ipynb part: %w", fs.ErrInvalid)
}

// GetDefinition uses workload-specific endpoints, not generic item definitions.
// Notebook format is always ipynb; environment format must be empty.
func (c *Client) GetDefinition(ctx context.Context, workspace, item, kind, format string) (Definition, error) {
	if err := c.ready(ctx, workspace, item); err != nil {
		return Definition{}, err
	}
	var collection string
	query := make(url.Values)
	switch kind {
	case "Notebook":
		if format != "" && format != "ipynb" {
			return Definition{}, fserrors.ErrUnsupported
		}
		collection = "notebooks"
		query.Set("format", "ipynb")
	case "Environment":
		if format != "" {
			return Definition{}, fserrors.ErrUnsupported
		}
		collection = "environments"
	default:
		return Definition{}, fserrors.ErrUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	target := c.http.URL("/v1/workspaces/"+workspace+"/"+collection+"/"+item+"/getDefinition", query)
	resp, err := c.operation(ctx, target, nil, true)
	if err != nil {
		return Definition{}, err
	}
	var envelope struct {
		Definition *Definition `json:"definition"`
	}
	if err := readJSON(resp, c.maxDefinitionBytes, &envelope); err != nil {
		return Definition{}, err
	}
	if envelope.Definition == nil {
		return Definition{}, invalidResponse("missing definition object")
	}
	if kind == "Notebook" {
		if err := notebookDefinition(envelope.Definition); err != nil {
			return Definition{}, err
		}
	}
	return *envelope.Definition, nil
}

// UpdateNotebook sends format in the body and deliberately omits updateMetadata
// and If-Match, neither of which provides notebook content concurrency control.
func (c *Client) UpdateNotebook(ctx context.Context, workspace, item string, definition Definition) error {
	if err := c.ready(ctx, workspace, item); err != nil {
		return err
	}
	if err := notebookDefinition(&definition); err != nil {
		return err
	}
	if definitionExceeds(definition, c.maxDefinitionBytes) {
		return fserrors.ErrTooLarge
	}
	body, err := json.Marshal(struct {
		Definition Definition `json:"definition"`
	}{Definition: definition})
	if err != nil {
		return fmt.Errorf("unable to encode notebook definition: %w", fs.ErrInvalid)
	}
	if int64(len(body)) > c.maxDefinitionBytes {
		return fserrors.ErrTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	target := c.http.URL("/v1/workspaces/"+workspace+"/notebooks/"+item+"/updateDefinition", nil)
	resp, err := c.operation(ctx, target, body, false)
	if err != nil {
		return err
	}
	if resp != nil {
		_, err = readResponse(resp, c.maxDefinitionBytes)
	}
	return err
}

// This lower bound avoids allocating an encoded copy of already oversized
// input. The final serialized envelope is checked as well, including escaping
// and the full base64 payload rather than just its decoded size.
func definitionExceeds(definition Definition, limit int64) bool {
	remaining := limit
	add := func(size int) bool {
		if int64(size) > remaining {
			return true
		}
		remaining -= int64(size)
		return false
	}
	if add(len(definition.Format)) {
		return true
	}
	for key, value := range definition.Extra {
		if add(len(key)) || add(len(value)) {
			return true
		}
	}
	for _, part := range definition.Parts {
		if add(len(part.Path)) || add(len(part.Payload)) || add(len(part.PayloadType)) {
			return true
		}
		for key, value := range part.Extra {
			if add(len(key)) || add(len(value)) {
				return true
			}
		}
	}
	return false
}

func operationHeaders(body []byte) http.Header {
	headers := jsonHeaders()
	if body != nil {
		headers.Set("Content-Type", "application/json")
	}
	return headers
}
