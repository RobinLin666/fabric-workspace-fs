package cachepolicy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"fabric-workspace-fs/internal/fabric"
)

// MaxConfigBytes bounds the entire JSON file, including whitespace.
const MaxConfigBytes = 1 << 20

// Load reads a mount-time JSON policy with the same size bound as Parse.
func Load(path string) (*Policy, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open cache config: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read cache config: %w", err)
	}
	return Parse(data)
}

// Parse accepts the strict schema documented by this package. It rejects
// unknown or duplicate keys, invalid scopes/durations/UUIDs, trailing JSON,
// invalid UTF-8, and inputs larger than MaxConfigBytes.
func Parse(data []byte) (*Policy, error) {
	if len(data) > MaxConfigBytes {
		return nil, invalid("cache config", "exceeds the 1 MiB limit")
	}
	if !utf8.Valid(data) {
		return nil, invalid("cache config", "must be UTF-8 JSON")
	}
	fields, err := object(data, "cache config")
	if err != nil {
		return nil, err
	}
	policy := Default()
	for key, data := range fields {
		path := "cache config." + key
		switch key {
		case "defaults":
			var fields map[string]json.RawMessage
			fields, err = object(data, path)
			if err == nil {
				policy.defaults, err = parseValues(fields, path, true, "")
			}
		case "types":
			policy.types, err = parseTypes(data, path)
		case "workspaces":
			policy.workspaces, err = parseWorkspaces(data, path)
		default:
			err = invalid(path, "unknown key")
		}
		if err != nil {
			return nil, err
		}
	}
	return policy, nil
}

func parseTypes(data []byte, path string) (map[string]typeOverrides, error) {
	fields, err := object(data, path)
	if err != nil {
		return nil, err
	}
	result := make(map[string]typeOverrides, len(fields))
	for kind, data := range fields {
		child := path + "." + kind
		if !validType(kind) {
			return nil, invalid(child, "unknown item type; use Notebook, Lakehouse, or Environment")
		}
		fields, err := object(data, child)
		if err != nil {
			return nil, err
		}
		result[kind], err = parseTypeFields(fields, child, kind)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func parseTypeFields(fields map[string]json.RawMessage, path, kind string) (typeOverrides, error) {
	var result typeOverrides
	if data, ok := fields["surfaces"]; ok {
		surfaces, err := object(data, path+".surfaces")
		if err != nil {
			return result, err
		}
		result.surfaces = make(map[string]overrides, len(surfaces))
		for name, data := range surfaces {
			child := path + ".surfaces." + name
			if !validSurface(kind, name) {
				return result, invalid(child, "unknown surface for %s", kind)
			}
			fields, err := object(data, child)
			if err != nil {
				return result, err
			}
			result.surfaces[name], err = parseValues(fields, child, false, kind)
			if err != nil {
				return result, err
			}
		}
		delete(fields, "surfaces")
	}
	var err error
	result.values, err = parseValues(fields, path, false, kind)
	return result, err
}

func parseWorkspaces(data []byte, path string) (map[string]workspaceOverrides, error) {
	fields, err := object(data, path)
	if err != nil {
		return nil, err
	}
	result := make(map[string]workspaceOverrides, len(fields))
	for id, data := range fields {
		child := path + "." + id
		if err := fabric.ValidateID(id); err != nil {
			return nil, invalid(child, "workspace key must be a UUID")
		}
		id = strings.ToLower(id)
		if _, exists := result[id]; exists {
			return nil, invalid(child, "duplicate workspace UUID")
		}
		fields, err := object(data, child)
		if err != nil {
			return nil, err
		}
		var workspace workspaceOverrides
		if data, ok := fields["types"]; ok {
			workspace.types, err = parseTypes(data, child+".types")
			if err != nil {
				return nil, err
			}
			delete(fields, "types")
		}
		if data, ok := fields["items"]; ok {
			workspace.items, err = parseItems(data, child+".items")
			if err != nil {
				return nil, err
			}
			delete(fields, "items")
		}
		workspace.values, err = parseValues(fields, child, true, "")
		if err != nil {
			return nil, err
		}
		result[id] = workspace
	}
	return result, nil
}

func parseItems(data []byte, path string) (map[string]itemOverrides, error) {
	fields, err := object(data, path)
	if err != nil {
		return nil, err
	}
	result := make(map[string]itemOverrides, len(fields))
	for id, data := range fields {
		child := path + "." + id
		if err := fabric.ValidateID(id); err != nil {
			return nil, invalid(child, "item key must be a UUID")
		}
		id = strings.ToLower(id)
		if _, exists := result[id]; exists {
			return nil, invalid(child, "duplicate item UUID")
		}
		fields, err := object(data, child)
		if err != nil {
			return nil, err
		}
		var item itemOverrides
		typeData, exists := fields["type"]
		if !exists || json.Unmarshal(typeData, &item.kind) != nil || !validType(item.kind) {
			return nil, invalid(child+".type", "item requires type Notebook, Lakehouse, or Environment")
		}
		delete(fields, "type")
		item.typeOverrides, err = parseTypeFields(fields, child, item.kind)
		if err != nil {
			return nil, err
		}
		result[id] = item
	}
	return result, nil
}

func parseValues(fields map[string]json.RawMessage, path string, catalog bool, kind string) (overrides, error) {
	result := make(overrides, len(fields))
	for name, data := range fields {
		child := path + "." + name
		switch name {
		case "catalog":
			if !catalog {
				return nil, invalid(child, "catalog is allowed only in defaults or a workspace")
			}
		case "attr", "directory", "definition", "content", "kernelAttr", "kernelEntry", "kernelNegative":
		default:
			return nil, invalid(child, "unknown key")
		}
		if kind == "Lakehouse" && name == "content" {
			return nil, invalid(child, "Lakehouse Files/Tables use streaming reads, not a shared content TTL cache")
		}
		if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
			continue
		}
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return nil, invalid(child, "must be a duration string or null")
		}
		duration, err := time.ParseDuration(text)
		if err != nil || duration < 0 || strings.HasPrefix(text, "-") {
			return nil, invalid(child, "must be a non-negative Go duration string (for example 2m or 0s)")
		}
		result[name] = duration
	}
	return result, nil
}

func validType(kind string) bool {
	return kind == "Notebook" || kind == "Lakehouse" || kind == "Environment"
}

func validSurface(kind, name string) bool {
	switch kind {
	case "Notebook":
		return name == "content" || name == "builtin"
	case "Lakehouse":
		return name == "Files" || name == "Tables"
	case "Environment":
		return name == "resources" || name == "definition"
	default:
		return false
	}
}

// Reading objects explicitly keeps names case-sensitive and detects duplicates,
// unlike unmarshaling into structs or maps.
func object(data []byte, path string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, invalid(path, "must be a JSON object")
	}
	result := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, invalid(path, "invalid JSON: %v", err)
		}
		key, ok := token.(string)
		if !ok {
			return nil, invalid(path, "object keys must be strings")
		}
		if _, exists := result[key]; exists {
			return nil, invalid(path+"."+key, "duplicate key")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, invalid(path+"."+key, "invalid JSON: %v", err)
		}
		result[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, invalid(path, "invalid JSON: %v", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, invalid(path, "unexpected trailing JSON")
	}
	return result, nil
}

func invalid(path, format string, args ...any) error {
	return fmt.Errorf("%s: %s: %w", path, fmt.Sprintf(format, args...), fs.ErrInvalid)
}
