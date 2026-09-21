package workspacefs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	pythonMetadataHeaderStart = "# fabric-workspace-fs metadata"
	pythonMetadataHeaderEnd   = "# ---"
)

type pythonNotebookIdentity struct {
	ID             string
	WorkspaceID    string
	DisplayName    string
	RemotePartPath string
}

func notebookToPython(data []byte, identity pythonNotebookIdentity) ([]byte, error) {
	var notebook struct {
		Cells []struct {
			Type   string          `json:"cell_type"`
			Source json.RawMessage `json:"source"`
		} `json:"cells"`
		Metadata map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(data, &notebook); err != nil {
		return nil, err
	}
	var out strings.Builder
	writePythonMetadataHeader(&out, notebook.Metadata, identity)
	for i, cell := range notebook.Cells {
		if i > 0 {
			out.WriteByte('\n')
		}
		if cell.Type == "markdown" {
			out.WriteString("# %% [markdown]\n")
			for _, line := range notebookSourceLines(cell.Source) {
				if line == "\n" || line == "" {
					out.WriteString("#\n")
					continue
				}
				out.WriteString("# ")
				out.WriteString(line)
				if !strings.HasSuffix(line, "\n") {
					out.WriteByte('\n')
				}
			}
			continue
		}
		out.WriteString("# %%\n")
		for _, line := range notebookSourceLines(cell.Source) {
			out.WriteString(line)
			if line != "" && !strings.HasSuffix(line, "\n") {
				out.WriteByte('\n')
			}
		}
	}
	return []byte(out.String()), nil
}

func pythonToNotebook(data, baseData []byte) ([]byte, error) {
	var notebook map[string]any
	if err := json.Unmarshal(baseData, &notebook); err != nil {
		return nil, err
	}
	notebook["nbformat"] = float64(4)
	if _, ok := notebook["nbformat_minor"]; !ok {
		notebook["nbformat_minor"] = float64(5)
	}
	notebook["cells"] = pythonCells(data)
	return json.Marshal(notebook)
}

func notebookSourceLines(raw json.RawMessage) []string {
	var lines []string
	if err := json.Unmarshal(raw, &lines); err == nil {
		return lines
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if text == "" {
			return nil
		}
		return splitNotebookSource(text)
	}
	return nil
}

func splitNotebookSource(text string) []string {
	if text == "" {
		return nil
	}
	parts := strings.SplitAfter(text, "\n")
	if parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func pythonCells(data []byte) []map[string]any {
	lines := splitNotebookSource(string(bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))))
	lines = stripPythonMetadataHeader(lines)
	var cells []map[string]any
	currentType := "code"
	var current []string
	seenMarker := false
	flush := func() {
		if !seenMarker && len(current) == 0 {
			return
		}
		cell := map[string]any{
			"cell_type": currentType,
			"metadata":  map[string]any{},
			"source":    append([]string(nil), current...),
		}
		if currentType == "code" {
			cell["execution_count"] = nil
			cell["outputs"] = []any{}
		}
		cells = append(cells, cell)
		current = nil
	}
	for _, line := range lines {
		marker, markdown := pythonCellMarker(line)
		if marker {
			if len(current) > 0 && current[len(current)-1] == "\n" {
				current = current[:len(current)-1]
			}
			if seenMarker || len(current) > 0 {
				flush()
			}
			seenMarker = true
			if markdown {
				currentType = "markdown"
			} else {
				currentType = "code"
			}
			continue
		}
		if currentType == "markdown" {
			current = append(current, uncommentMarkdownLine(line))
		} else {
			current = append(current, line)
		}
	}
	flush()
	return cells
}

func writePythonMetadataHeader(out *strings.Builder, metadata map[string]any, identity pythonNotebookIdentity) {
	values := []struct {
		key   string
		value string
	}{
		{"id", identity.ID},
		{"workspaceId", identity.WorkspaceID},
		{"displayName", identity.DisplayName},
		{"remotePartPath", identity.RemotePartPath},
		{"language", notebookLanguage(metadata)},
		{"kernelName", nestedString(metadata, "kernelspec", "name")},
		{"kernelDisplayName", nestedString(metadata, "kernelspec", "display_name")},
		{"defaultLakehouse", firstMetadataString(metadata, "defaultLakehouse", "default_lakehouse", "defaultLakehouseId", "default_lakehouse_id")},
		{"defaultLakehouseName", firstMetadataString(metadata, "defaultLakehouseName", "default_lakehouse_name")},
		{"defaultLakehouseWorkspaceId", firstMetadataString(metadata, "defaultLakehouseWorkspaceId", "default_lakehouse_workspace_id")},
		{"environmentId", firstMetadataString(metadata, "environmentId", "environment_id")},
		{"environmentName", firstMetadataString(metadata, "environmentName", "environment_name")},
	}
	out.WriteString(pythonMetadataHeaderStart)
	out.WriteByte('\n')
	for _, item := range values {
		if item.value == "" {
			continue
		}
		encoded, _ := json.Marshal(item.value)
		fmt.Fprintf(out, "# %s: %s\n", item.key, encoded)
	}
	out.WriteString(pythonMetadataHeaderEnd)
	out.WriteString("\n\n")
}

func stripPythonMetadataHeader(lines []string) []string {
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != pythonMetadataHeaderStart {
		return lines
	}
	for i, line := range lines[1:] {
		if strings.TrimSpace(line) == pythonMetadataHeaderEnd {
			rest := lines[i+2:]
			for len(rest) > 0 && strings.TrimSpace(rest[0]) == "" {
				rest = rest[1:]
			}
			return rest
		}
	}
	return lines
}

func notebookLanguage(metadata map[string]any) string {
	for _, value := range []string{
		nestedString(metadata, "kernelspec", "language"),
		nestedString(metadata, "language_info", "name"),
		firstMetadataString(metadata, "language", "languageName"),
	} {
		if value != "" {
			return value
		}
	}
	return ""
}

func nestedString(values map[string]any, path ...string) string {
	var current any = values
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = object[key]
		if !ok {
			return ""
		}
	}
	text, _ := current.(string)
	return text
}

func firstMetadataString(metadata map[string]any, keys ...string) string {
	wanted := make(map[string]bool, len(keys))
	for _, key := range keys {
		wanted[normalizeMetadataKey(key)] = true
	}
	return findMetadataString(metadata, wanted)
}

func findMetadataString(value any, wanted map[string]bool) string {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if wanted[normalizeMetadataKey(key)] {
				if text, ok := child.(string); ok {
					return text
				}
			}
		}
		for _, child := range typed {
			if text := findMetadataString(child, wanted); text != "" {
				return text
			}
		}
	case []any:
		for _, child := range typed {
			if text := findMetadataString(child, wanted); text != "" {
				return text
			}
		}
	}
	return ""
}

func normalizeMetadataKey(key string) string {
	key = strings.ToLower(key)
	key = strings.ReplaceAll(key, "_", "")
	key = strings.ReplaceAll(key, "-", "")
	return key
}

func pythonCellMarker(line string) (bool, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "# %%") {
		return false, false
	}
	return true, strings.Contains(trimmed, "[markdown]")
}

func uncommentMarkdownLine(line string) string {
	trimmed := strings.TrimPrefix(line, "#")
	if trimmed != line {
		if strings.HasPrefix(trimmed, " ") {
			return trimmed[1:]
		}
		if strings.HasPrefix(trimmed, "\t") {
			return trimmed[1:]
		}
		return trimmed
	}
	return line
}
