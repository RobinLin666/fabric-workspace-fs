package resources

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
)

// EnvironmentBinding uses only the Notebook's explicit immutable IDs. A
// logicalId or display name is not a substitute for an Environment GUID.
func EnvironmentBinding(notebookJSON []byte, workspace string) (*Target, error) {
	var document struct {
		Metadata struct {
			Dependencies map[string]json.RawMessage `json:"dependencies"`
			Trident      map[string]json.RawMessage `json:"trident"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(notebookJSON, &document); err != nil {
		return nil, fmt.Errorf("invalid Notebook binding metadata: %w", fs.ErrInvalid)
	}
	raw, found := document.Metadata.Dependencies["environment"]
	if !found || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		raw, found = document.Metadata.Trident["environment"]
	}
	if !found || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var binding struct {
		EnvironmentID string `json:"environmentId"`
		WorkspaceID   string `json:"workspaceId"`
		LogicalID     string `json:"logicalId"`
	}
	if err := json.Unmarshal(raw, &binding); err != nil {
		return nil, fmt.Errorf("invalid Environment binding object: %w", fs.ErrInvalid)
	}
	if binding.EnvironmentID == "" && binding.WorkspaceID == "" && binding.LogicalID == "" {
		return nil, nil
	}
	if binding.EnvironmentID == "" {
		return nil, fmt.Errorf("Environment binding has no immutable environmentId: %w", fs.ErrInvalid)
	}
	if binding.WorkspaceID == "" {
		binding.WorkspaceID = workspace
	}
	target := Target{WorkspaceID: binding.WorkspaceID, ItemID: binding.EnvironmentID, Kind: "Environment"}
	if err := ValidateTarget(target); err != nil {
		return nil, err
	}
	return &target, nil
}
