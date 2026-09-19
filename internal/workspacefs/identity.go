package workspacefs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"

	"fabric-workspace-fs/internal/fabric"
)

const identityFileName = ".fabric.json"

func identityContainer(e Entry) bool {
	return managedContainer(e) || e.Kind == Notebook || e.Kind == Lakehouse || e.Kind == Environment
}

func (s *FS) identityEntry(ctx context.Context, parent Entry) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	if parent.Item.Type == "Notebook" {
		parent.RemotePartPath = ""
		if snapshot, ok := s.peekSnapshot(parent, s.policy(parent).Definition); ok {
			parent.RemotePartPath = snapshot.notebook
		}
	}
	entry := Entry{
		Name: identityFileName, Kind: IdentityFile, Workspace: parent.Workspace,
		Label: parent.Label, Item: parent.Item, Part: identityFileName, Modified: parent.Modified,
		Folder: parent.Folder, RemotePartPath: parent.RemotePartPath,
		ValidUntil: parent.ValidUntil,
	}
	data, err := identityData(entry)
	entry.Size = int64(len(data))
	return entry, err
}

func identityData(e Entry) ([]byte, error) {
	if e.Kind != IdentityFile {
		return nil, fs.ErrInvalid
	}
	id := e.Item.ID
	kind, displayName, workspace := e.Item.Type, e.Item.DisplayName, e.Workspace
	if id == "" && e.Folder.ID != "" {
		id = e.Folder.ID
		kind, displayName = "Folder", e.Folder.DisplayName
	} else if id == "" {
		id = e.Workspace
		kind, displayName, workspace = "Workspace", e.Label, ""
	}
	if err := fabric.ValidateID(id); err != nil {
		return nil, fmt.Errorf("invalid identity metadata: %w", err)
	}
	data, err := json.MarshalIndent(struct {
		ID             string `json:"id"`
		Type           string `json:"type"`
		DisplayName    string `json:"displayName"`
		WorkspaceID    string `json:"workspaceId,omitempty"`
		FolderID       string `json:"folderId,omitempty"`
		ParentFolderID string `json:"parentFolderId,omitempty"`
		RemotePartPath string `json:"remotePartPath,omitempty"`
	}{strings.ToLower(id), kind, displayName, workspace, e.Item.FolderID, e.Folder.ParentFolderID, e.RemotePartPath}, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
