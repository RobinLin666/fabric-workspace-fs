package workspacefs

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/namespace"
)

type folderTree struct {
	folders        map[string]fabric.Folder
	items          map[string]fabric.Item
	folderChildren map[string][]fabric.Folder
	itemChildren   map[string][]fabric.Item
	bytes          int64
	observedAt     time.Time
}

func buildFolderTree(folders []fabric.Folder, items []fabric.Item) (*folderTree, error) {
	tree := &folderTree{
		folders: make(map[string]fabric.Folder), items: make(map[string]fabric.Item),
		folderChildren: make(map[string][]fabric.Folder), itemChildren: make(map[string][]fabric.Item),
	}
	for _, folder := range folders {
		folder.ID, folder.ParentFolderID = strings.ToLower(folder.ID), strings.ToLower(folder.ParentFolderID)
		if fabric.ValidateID(folder.ID) != nil || (folder.ParentFolderID != "" && fabric.ValidateID(folder.ParentFolderID) != nil) || folder.DisplayName == "" {
			return nil, fmt.Errorf("invalid Fabric folder identity: %w", fs.ErrInvalid)
		}
		if _, exists := tree.folders[folder.ID]; exists {
			return nil, fmt.Errorf("duplicate Fabric folder identity/parent: %w", fs.ErrInvalid)
		}
		tree.folders[folder.ID] = folder
		tree.bytes += int64(256 + len(folder.ID) + len(folder.ParentFolderID) + len(folder.DisplayName))
	}
	states := make(map[string]uint8, len(tree.folders))
	for id := range tree.folders {
		current := id
		var visited []string
		for current != "" && states[current] != 2 {
			folder, exists := tree.folders[current]
			if !exists {
				return nil, fmt.Errorf("orphaned Fabric folder parent: %w", fs.ErrInvalid)
			}
			if states[current] == 1 {
				return nil, fmt.Errorf("cyclic Fabric folder hierarchy: %w", fs.ErrInvalid)
			}
			states[current] = 1
			visited = append(visited, current)
			current = folder.ParentFolderID
		}
		for _, path := range visited {
			states[path] = 2
		}
	}
	for _, folder := range tree.folders {
		tree.folderChildren[folder.ParentFolderID] = append(tree.folderChildren[folder.ParentFolderID], folder)
	}
	for _, item := range items {
		item.ID, item.FolderID = strings.ToLower(item.ID), strings.ToLower(item.FolderID)
		if fabric.ValidateID(item.ID) != nil || item.DisplayName == "" || item.Type == "" {
			return nil, fmt.Errorf("invalid Fabric item identity: %w", fs.ErrInvalid)
		}
		if _, exists := tree.items[item.ID]; exists {
			return nil, fmt.Errorf("duplicate Fabric item identity: %w", fs.ErrInvalid)
		}
		if _, exists := tree.folders[item.ID]; exists {
			return nil, fmt.Errorf("Fabric item/folder identity collision: %w", fs.ErrInvalid)
		}
		if item.FolderID != "" {
			if _, exists := tree.folders[item.FolderID]; !exists {
				return nil, fmt.Errorf("orphaned Fabric item folder: %w", fs.ErrInvalid)
			}
		}
		tree.items[item.ID] = item
		tree.itemChildren[item.FolderID] = append(tree.itemChildren[item.FolderID], item)
		tree.bytes += int64(256 + len(item.ID) + len(item.FolderID) + len(item.DisplayName) + len(item.Description) + len(item.Type))
	}
	for _, children := range tree.folderChildren {
		sort.Slice(children, func(i, j int) bool { return children[i].ID < children[j].ID })
	}
	for _, children := range tree.itemChildren {
		sort.Slice(children, func(i, j int) bool { return children[i].ID < children[j].ID })
	}
	return tree, nil
}

func (s *FS) folderTree(ctx context.Context, workspace string) (*folderTree, error) {
	return s.catalogs.GetWithTTL(ctx, workspace, s.opts.CachePolicy.CatalogTTL(workspace), func(ctx context.Context) (*folderTree, error) {
		// Refresh both halves of a tree together rather than mixing independent
		// TTL generations after a folder move/rename or an item creation.
		folders, err := s.fabric.FabricAPI.ListFolders(ctx, workspace)
		if err != nil {
			return nil, err
		}
		items, err := s.fabric.FabricAPI.ListItems(ctx, workspace)
		if err != nil {
			return nil, err
		}
		tree, err := buildFolderTree(folders, items)
		if err != nil {
			return nil, err
		}
		tree.observedAt = s.now()
		return tree, nil
	})
}

func (s *FS) InvalidateCatalog(workspace string) {
	s.catalogs.Invalidate(workspace)
	s.fabric.items.Invalidate(workspace)
}

func managedContainer(e Entry) bool { return e.Kind == Workspace || e.Kind == FabricFolder }

func (e Entry) ParentFolderID() string {
	if e.Kind == FabricFolder {
		return e.Folder.ID
	}
	return ""
}

func itemKind(kind string) (Kind, bool) {
	switch kind {
	case "Notebook":
		return Notebook, true
	case "Lakehouse":
		return Lakehouse, true
	case "Environment":
		return Environment, true
	default:
		return 0, false
	}
}

func (s *FS) managedChildren(ctx context.Context, parent Entry) ([]Entry, error) {
	tree, err := s.folderTree(ctx, parent.Workspace)
	if err != nil {
		return nil, err
	}
	parentID := parent.ParentFolderID()
	if parent.Kind == FabricFolder {
		if _, exists := tree.folders[parentID]; !exists {
			return nil, fs.ErrNotExist
		}
	}
	var out []Entry
	var labels []namespace.Label
	for _, folder := range tree.folderChildren[parentID] {
		out = append(out, Entry{Kind: FabricFolder, Directory: true, Workspace: parent.Workspace, Folder: folder, Modified: s.start})
		labels = append(labels, namespace.Label{ID: folder.ID, DisplayName: folder.DisplayName})
	}
	for _, item := range tree.itemChildren[parentID] {
		kind, supported := itemKind(item.Type)
		if !supported {
			continue
		}
		out = append(out, Entry{Kind: kind, Directory: true, Workspace: parent.Workspace, Item: item, Modified: s.start})
		labels = append(labels, namespace.Label{ID: item.ID, DisplayName: item.DisplayName, Suffix: "." + item.Type})
	}
	names, err := s.names.Names(parent.Workspace+"/folder/"+parentID, labels)
	if err != nil {
		return nil, err
	}
	for i, name := range names {
		out[i].Name = name
		out[i].ValidUntil = tree.observedAt.Add(s.opts.CachePolicy.CatalogTTL(parent.Workspace))
	}
	return out, nil
}
