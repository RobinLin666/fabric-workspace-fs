package workspacefs

import (
	"context"
	"fmt"
	"io/fs"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/onelake"
)

type fabricManagement interface {
	CreateItem(context.Context, string, string, string, string) (fabric.Item, error)
	DeleteItem(context.Context, string, string) error
	CreateFolder(context.Context, string, string, string) (fabric.Folder, error)
	DeleteFolder(context.Context, string, string) error
}

func (s *FS) management() (fabricManagement, error) {
	api, ok := s.fabric.FabricAPI.(fabricManagement)
	if !ok {
		return nil, fserrors.ErrUnsupported
	}
	return api, nil
}

func (s *FS) mkdirManaged(ctx context.Context, parent Entry, name string) (Entry, error) {
	if s.opts.ReadOnly {
		return Entry{}, fserrors.ErrReadOnly
	}
	if !managedContainer(parent) || !parent.Directory {
		return Entry{}, fserrors.ErrReadOnly
	}
	create, err := namespace.ParseCreation(name)
	if err != nil {
		return Entry{}, err
	}
	api, err := s.management()
	if err != nil {
		return Entry{}, err
	}
	done, err := s.mutation(ctx)
	if err != nil {
		return Entry{}, err
	}
	defer done()
	children, err := s.managedChildren(ctx, parent)
	if err != nil {
		return Entry{}, err
	}
	for _, child := range children {
		if child.Name == name {
			return Entry{}, fs.ErrExist
		}
	}
	scope := parent.Workspace + "/folder/" + parent.ParentFolderID()
	if s.names.Reserved(scope, name) {
		return Entry{}, fmt.Errorf("directory name is reserved for a prior identity; choose another name: %w", fs.ErrExist)
	}
	defer s.InvalidateCatalog(parent.Workspace)
	entry := Entry{Workspace: parent.Workspace, Directory: true, Modified: s.start}
	var label namespace.Label
	if create.Type == "Folder" {
		folder, err := api.CreateFolder(ctx, parent.Workspace, create.DisplayName, parent.ParentFolderID())
		if err != nil {
			return Entry{}, err
		}
		entry.Kind, entry.Folder = FabricFolder, folder
		label = namespace.Label{ID: folder.ID, DisplayName: folder.DisplayName}
	} else {
		item, err := api.CreateItem(ctx, parent.Workspace, create.Type, create.DisplayName, parent.ParentFolderID())
		if err != nil {
			return Entry{}, err
		}
		entry.Kind, _ = itemKind(item.Type)
		entry.Item = item
		label = namespace.Label{ID: item.ID, DisplayName: item.DisplayName, Suffix: "." + item.Type}
	}
	names, err := s.names.Names(scope, []namespace.Label{label})
	if err != nil {
		return Entry{}, err
	}
	entry.Name = names[0]
	return entry, nil
}

func (s *FS) removeManaged(ctx context.Context, e Entry, directory bool) error {
	if s.opts.ReadOnly {
		return fserrors.ErrReadOnly
	}
	if e.Kind == Environment || e.Kind == Notebook {
		return fmt.Errorf("public definitions cannot prove builtin/Resources are empty; item delete is not supported through FUSE: %w", fserrors.ErrUnsupported)
	}
	if !directory {
		return fserrors.ErrIsDir
	}
	if e.Kind == Workspace {
		return fserrors.ErrReadOnly
	}
	api, err := s.management()
	if err != nil {
		return err
	}
	key := e.Workspace + "/" + e.Item.ID
	if e.Kind == FabricFolder {
		key = e.Workspace + "/folders/" + e.Folder.ID
	}
	done, err := s.mutation(ctx, key)
	if err != nil {
		return err
	}
	defer done()
	s.InvalidateCatalog(e.Workspace)
	if e.Kind == FabricFolder {
		tree, err := s.folderTree(ctx, e.Workspace)
		if err != nil {
			return err
		}
		if _, ok := tree.folders[e.Folder.ID]; !ok {
			return fs.ErrNotExist
		}
		if len(tree.folderChildren[e.Folder.ID]) != 0 || len(tree.itemChildren[e.Folder.ID]) != 0 {
			return fserrors.ErrNotEmpty
		}
		defer s.InvalidateCatalog(e.Workspace)
		return api.DeleteFolder(ctx, e.Workspace, e.Folder.ID)
	}
	tree, err := s.folderTree(ctx, e.Workspace)
	if err != nil {
		return err
	}
	current, exists := tree.items[e.Item.ID]
	if !exists {
		return fs.ErrNotExist
	}
	if current.Type != e.Item.Type {
		return fserrors.ErrConflict
	}
	e.Item = current
	switch e.Kind {
	case Lakehouse:
		for _, root := range []string{"Files", "Tables"} {
			p := onelake.Path{Workspace: e.Workspace, Item: e.Item.ID, Relative: root}
			children, err := s.lake.LakeAPI.List(ctx, p)
			if err != nil {
				return err
			}
			if len(children) != 0 {
				return fserrors.ErrNotEmpty
			}
		}
	default:
		return fserrors.ErrReadOnly
	}
	defer s.InvalidateCatalog(e.Workspace)
	defer s.snapshots.InvalidatePrefix(e.Workspace + "/" + e.Item.ID + "/")
	return api.DeleteItem(ctx, e.Workspace, e.Item.ID)
}

func managedItem(e Entry) bool {
	return e.Kind == Notebook || e.Kind == Lakehouse || e.Kind == Environment
}
