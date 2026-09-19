package workspacefs

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/onelake"
)

func (s *FS) workspaceList(ctx context.Context) ([]fabric.Workspace, error) {
	if s.opts.AllWorkspaces {
		return s.fabric.ListWorkspaces(ctx)
	}
	out := make([]fabric.Workspace, 0, len(s.opts.WorkspaceIDs))
	for _, id := range s.opts.WorkspaceIDs {
		workspace, err := s.fabric.GetWorkspace(ctx, id)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(workspace.ID, id) {
			return nil, fmt.Errorf("workspace response identity mismatch: %w", fs.ErrInvalid)
		}
		out = append(out, workspace)
	}
	return out, nil
}

func (s *FS) ReadDir(ctx context.Context, parent Entry) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !parent.Directory {
		return nil, fserrors.ErrNotDir
	}
	var out []Entry
	switch parent.Kind {
	case Root, Workspaces:
		items, err := s.workspaceList(ctx)
		if err != nil {
			return nil, err
		}
		labels := make([]namespace.Label, len(items))
		for i, item := range items {
			labels[i] = namespace.Label{ID: item.ID, DisplayName: item.DisplayName}
		}
		names, err := s.names.Names("workspaces", labels)
		if err != nil {
			return nil, err
		}
		for i, item := range items {
			out = append(out, Entry{Name: names[i], Label: item.DisplayName, Kind: Workspace, Directory: true, Workspace: strings.ToLower(item.ID), Modified: s.start,
				ValidUntil: s.workspaceDeadline(strings.ToLower(item.ID))})
		}
		if parent.Kind == Root {
			out = append(out, s.agentRoot())
		}
	case Workspace, FabricFolder:
		var err error
		out, err = s.managedChildren(ctx, parent)
		if err != nil {
			return nil, err
		}
	case Notebook:
		// Readdir supplies names/types only; Lookup/Stat obtain accurate body
		// attributes from the immutable decoded source snapshot when required.
		out = append(out, Entry{Name: namespace.NotebookContentName, Kind: NotebookContent, Workspace: parent.Workspace, Item: parent.Item, Size: -1})
	case Environment, DefinitionDirectory:
		var err error
		out, err = s.environmentChildren(ctx, parent, false)
		if err != nil {
			return nil, err
		}
	case Lakehouse:
		for _, root := range []string{"Files", "Tables"} {
			// These are documented protected roots, not an authorization probe.
			// Looking up Files must not require permission on optional Tables.
			out = append(out, lakeEntry(parent, root, root, onelake.Info{IsDir: true}))
		}
	case LakeDirectory:
		items, err := s.lake.List(ctx, parent.LakePath())
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			prefix := parent.Remote + "/"
			if !strings.HasPrefix(item.Path, prefix) {
				return nil, fmt.Errorf("OneLake list entry escaped its directory: %w", fs.ErrInvalid)
			}
			raw := strings.TrimPrefix(item.Path, prefix)
			name, err := namespace.FileName(raw)
			if err != nil {
				return nil, err
			}
			out = append(out, lakeEntry(parent, name, item.Path, item))
		}
	case OverlayDirectory:
		return nil, fserrors.ErrUnsupported
	case ResourceDirectory:
		var err error
		out, err = s.resourceChildren(ctx, parent)
		if err != nil {
			return nil, err
		}
	case AgentDirectory:
		var err error
		out, err = s.agentChildren(parent)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fserrors.ErrNotDir
	}
	if parent.Kind == Notebook || parent.Kind == Environment {
		roots, err := s.resourceRoots(ctx, parent)
		if err != nil {
			return nil, err
		}
		out = append(out, roots...)
	}
	if identityContainer(parent) {
		identity, err := s.identityEntry(ctx, parent)
		if err != nil {
			return nil, err
		}
		out = append(out, identity)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	for i := 1; i < len(out); i++ {
		if out[i-1].Name == out[i].Name {
			return nil, fmt.Errorf("remote/local namespace name conflict; neither object is hidden: %w", fs.ErrExist)
		}
	}
	return out, nil
}

func lakeEntry(parent Entry, name, remote string, info onelake.Info) Entry {
	kind := LakeFile
	if info.IsDir {
		kind = LakeDirectory
	}
	return Entry{
		Name: name, Kind: kind, Directory: info.IsDir, Workspace: parent.Workspace,
		Item: parent.Item, Remote: remote, Size: info.Size, Modified: info.ModTime, ValidUntil: info.ValidUntil,
	}
}

func (s *FS) Lookup(ctx context.Context, parent Entry, name string) (Entry, error) {
	if err := namespace.Component(name); err != nil {
		return Entry{}, err
	}
	if !parent.Directory {
		return Entry{}, fserrors.ErrNotDir
	}
	if parent.Kind == Root && name == namespace.AgentRootName {
		return s.agentRoot(), ctx.Err()
	}
	if parent.Kind == AgentDirectory {
		children, err := s.agentChildren(parent)
		if err != nil {
			return Entry{}, err
		}
		for _, child := range children {
			if child.Name == name {
				return child, ctx.Err()
			}
		}
		return Entry{}, fs.ErrNotExist
	}
	if (parent.Kind == Notebook && name == "builtin") || (parent.Kind == Environment && name == "resources") {
		return s.lookupResourceRoot(ctx, parent, name)
	}
	if parent.Kind == ResourceDirectory {
		path, err := s.newResourceChild(parent, name, false)
		if err != nil {
			return Entry{}, err
		}
		info, err := s.resources.Stat(ctx, path)
		if err != nil {
			return Entry{}, err
		}
		return resourceEntry(parent, name, path, info), nil
	}
	if identityContainer(parent) && name == identityFileName {
		return s.identityEntry(ctx, parent)
	}
	if parent.Kind == Notebook {
		return s.lookupNotebookPart(ctx, parent, name)
	}
	if parent.Kind == Environment || parent.Kind == DefinitionDirectory {
		if parent.Kind == Environment && name == ".platform" {
			return Entry{}, fs.ErrNotExist
		}
		for _, e := range s.fixedEnvironmentChildren(parent) {
			if e.Name == name {
				return e, nil
			}
		}
		children, err := s.environmentChildren(ctx, parent, true)
		if err != nil {
			return Entry{}, err
		}
		for _, child := range children {
			if child.Name == name {
				return child, nil
			}
		}
		return Entry{}, fs.ErrNotExist
	}
	if parent.Kind == OverlayDirectory {
		return Entry{}, fserrors.ErrUnsupported
	}
	if parent.Kind == Lakehouse {
		if name != "Files" && name != "Tables" {
			return Entry{}, fs.ErrNotExist
		}
		path := onelake.Path{Workspace: parent.Workspace, Item: parent.Item.ID, Relative: name}
		info, err := s.lake.Stat(ctx, path)
		if err != nil {
			return Entry{}, err
		}
		if !info.IsDir {
			return Entry{}, fmt.Errorf("protected OneLake root is not a directory: %w", fs.ErrInvalid)
		}
		return lakeEntry(parent, name, name, info), nil
	}
	if parent.Kind == LakeDirectory {
		raw, err := namespace.ParseFileName(name)
		if err != nil {
			return Entry{}, err
		}
		path := parent.LakePath()
		path.Relative += "/" + raw
		info, err := s.lake.Stat(ctx, path)
		if err != nil {
			return Entry{}, err
		}
		return lakeEntry(parent, name, path.Relative, info), nil
	}
	children, err := s.ReadDir(ctx, parent)
	if err != nil {
		return Entry{}, err
	}
	for _, child := range children {
		if child.Name == name {
			return child, nil
		}
	}
	return Entry{}, fs.ErrNotExist
}

func (s *FS) Stat(ctx context.Context, e Entry) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	if e.Kind == IdentityFile {
		return s.identityEntry(ctx, e)
	}
	if e.Kind == AgentFile {
		data, exists := s.agentFiles[e.Part]
		if !exists {
			return Entry{}, fs.ErrNotExist
		}
		e.Size = int64(len(data))
		return e, nil
	}
	if e.Fixed && e.Directory {
		return e, nil
	}
	if e.Kind == OverlayDirectory || e.Kind == OverlayFile {
		return Entry{}, fserrors.ErrUnsupported
	}
	if e.Kind == FabricFolder {
		tree, err := s.folderTree(ctx, e.Workspace)
		if err != nil {
			return Entry{}, err
		}
		folder, exists := tree.folders[e.Folder.ID]
		if !exists {
			return Entry{}, fs.ErrNotExist
		}
		e.Folder = folder
		e.ValidUntil = tree.observedAt.Add(s.opts.CachePolicy.CatalogTTL(e.Workspace))
	}
	if managedItem(e) {
		tree, err := s.folderTree(ctx, e.Workspace)
		if err != nil {
			return Entry{}, err
		}
		current, exists := tree.items[e.Item.ID]
		if !exists {
			return Entry{}, fs.ErrNotExist
		}
		if current.Type != e.Item.Type {
			return Entry{}, fserrors.ErrConflict
		}
		e.Item = current
		e.ValidUntil = tree.observedAt.Add(s.opts.CachePolicy.CatalogTTL(e.Workspace))
	}
	if e.Kind == Workspace {
		workspaces, err := s.workspaceList(ctx)
		if err != nil {
			return Entry{}, err
		}
		found := false
		for _, workspace := range workspaces {
			if strings.EqualFold(workspace.ID, e.Workspace) {
				e.Label, found = workspace.DisplayName, true
				e.ValidUntil = s.workspaceDeadline(e.Workspace)
				break
			}
		}
		if !found {
			return Entry{}, fs.ErrNotExist
		}
	}
	if size, ok := s.localSize(e); ok && !e.Directory {
		e.Size = size
		e.ValidUntil = s.now()
		return e, nil
	}
	if e.Kind == ResourceDirectory || e.Kind == ResourceFile {
		if s.resources == nil {
			return Entry{}, fserrors.ErrUnsupported
		}
		info, err := s.resources.Stat(ctx, e.Resource)
		if err != nil {
			return Entry{}, err
		}
		return resourceEntry(e, e.Name, e.Resource, info), nil
	}
	if e.Kind == LakeFile || e.Kind == LakeDirectory {
		info, err := s.lake.Stat(ctx, e.LakePath())
		if err != nil {
			return Entry{}, err
		}
		return lakeEntry(e, e.Name, e.Remote, info), nil
	}
	if e.Kind == NotebookContent || e.Kind == DefinitionFile {
		return s.statDefinition(ctx, e)
	}
	if e.Kind == DefinitionDirectory {
		policy := s.policy(e)
		snapshot, err := s.sourceSnapshot(ctx, e, policy.Attr, false)
		if err != nil {
			return Entry{}, err
		}
		if _, err := s.definitionChildren(e, snapshot); err != nil {
			return Entry{}, err
		}
		e.Modified, e.ValidUntil = snapshot.observedAt, snapshot.observedAt.Add(min(policy.Attr, policy.Definition))
	}
	return e, nil
}

// These are categories from the public Environment definition contract, not
// claims that any optional YAML/library file exists. "Setting" is singular.
func (s *FS) fixedEnvironmentChildren(parent Entry) []Entry {
	var names []string
	switch {
	case parent.Kind == Environment:
		names = []string{"Libraries", "Setting"}
	case parent.Kind == DefinitionDirectory && parent.Part == "Libraries":
		names = []string{"CustomLibraries", "PublicLibraries"}
	}
	out := make([]Entry, 0, len(names))
	for _, name := range names {
		part := name
		if parent.Part != "" {
			part = parent.Part + "/" + name
		}
		out = append(out, Entry{Name: name, Kind: DefinitionDirectory, Directory: true, Fixed: true,
			Workspace: parent.Workspace, Item: parent.Item, Part: part, Modified: s.start})
	}
	return out
}

func (s *FS) environmentChildren(ctx context.Context, parent Entry, lookup bool) ([]Entry, error) {
	out := s.fixedEnvironmentChildren(parent)
	policy := s.policy(parent)
	age := policy.Directory
	if lookup {
		age = min(age, policy.Attr)
	}
	snapshot, ok := s.peekSnapshot(parent, age)
	if !ok && (lookup || len(out) == 0) {
		var err error
		snapshot, err = s.sourceSnapshot(ctx, parent, age, false)
		if err != nil {
			return nil, err
		}
	}
	if snapshot == nil {
		return out, nil
	}
	dynamic, err := s.definitionChildren(parent, snapshot)
	if err != nil {
		return nil, err
	}
	for _, child := range dynamic {
		fixed := false
		for _, known := range out {
			if known.Name == child.Name {
				fixed = true
				break
			}
		}
		if !fixed {
			out = append(out, child)
		}
	}
	return out, nil
}
