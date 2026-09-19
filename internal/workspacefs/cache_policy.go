package workspacefs

import (
	"strings"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
)

func (s *FS) policy(e Entry) cachepolicy.Values {
	selector := cachepolicy.Selector{Workspace: e.Workspace, Item: e.Item.ID, Type: e.Item.Type}
	switch e.Kind {
	case NotebookContent:
		selector.Surface = "content"
	case DefinitionFile, DefinitionDirectory:
		selector.Surface = "definition"
	case LakeFile, LakeDirectory:
		selector.Surface, _, _ = strings.Cut(e.Remote, "/")
	case ResourceFile, ResourceDirectory:
		selector.Workspace, selector.Item, selector.Type = e.Resource.Target.WorkspaceID, e.Resource.Target.ItemID, e.Resource.Target.Kind
		selector.Surface = "builtin"
		if selector.Type == "Environment" {
			selector.Surface = "resources"
		}
	}
	return s.opts.CachePolicy.Resolve(selector)
}

// CacheTimeouts bounds kernel caching by both presentation policy and the
// original source deadline. Local writable spools always own current file size.
func (s *FS) CacheTimeouts(e Entry) (attr, entry, negative time.Duration) {
	policy := s.policy(e)
	attr, entry, negative = policy.KernelAttr, policy.KernelEntry, policy.KernelNegative
	if !e.ValidUntil.IsZero() {
		remaining := max(time.Duration(0), e.ValidUntil.Sub(s.now()))
		attr, entry = min(attr, remaining), min(entry, remaining)
		negative = min(negative, remaining)
	}
	if e.Kind == Root || e.Kind == Workspaces {
		negative = min(negative, max(time.Duration(0), s.workspaceDeadline("").Sub(s.now())))
	}
	if managedContainer(e) {
		if tree, ok := s.catalogs.Peek(e.Workspace); ok {
			remaining := max(time.Duration(0), tree.observedAt.Add(s.opts.CachePolicy.CatalogTTL(e.Workspace)).Sub(s.now()))
			negative = min(negative, remaining)
		} else {
			negative = 0
		}
		if e.Kind == Environment || e.Kind == DefinitionDirectory {
			if snapshot, ok := s.peekSnapshot(e, min(policy.Directory, policy.Definition)); ok {
				remaining := max(time.Duration(0), snapshot.observedAt.Add(min(policy.Directory, policy.Definition)).Sub(s.now()))
				negative = min(negative, remaining)
			} else {
				negative = 0
			}
		}
	}
	// An optional Notebook part path may become known (or be invalidated)
	// without changing basic identity. Its tiny generated file is local-only.
	if e.Kind == IdentityFile && e.Item.Type == "Notebook" {
		attr = 0
	}
	s.mu.Lock()
	writer := s.active[e.Key()].writers != 0
	s.mu.Unlock()
	if writer {
		attr = 0
	}
	return attr, entry, negative
}

func (s *FS) workspaceDeadline(id string) time.Time {
	if s.opts.AllWorkspaces {
		_, observed, ok := s.fabric.workspaces.PeekWithin("workspaces", s.opts.CachePolicy.CatalogTTL(""))
		if ok {
			return observed.Add(s.opts.CachePolicy.CatalogTTL(""))
		}
		return s.now()
	}
	deadline := time.Time{}
	for _, selected := range s.opts.WorkspaceIDs {
		if id != "" && selected != id {
			continue
		}
		ttl := s.opts.CachePolicy.CatalogTTL(selected)
		_, observed, ok := s.fabric.workspace.PeekWithin(selected, ttl)
		if !ok {
			return s.now()
		}
		end := observed.Add(ttl)
		if deadline.IsZero() || end.Before(deadline) {
			deadline = end
		}
	}
	if deadline.IsZero() {
		return s.now()
	}
	return deadline
}
