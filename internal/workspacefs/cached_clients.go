package workspacefs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"strings"
	"time"

	"fabric-workspace-fs/internal/cache"
	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/onelake"
)

type cachedFabricAPI struct {
	FabricAPI
	workspaces *cache.Cache[[]fabric.Workspace]
	workspace  *cache.Cache[fabric.Workspace]
	items      *cache.Cache[[]fabric.Item]
	policy     *cachepolicy.Policy
}

func newCachedFabric(api FabricAPI, policy *cachepolicy.Policy, now func() time.Time) *cachedFabricAPI {
	return &cachedFabricAPI{
		FabricAPI: api, policy: policy,
		workspaces: cache.New(cache.Options[[]fabric.Workspace]{
			TTL: cachepolicy.DefaultTTL, MaxEntries: 1, MaxBytes: 8 << 20, Now: now,
			Size: func(items []fabric.Workspace) int64 {
				size := int64(0)
				for _, item := range items {
					size += int64(64 + len(item.ID) + len(item.DisplayName) + len(item.CapacityID))
				}
				return size
			},
			Clone: func(items []fabric.Workspace) []fabric.Workspace { return append([]fabric.Workspace(nil), items...) },
		}),
		workspace: cache.New(cache.Options[fabric.Workspace]{
			TTL: cachepolicy.DefaultTTL, MaxEntries: 64, MaxBytes: 1 << 20, Now: now,
			Size: func(item fabric.Workspace) int64 {
				return int64(64 + len(item.ID) + len(item.DisplayName) + len(item.CapacityID))
			},
		}),
		items: cache.New(cache.Options[[]fabric.Item]{
			TTL: cachepolicy.DefaultTTL, MaxEntries: 32, MaxBytes: 16 << 20, Now: now,
			Size: func(items []fabric.Item) int64 {
				size := int64(0)
				for _, item := range items {
					size += int64(128 + len(item.ID) + len(item.DisplayName) + len(item.Description) + len(item.Type) + len(item.FolderID))
				}
				return size
			},
			Clone: func(items []fabric.Item) []fabric.Item { return append([]fabric.Item(nil), items...) },
		}),
	}
}

func (c *cachedFabricAPI) ListWorkspaces(ctx context.Context) ([]fabric.Workspace, error) {
	return c.workspaces.GetWithTTL(ctx, "workspaces", c.policy.CatalogTTL(""), c.FabricAPI.ListWorkspaces)
}

func (c *cachedFabricAPI) GetWorkspace(ctx context.Context, id string) (fabric.Workspace, error) {
	return c.workspace.GetWithTTL(ctx, id, c.policy.CatalogTTL(id), func(ctx context.Context) (fabric.Workspace, error) { return c.FabricAPI.GetWorkspace(ctx, id) })
}

func (c *cachedFabricAPI) ListItems(ctx context.Context, workspace string) ([]fabric.Item, error) {
	return c.items.GetWithTTL(ctx, workspace, c.policy.CatalogTTL(workspace), func(ctx context.Context) ([]fabric.Item, error) { return c.FabricAPI.ListItems(ctx, workspace) })
}

func definitionKey(workspace, item, kind, format string) string {
	return workspace + "/" + item + "/" + kind + "/" + format
}

func definitionSize(def fabric.Definition) int64 {
	size := int64(128 + len(def.Format))
	for name, value := range def.Extra {
		size += int64(32 + len(name) + len(value))
	}
	for _, part := range def.Parts {
		size += int64(128 + len(part.Path) + len(part.Payload) + len(part.PayloadType))
		for name, value := range part.Extra {
			size += int64(32 + len(name) + len(value))
		}
	}
	return size
}

func cloneDefinition(def fabric.Definition) fabric.Definition {
	copyExtra := func(extra map[string]json.RawMessage) map[string]json.RawMessage {
		out := make(map[string]json.RawMessage, len(extra))
		for key, data := range extra {
			out[key] = append(json.RawMessage(nil), data...)
		}
		return out
	}
	def.Parts = append([]fabric.Part(nil), def.Parts...)
	def.Extra = copyExtra(def.Extra)
	for i := range def.Parts {
		def.Parts[i].Extra = copyExtra(def.Parts[i].Extra)
	}
	return def
}

type cachedLakeAPI struct {
	LakeAPI
	stats  *cache.Cache[onelake.Info]
	lists  *cache.Cache[lakeListing]
	policy *cachepolicy.Policy
	now    func() time.Time
}

type lakeListing struct {
	items      []onelake.Info
	observedAt time.Time
}

func newCachedLake(api LakeAPI, policy *cachepolicy.Policy, now func() time.Time) *cachedLakeAPI {
	return &cachedLakeAPI{
		LakeAPI: api, policy: policy, now: now,
		stats: cache.New(cache.Options[onelake.Info]{
			TTL: cachepolicy.DefaultTTL, MaxEntries: 4096, MaxBytes: 8 << 20, Now: now,
			Size:       func(info onelake.Info) int64 { return int64(192 + len(info.Path) + len(info.ETag)) },
			ObservedAt: func(info onelake.Info) time.Time { return info.ObservedAt },
		}),
		lists: cache.New(cache.Options[lakeListing]{
			TTL: cachepolicy.DefaultTTL, MaxEntries: 64, MaxBytes: 16 << 20, Now: now,
			Size: func(list lakeListing) int64 {
				size := int64(64)
				for _, item := range list.items {
					size += int64(192 + len(item.Path) + len(item.ETag))
				}
				return size
			},
			Clone: func(list lakeListing) lakeListing {
				list.items = append([]onelake.Info(nil), list.items...)
				return list
			},
			ObservedAt: func(list lakeListing) time.Time { return list.observedAt },
		}),
	}
}

func lakeKey(p onelake.Path) string { return p.Workspace + "/" + p.Item + "/" + p.Relative }

func (c *cachedLakeAPI) values(p onelake.Path) cachepolicy.Values {
	surface, _, _ := strings.Cut(p.Relative, "/")
	return c.policy.Resolve(cachepolicy.Selector{Workspace: p.Workspace, Item: p.Item, Type: "Lakehouse", Surface: surface})
}

func (c *cachedLakeAPI) Stat(ctx context.Context, p onelake.Path) (onelake.Info, error) {
	policy := c.values(p)
	if split := strings.LastIndexByte(p.Relative, '/'); split >= 0 {
		parent := p
		parent.Relative = p.Relative[:split]
		if list, _, ok := c.lists.PeekWithin(lakeKey(parent), min(policy.Attr, policy.Directory)); ok {
			for _, info := range list.items {
				if info.Path == p.Relative && !info.ModTime.IsZero() && (info.IsDir || info.ETag != "") {
					info.ValidUntil = list.observedAt.Add(min(policy.Attr, policy.Directory))
					return info, ctx.Err()
				}
			}
		}
	}
	return c.stats.GetWithTTL(ctx, lakeKey(p), policy.Attr, func(ctx context.Context) (onelake.Info, error) {
		info, err := c.LakeAPI.Stat(ctx, p)
		info.ObservedAt = c.now()
		info.ValidUntil = info.ObservedAt.Add(policy.Attr)
		return info, err
	})
}

func (c *cachedLakeAPI) FreshStat(ctx context.Context, p onelake.Path) (onelake.Info, error) {
	c.stats.Invalidate(lakeKey(p))
	// A write comparison must not reuse even a recent directory's ETag.
	info, err := c.LakeAPI.Stat(ctx, p)
	info.ObservedAt = c.now()
	info.ValidUntil = info.ObservedAt.Add(c.values(p).Attr)
	return info, err
}

func (c *cachedLakeAPI) List(ctx context.Context, p onelake.Path) ([]onelake.Info, error) {
	policy := c.values(p)
	list, err := c.lists.GetWithTTL(ctx, lakeKey(p), policy.Directory, func(ctx context.Context) (lakeListing, error) {
		items, err := c.LakeAPI.List(ctx, p)
		observed := c.now()
		for i := range items {
			items[i].ObservedAt = observed
			items[i].ValidUntil = observed.Add(min(policy.Attr, policy.Directory))
		}
		return lakeListing{items: items, observedAt: observed}, err
	})
	return list.items, err
}

func (c *cachedLakeAPI) Read(ctx context.Context, p onelake.Path, offset int64, dest []byte, etag string) (int, error) {
	n, err := c.LakeAPI.Read(ctx, p, offset, dest, etag)
	if errors.Is(err, fserrors.ErrConflict) || errors.Is(err, fs.ErrNotExist) {
		c.invalidate(p)
	}
	return n, err
}

func (c *cachedLakeAPI) invalidate(p onelake.Path) {
	key := lakeKey(p)
	c.stats.Invalidate(key)
	c.stats.InvalidatePrefix(key + "/")
	c.lists.Invalidate(key)
	c.lists.InvalidatePrefix(key + "/")
	if split := strings.LastIndexByte(key, '/'); split >= 0 {
		c.stats.Invalidate(key[:split])
		c.lists.Invalidate(key[:split])
	}
}

func (c *cachedLakeAPI) Put(ctx context.Context, p onelake.Path, source io.ReaderAt, size int64, etag string) (onelake.Info, error) {
	defer c.invalidate(p)
	return c.LakeAPI.Put(ctx, p, source, size, etag)
}

func (c *cachedLakeAPI) Mkdir(ctx context.Context, p onelake.Path) error {
	defer c.invalidate(p)
	return c.LakeAPI.Mkdir(ctx, p)
}

func (c *cachedLakeAPI) Remove(ctx context.Context, p onelake.Path, directory bool, etag string) error {
	defer c.invalidate(p)
	return c.LakeAPI.Remove(ctx, p, directory, etag)
}

func (c *cachedLakeAPI) Rename(ctx context.Context, source, destination onelake.Path, sourceETag, destinationETag string, noReplace bool) (onelake.Info, error) {
	defer c.invalidate(source)
	defer c.invalidate(destination)
	return c.LakeAPI.Rename(ctx, source, destination, sourceETag, destinationETag, noReplace)
}

func (s *FS) CacheStats() map[string]cache.Stats {
	return map[string]cache.Stats{
		"workspaces": s.fabric.workspaces.Stats(), "workspace": s.fabric.workspace.Stats(),
		"folder-tree": s.catalogs.Stats(),
		"items":       s.fabric.items.Stats(), "definitions": s.snapshots.Stats(),
		"stat": s.lake.stats.Stats(), "directories": s.lake.lists.Stats(),
	}
}
