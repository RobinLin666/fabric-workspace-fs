package workspacefs

import (
	"context"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cache"
	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/testutil"
)

func TestCatalogSnapshotsCannotBypassOversizeBudget(t *testing.T) {
	s, api, _ := managedTestFS(t, false)
	api.folders[testFolderA] = fabric.Folder{ID: testFolderA, DisplayName: "Known folder"}
	s.catalogs = cache.New(cache.Options[*folderTree]{
		TTL: cachepolicy.DefaultTTL, MaxEntries: 32, MaxBytes: 1,
		Size: func(tree *folderTree) int64 { return tree.bytes },
	})
	tree, err := s.folderTree(context.Background(), testutil.WorkspaceID)
	if err != nil || len(tree.folders) != 1 {
		t.Fatal("oversized successful catalog was discarded", err)
	}
	if _, ok := s.catalogs.Peek(testutil.WorkspaceID); ok {
		t.Fatal("oversized catalog retained outside the byte budget")
	}
	if stats := s.CacheStats()["folder-tree"]; stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatal("oversized catalog still owned", stats)
	}
	before, _ := api.counts()
	agents := lookup(t, s, s.Root(), ".agents")
	lookup(t, s, agents, "AGENT.md")
	after, _ := api.counts()
	if before != after {
		t.Fatal("uncached catalogs made injected local access issue HTTP calls")
	}
}

func TestAllWorkspaceCatalogSnapshotsShareBytesAndExpiry(t *testing.T) {
	s, api, _ := managedTestFS(t, false)
	clock := newSnapshotClock()
	s.now, s.opts.Now, s.opts.CachePolicy = clock.Now, clock.Now, cachepolicy.Default()
	folder := fabric.Folder{ID: testFolderA, DisplayName: "Known folder"}
	api.folders[testFolderA] = folder
	sized, err := buildFolderTree([]fabric.Folder{folder}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.catalogs = cache.New(cache.Options[*folderTree]{
		TTL: cachepolicy.DefaultTTL, MaxEntries: 32, MaxBytes: 2 * sized.bytes, Now: clock.Now,
		Size:       func(tree *folderTree) int64 { return tree.bytes },
		ObservedAt: func(tree *folderTree) time.Time { return tree.observedAt },
	})
	workspaces := []string{testutil.WorkspaceID, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}
	for _, workspace := range workspaces {
		if _, err := s.folderTree(context.Background(), workspace); err != nil {
			t.Fatal(err)
		}
	}
	if stats := s.catalogs.Stats(); stats.Entries != 2 || stats.Bytes != 2*sized.bytes {
		t.Fatal("catalog snapshots bypassed the aggregate byte budget", stats)
	}
	if _, ok := s.catalogs.Peek(workspaces[0]); ok {
		t.Fatal("old workspace retained after LRU eviction")
	}
	clock.Advance(2 * time.Minute)
	s.catalogs.Peek("not-present")
	if stats := s.catalogs.Stats(); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatal("expired workspace tree references were not released", stats)
	}
	before, _ := api.counts()
	lookup(t, s, s.Root(), ".agents")
	after, _ := api.counts()
	if before != after {
		t.Fatal("expired catalog caused an injected-bundle HTTP request")
	}
}
