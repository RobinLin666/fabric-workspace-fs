package workspacefs

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fabric-workspace-fs/internal/cache"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
)

// Raw definitions and decoded bodies have one retention owner. Callers never
// mutate a snapshot; a writable save constructs a separate multipart request.
type definitionSnapshot struct {
	definition  fabric.Definition
	parts       map[string][]byte
	notebook    string
	contentETag string
	contentAPI  bool
	observedAt  time.Time
	bytes       int64
	version     [32]byte
	versionErr  error
	versionOne  sync.Once
	prewarmed   atomic.Bool
}

func snapshotKey(e Entry) string {
	format := ""
	if e.Item.Type == "Notebook" {
		format = "ipynb"
	}
	return definitionKey(e.Workspace, e.Item.ID, e.Item.Type, format)
}

func (s *FS) sourceSnapshot(ctx context.Context, e Entry, maxAge time.Duration, fresh bool) (*definitionSnapshot, error) {
	if e.Item.Type != "Notebook" && e.Item.Type != "Environment" {
		return nil, fs.ErrInvalid
	}
	policy := s.policy(e)
	key := snapshotKey(e)
	if fresh {
		s.snapshots.Invalidate(key)
		maxAge = 0
	}
	snapshot, err := s.snapshots.GetWithin(ctx, key, min(maxAge, policy.Definition), policy.Definition, func(ctx context.Context) (*definitionSnapshot, error) {
		if e.Item.Type == "Notebook" && s.notebookContent != nil {
			start := time.Now()
			data, etag, err := s.notebookContent.GetNotebookContent(ctx, e.Workspace, e.Item.ID)
			s.observeNotebook("content_get", time.Since(start), err)
			if err != nil {
				return nil, err
			}
			if int64(len(data)) > s.opts.MaxNotebookSize {
				return nil, fserrors.ErrTooLarge
			}
			start = time.Now()
			if err := validNotebook(data); err != nil {
				s.observeNotebook("decode_validate", time.Since(start), err)
				return nil, err
			}
			s.observeNotebook("decode_validate", time.Since(start), nil)
			s.decodes.Add(1)
			return &definitionSnapshot{
				parts: map[string][]byte{"": append([]byte(nil), data...)}, notebook: "", contentETag: etag, contentAPI: true,
				observedAt: s.now(), bytes: int64(len(data)) + 256,
			}, nil
		}
		def, err := s.fetchDefinition(ctx, e)
		if err != nil {
			return nil, err
		}
		start := time.Now()
		value, err := s.makeSnapshot(def, e.Item.Type, s.now())
		if e.Item.Type == "Notebook" {
			s.observeNotebook("decode_validate", time.Since(start), err)
		}
		if value != nil && ctx.Value(prewarmSlotKey{}) == true {
			value.prewarmed.Store(true)
		}
		return value, err
	})
	if err == nil && ctx.Value(prewarmSlotKey{}) != true && s.prewarm != nil && snapshot.prewarmed.Swap(false) {
		s.prewarm.mu.Lock()
		s.prewarm.stats.Used++
		s.prewarm.mu.Unlock()
	}
	return snapshot, err
}

// fetchDefinition obtains one source generation without decoding its parts. Save
// conflict detection hashes the full wire definition, so decoding the current
// notebook body would add CPU and allocations without affecting that comparison.
func (s *FS) fetchDefinition(ctx context.Context, e Entry) (result fabric.Definition, resultErr error) {
	if e.Item.Type != "Notebook" && e.Item.Type != "Environment" {
		return fabric.Definition{}, fs.ErrInvalid
	}
	if ctx.Value(prewarmSlotKey{}) != true {
		start := time.Now()
		release, err := s.definitionGate.acquire(ctx)
		if e.Item.Type == "Notebook" {
			s.observeNotebook("definition_wait", time.Since(start), err)
		}
		if err != nil {
			return fabric.Definition{}, err
		}
		defer release()
	}
	start := time.Now()
	if e.Item.Type == "Notebook" {
		defer func() { s.observeNotebook("definition_export", time.Since(start), resultErr) }()
	}
	format := ""
	if e.Item.Type == "Notebook" {
		format = "ipynb"
	}
	def, err := s.fabric.FabricAPI.GetDefinition(ctx, e.Workspace, e.Item.ID, e.Item.Type, format)
	if err != nil {
		return fabric.Definition{}, err
	}
	if format != "" {
		def.Format = format
	}
	if definitionSize(def)+256 > max(int64(128<<20), 3*s.opts.MaxNotebookSize) {
		return fabric.Definition{}, fmt.Errorf("definition snapshot exceeds its byte budget: %w", fserrors.ErrTooLarge)
	}
	return def, nil
}

func (s *FS) makeSnapshot(def fabric.Definition, kind string, observed time.Time) (*definitionSnapshot, error) {
	snapshot := &definitionSnapshot{
		definition: def, parts: make(map[string][]byte), observedAt: observed,
		bytes: definitionSize(def) + 256,
	}
	limit := max(int64(128<<20), 3*s.opts.MaxNotebookSize)
	if snapshot.bytes > limit {
		return nil, fmt.Errorf("definition snapshot exceeds its byte budget: %w", fserrors.ErrTooLarge)
	}
	seen := make(map[string]bool, len(def.Parts))
	for _, part := range def.Parts {
		if err := namespace.PartPath(part.Path); err != nil {
			return nil, err
		}
		if seen[part.Path] {
			return nil, fmt.Errorf("duplicate definition part: %w", fs.ErrInvalid)
		}
		seen[part.Path] = true
	}
	for path := range seen {
		segments := strings.Split(path, "/")
		for i := 1; i < len(segments); i++ {
			if seen[strings.Join(segments[:i], "/")] {
				return nil, fmt.Errorf("definition file/directory collision: %w", fs.ErrInvalid)
			}
		}
	}
	if kind == "Notebook" {
		part, err := notebookPart(def)
		if err != nil {
			return nil, err
		}
		snapshot.notebook = part.Path
	}
	for _, part := range def.Parts {
		if (kind == "Notebook" && part.Path != snapshot.notebook) || hiddenPlatform(part.Path) {
			continue
		}
		if kind == "Environment" && (part.Path == "Libraries" || part.Path == "Setting" ||
			part.Path == "Libraries/CustomLibraries" || part.Path == "Libraries/PublicLibraries") {
			return nil, fmt.Errorf("definition file collides with a documented directory: %w", fs.ErrInvalid)
		}
		partLimit := limit - snapshot.bytes - int64(64+len(part.Path))
		if kind == "Notebook" {
			partLimit = min(partLimit, s.opts.MaxNotebookSize)
		}
		if partLimit < 0 {
			return nil, fserrors.ErrTooLarge
		}
		data, err := partData(part, partLimit)
		if err != nil {
			return nil, err
		}
		s.decodes.Add(1)
		if kind == "Notebook" {
			if err := validNotebook(data); err != nil {
				return nil, err
			}
		}
		snapshot.parts[part.Path] = data
		snapshot.bytes += int64(64 + len(part.Path) + len(data))
	}
	return snapshot, nil
}

func hiddenPlatform(path string) bool {
	return path == ".platform" || strings.HasPrefix(path, ".platform/")
}

func (s *FS) snapshotDigest(snapshot *definitionSnapshot) ([32]byte, error) {
	snapshot.versionOne.Do(func() {
		start := time.Now()
		s.digests.Add(1)
		snapshot.version, snapshot.versionErr = digest(snapshot.definition)
		s.observeNotebook("digest", time.Since(start), snapshot.versionErr)
	})
	return snapshot.version, snapshot.versionErr
}

func (s *FS) peekSnapshot(e Entry, maxAge time.Duration) (*definitionSnapshot, bool) {
	snapshot, _, ok := s.snapshots.PeekWithin(snapshotKey(e), min(maxAge, s.policy(e).Definition))
	return snapshot, ok
}

func (s *FS) invalidateDefinition(e Entry) {
	s.snapshots.Invalidate(snapshotKey(e))
}

func (s *FS) pinSnapshot(snapshot *definitionSnapshot, write bool) (func(), error) {
	charge := snapshot.bytes
	if write {
		// Reserve commit's decoded buffer and replacement base64 payload too.
		charge += 3 * s.opts.MaxNotebookSize
	}
	return s.reserveSnapshotBytes(charge)
}

func (s *FS) reserveSnapshotBytes(charge int64) (func(), error) {
	limit := max(int64(256<<20), 6*s.opts.MaxNotebookSize, 4*s.opts.MaxResourceSize)
	s.pinMu.Lock()
	if charge < 0 || charge > limit-s.pinnedBytes {
		s.pinMu.Unlock()
		return nil, fmt.Errorf("open snapshot byte budget exhausted: %w", fserrors.ErrBusy)
	}
	s.pinnedBytes += charge
	s.pinnedCount++
	s.pinMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.pinMu.Lock()
			s.pinnedBytes -= charge
			s.pinnedCount--
			s.pinMu.Unlock()
		})
	}, nil
}

type SnapshotStats struct {
	Loads, Decodes, Digests    uint64
	RetainedBytes, PinnedBytes int64
	PinnedHandles              int
}

// DefinitionCacheStats includes shared-flight and hit/miss counts for the one
// Notebook/Environment snapshot cache, without changing SnapshotStats.Loads.
func (s *FS) DefinitionCacheStats() cache.Stats { return s.snapshots.Stats() }

// SnapshotStats counts raw payloads, decoded bodies, and conservative per-handle
// reservations even when several handles share a single immutable allocation.
func (s *FS) SnapshotStats() SnapshotStats {
	cache := s.snapshots.Stats()
	stats := SnapshotStats{Loads: cache.Loads, Decodes: s.decodes.Load(), Digests: s.digests.Load(), RetainedBytes: cache.Bytes}
	s.pinMu.Lock()
	stats.PinnedBytes, stats.PinnedHandles = s.pinnedBytes, s.pinnedCount
	s.pinMu.Unlock()
	return stats
}
