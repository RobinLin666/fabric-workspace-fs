package workspacefs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
)

const offlineNotebookWorkspace = "11111111-1111-4111-8111-111111111111"

// These benchmarks measure local snapshot/JSON/base64/writeback work, including
// local spool I/O for saves. They do not measure HTTP, authentication, retries,
// Fabric execution, or cloud latency. Immutable payload strings are shared by
// the fake; each response owns its mutable definition metadata.
type offlineNotebookAPI struct {
	FabricAPI // Catalog operations are intentionally unavailable for direct entries.
	mu        sync.Mutex
	def       fabric.Definition
	reads     uint64
	updates   uint64
}

func (a *offlineNotebookAPI) GetDefinition(_ context.Context, workspace, item, kind, format string) (fabric.Definition, error) {
	if workspace != offlineNotebookWorkspace || item != offlineNotebookEntry().Item.ID || kind != "Notebook" || format != "ipynb" {
		return fabric.Definition{}, fmt.Errorf("unexpected offline definition request")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reads++
	return cloneDefinition(a.def), nil
}

func (a *offlineNotebookAPI) UpdateNotebook(_ context.Context, workspace, item string, def fabric.Definition) error {
	if workspace != offlineNotebookWorkspace || item != offlineNotebookEntry().Item.ID {
		return fmt.Errorf("unexpected offline notebook update")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.updates++
	a.def = cloneDefinition(def)
	return nil
}

type offlineNotebookLake struct {
	LakeAPI // Notebook-only operations must never call OneLake.
}

func offlineNotebookEntry() Entry {
	return Entry{
		Name: "content.ipynb", Kind: NotebookContent, Workspace: offlineNotebookWorkspace,
		Item: fabric.Item{ID: "22222222-2222-4222-8222-222222222222", Type: "Notebook", DisplayName: "Offline benchmark"},
	}
}

var offlineNotebookSizes = []struct {
	name string
	size int
}{
	{"Small", 1 << 10},
	{"1MiB", 1 << 20},
	{"Near16MiB", (16 << 20) - 1024},
}

func offlineNotebookFixture(tb testing.TB, size int) ([]byte, []byte, fabric.Definition) {
	tb.Helper()
	// A markdown cell avoids executing any code. Unknown notebook, cell,
	// definition, and part fields all participate in preservation checks.
	const prefix = `{"nbformat":4,"nbformat_minor":5,"metadata":{"revision":0,"unknown":{"keep":true}},"unknown_top":[1,2],"cells":[{"cell_type":"markdown","id":"fixture","metadata":{"unknown_cell":true},"source":["`
	const suffix = `"],"unknown_cell_field":{"keep":"value"}}]}`
	if size < len(prefix)+len(suffix) {
		tb.Fatal("fixture size is too small")
	}
	body := []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
	changed := bytes.Clone(body)
	changed[bytes.Index(changed, []byte(`"revision":0`))+len(`"revision":`)] = '1'
	for _, data := range [][]byte{body, changed} {
		if len(data) != size || validNotebook(data) != nil {
			tb.Fatal("invalid notebook fixture")
		}
	}
	def := fabric.Definition{
		Format: "ipynb",
		Extra:  map[string]json.RawMessage{"unknown_definition": json.RawMessage(`{"keep":[1,2]}`)},
		Parts: []fabric.Part{
			{Path: ".platform", PayloadType: "InlineBase64", Payload: base64.StdEncoding.EncodeToString([]byte(`{"metadata":{"type":"Notebook"}}`)),
				Extra: map[string]json.RawMessage{"unknown_platform": json.RawMessage(`true`)}},
			{Path: "notebook-content.ipynb", PayloadType: "InlineBase64", Payload: base64.StdEncoding.EncodeToString(body),
				Extra: map[string]json.RawMessage{"unknown_part": json.RawMessage(`{"keep":"yes"}`)}},
			{Path: "extras/settings.json", PayloadType: "InlineBase64", Payload: base64.StdEncoding.EncodeToString([]byte(`{"binding":"preserve"}`)),
				Extra: map[string]json.RawMessage{"unknown_extra": json.RawMessage(`[1,"two"]`)}},
		},
	}
	return body, changed, def
}

type offlineNotebookCounts struct {
	reads, updates, loads, decodes, snapshotDigests uint64
}

func (c offlineNotebookCounts) sub(before offlineNotebookCounts) offlineNotebookCounts {
	return offlineNotebookCounts{c.reads - before.reads, c.updates - before.updates,
		c.loads - before.loads, c.decodes - before.decodes, c.snapshotDigests - before.snapshotDigests}
}

func (c offlineNotebookCounts) add(other offlineNotebookCounts) offlineNotebookCounts {
	return offlineNotebookCounts{c.reads + other.reads, c.updates + other.updates,
		c.loads + other.loads, c.decodes + other.decodes, c.snapshotDigests + other.snapshotDigests}
}

func (c offlineNotebookCounts) report(b *testing.B) {
	b.ReportMetric(float64(c.reads)/float64(b.N), "DefinitionReads/op")
	b.ReportMetric(float64(c.updates)/float64(b.N), "NotebookUpdates/op")
	b.ReportMetric(float64(c.loads)/float64(b.N), "SnapshotLoads/op")
	b.ReportMetric(float64(c.decodes)/float64(b.N), "Decodes/op")
	// SnapshotStats.Digests counts memoized writer-base digests, not the two
	// full-definition digest calls performed by each save preflight.
	b.ReportMetric(float64(c.snapshotDigests)/float64(b.N), "SnapshotDigests/op")
}

type offlineNotebookHarness struct {
	fs       *FS
	api      *offlineNotebookAPI
	entry    Entry
	ctx      context.Context
	original []byte
	changed  []byte
	def      fabric.Definition
	buffer   []byte
	handle   Handle
}

func newOfflineNotebookHarness(tb testing.TB, size int) *offlineNotebookHarness {
	tb.Helper()
	original, changed, def := offlineNotebookFixture(tb, size)
	api := &offlineNotebookAPI{def: cloneDefinition(def)}
	// Keep scratch writes under the working directory, not the system temp tree.
	spool, err := os.MkdirTemp(".", "notebook-benchmark-spool-")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := os.RemoveAll(spool); err != nil {
			tb.Error(err)
		}
	})
	opts := DefaultOptions()
	opts.WorkspaceIDs, opts.SpoolDirectory = []string{offlineNotebookWorkspace}, spool
	// Frozen time makes cache reuse independent of machine speed and b.N.
	opts.Now = func() time.Time { return time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC) }
	s, err := New(api, &offlineNotebookLake{}, opts)
	if err != nil {
		tb.Fatal(err)
	}
	h := &offlineNotebookHarness{
		fs: s, api: api, entry: offlineNotebookEntry(), ctx: context.Background(),
		original: original, changed: changed, def: def, buffer: make([]byte, 64<<10),
	}
	tb.Cleanup(func() {
		if h.handle != nil {
			h.handle.Close()
		}
		if err := s.Close(); err != nil {
			tb.Error(err)
		}
	})
	return h
}

func (h *offlineNotebookHarness) counts() offlineNotebookCounts {
	h.api.mu.Lock()
	reads, updates := h.api.reads, h.api.updates
	h.api.mu.Unlock()
	stats := h.fs.SnapshotStats()
	return offlineNotebookCounts{reads, updates, stats.Loads, stats.Decodes, stats.Digests}
}

var offlineNotebookCases = []struct {
	name string
	want offlineNotebookCounts
}{
	{"ColdStat", offlineNotebookCounts{reads: 1, loads: 1, decodes: 1}},
	{"ColdOpen", offlineNotebookCounts{reads: 1, loads: 1, decodes: 1}},
	{"WarmOpen", offlineNotebookCounts{}},
	{"WarmChunkReads", offlineNotebookCounts{}},
	{"CleanFlush", offlineNotebookCounts{}},
	{"DirtySave", offlineNotebookCounts{reads: 1, updates: 1, snapshotDigests: 1}},
	{"IdenticalRewrite", offlineNotebookCounts{reads: 1, snapshotDigests: 1}},
	{"SaveThenReopen", offlineNotebookCounts{reads: 2, updates: 1, loads: 1, decodes: 1, snapshotDigests: 1}},
	{"ConcurrentColdOpens4", offlineNotebookCounts{reads: 1, loads: 1, decodes: 1}},
}

func (h *offlineNotebookHarness) prepare(name string) error {
	switch name {
	case "ColdStat", "ColdOpen", "ConcurrentColdOpens4":
		h.fs.invalidateDefinition(h.entry)
		return nil
	case "DirtySave", "IdenticalRewrite", "SaveThenReopen":
		// Each operation starts with the same generation and a decoded, but
		// not yet digested, snapshot. Setup requests are excluded from metrics.
		h.api.mu.Lock()
		h.api.def = cloneDefinition(h.def)
		h.api.mu.Unlock()
		h.fs.invalidateDefinition(h.entry)
	}
	if _, err := h.fs.Stat(h.ctx, h.entry); err != nil {
		return err
	}
	flags := os.O_RDONLY
	if name == "CleanFlush" {
		flags = os.O_RDWR
	}
	if name == "WarmChunkReads" || name == "CleanFlush" {
		var err error
		h.handle, err = h.fs.Open(h.ctx, h.entry, flags)
		return err
	}
	return nil
}

func (h *offlineNotebookHarness) openAndClose() error {
	handle, err := h.fs.Open(h.ctx, h.entry, os.O_RDONLY)
	if err != nil {
		return err
	}
	if handle.Size() != int64(len(h.original)) {
		return errors.Join(fmt.Errorf("unexpected notebook size: %d", handle.Size()), handle.Close())
	}
	return handle.Close()
}

func (h *offlineNotebookHarness) run(name string) error {
	switch name {
	case "ColdStat":
		entry, err := h.fs.Stat(h.ctx, h.entry)
		if err == nil && entry.Size != int64(len(h.original)) {
			return fmt.Errorf("unexpected stat size: %d", entry.Size)
		}
		return err
	case "ColdOpen", "WarmOpen":
		return h.openAndClose()
	case "WarmChunkReads":
		for offset := 0; offset < len(h.original); {
			part := h.buffer[:min(len(h.buffer), len(h.original)-offset)]
			n, err := h.handle.ReadAt(h.ctx, part, int64(offset))
			if err != nil {
				return err
			}
			if n != len(part) || !bytes.Equal(part, h.original[offset:offset+n]) {
				return fmt.Errorf("incorrect chunk at %d", offset)
			}
			offset += n
		}
		return nil
	case "CleanFlush":
		return h.handle.Flush(h.ctx)
	case "DirtySave", "IdenticalRewrite", "SaveThenReopen":
		handle, err := h.fs.Open(h.ctx, h.entry, os.O_RDWR)
		if err != nil {
			return err
		}
		defer handle.Close()
		body := h.changed
		if name == "IdenticalRewrite" {
			body = h.original
		}
		n, err := handle.WriteAt(h.ctx, body, 0)
		if err != nil {
			return err
		}
		if n != len(body) {
			return fmt.Errorf("short notebook write: %d", n)
		}
		if err := handle.Flush(h.ctx); err != nil {
			return err
		}
		if err := handle.Close(); err != nil {
			return err
		}
		if name == "SaveThenReopen" {
			return h.openAndClose()
		}
		return nil
	case "ConcurrentColdOpens4":
		// A batch is one benchmark operation. Four handles fit the default
		// pinned-byte budget even for the near-16-MiB fixture.
		type result struct {
			handle Handle
			err    error
		}
		start := make(chan struct{})
		results := make(chan result, 4)
		for range 4 {
			go func() {
				<-start
				handle, err := h.fs.Open(h.ctx, h.entry, os.O_RDONLY)
				results <- result{handle, err}
			}()
		}
		close(start)
		opened := make([]result, 0, 4)
		for range 4 {
			opened = append(opened, <-results)
		}
		var err error
		for _, result := range opened {
			err = errors.Join(err, result.err)
			if result.handle != nil {
				if result.handle.Size() != int64(len(h.original)) {
					err = errors.Join(err, fmt.Errorf("incorrect concurrent handle size"))
				}
				err = errors.Join(err, result.handle.Close())
			}
		}
		return err
	}
	return fmt.Errorf("unknown workload %q", name)
}

func (h *offlineNotebookHarness) finish() error {
	if h.handle != nil {
		err := h.handle.Close()
		h.handle = nil
		return err
	}
	return nil
}

func (h *offlineNotebookHarness) verify(tb testing.TB, name string) {
	tb.Helper()
	stats := h.fs.SnapshotStats()
	if stats.PinnedBytes != 0 || stats.PinnedHandles != 0 {
		tb.Fatalf("snapshot pins leaked: %+v", stats)
	}
	h.api.mu.Lock()
	actual := cloneDefinition(h.api.def)
	h.api.mu.Unlock()
	wantBody := h.original
	if name == "DirtySave" || name == "SaveThenReopen" {
		wantBody = h.changed
	}
	body, err := actual.Parts[1].Decode()
	if err != nil || !bytes.Equal(body, wantBody) {
		tb.Fatalf("saved notebook did not preserve its content and unknown fields: %v", err)
	}
	// Ignore only the intentionally replaced payload; compare every other
	// definition field and part, including .platform and unknown metadata.
	actual.Parts[1].Payload = h.def.Parts[1].Payload
	if !reflect.DeepEqual(actual, h.def) {
		tb.Fatal("save changed extra parts or unknown definition/part fields")
	}
	if name == "SaveThenReopen" {
		snapshot, ok := h.fs.peekSnapshot(h.entry, h.fs.opts.CacheTTL)
		if !ok || !bytes.Equal(snapshot.parts[snapshot.notebook], wantBody) {
			tb.Fatal("reopen did not observe the saved server generation")
		}
	}
}

// Request-count baselines are assertions, not timing thresholds. In particular,
// a dirty or identical save performs a fresh DefinitionRead even though it does
// not load/decode a snapshot; a clean Flush does not contact the API at all.
func TestNotebookOfflineRequestBaselines(t *testing.T) {
	for _, size := range offlineNotebookSizes {
		t.Run(size.name, func(t *testing.T) {
			for _, workload := range offlineNotebookCases {
				t.Run(workload.name, func(t *testing.T) {
					h := newOfflineNotebookHarness(t, size.size)
					// Repeat to catch stale post-save state and accidental
					// reloads on what should remain warm operations.
					for range 2 {
						if err := h.prepare(workload.name); err != nil {
							t.Fatal(err)
						}
						before := h.counts()
						if err := h.run(workload.name); err != nil {
							t.Fatal(err)
						}
						if err := h.finish(); err != nil {
							t.Fatal(err)
						}
						if got := h.counts().sub(before); got != workload.want {
							t.Fatalf("request/snapshot delta = %+v, want %+v", got, workload.want)
						}
						h.verify(t, workload.name)
					}
				})
			}
		})
	}
}

func BenchmarkNotebookOffline(b *testing.B) {
	for _, size := range offlineNotebookSizes {
		b.Run(size.name, func(b *testing.B) {
			for _, workload := range offlineNotebookCases {
				b.Run(workload.name, func(b *testing.B) {
					b.StopTimer()
					h := newOfflineNotebookHarness(b, size.size)
					var total offlineNotebookCounts
					b.ReportAllocs()
					if workload.name == "WarmChunkReads" {
						b.SetBytes(int64(size.size))
					}
					b.ResetTimer()
					for range b.N {
						if err := h.prepare(workload.name); err != nil {
							b.Fatal(err)
						}
						before := h.counts()
						b.StartTimer()
						err := h.run(workload.name)
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						if err := h.finish(); err != nil {
							b.Fatal(err)
						}
						delta := h.counts().sub(before)
						if delta != workload.want {
							b.Fatalf("request/snapshot delta = %+v, want %+v", delta, workload.want)
						}
						total = total.add(delta)
					}
					h.verify(b, workload.name)
					total.report(b)
				})
			}
		})
	}
}
