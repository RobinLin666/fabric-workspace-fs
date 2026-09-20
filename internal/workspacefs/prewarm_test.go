package workspacefs

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/testutil"
)

func prewarmOptions(o *Options) {
	o.CacheTTL = cachepolicy.DefaultTTL
	o.PrewarmNotebookCount = 1
}

func prewarmEntry() Entry {
	return prewarmTarget().entry()
}

func prewarmTarget() NotebookTarget {
	return NotebookTarget{Workspace: testutil.WorkspaceID, Notebook: testutil.NotebookID}
}

func waitPrewarm(t *testing.T, s *FS, ready func(PrewarmStats) bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if ready(s.PrewarmStats()) {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("prewarm did not finish: %+v", s.PrewarmStats())
		case <-tick.C:
		}
	}
}

func TestPrewarmIsMountLevelAndReusesSnapshot(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.ReadDir(context.Background(), directNotebook()); err != nil {
		t.Fatal(err)
	}
	if remote.Counts().DefinitionReads != 0 || s.PrewarmStats().Queued != 0 {
		t.Fatal("configuration or directory browsing started prewarm")
	}
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 1 && !stats.Running })
	e := prewarmEntry()
	if _, err := s.Stat(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(context.Background(), e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, h) != testutil.InitialNotebook {
		t.Fatal("prewarm changed content")
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if remote.Counts().DefinitionReads != 1 || s.SnapshotStats().Decodes != 1 || s.PrewarmStats().Used != 1 {
		t.Fatal(remote.Counts(), s.SnapshotStats(), s.PrewarmStats())
	}
	if err := s.StartPrewarm(context.Background()); err != nil || s.PrewarmStats().Queued != 1 {
		t.Fatal("start was not idempotent", err)
	}
}

func TestPrewarmAfterSaveReadsActualRemoteVersion(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 1 && !stats.Running })
	e := prewarmEntry()
	h, err := s.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	data := `{"nbformat":4,"cells":[],"metadata":{"saved":"new"}}`
	save(t, h, data)
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 2 && !stats.Running })
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := s.Open(context.Background(), e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if read(t, reopened) != data || remote.Counts().DefinitionReads != 3 || remote.Counts().NotebookUpdates != 1 {
		t.Fatal("expected startup + fresh preflight + remote rewarm", remote.Counts())
	}
	stats := s.NotebookStats()
	if stats["definition_export"].Calls != 3 || stats["save_update"].Calls != 1 || stats["spool_sync"].Calls != 1 {
		t.Fatal("diagnostics omitted preflight or spool", stats)
	}
}

func TestPrewarmLeaderSharesForegroundLoadAndCloseCancels(t *testing.T) {
	s, _ := newTestFS(t, prewarmOptions)
	delayed := &delayedDefinitionAPI{FabricAPI: s.fabric.FabricAPI, started: make(chan struct{}), resume: make(chan struct{})}
	s.fabric.FabricAPI = delayed
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	select {
	case <-delayed.started:
	case <-time.After(5 * time.Second):
		t.Fatal("prewarm did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Open(ctx, prewarmEntry(), os.O_RDONLY); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.StartPrewarm(context.Background()); !errors.Is(err, fserrors.ErrClosed) {
		t.Fatal("closed warmer restarted", err)
	}
	if s.SnapshotStats().RetainedBytes != 0 || s.PrewarmStats().Running {
		t.Fatal("cancelled load survived shutdown")
	}
}

func TestPrewarmForegroundJoinsOneExport(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	delayed := &delayedDefinitionAPI{FabricAPI: s.fabric.FabricAPI, started: make(chan struct{}), resume: make(chan struct{})}
	s.fabric.FabricAPI = delayed
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delayed.started:
	case <-time.After(5 * time.Second):
		t.Fatal("prewarm did not start")
	}
	result := make(chan error, 1)
	go func() {
		h, err := s.Open(context.Background(), prewarmEntry(), os.O_RDONLY)
		if err == nil {
			err = h.Close()
		}
		result <- err
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for s.snapshots.Stats().Shared == 0 {
		select {
		case <-deadline.C:
			close(delayed.resume)
			t.Fatal("foreground did not share flight")
		case <-time.After(time.Millisecond):
		}
	}
	close(delayed.resume)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if remote.Counts().DefinitionReads != 1 {
		t.Fatal("foreground repeated prewarm export", remote.Counts())
	}
}

func TestPrewarmSelectsNotebookOnly(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 1 && !stats.Running })
	if remote.Counts().DefinitionReads != 1 {
		t.Fatal("prewarm did not select exactly one Notebook", remote.Counts())
	}
	if remote.Counts().CatalogReads != 1 {
		t.Fatal("prewarm listed folders unnecessarily", remote.Counts())
	}
}

func TestPrewarmSkipsItemsWithDisabledRetention(t *testing.T) {
	s, remote := newTestFS(t, func(o *Options) {
		o.CacheTTL = 0
		o.PrewarmNotebookCount = 1
	})
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Skipped == 1 && !stats.Running })
	if remote.Counts().DefinitionReads != 0 {
		t.Fatal("prewarm bypassed disabled retention", remote.Counts())
	}
}

func TestPrewarmPreservesSnapshotCacheBudget(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	_, err := s.snapshots.Get(context.Background(), "foreground-pressure", func(context.Context) (*definitionSnapshot, error) {
		return &definitionSnapshot{bytes: prewarmRetainedByteLimit}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.BudgetSkipped == 1 && !stats.Running })
	if remote.Counts().DefinitionReads != 0 || s.PrewarmStats().Completed != 0 {
		t.Fatal("prewarm ignored retained snapshot pressure", remote.Counts(), s.PrewarmStats())
	}
}

type prewarmCatalogAPI struct {
	FabricAPI
	items     []fabric.Item
	requested []string
}

func (a *prewarmCatalogAPI) ListItems(context.Context, string) ([]fabric.Item, error) {
	return append([]fabric.Item(nil), a.items...), nil
}

func (a *prewarmCatalogAPI) GetDefinition(ctx context.Context, workspace, item, kind, format string) (fabric.Definition, error) {
	a.requested = append(a.requested, item)
	// The fixture stores one Notebook definition; target selection is the
	// behavior under test, not differing server-side payloads.
	return a.FabricAPI.GetDefinition(ctx, workspace, testutil.NotebookID, kind, format)
}

func TestPrewarmLimitsAndStablyRanksCatalogNotebooks(t *testing.T) {
	s, _ := newTestFS(t, prewarmOptions)
	alpha := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	bravo := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	catalog := &prewarmCatalogAPI{FabricAPI: s.fabric.FabricAPI, items: []fabric.Item{
		{ID: bravo, Type: "Notebook", DisplayName: "Bravo"},
		{ID: testutil.LakehouseID, Type: "Lakehouse", DisplayName: "Ignored"},
		{ID: alpha, Type: "Notebook", DisplayName: "alpha"},
	}}
	s.fabric.FabricAPI = catalog
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 1 && !stats.Running })
	if len(catalog.requested) != 1 || catalog.requested[0] != alpha {
		t.Fatal("prewarm count/order", catalog.requested)
	}
}

func TestPrewarmConfigurationAndGateBounds(t *testing.T) {
	for _, count := range []int{-1, maxPrewarmTargets + 1} {
		if err := ValidatePrewarmCount(count); err == nil {
			t.Fatal("invalid prewarm count accepted", count)
		}
	}
	if err := ValidatePrewarmCount(0); err != nil {
		t.Fatal(err)
	}
}

func TestPrewarmOldFlightCannotOverwriteSuccessfulSave(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	// Open the writer first so a blocked prewarm cannot be its base loader.
	e := prewarmEntry()
	writer, err := s.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	s.invalidateDefinition(e)
	delayed := &delayedDefinitionAPI{FabricAPI: s.fabric.FabricAPI, started: make(chan struct{}), resume: make(chan struct{})}
	s.fabric.FabricAPI = delayed
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delayed.started:
	case <-time.After(5 * time.Second):
		t.Fatal("prewarm did not start")
	}
	updated := `{"nbformat":4,"cells":[],"metadata":{"version":2}}`
	save(t, writer, updated)
	close(delayed.resume)
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 2 && !stats.Running })
	reader, err := s.Open(context.Background(), e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if read(t, reader) != updated || remote.Counts().NotebookUpdates != 1 {
		t.Fatal("old prewarm replaced saved version")
	}
}

type normalizedNotebookAPI struct {
	FabricAPI
	remote *testutil.Service
}

func (a normalizedNotebookAPI) UpdateNotebook(ctx context.Context, ws, item string, def fabric.Definition) error {
	if err := a.FabricAPI.UpdateNotebook(ctx, ws, item, def); err != nil {
		return err
	}
	changed := a.remote.Definition()
	changed.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(`{"nbformat":4,"cells":[],"metadata":{"server":"normalized"}}`))
	a.remote.SetDefinition(changed)
	return nil
}

func TestPrewarmNeverPublishesUnobservedUploadBody(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	s.fabric.FabricAPI = normalizedNotebookAPI{FabricAPI: s.fabric.FabricAPI, remote: remote}
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 1 && !stats.Running })
	e := prewarmEntry()
	h, err := s.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	save(t, h, `{"nbformat":4,"cells":[],"metadata":{"client":"submitted"}}`)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Completed == 2 && !stats.Running })
	h, err = s.Open(context.Background(), e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if !strings.Contains(read(t, h), `"server":"normalized"`) {
		t.Fatal("submitted rather than observed content cached")
	}
}

func TestPrewarmFailureDoesNotPoisonOnDemandRead(t *testing.T) {
	failures := make(chan error, 1)
	s, remote := newTestFS(t, func(o *Options) {
		prewarmOptions(o)
		o.LogPrewarmError = func(err error) { failures <- err }
	})
	original := remote.Definition()
	broken := cloneDefinition(original)
	broken.Parts[1].Payload = "not-base64"
	remote.SetDefinition(broken)
	t.Cleanup(func() { s.Close() })
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failures:
	case <-time.After(5 * time.Second):
		t.Fatal("failed export was not reported")
	}
	if s.SnapshotStats().RetainedBytes != 0 {
		t.Fatal("failed snapshot retained")
	}
	remote.SetDefinition(original)
	h, err := s.Open(context.Background(), prewarmEntry(), os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if read(t, h) != testutil.InitialNotebook || remote.Counts().DefinitionReads != 2 {
		t.Fatal("failed warm poisoned normal read")
	}
}

func TestPrewarmQueueDeduplicatesAndSkipsSaturatedExports(t *testing.T) {
	s, remote := newTestFS(t, prewarmOptions)
	releases := make([]func(), 4)
	for i := range releases {
		var err error
		releases[i], err = s.definitionGate.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	t.Cleanup(func() { s.Close() })
	p := s.prewarm
	// Queueing before startup cannot start or retain speculative tasks.
	p.mu.Lock()
	p.enqueueLocked(prewarmTarget())
	p.mu.Unlock()
	if s.PrewarmStats().Pending != 0 {
		t.Fatal("work queued before startup")
	}
	if err := s.StartPrewarm(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPrewarm(t, s, func(stats PrewarmStats) bool { return stats.Skipped == 1 && !stats.Running })
	if remote.Counts().DefinitionReads != 0 {
		t.Fatal("prewarm exported under foreground saturation")
	}
}

func TestPrewarmDedupAndGatePriority(t *testing.T) {
	var gate definitionGate
	var err error
	releases := make([]func(), 4)
	for i := range releases {
		releases[i], err = gate.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := gate.tryBackground(); ok {
		t.Fatal("background exceeded four slots")
	}
	for _, release := range releases {
		release()
	}
	gate.mu.Lock()
	gate.waiting = 1
	gate.mu.Unlock()
	if _, ok := gate.tryBackground(); ok {
		t.Fatal("background overtook queued foreground")
	}
}
