package workspacefs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cache"
	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/testutil"
)

type snapshotClock struct {
	mu sync.Mutex
	at time.Time
}

func newSnapshotClock() *snapshotClock {
	return &snapshotClock{at: time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)}
}
func (c *snapshotClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}
func (c *snapshotClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

func directNotebook() Entry {
	return Entry{Kind: Notebook, Directory: true, Workspace: testutil.WorkspaceID,
		Item: fabric.Item{ID: testutil.NotebookID, Type: "Notebook", DisplayName: "Sample notebook"}}
}

func namesOf(entries []Entry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name
	}
	return names
}

func TestFixedItemRootsAndIdentityDoNotExportDefinitions(t *testing.T) {
	s, remote := newTestFS(t, nil)
	nb := directNotebook()
	env := Entry{Kind: Environment, Directory: true, Workspace: testutil.WorkspaceID,
		Item: fabric.Item{ID: testutil.EnvironmentID, Type: "Environment", DisplayName: "Environment"}}
	lake := Entry{Kind: Lakehouse, Directory: true, Workspace: testutil.WorkspaceID,
		Item: fabric.Item{ID: testutil.LakehouseID, Type: "Lakehouse", DisplayName: "Lakehouse"}}
	ctx := context.Background()
	for _, test := range []struct {
		entry Entry
		names []string
	}{
		{nb, []string{".fabric.json", "Sample notebook.ipynb", "builtin"}},
		{env, []string{".fabric.json", "Libraries", "Setting", "resources"}},
		{lake, []string{".fabric.json", "Files", "Tables"}},
	} {
		for range 3 {
			entries, err := s.ReadDir(ctx, test.entry)
			if err != nil || !slices.Equal(namesOf(entries), test.names) {
				t.Fatalf("fixed roots %+v: %v, %v", test.entry.Item.Type, namesOf(entries), err)
			}
			identity := lookup(t, s, test.entry, identityFileName)
			h, err := s.Open(ctx, identity, os.O_RDONLY)
			if err != nil {
				t.Fatal(err)
			}
			var metadata map[string]string
			err = json.Unmarshal([]byte(read(t, h)), &metadata)
			h.Close()
			if err != nil || metadata["id"] != test.entry.Item.ID || metadata["remotePartPath"] != "" {
				t.Fatal("basic identity invented an unobserved definition path", metadata, err)
			}
		}
	}
	for _, parent := range []Entry{nb, env} {
		if _, err := s.Lookup(ctx, parent, ".platform"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("hidden platform exposed", err)
		}
	}
	if _, err := s.Lookup(ctx, nb, "env"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("hidden binding alias exposed", err)
	}
	libraries := lookup(t, s, env, "Libraries")
	if entries, err := s.ReadDir(ctx, libraries); err != nil ||
		!slices.Equal(namesOf(entries), []string{"CustomLibraries", "PublicLibraries"}) {
		t.Fatal("documented library categories not local", entries, err)
	}
	builtin := lookup(t, s, nb, "builtin")
	if _, err := s.Stat(ctx, builtin); err != nil {
		t.Fatal("fixed descriptor stat probed an unavailable provider", err)
	}
	if _, err := s.ReadDir(ctx, builtin); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("unavailable provider pretended to contain an empty directory", err)
	}
	if counts := remote.Counts(); counts != (testutil.Counts{}) {
		t.Fatal("fixed roots or identity invoked an API", counts)
	}
}

func TestDecodedNotebookSnapshotLastsExactlyTwoMinutes(t *testing.T) {
	clock := newSnapshotClock()
	s, remote := newTestFS(t, func(o *Options) { o.CacheTTL, o.Now = cachepolicy.DefaultTTL, clock.Now })
	nb := directNotebook()
	content := Entry{Name: "Sample notebook.ipynb", Kind: NotebookContent, Workspace: nb.Workspace, Item: nb.Item}
	start := clock.Now()
	for _, seconds := range []int{0, 10, 60, 119, 121} {
		clock.Advance(start.Add(time.Duration(seconds) * time.Second).Sub(clock.Now()))
		if _, err := s.ReadDir(context.Background(), nb); err != nil {
			t.Fatal(err)
		}
		stat, err := s.Stat(context.Background(), content)
		if err != nil || stat.Size != int64(len(testutil.InitialNotebook)) {
			t.Fatal("inaccurate Notebook size", stat, err)
		}
		h, err := s.Open(context.Background(), content, os.O_RDONLY)
		if err != nil {
			t.Fatal(err)
		}
		if read(t, h) != testutil.InitialNotebook {
			t.Fatal("snapshot body changed")
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		want := uint64(1)
		if seconds > 120 {
			want = 2
		}
		stats := s.SnapshotStats()
		if stats.Loads != want || stats.Decodes != want || stats.Digests != 0 || stats.PinnedBytes != 0 ||
			remote.Counts().DefinitionReads != int(want) {
			t.Fatalf("t=%ds snapshot=%+v remote=%+v", seconds, stats, remote.Counts())
		}
		attr, entry, _ := s.CacheTimeouts(stat)
		if seconds == 119 && (attr != time.Second || entry != time.Second) {
			t.Fatal("kernel timeout extended a nearly-expired snapshot", attr, entry)
		}
	}
}

func TestSnapshotIdentityPathIsOptionalAndExpiresWithSource(t *testing.T) {
	clock := newSnapshotClock()
	s, _ := newTestFS(t, func(o *Options) { o.CacheTTL, o.Now = cachepolicy.DefaultTTL, clock.Now })
	nb := directNotebook()
	metadata := lookup(t, s, nb, identityFileName)
	if metadata.RemotePartPath != "" {
		t.Fatal("cold identity fabricated a part path")
	}
	content := lookup(t, s, nb, "Sample notebook.ipynb")
	metadata = lookup(t, s, nb, identityFileName)
	if metadata.RemotePartPath != content.Part {
		t.Fatal("observed part identity not reused")
	}
	clock.Advance(2 * time.Minute)
	metadata, err := s.Stat(context.Background(), metadata)
	if err != nil || metadata.RemotePartPath != "" {
		t.Fatal("identity held an unbounded/stale part snapshot", metadata, err)
	}
	if s.SnapshotStats().Loads != 1 {
		t.Fatal("identity expiry exported a Notebook")
	}
}

func TestOpenHandlesPinOneVersionAndAccountBytes(t *testing.T) {
	clock := newSnapshotClock()
	s, remote := newTestFS(t, func(o *Options) { o.CacheTTL, o.Now = cachepolicy.DefaultTTL, clock.Now })
	_, entry := notebook(t, s)
	old, err := s.Open(context.Background(), entry, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { old.Close() })
	before := s.SnapshotStats()
	if before.PinnedHandles != 1 || before.PinnedBytes < before.RetainedBytes || before.PinnedBytes <= int64(len(testutil.InitialNotebook)) {
		t.Fatal("raw or decoded bytes were not charged to the pinned handle", before)
	}
	updated := `{"cells":[],"nbformat":4,"metadata":{"generation":2}}`
	def := remote.Definition()
	def.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(updated))
	remote.SetDefinition(def)
	clock.Advance(121 * time.Second)
	fresh, err := s.Open(context.Background(), entry, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, old) != testutil.InitialNotebook || read(t, fresh) != updated {
		t.Fatal("an open read handle changed version on TTL refresh")
	}
	fresh.Close()
	old.Close()
	old.Close()
	if stats := s.SnapshotStats(); stats.PinnedBytes != 0 || stats.PinnedHandles != 0 {
		t.Fatal("closed snapshots leaked byte reservations", stats)
	}
	reservation, err := s.reserveSnapshotBytes(256 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(context.Background(), entry, os.O_RDONLY); !errors.Is(err, fserrors.ErrBusy) {
		t.Fatal("open snapshot bypassed total pinned-byte budget", err)
	}
	reservation()
	if stats := s.SnapshotStats(); stats.PinnedBytes != 0 {
		t.Fatal(stats)
	}
}

func TestEnvironmentDynamicPartsAreDiscoveredWithoutPlatform(t *testing.T) {
	s, remote := newTestFS(t, func(o *Options) { o.CacheTTL = cachepolicy.DefaultTTL })
	remote.SetEnvironment(fabric.Definition{Parts: []fabric.Part{
		{Path: ".platform", Payload: "e30=", PayloadType: "InlineBase64"},
		{Path: "Extension/config.txt", Payload: "dmFsdWU=", PayloadType: "InlineBase64"},
	}})
	env := Entry{Kind: Environment, Directory: true, Workspace: testutil.WorkspaceID, Item: fabric.Item{ID: testutil.EnvironmentID, Type: "Environment", DisplayName: "Environment"}}
	if entries, err := s.ReadDir(context.Background(), env); err != nil || len(entries) != 4 || remote.Counts().DefinitionReads != 0 {
		t.Fatal("cold fixed categories required export", entries, err)
	}
	extension := lookup(t, s, env, "Extension")
	file := lookup(t, s, extension, "config.txt")
	h, err := s.Open(context.Background(), file, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, h) != "value" {
		t.Fatal("unknown public definition part disappeared")
	}
	h.Close()
	entries, err := s.ReadDir(context.Background(), env)
	if err != nil || !slices.Equal(namesOf(entries), []string{".fabric.json", "Extension", "Libraries", "Setting", "resources"}) {
		t.Fatal("discovered dynamic root missing or platform leaked", entries, err)
	}
	if remote.Counts().DefinitionReads != 1 || s.SnapshotStats().Decodes != 1 {
		t.Fatal("fixed/dynamic enumeration repeatedly exported or decoded")
	}
}

func TestOversizedSnapshotIsNotRetainedByIdentity(t *testing.T) {
	s, _ := newTestFS(t, func(o *Options) { o.CacheTTL = cachepolicy.DefaultTTL })
	s.snapshots = cache.New(cache.Options[*definitionSnapshot]{
		TTL: cachepolicy.DefaultTTL, MaxEntries: 32, MaxBytes: 1,
		Size: func(snapshot *definitionSnapshot) int64 { return snapshot.bytes },
	})
	nb := directNotebook()
	content := lookup(t, s, nb, "Sample notebook.ipynb")
	if content.Size <= 0 {
		t.Fatal("uncached oversized result was discarded")
	}
	identity := lookup(t, s, nb, identityFileName)
	if identity.RemotePartPath != "" || s.SnapshotStats().RetainedBytes != 0 {
		t.Fatal("optional identity bypassed snapshot retention budget")
	}
}

func TestDefinitionOperationPoliciesUseOneSourceGeneration(t *testing.T) {
	policy, err := cachepolicy.Parse([]byte(`{"types":{"Notebook":{"attr":"1s"},"Environment":{"definition":"10s"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	clock := newSnapshotClock()
	s, remote := newTestFS(t, func(o *Options) { o.CachePolicy, o.Now = policy, clock.Now })
	_, content := notebook(t, s)
	clock.Advance(10 * time.Second)
	h, err := s.Open(context.Background(), content, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	if remote.Counts().DefinitionReads != 1 {
		t.Fatal("content incorrectly used the shorter attr freshness")
	}
	stat, err := s.Stat(context.Background(), content)
	if err != nil || remote.Counts().DefinitionReads != 2 {
		t.Fatal("short attr did not require a new source generation", err)
	}
	if attr, _, _ := s.CacheTimeouts(stat); attr != time.Second {
		t.Fatal("kernel attributes exceeded source freshness", attr)
	}
	envFile := Entry{Kind: DefinitionFile, Part: "Libraries/PublicLibraries/environment.yml", Workspace: testutil.WorkspaceID,
		Item: fabric.Item{ID: testutil.EnvironmentID, Type: "Environment"}}
	if _, err := s.Stat(context.Background(), envFile); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	if _, err := s.Stat(context.Background(), envFile); err != nil || remote.Counts().DefinitionReads != 4 {
		t.Fatal("Environment override did not expire independently", err)
	}
	h, err = s.Open(context.Background(), content, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	if remote.Counts().DefinitionReads != 4 {
		t.Fatal("Environment TTL invalidated the unrelated Notebook snapshot")
	}
}

func TestExplicitZeroContentPolicyNeverHitsRetainedBody(t *testing.T) {
	policy, err := cachepolicy.Parse([]byte(`{"types":{"Notebook":{"surfaces":{"content":{"content":"0s"}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	s, remote := newTestFS(t, func(o *Options) { o.CachePolicy = policy })
	_, content := notebook(t, s)
	for range 3 {
		h, err := s.Open(context.Background(), content, os.O_RDONLY)
		if err != nil {
			t.Fatal(err)
		}
		h.Close()
	}
	if remote.Counts().DefinitionReads != 4 || s.SnapshotStats().Decodes != 4 {
		t.Fatal("explicit content zero was treated as inherited", remote.Counts(), s.SnapshotStats())
	}
}
