package workspacefs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/testutil"
)

type managementFixture struct {
	mu                     sync.Mutex
	calls, mutations, next int
	items                  map[string]fabric.Item
	folders                map[string]fabric.Folder
	definitions            map[string]fabric.Definition
	fail                   error
}

func (m *managementFixture) ListWorkspaces(context.Context) ([]fabric.Workspace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return []fabric.Workspace{{ID: testutil.WorkspaceID, DisplayName: "Sample workspace"}}, nil
}
func (m *managementFixture) GetWorkspace(context.Context, string) (fabric.Workspace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return fabric.Workspace{ID: testutil.WorkspaceID, DisplayName: "Sample workspace"}, nil
}
func (m *managementFixture) ListItems(context.Context, string) ([]fabric.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	out := make([]fabric.Item, 0, len(m.items))
	for _, i := range m.items {
		out = append(out, i)
	}
	return out, nil
}
func (m *managementFixture) ListFolders(context.Context, string) ([]fabric.Folder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	out := make([]fabric.Folder, 0, len(m.folders))
	for _, i := range m.folders {
		out = append(out, i)
	}
	return out, nil
}
func (m *managementFixture) GetDefinition(_ context.Context, _ string, id, _, _ string) (fabric.Definition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	def, found := m.definitions[id]
	if !found {
		return fabric.Definition{}, fs.ErrNotExist
	}
	return cloneDefinition(def), nil
}
func (m *managementFixture) UpdateNotebook(_ context.Context, _, id string, def fabric.Definition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.mutations++
	m.definitions[id] = cloneDefinition(def)
	return nil
}
func (m *managementFixture) newID() string {
	m.next++
	return fmt.Sprintf("99999999-9999-9999-9999-%012d", m.next)
}
func (m *managementFixture) CreateItem(_ context.Context, _, kind, name, folder string) (fabric.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.mutations++
	if m.fail != nil {
		return fabric.Item{}, m.fail
	}
	id := m.newID()
	item := fabric.Item{ID: id, Type: kind, DisplayName: name, FolderID: folder}
	m.items[id] = item
	parts := []fabric.Part{{Path: ".platform", Payload: "e30=", PayloadType: "InlineBase64"}}
	if kind == "Notebook" {
		parts = append(parts, fabric.Part{Path: "artifact.content.ipynb", Payload: base64.StdEncoding.EncodeToString([]byte(testutil.InitialNotebook)), PayloadType: "InlineBase64"})
	}
	if kind == "Environment" {
		parts = append(parts, fabric.Part{Path: "Setting/Sparkcompute.yml", Payload: "e30=", PayloadType: "InlineBase64"})
	}
	m.definitions[id] = fabric.Definition{Parts: parts}
	return item, nil
}
func (m *managementFixture) DeleteItem(_ context.Context, _, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.mutations++
	if _, ok := m.items[id]; !ok {
		return fs.ErrNotExist
	}
	delete(m.items, id)
	delete(m.definitions, id)
	return nil
}
func (m *managementFixture) CreateFolder(_ context.Context, _, name, parent string) (fabric.Folder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.mutations++
	if m.fail != nil {
		return fabric.Folder{}, m.fail
	}
	folder := fabric.Folder{ID: m.newID(), DisplayName: name, ParentFolderID: parent}
	m.folders[folder.ID] = folder
	return folder, nil
}
func (m *managementFixture) DeleteFolder(_ context.Context, _, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.mutations++
	for _, item := range m.items {
		if item.FolderID == id {
			return fserrors.ErrNotEmpty
		}
	}
	for _, folder := range m.folders {
		if folder.ParentFolderID == id {
			return fserrors.ErrNotEmpty
		}
	}
	if _, ok := m.folders[id]; !ok {
		return fs.ErrNotExist
	}
	delete(m.folders, id)
	return nil
}
func (m *managementFixture) counts() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.mutations
}

type emptyManagementLake struct{ nonempty bool }

func (l *emptyManagementLake) List(context.Context, onelake.Path) ([]onelake.Info, error) {
	if l.nonempty {
		return []onelake.Info{{Path: "Files/data", Size: 1}}, nil
	}
	return []onelake.Info{}, nil
}
func (*emptyManagementLake) Stat(context.Context, onelake.Path) (onelake.Info, error) {
	return onelake.Info{IsDir: true, ETag: `"empty"`}, nil
}
func (*emptyManagementLake) Read(context.Context, onelake.Path, int64, []byte, string) (int, error) {
	return 0, io.EOF
}
func (*emptyManagementLake) Put(context.Context, onelake.Path, io.ReaderAt, int64, string) (onelake.Info, error) {
	return onelake.Info{}, fserrors.ErrUnsupported
}
func (*emptyManagementLake) Mkdir(context.Context, onelake.Path) error {
	return fserrors.ErrUnsupported
}
func (*emptyManagementLake) Remove(context.Context, onelake.Path, bool, string) error {
	return fserrors.ErrUnsupported
}
func (*emptyManagementLake) Rename(context.Context, onelake.Path, onelake.Path, string, string, bool) (onelake.Info, error) {
	return onelake.Info{}, fserrors.ErrUnsupported
}

func managedTestFS(t *testing.T, readOnly bool) (*FS, *managementFixture, *emptyManagementLake) {
	t.Helper()
	api := &managementFixture{items: make(map[string]fabric.Item), folders: make(map[string]fabric.Folder), definitions: make(map[string]fabric.Definition)}
	lake := &emptyManagementLake{}
	opts := DefaultOptions()
	opts.WorkspaceIDs = []string{testutil.WorkspaceID}
	opts.SpoolDirectory = t.TempDir()
	opts.CacheTTL = time.Hour
	opts.ReadOnly = readOnly
	s, err := New(api, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, api, lake
}

func TestManagedMkdirRoutesItemsFoldersAndDeletionByIdentity(t *testing.T) {
	s, api, _ := managedTestFS(t, false)
	root := workspaceRoot(t, s)
	ctx := context.Background()
	parent, err := s.Mkdir(ctx, root, "Project")
	if err != nil || parent.Kind != FabricFolder || parent.Folder.DisplayName != "Project" {
		t.Fatal(parent, err)
	}
	for _, kind := range []string{"Notebook", "Lakehouse", "Environment"} {
		name := "Default." + kind
		item, err := s.Mkdir(ctx, parent, name)
		if err != nil || item.Item.Type != kind || item.Item.FolderID != parent.Folder.ID {
			t.Fatal("typed creation failed", item, err)
		}
		looked := lookup(t, s, parent, name)
		if looked.Item.ID != item.Item.ID {
			t.Fatal("create lookup used a stale or incorrect identity")
		}
		if _, err := s.Mkdir(ctx, parent, name); !errors.Is(err, fs.ErrExist) {
			t.Fatal("duplicate create", err)
		}
		if _, err := s.Rename(ctx, item, parent, "renamed."+kind, false); !errors.Is(err, fserrors.ErrUnsupported) {
			t.Fatal("remote rename was faked", err)
		}
		if kind == "Lakehouse" {
			if err := s.Remove(ctx, item, true); err != nil {
				t.Fatal("empty Lakehouse deletion", err)
			}
		} else {
			before, _ := api.counts()
			if err := s.Remove(ctx, item, true); !errors.Is(err, fserrors.ErrUnsupported) {
				t.Fatal("hidden-resource item deletion was allowed", kind, err)
			}
			after, _ := api.counts()
			if after != before {
				t.Fatal("unsupported item deletion sent an API request")
			}
			// Explicit cleanup applies only to this test's newly allocated ID.
			if err := api.DeleteItem(ctx, item.Workspace, item.Item.ID); err != nil {
				t.Fatal(err)
			}
			s.InvalidateCatalog(item.Workspace)
		}
		if _, err := s.Lookup(ctx, parent, name); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("deleted item cached", err)
		}
		_, before := api.counts()
		if _, err := s.Mkdir(ctx, parent, name); !errors.Is(err, fs.ErrExist) {
			t.Fatal("deleted identity alias reused", err)
		}
		_, after := api.counts()
		if after != before {
			t.Fatal("reserved alias created wrong remote identity")
		}
	}
	if err := s.Remove(ctx, parent, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lookup(ctx, root, parent.Name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("deleted folder cached", err)
	}
}

func TestManagedDeleteRefusesNonemptyOrOpenItems(t *testing.T) {
	s, api, lake := managedTestFS(t, false)
	root := workspaceRoot(t, s)
	ctx := context.Background()
	nb, err := s.Mkdir(ctx, root, "N.Notebook")
	if err != nil {
		t.Fatal(err)
	}
	content := lookup(t, s, nb, "N.ipynb")
	h, err := s.Open(ctx, content, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := api.counts()
	if err := s.Remove(ctx, nb, true); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("deleted open Notebook", err)
	}
	_ = h.Close()
	after, _ := api.counts()
	if before != after {
		t.Fatal("Notebook rmdir attempted remote I/O")
	}
	api.mu.Lock()
	def := api.definitions[nb.Item.ID]
	def.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(`{"nbformat":4,"cells":[{"cell_type":"markdown","source":["data"],"metadata":{}}]}`))
	api.definitions[nb.Item.ID] = def
	api.mu.Unlock()
	if err := s.Remove(ctx, nb, true); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("deleted nonempty Notebook", err)
	}
	lh, err := s.Mkdir(ctx, root, "L.Lakehouse")
	if err != nil {
		t.Fatal(err)
	}
	lake.nonempty = true
	if err := s.Remove(ctx, lh, true); !errors.Is(err, fserrors.ErrNotEmpty) {
		t.Fatal("deleted nonempty Lakehouse", err)
	}
	env, err := s.Mkdir(ctx, root, "E.Environment")
	if err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	def = api.definitions[env.Item.ID]
	def.Parts[1].Payload = "Y2hhbmdlZA=="
	api.definitions[env.Item.ID] = def
	api.mu.Unlock()
	before, _ = api.counts()
	if err := s.Remove(ctx, env, true); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("deleted modified Environment", err)
	}
	after, _ = api.counts()
	if before != after {
		t.Fatal("Environment rmdir attempted remote I/O")
	}
}

func TestInjectedAgentBundleIsReadonlyAndNeverCallsFabric(t *testing.T) {
	s, api, _ := managedTestFS(t, false)
	root := s.Root()
	ctx := context.Background()
	before, _ := api.counts()
	dot := lookup(t, s, root, ".agents")
	e := lookup(t, s, root, "AGENTS.md")
	h, err := s.Open(ctx, e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, h) == "" {
		t.Fatal("empty injected instructions")
	}
	_ = h.Close()
	if _, err := s.Stat(ctx, e); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(ctx, e, os.O_RDWR|os.O_TRUNC); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("bundle is writable", err)
	}
	if err := s.Truncate(ctx, e, 0); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("bundle truncate permitted", err)
	}
	for _, parent := range []Entry{root, dot} {
		if _, _, err := s.Create(ctx, parent, "AGENTS.md", os.O_WRONLY); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Fatal("bundle create permitted", err)
		}
		if _, err := s.Mkdir(ctx, parent, ".agents"); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Fatal("bundle mkdir permitted", err)
		}
	}
	if _, err := s.Rename(ctx, e, dot, "overwritten", false); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("bundle rename permitted", err)
	}
	if err := s.Remove(ctx, e, false); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("bundle unlink permitted", err)
	}
	if err := s.Remove(ctx, dot, true); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("bundle rmdir permitted", err)
	}
	after, _ := api.counts()
	if after != before {
		t.Fatalf("injected bundle made Fabric calls: before=%d after=%d", before, after)
	}
}

func TestDeprecatedOverlayOptionsDoNotTouchOldData(t *testing.T) {
	remote := testutil.New(t)
	fab, lake := remote.Clients()
	opts := DefaultOptions()
	opts.WorkspaceIDs, opts.SpoolDirectory = []string{testutil.WorkspaceID}, t.TempDir()
	opts.OverlayDirectory = t.TempDir()
	sentinel := filepath.Join(opts.OverlayDirectory, "old-agent-state")
	if err := os.WriteFile(sentinel, []byte("keep old data"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(fab, lake, opts); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("deprecated overlay option was accepted", err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep old data" {
		t.Fatal("deprecated overlay data changed", err)
	}
	if remote.Counts() != (testutil.Counts{}) {
		t.Fatal("deprecated overlay configuration invoked remote APIs")
	}
}

func TestManagedFailureIsNotAPhantomAndReadonlyBlocksAllRoutes(t *testing.T) {
	s, api, _ := managedTestFS(t, false)
	root := workspaceRoot(t, s)
	ctx := context.Background()
	api.fail = errors.New("injected remote creation failure")
	for _, name := range []string{"Failed.Notebook", "Failed.Lakehouse", "Failed.Environment", "FailedFolder"} {
		if _, err := s.Mkdir(ctx, root, name); err == nil {
			t.Fatal("creation failure hidden")
		}
		if _, err := s.Lookup(ctx, root, name); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("phantom creation", name, err)
		}
	}
	readonly, remote, _ := managedTestFS(t, true)
	roRoot := workspaceRoot(t, readonly)
	before, _ := remote.counts()
	for _, name := range []string{".agents", "New.Notebook", "New.Lakehouse", "New.Environment", "NewFolder"} {
		if _, err := readonly.Mkdir(ctx, roRoot, name); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Fatal("readonly creation allowed", name, err)
		}
	}
	after, _ := remote.counts()
	if after != before {
		t.Fatal("readonly routes called remote APIs")
	}
}

func TestWorkspaceDotFolderIsNotShadowedByInjectedRoot(t *testing.T) {
	s, api, _ := managedTestFS(t, false)
	root := workspaceRoot(t, s)
	ctx := context.Background()
	api.mu.Lock()
	id := api.newID()
	api.folders[id] = fabric.Folder{ID: id, DisplayName: ".agents"}
	api.mu.Unlock()
	if _, err := s.ReadDir(ctx, root); err != nil {
		t.Fatal(err)
	}
	before, _ := api.counts()
	if _, err := s.Mkdir(ctx, root, ".agents"); !errors.Is(err, fs.ErrExist) {
		t.Fatal("injected root hid an existing workspace folder", err)
	}
	after, _ := api.counts()
	if after != before {
		t.Fatal("local conflict check fetched remote APIs")
	}
	meta := lookup(t, s, lookup(t, s, root, ".agents"), identityFileName)
	h, err := s.Open(ctx, meta, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]string
	if err := json.Unmarshal([]byte(read(t, h)), &value); err != nil {
		t.Fatal(err)
	}
	_ = h.Close()
	if value["id"] != id {
		t.Fatal("remote identity was obscured", value)
	}
}
