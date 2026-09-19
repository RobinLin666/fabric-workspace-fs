package workspacefs

import (
	"context"
	"encoding/base64"
	"errors"
	"io/fs"
	"os"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/testutil"
)

func resourceTestFS(t *testing.T) (*FS, *testutil.Service, *testutil.ResourceStore) {
	t.Helper()
	store := testutil.NewResources()
	s, remote := newTestFS(t, func(o *Options) { o.ResourceBackend = store })
	return s, remote, store
}

func TestNotebookBuiltinUsesBoundedSpoolCRUD(t *testing.T) {
	s, _, store := resourceTestFS(t)
	nb, _ := notebook(t, s)
	builtin := lookup(t, s, nb, "builtin")
	ctx := context.Background()
	dir, err := s.Mkdir(ctx, builtin, "data")
	if err != nil {
		t.Fatal(err)
	}
	e, h, err := s.Create(ctx, dir, "file.bin", os.O_RDWR|os.O_EXCL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt(ctx, []byte("abc"), 2); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if read(t, h) != "\x00\x00ab" {
		t.Fatal("resource writer cannot read its spool")
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, writes := store.Counts()
	for range 3 {
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_, after := store.Counts()
	if writes != after {
		t.Fatal("repeated clean resource flush uploaded again")
	}
	if _, err := s.Rename(ctx, e, dir, "renamed", false); !errors.Is(err, fserrors.ErrBusy) {
		t.Fatal("open resource rename allowed", err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	renamed, err := s.Rename(ctx, e, dir, "renamed", false)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := s.Open(ctx, renamed, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, reader) != "\x00\x00ab" {
		t.Fatal("resource save/rename lost bytes")
	}
	_ = reader.Close()
	if err := s.Remove(ctx, dir, true); !errors.Is(err, fserrors.ErrNotEmpty) {
		t.Fatal("resource rmdir recursed", err)
	}
	if err := s.Remove(ctx, renamed, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, dir, true); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, builtin, true); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("builtin root mutation allowed", err)
	}
}

func TestNotebookEnvHiddenAndEnvironmentResourcesStayReadonly(t *testing.T) {
	s, remote, store := resourceTestFS(t)
	const otherWorkspace = "77777777-7777-7777-7777-777777777777"
	def := remote.Definition()
	def.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(`{"nbformat":4,"cells":[],"metadata":{"dependencies":{"environment":{"environmentId":"` + testutil.EnvironmentID + `","workspaceId":"` + otherWorkspace + `"}}}}`))
	remote.SetDefinition(def)
	envTarget := resources.Target{WorkspaceID: otherWorkspace, ItemID: testutil.EnvironmentID, Kind: "Environment"}
	store.Seed(resources.Path{Target: envTarget, Relative: "shared.txt"}, []byte("shared read-only"))
	nb, _ := notebook(t, s)
	beforeDefinition := remote.Counts().DefinitionReads
	if _, err := s.Lookup(context.Background(), nb, "env"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("removed Notebook env alias remains visible", err)
	}
	if remote.Counts().DefinitionReads != beforeDefinition {
		t.Fatal("hidden alias lookup parsed the Notebook binding")
	}
	envEntry := Entry{Kind: Environment, Directory: true, Workspace: otherWorkspace, Item: fabric.Item{ID: testutil.EnvironmentID, Type: "Environment", DisplayName: "Bound"}}
	alias := lookup(t, s, envEntry, "resources")
	if alias.Resource.Target != envTarget {
		t.Fatal("direct Environment resource identity differed", alias)
	}
	file := lookup(t, s, alias, "shared.txt")
	reader, err := s.Open(context.Background(), file, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, reader) != "shared read-only" {
		t.Fatal("wrong target resource")
	}
	_ = reader.Close()
	_, before := store.Counts()
	if _, err := s.Open(context.Background(), file, os.O_WRONLY|os.O_TRUNC); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("Environment alias write bypass", err)
	}
	if err := s.Truncate(context.Background(), file, 0); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("Environment alias truncate bypass", err)
	}
	if _, _, err := s.Create(context.Background(), alias, "new", os.O_RDWR); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("Environment alias create bypass", err)
	}
	if _, err := s.Mkdir(context.Background(), alias, "new"); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("Environment alias mkdir bypass", err)
	}
	if err := s.Remove(context.Background(), file, false); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("Environment alias delete bypass", err)
	}
	builtin := lookup(t, s, nb, "builtin")
	source, h, err := s.Create(context.Background(), builtin, "owned", os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	_ = h.Close()
	_, before = store.Counts()
	if _, err := s.Rename(context.Background(), source, alias, "shared.txt", false); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("Environment target rename bypass", err)
	}
	_, after := store.Counts()
	if after != before {
		t.Fatal("readonly alias reached backend write")
	}
}

func TestResourceFailuresDoNotBlockNotebookContent(t *testing.T) {
	s, _, store := resourceTestFS(t)
	store.DenyNotebook = true
	nb := item(t, s, "Notebooks", "Sample notebook", testutil.NotebookID)
	content := lookup(t, s, nb, "content.ipynb")
	h, err := s.Open(context.Background(), content, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, h) != testutil.InitialNotebook {
		t.Fatal("content changed")
	}
	_ = h.Close()
	builtin := lookup(t, s, nb, "builtin")
	if _, err := s.ReadDir(context.Background(), builtin); !errors.Is(err, fs.ErrPermission) {
		t.Fatal("resource permission failure was hidden", err)
	}
}

func TestEnvironmentResourceLookupDoesNotExportDefinition(t *testing.T) {
	store := testutil.NewResources()
	remote := testutil.New(t)
	fab, lake := remote.Clients()
	opts := DefaultOptions()
	opts.WorkspaceIDs = []string{testutil.WorkspaceID}
	opts.SpoolDirectory = t.TempDir()
	opts.ResourceBackend = store
	s, err := New(&failingDefinitionAPI{FabricAPI: fab, status: 503, remaining: 10}, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	env := item(t, s, "Environments", "Sample environment", testutil.EnvironmentID)
	root := lookup(t, s, env, "resources")
	if _, err := s.ReadDir(context.Background(), root); err != nil {
		t.Fatal("unready definition blocked independent resources", err)
	}
}
