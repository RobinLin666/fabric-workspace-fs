package workspacefs

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/testutil"
)

const (
	testFolderA = "77777777-7777-7777-7777-777777777777"
	testFolderB = "88888888-8888-8888-8888-888888888888"
)

func workspaceRoot(t *testing.T, s *FS) Entry {
	t.Helper()
	return lookup(t, s, s.Root(), "Sample workspace")
}

func TestFabricFoldersAndTypedItemsReplaceGroups(t *testing.T) {
	s, remote := newTestFS(t, func(o *Options) { o.CacheTTL = time.Minute })
	remote.SetFolders([]fabric.Folder{
		{ID: testFolderB, DisplayName: "Nested", ParentFolderID: testFolderA},
		{ID: testFolderA, DisplayName: "Projects"},
	})
	remote.SetItemFolder(testutil.NotebookID, testFolderB)
	workspace := workspaceRoot(t, s)
	entries, err := s.ReadDir(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == "Notebooks" || e.Name == "Lakehouses" || e.Name == "Environments" {
			t.Fatal("old type group exposed", e.Name)
		}
	}
	project := lookup(t, s, workspace, "Projects")
	nested := lookup(t, s, project, "Nested")
	nb := lookup(t, s, nested, "Sample notebook.Notebook")
	content := lookup(t, s, nb, namespace.NotebookContentFileName(nb.Item.DisplayName))
	if content.Part != "notebook-content.ipynb" || nb.Item.FolderID != testFolderB {
		t.Fatal("remote identity/part path changed", content, nb)
	}
	if _, err := s.Lookup(context.Background(), nb, namespace.NotebookContentName); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("legacy notebook filename exposed", err)
	}
	builtin := lookup(t, s, nb, "builtin")
	if _, err := s.ReadDir(context.Background(), builtin); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("disabled provider fabricated empty builtin contents", err)
	}
	for _, container := range []Entry{project, nested, nb} {
		meta := lookup(t, s, container, identityFileName)
		h, err := s.Open(context.Background(), meta, os.O_RDONLY)
		if err != nil {
			t.Fatal(err)
		}
		data := read(t, h)
		_ = h.Close()
		var value map[string]string
		if err := json.Unmarshal([]byte(data), &value); err != nil {
			t.Fatal(err)
		}
		if value["workspaceId"] != testutil.WorkspaceID {
			t.Fatal("metadata missing workspace", value)
		}
		if container.Kind == Notebook && (value["folderId"] != testFolderB || value["remotePartPath"] != content.Part) {
			t.Fatal("notebook metadata lost hierarchy/part identity", value)
		}
	}
	lake := lookup(t, s, workspace, "Sample lakehouse.Lakehouse")
	lookup(t, s, lake, "Files")
	lookup(t, s, lake, "Tables")
	lookup(t, s, workspace, "Sample environment.Environment")
}

func TestFolderGraphRejectsOrphansCyclesAndDuplicateParents(t *testing.T) {
	valid := fabric.Folder{ID: testFolderA, DisplayName: "Parent"}
	for _, test := range []struct {
		name    string
		folders []fabric.Folder
		items   []fabric.Item
	}{
		{"orphan-parent", []fabric.Folder{{ID: testFolderA, DisplayName: "A", ParentFolderID: testFolderB}}, nil},
		{"self-cycle", []fabric.Folder{{ID: testFolderA, DisplayName: "A", ParentFolderID: testFolderA}}, nil},
		{"two-cycle", []fabric.Folder{{ID: testFolderA, DisplayName: "A", ParentFolderID: testFolderB}, {ID: testFolderB, DisplayName: "B", ParentFolderID: testFolderA}}, nil},
		{"duplicate-parent", []fabric.Folder{valid, {ID: testFolderA, DisplayName: "Other", ParentFolderID: testFolderB}}, nil},
		{"orphan-item", nil, []fabric.Item{{ID: testutil.NotebookID, Type: "Notebook", DisplayName: "N", FolderID: testFolderA}}},
		{"repeated-item", nil, []fabric.Item{{ID: testutil.NotebookID, Type: "Notebook", DisplayName: "N"}, {ID: testutil.NotebookID, Type: "Notebook", DisplayName: "Other"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := buildFolderTree(test.folders, test.items); !errors.Is(err, fs.ErrInvalid) {
				t.Fatal("invalid hierarchy accepted", err)
			}
		})
	}
}

func TestFolderRenameRefreshPreservesIDAndDescendants(t *testing.T) {
	s, remote := newTestFS(t, func(o *Options) { o.CacheTTL = time.Hour })
	remote.SetFolders([]fabric.Folder{{ID: testFolderA, DisplayName: "Before"}})
	remote.SetItemFolder(testutil.NotebookID, testFolderA)
	root := workspaceRoot(t, s)
	before := lookup(t, s, root, "Before")
	remote.SetFolders([]fabric.Folder{{ID: testFolderA, DisplayName: "After"}})
	s.InvalidateCatalog(testutil.WorkspaceID)
	after := lookup(t, s, root, "After")
	if before.Folder.ID != after.Folder.ID {
		t.Fatal("folder rename rebound identity")
	}
	if _, err := s.Lookup(context.Background(), root, "Before"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("old folder name stayed visible", err)
	}
	lookup(t, s, after, "Sample notebook.Notebook")
	remote.SetFolders([]fabric.Folder{{ID: testFolderA, DisplayName: "After"}, {ID: testFolderB, DisplayName: "Before"}})
	s.InvalidateCatalog(testutil.WorkspaceID)
	newFolder := lookup(t, s, root, "Before (2)")
	if newFolder.Folder.ID != testFolderB {
		t.Fatal("new folder inherited old path's ID")
	}
}
