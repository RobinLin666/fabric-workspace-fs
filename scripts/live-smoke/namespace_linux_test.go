//go:build linux

package main

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/workspacefs"
)

func TestRootFixtureDiscoveryUsesIDAndDoesNotDescend(t *testing.T) {
	root := location{
		entry: workspacefs.Entry{Kind: workspacefs.Workspace, Directory: true, Workspace: testWorkspace},
		path:  "/private-test-mount/Workspace display",
	}
	owned := workspacefs.Entry{
		Name: "Fixture.Notebook", Kind: workspacefs.Notebook, Directory: true, Workspace: testWorkspace,
		Item: fabric.Item{ID: testNotebook, Type: "Notebook", DisplayName: "Fixture"},
	}
	folder := workspacefs.Entry{
		Name: "Real Fabric folder", Kind: workspacefs.FabricFolder, Directory: true, Workspace: testWorkspace,
		Folder: fabric.Folder{ID: testOperation, DisplayName: "Real Fabric folder"},
	}
	other := owned
	other.Item.ID = testDeleteOp
	other.Name = "Other.Notebook"
	reads, lookups := 0, 0
	read := func(_ context.Context, parent workspacefs.Entry) ([]workspacefs.Entry, error) {
		reads++
		if parent.Kind != workspacefs.Workspace {
			t.Fatal("root fixture discovery descended into a folder or item")
		}
		return []workspacefs.Entry{folder, other, owned}, nil
	}
	lookup := func(_ context.Context, parent workspacefs.Entry, name string) (workspacefs.Entry, error) {
		lookups++
		if parent.Kind != workspacefs.Workspace || name != owned.Name {
			t.Fatal("root fixture lookup guessed a name or selected an existing notebook")
		}
		return owned, nil
	}
	found, probeFolder, err := rootNotebookLocation(context.Background(), read, lookup, root, testNotebook)
	if err != nil || reads != 1 || lookups != 1 ||
		found.path != root.path+"/Fixture.Notebook" || probeFolder.path != root.path+"/Real Fabric folder" {
		t.Fatal("root fixture or Fabric-folder metadata path was not discovered from descriptors")
	}
}

func TestMissingRootFixtureDoesNotSearchFolders(t *testing.T) {
	root := location{
		entry: workspacefs.Entry{Kind: workspacefs.Workspace, Directory: true, Workspace: testWorkspace},
		path:  "/private-test-mount/Workspace display",
	}
	reads := 0
	read := func(_ context.Context, parent workspacefs.Entry) ([]workspacefs.Entry, error) {
		reads++
		if parent.Kind != workspacefs.Workspace {
			t.Fatal("missing root fixture caused recursive discovery")
		}
		return []workspacefs.Entry{{
			Name: "Folder", Kind: workspacefs.FabricFolder, Directory: true, Workspace: testWorkspace,
			Folder: fabric.Folder{ID: testOperation, DisplayName: "Folder"},
		}}, nil
	}
	lookup := func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error) {
		t.Fatal("missing root fixture caused an unrelated lookup")
		return workspacefs.Entry{}, nil
	}
	if _, _, err := rootNotebookLocation(context.Background(), read, lookup, root, testNotebook); !errors.Is(err, fs.ErrNotExist) || reads != 1 {
		t.Fatal("missing root fixture was not reported without descending")
	}
}

func TestRootFixtureRejectsNonRootIdentity(t *testing.T) {
	root := location{
		entry: workspacefs.Entry{Kind: workspacefs.Workspace, Directory: true, Workspace: testWorkspace},
		path:  "/private-test-mount/Workspace display",
	}
	child := workspacefs.Entry{
		Name: "Fixture.Notebook", Kind: workspacefs.Notebook, Directory: true, Workspace: testWorkspace,
		Item: fabric.Item{ID: testNotebook, Type: "Notebook", DisplayName: "Fixture", FolderID: testOperation},
	}
	read := func(context.Context, workspacefs.Entry) ([]workspacefs.Entry, error) {
		return []workspacefs.Entry{child}, nil
	}
	lookup := func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error) {
		t.Fatal("non-root fixture was trusted for lookup")
		return workspacefs.Entry{}, nil
	}
	if _, _, err := rootNotebookLocation(context.Background(), read, lookup, root, testNotebook); err == nil {
		t.Fatal("fixture with a non-root folder ID was accepted at the workspace root")
	}
}

func notebookLocationForTest() location {
	return location{
		entry: workspacefs.Entry{
			Name: "Display.Notebook", Kind: workspacefs.Notebook, Directory: true, Workspace: testWorkspace,
			Item: fabric.Item{ID: testNotebook, Type: "Notebook", DisplayName: "Display"},
		},
		path: "/private-test-mount/Workspace/Display.Notebook",
	}
}

func notebookChildrenForTest(parent location) []workspacefs.Entry {
	return []workspacefs.Entry{{
		Name: "content.ipynb", Kind: workspacefs.NotebookContent, Workspace: testWorkspace,
		Item: parent.entry.Item, Size: -1,
	}, {
		Name: ".fabric.json", Kind: workspacefs.IdentityFile, Workspace: testWorkspace,
		Item: parent.entry.Item, Part: ".fabric.json",
	}, {
		Name: "builtin", Kind: workspacefs.ResourceDirectory, Directory: true, Fixed: true,
		Workspace: testWorkspace, Item: parent.entry.Item,
		Resource: resources.Path{Target: resources.Target{
			WorkspaceID: testWorkspace, ItemID: testNotebook, Kind: "Notebook",
		}},
	}}
}

func TestContentIPYNBResolvesLazyDescriptorAndPreservesRemotePartPath(t *testing.T) {
	parent := notebookLocationForTest()
	children := notebookChildrenForTest(parent)
	resolved := children[0]
	resolved.Part, resolved.Size = "original-server-notebook-part.ipynb", int64(len(initialNotebook()))
	lookups := 0
	lookup := func(_ context.Context, owner workspacefs.Entry, name string) (workspacefs.Entry, error) {
		lookups++
		if owner.Kind != workspacefs.Notebook || owner.Item.ID != testNotebook || name != "content.ipynb" {
			t.Fatal("body resolution guessed a platform/env path or changed its owner")
		}
		return resolved, nil
	}
	found, identity, err := notebookContentLocations(context.Background(), lookup, parent, children)
	if err != nil || lookups != 1 || found.path != parent.path+"/content.ipynb" ||
		found.entry.Part != resolved.Part || found.entry.Size != resolved.Size ||
		identity.path != parent.path+"/.fabric.json" || identity.entry.Kind != workspacefs.IdentityFile {
		t.Fatal("local content.ipynb was confused with the remote definition part path")
	}
	if children[0].Size != -1 || children[0].Part != "" || identity.entry.RemotePartPath != "" {
		t.Fatal("lazy listing or cold metadata was rewritten to invent a remote part path")
	}
	for _, mutation := range []string{
		"display-name filename", "wrong owner", "wrong workspace", "wrong item type", "duplicate",
		"missing builtin", "missing identity", "wrong builtin target", "name-only builtin", "legacy platform", "legacy env",
	} {
		t.Run(mutation, func(t *testing.T) {
			children := notebookChildrenForTest(parent)
			switch mutation {
			case "display-name filename":
				children[0].Name = "Display.ipynb"
			case "wrong owner":
				children[0].Item.ID = testLakehouse
			case "wrong workspace":
				children[0].Workspace = testLakehouse
			case "wrong item type":
				children[0].Item.Type = "Environment"
			case "duplicate":
				children = append(children, children[0])
			case "missing builtin":
				children = children[:2]
			case "missing identity":
				children = []workspacefs.Entry{children[0], children[2]}
			case "wrong builtin target":
				children[2].Resource.Target.ItemID = testLakehouse
			case "name-only builtin":
				children[2].Kind = workspacefs.DefinitionDirectory
			case "legacy platform":
				children = append(children, workspacefs.Entry{
					Name: ".platform", Part: ".platform", Kind: workspacefs.DefinitionFile,
					Workspace: testWorkspace, Item: parent.entry.Item,
				})
			case "legacy env":
				alias := children[2]
				alias.Name = "env"
				children = append(children, alias)
			}
			before := lookups
			if _, _, err := notebookContentLocations(context.Background(), lookup, parent, children); err == nil || before != lookups {
				t.Fatal("invalid notebook content descriptor was accepted")
			}
		})
	}
}

func TestContentLookupMustResolveAccurateSnapshotForSameOwner(t *testing.T) {
	parent := notebookLocationForTest()
	children := notebookChildrenForTest(parent)
	for _, mutation := range []string{"missing part", "unknown size", "wrong owner", "wrong workspace", "wrong name", "wrong kind", "directory"} {
		t.Run(mutation, func(t *testing.T) {
			resolved := children[0]
			resolved.Part, resolved.Size = "real-server-body.ipynb", 100
			switch mutation {
			case "missing part":
				resolved.Part = ""
			case "unknown size":
				resolved.Size = -1
			case "wrong owner":
				resolved.Item.ID = testLakehouse
			case "wrong workspace":
				resolved.Workspace = testLakehouse
			case "wrong name":
				resolved.Name = "Display.ipynb"
			case "wrong kind":
				resolved.Kind = workspacefs.DefinitionFile
			case "directory":
				resolved.Directory = true
			}
			lookup := func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error) {
				return resolved, nil
			}
			if _, _, err := notebookContentLocations(context.Background(), lookup, parent, children); err == nil {
				t.Fatal("incorrect snapshot resolution was accepted")
			}
		})
	}
	lookup := func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error) {
		return workspacefs.Entry{}, context.Canceled
	}
	if _, _, err := notebookContentLocations(context.Background(), lookup, parent, children); !errors.Is(err, context.Canceled) {
		t.Fatal("body lookup error was hidden")
	}
}

func TestIdentityReadonlyAllowsOnlyOptionalVerifiedPartToExpire(t *testing.T) {
	const stable = `{"id":"owned-id","extra":{"large":9007199254740993}}`
	const bound = `{"id":"owned-id","extra":{"large":9007199254740993},"remotePartPath":"original-server-notebook-part.ipynb"}`
	for _, pair := range [][2]string{{stable, stable}, {bound, stable}, {stable, bound}, {bound, bound}} {
		if err := unchangedIdentityMetadata([]byte(pair[0]), []byte(pair[1]), testRemoteNotebookPart); err != nil {
			t.Fatal("legitimate identity snapshot expiry was rejected")
		}
	}
	for _, changed := range []string{
		`{"id":"different-id","extra":{"large":9007199254740993}}`,
		`{"id":"owned-id"}`,
		`{"id":"owned-id","extra":{"large":9007199254740992}}`,
		`{"id":"owned-id","extra":{"large":9007199254740993},"remotePartPath":"content.ipynb"}`,
		`{"id":"owned-id","extra":{"large":9007199254740993},"remotePartPath":null}`,
		`null`, `[]`,
	} {
		if err := unchangedIdentityMetadata([]byte(bound), []byte(changed), testRemoteNotebookPart); err == nil {
			t.Fatal("readonly identity check ignored a metadata change")
		}
	}
}
