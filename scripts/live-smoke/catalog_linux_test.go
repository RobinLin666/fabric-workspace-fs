//go:build linux

package main

import (
	"context"
	"errors"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/workspacefs"
)

func TestCatalogDiscoveryUsesIDsAndNeverReadsExistingItemContents(t *testing.T) {
	root := location{
		entry: workspacefs.Entry{Kind: workspacefs.Workspace, Directory: true, Workspace: testWorkspace},
		path:  "/private-test-mount/Workspace",
	}
	folder := workspacefs.Entry{
		Name: "Project (disambiguated)", Kind: workspacefs.FabricFolder, Directory: true, Workspace: testWorkspace,
		Folder: fabric.Folder{ID: testOperation, DisplayName: "Project"},
	}
	notebook := workspacefs.Entry{
		Name: "Shared name.Notebook", Kind: workspacefs.Notebook, Directory: true, Workspace: testWorkspace,
		Item: fabric.Item{ID: testNotebook, Type: "Notebook"},
	}
	lake := workspacefs.Entry{
		Name: "Shared name.Lakehouse", Kind: workspacefs.Lakehouse, Directory: true, Workspace: testWorkspace,
		Item: fabric.Item{ID: testLakehouse, Type: "Lakehouse", FolderID: testOperation},
	}
	environment := workspacefs.Entry{
		Name: "Environment display name.Environment", Kind: workspacefs.Environment, Directory: true, Workspace: testWorkspace,
		Item: fabric.Item{ID: testDeleteOp, Type: "Environment", FolderID: testOperation},
	}
	reads := 0
	children := func(parent workspacefs.Entry) []workspacefs.Entry {
		switch parent.Kind {
		case workspacefs.Workspace:
			return []workspacefs.Entry{folder, notebook}
		case workspacefs.FabricFolder:
			return []workspacefs.Entry{lake, environment}
		default:
			t.Fatal("catalog discovery entered item contents")
			return nil
		}
	}
	read := func(_ context.Context, parent workspacefs.Entry) ([]workspacefs.Entry, error) {
		reads++
		return children(parent), nil
	}
	lookup := func(_ context.Context, parent workspacefs.Entry, name string) (workspacefs.Entry, error) {
		for _, child := range children(parent) {
			if child.Name == name {
				return child, nil
			}
		}
		return workspacefs.Entry{}, fail("unexpected lookup")
	}
	found, err := catalogLocations(context.Background(), read, lookup, root, map[string]workspacefs.Kind{
		testNotebook: workspacefs.Notebook, testLakehouse: workspacefs.Lakehouse, testDeleteOp: workspacefs.Environment,
	})
	if err != nil || reads != 2 || len(found) != 3 ||
		found[testNotebook].path != root.path+"/Shared name.Notebook" ||
		found[testLakehouse].path != root.path+"/Project (disambiguated)/Shared name.Lakehouse" {
		t.Fatal("display-name paths were guessed or immutable catalog identity was lost")
	}
}

func TestCatalogDiscoveryRejectsFolderCyclesAndCanceledWork(t *testing.T) {
	root := location{
		entry: workspacefs.Entry{Kind: workspacefs.Workspace, Directory: true, Workspace: testWorkspace},
		path:  "/private-test-mount/Workspace",
	}
	folder := workspacefs.Entry{
		Name: "Loop", Kind: workspacefs.FabricFolder, Directory: true, Workspace: testWorkspace,
		Folder: fabric.Folder{ID: testOperation},
	}
	reads := 0
	read := func(context.Context, workspacefs.Entry) ([]workspacefs.Entry, error) {
		reads++
		return []workspacefs.Entry{folder}, nil
	}
	lookup := func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error) {
		t.Fatal("unexpected item lookup")
		return workspacefs.Entry{}, nil
	}
	wanted := map[string]workspacefs.Kind{testNotebook: workspacefs.Notebook}
	if _, err := catalogLocations(context.Background(), read, lookup, root, wanted); err == nil || reads != 2 {
		t.Fatal("folder cycle was not rejected within its metadata bound")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := catalogLocations(ctx, read, lookup, root, wanted); !errors.Is(err, context.Canceled) || reads != 2 {
		t.Fatal("canceled discovery issued catalog requests")
	}
}
