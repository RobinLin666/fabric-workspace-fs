//go:build linux

package main

import (
	"context"
	"io"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/workspacefs"
)

// FUSE creates its own request contexts. Tie API calls to the smoke deadline
// too, rather than assuming cancellation of the CLI cancels kernel requests.
type cleanupAPIScopeKey struct{}

// Only post-unmount, bound-ID cleanup creates this private marker. Ordinary
// kernel requests cannot opt out of the main smoke deadline.
func backendCleanupContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, cleanupAPIScopeKey{}, true)
}

func boundCall[T any](run, request context.Context, call func(context.Context) (T, error)) (T, error) {
	if request.Value(cleanupAPIScopeKey{}) == true {
		deadline, bounded := request.Deadline()
		if !bounded || time.Until(deadline) > cleanupTimeout {
			var zero T
			return zero, fail("post-unmount backend cleanup requires its short explicit deadline")
		}
		return call(request)
	}
	ctx, cancel := context.WithCancel(request)
	stop := context.AfterFunc(run, cancel)
	defer stop()
	defer cancel()
	if run.Err() != nil {
		cancel()
	}
	return call(ctx)
}

func boundVoid(run, request context.Context, call func(context.Context) error) error {
	_, err := boundCall(run, request, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, call(ctx)
	})
	return err
}

type boundedFabric struct {
	api   *fabric.Client
	run   context.Context
	guard *scopeGuard
}

func (b boundedFabric) ListWorkspaces(ctx context.Context) ([]fabric.Workspace, error) {
	return boundCall(b.run, ctx, b.api.ListWorkspaces)
}

func (b boundedFabric) GetWorkspace(ctx context.Context, id string) (fabric.Workspace, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (fabric.Workspace, error) { return b.api.GetWorkspace(ctx, id) })
}

func (b boundedFabric) ListItems(ctx context.Context, ws string) ([]fabric.Item, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) ([]fabric.Item, error) { return b.api.ListItems(ctx, ws) })
}

func (b boundedFabric) ListFolders(ctx context.Context, ws string) ([]fabric.Folder, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) ([]fabric.Folder, error) { return b.api.ListFolders(ctx, ws) })
}

func (b boundedFabric) GetDefinition(ctx context.Context, ws, id, kind, format string) (fabric.Definition, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (fabric.Definition, error) {
		return b.api.GetDefinition(ctx, ws, id, kind, format)
	})
}

func (b boundedFabric) UpdateNotebook(ctx context.Context, ws, id string, def fabric.Definition) error {
	return boundVoid(b.run, ctx, func(ctx context.Context) error { return b.api.UpdateNotebook(ctx, ws, id, def) })
}

func (b boundedFabric) CreateItem(ctx context.Context, ws, kind, name, folderID string) (fabric.Item, error) {
	value, err := boundCall(b.run, ctx, func(ctx context.Context) (fabric.Item, error) {
		return b.api.CreateItem(ctx, ws, kind, name, folderID)
	})
	if err == nil {
		err = b.guard.noteManagedCreation(managedResource{
			ID: value.ID, Type: value.Type, DisplayName: value.DisplayName, FolderID: value.FolderID,
		})
	}
	return value, err
}

func (b boundedFabric) CreateFolder(ctx context.Context, ws, name, parentID string) (fabric.Folder, error) {
	value, err := boundCall(b.run, ctx, func(ctx context.Context) (fabric.Folder, error) {
		return b.api.CreateFolder(ctx, ws, name, parentID)
	})
	if err == nil {
		err = b.guard.noteManagedCreation(managedResource{
			ID: value.ID, Type: "Folder", DisplayName: value.DisplayName, FolderID: value.ParentFolderID,
		})
	}
	return value, err
}

func (b boundedFabric) GetItem(ctx context.Context, ws, id string) (fabric.Item, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (fabric.Item, error) { return b.api.GetItem(ctx, ws, id) })
}

func (b boundedFabric) GetFolder(ctx context.Context, ws, id string) (fabric.Folder, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (fabric.Folder, error) { return b.api.GetFolder(ctx, ws, id) })
}

func (b boundedFabric) DeleteItem(ctx context.Context, ws, id string) error {
	return boundVoid(b.run, ctx, func(ctx context.Context) error { return b.api.DeleteItem(ctx, ws, id) })
}

func (b boundedFabric) DeleteFolder(ctx context.Context, ws, id string) error {
	return boundVoid(b.run, ctx, func(ctx context.Context) error { return b.api.DeleteFolder(ctx, ws, id) })
}

type boundedLake struct {
	api workspacefs.LakeAPI
	run context.Context
}

func (b boundedLake) Stat(ctx context.Context, p onelake.Path) (onelake.Info, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (onelake.Info, error) { return b.api.Stat(ctx, p) })
}

func (b boundedLake) List(ctx context.Context, p onelake.Path) ([]onelake.Info, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) ([]onelake.Info, error) { return b.api.List(ctx, p) })
}

func (b boundedLake) Read(ctx context.Context, p onelake.Path, off int64, dest []byte, etag string) (int, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (int, error) { return b.api.Read(ctx, p, off, dest, etag) })
}

func (b boundedLake) Put(ctx context.Context, p onelake.Path, source io.ReaderAt, size int64, etag string) (onelake.Info, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (onelake.Info, error) { return b.api.Put(ctx, p, source, size, etag) })
}

func (b boundedLake) Mkdir(ctx context.Context, p onelake.Path) error {
	return boundVoid(b.run, ctx, func(ctx context.Context) error { return b.api.Mkdir(ctx, p) })
}

func (b boundedLake) Remove(ctx context.Context, p onelake.Path, directory bool, etag string) error {
	return boundVoid(b.run, ctx, func(ctx context.Context) error { return b.api.Remove(ctx, p, directory, etag) })
}

func (b boundedLake) Rename(ctx context.Context, a, z onelake.Path, sourceETag, destinationETag string, noReplace bool) (onelake.Info, error) {
	return boundCall(b.run, ctx, func(ctx context.Context) (onelake.Info, error) {
		return b.api.Rename(ctx, a, z, sourceETag, destinationETag, noReplace)
	})
}
