//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/workspacefs"
)

func managedEntryKind(kind string) (workspacefs.Kind, error) {
	switch kind {
	case "Folder":
		return workspacefs.FabricFolder, nil
	case "Notebook":
		return workspacefs.Notebook, nil
	case "Lakehouse":
		return workspacefs.Lakehouse, nil
	case "Environment":
		return workspacefs.Environment, nil
	default:
		return 0, fail("unknown managed fixture type")
	}
}

func (r *runner) readManagedIdentity(ctx context.Context, receipt managedResource) (managedResource, error) {
	if receipt.Type == "Folder" {
		value, err := r.fab.GetFolder(ctx, r.opts.Workspace, receipt.ID)
		if err != nil {
			return managedResource{}, err
		}
		return managedResource{ID: value.ID, Type: "Folder", DisplayName: value.DisplayName, FolderID: value.ParentFolderID}, nil
	}
	value, err := r.fab.GetItem(ctx, r.opts.Workspace, receipt.ID)
	if err != nil {
		return managedResource{}, err
	}
	return managedResource{ID: value.ID, Type: value.Type, DisplayName: value.DisplayName, FolderID: value.FolderID}, nil
}

func (r *runner) verifyManagedIdentity(ctx context.Context, kind string, waitForVisibility bool) (managedResource, error) {
	receipt, exists := r.guard.managedReceipt(kind)
	if !exists {
		return managedResource{}, fail("FUSE mkdir did not produce a validated create-API identity receipt")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		current, err := r.readManagedIdentity(ctx, receipt)
		if isNotFound(err) && waitForVisibility {
			if err := waitRetryAfter(ctx, ""); err != nil {
				return managedResource{}, err
			}
			continue
		}
		if err != nil {
			return managedResource{}, err
		}
		if err := r.guard.confirmManagedIdentity(current); err != nil {
			return managedResource{}, err
		}
		return receipt, nil
	}
}

func (r *runner) managedEntry(receipt managedResource) (workspacefs.Entry, error) {
	kind, err := managedEntryKind(receipt.Type)
	if err != nil {
		return workspacefs.Entry{}, err
	}
	entry := workspacefs.Entry{Kind: kind, Directory: true, Workspace: r.opts.Workspace}
	if receipt.Type == "Folder" {
		entry.Name = r.doc.Plan.Managed.FolderName
		entry.Folder = fabric.Folder{ID: receipt.ID, DisplayName: receipt.DisplayName, ParentFolderID: receipt.FolderID}
	} else {
		entry.Name = receipt.DisplayName + "." + receipt.Type
		entry.Item = fabric.Item{ID: receipt.ID, Type: receipt.Type, DisplayName: receipt.DisplayName, FolderID: receipt.FolderID}
	}
	return entry, nil
}

func (r *runner) setManagedStatus(kind, status, path string) error {
	snapshot := r.guard.managedEvidenceSnapshot()[kind]
	snapshot.Status = status
	if path == "" {
		path = r.doc.ManagedResources[kind].LocalPath
	}
	snapshot.LocalPath = path
	r.doc.ManagedResources[kind] = snapshot
	return r.save()
}

func (r *runner) bindCreatedManaged(kind string, parent location, expectedName string) error {
	receipt, err := r.verifyManagedIdentity(r.ctx, kind, true)
	if err != nil {
		return err
	}
	entryKind, err := managedEntryKind(kind)
	if err != nil {
		return err
	}
	local, err := r.findChild(r.ctx, parent, func(entry workspacefs.Entry) bool {
		id := entry.Item.ID
		if entry.Kind == workspacefs.FabricFolder {
			id = entry.Folder.ID
		}
		return entry.Kind == entryKind && strings.EqualFold(id, receipt.ID)
	})
	if err != nil {
		return err
	}
	if local.entry.Name != expectedName || local.path != filepath.Join(parent.path, expectedName) {
		return fail("FUSE-created fixture did not retain its exact unique namespace name")
	}
	r.managedLocations[kind] = local
	if kind == "Folder" {
		r.paths.folder = local
	}
	var expectedPart string
	if kind != "Folder" {
		children, err := r.backend.ReadDir(r.ctx, local.entry)
		if err != nil {
			return err
		}
		if _, err := fixedItemLocations(local, children); err != nil {
			return err
		}
		if kind == "Notebook" {
			content, _, err := notebookContentLocations(r.ctx, r.backend.Lookup, local, children)
			if err != nil {
				return err
			}
			expectedPart = content.entry.Part
		}
	}
	data, err := r.readSmall(filepath.Join(local.path, ".fabric.json"), 64<<10)
	if err != nil {
		return err
	}
	var identity identityMetadata
	if json.Unmarshal(data, &identity) != nil || !strings.EqualFold(identity.ID, receipt.ID) ||
		identity.Type != kind || identity.DisplayName != receipt.DisplayName ||
		!strings.EqualFold(identity.WorkspaceID, r.opts.Workspace) {
		return fail("FUSE-created resource metadata did not match its verified immutable identity")
	}
	parentID := identity.FolderID
	if kind == "Folder" {
		parentID = identity.ParentFolderID
	}
	if !strings.EqualFold(parentID, receipt.FolderID) ||
		(identity.RemotePartPath != "" && identity.RemotePartPath != expectedPart) {
		return fail("FUSE-created metadata lost its bound parent or remote notebook part path")
	}
	return r.setManagedStatus(kind, "fuse-created-and-identity-verified", local.path)
}

func (r *runner) createManagedFixtures() error {
	r.managedCreateAttempted = true
	r.doc.Cleanup.Managed = "creating-owned-fixtures"
	if err := r.measure("managed.folder-mkdir", func() (int, error) {
		name := r.doc.Plan.Managed.FolderName
		target := filepath.Join(r.paths.workspace.path, name)
		if _, err := os.Lstat(target); !errors.Is(err, fs.ErrNotExist) {
			return 0, fail("managed fixture folder name is not absent")
		}
		if err := os.Mkdir(target, 0700); err != nil {
			return 0, err
		}
		if err := r.bindCreatedManaged("Folder", r.paths.workspace, name); err != nil {
			return 0, err
		}
		return 0, r.assertManagedFolderEmpty(r.ctx)
	}); err != nil {
		return err
	}
	for _, spec := range r.doc.Plan.Managed.Items {
		if err := r.measure("managed.typed-mkdir."+spec.Type, func() (int, error) {
			parent := r.managedLocations["Folder"]
			target := filepath.Join(parent.path, spec.LocalName)
			if _, err := os.Lstat(target); !errors.Is(err, fs.ErrNotExist) {
				return 0, fail("typed fixture name is not absent in the newly owned folder")
			}
			if err := os.Mkdir(target, 0700); err != nil {
				return 0, err
			}
			return 0, r.bindCreatedManaged(spec.Type, parent, spec.LocalName)
		}); err != nil {
			return err
		}
	}
	r.doc.Cleanup.Managed = "owned-fixtures-created"
	return r.save()
}

func (r *runner) assertManagedFolderEmpty(ctx context.Context) error {
	folder, err := r.verifyManagedIdentity(ctx, "Folder", false)
	if err != nil {
		return err
	}
	items, err := r.fab.ListItems(ctx, r.opts.Workspace)
	if err != nil {
		return err
	}
	for _, item := range items {
		if strings.EqualFold(item.FolderID, folder.ID) {
			return fail("owned Fabric folder is not empty; recursive deletion is forbidden")
		}
	}
	folders, err := r.fab.ListFolders(ctx, r.opts.Workspace)
	if err != nil {
		return err
	}
	for _, child := range folders {
		if strings.EqualFold(child.ParentFolderID, folder.ID) {
			return fail("owned Fabric folder contains another folder; preserving it")
		}
	}
	return nil
}

func (r *runner) confirmManagedAbsent(ctx context.Context, value managedResource) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		_, err := r.readManagedIdentity(ctx, value)
		if isNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := waitRetryAfter(ctx, ""); err != nil {
			return err
		}
	}
}

func (r *runner) removeManaged(ctx context.Context, kind string, throughMount bool) error {
	restOnly := kind == "Notebook" || kind == "Environment"
	if throughMount && restOnly {
		return fail("Notebook and Environment fixture cleanup must not use product rmdir")
	}
	value, err := r.verifyManagedIdentity(ctx, kind, false)
	if isNotFound(err) && !throughMount {
		return r.setManagedStatus(kind, "confirmed-absent", "")
	}
	if err != nil {
		return err
	}
	if kind == "Folder" {
		if err := r.assertManagedFolderEmpty(ctx); err != nil {
			return err
		}
	}
	if err := r.guard.permitManagedDelete(value.ID, true); err != nil {
		return err
	}
	defer r.guard.permitManagedDelete(value.ID, false)
	if restOnly {
		if err := r.setManagedStatus(kind, "explicit-owned-rest-cleanup-requested", ""); err != nil {
			return err
		}
		// Test-harness authorization is narrower than product rmdir: this ID
		// came from our exact create request and was freshly revalidated.
		// No public-definition "empty" heuristic is used for these types.
		if err := r.fab.DeleteItem(ctx, r.opts.Workspace, value.ID); err != nil {
			return err
		}
	} else if throughMount {
		local, exists := r.managedLocations[kind]
		if !exists {
			return fail("FUSE rmdir has no owned descriptor path")
		}
		if err := os.Remove(local.path); err != nil {
			return err
		}
		if _, err := os.Stat(local.path); !errors.Is(err, fs.ErrNotExist) {
			return fail("FUSE catalog did not reflect deletion of the exact owned resource")
		}
	} else {
		entry, err := r.managedEntry(value)
		if err != nil {
			return err
		}
		// Keep the product's fresh Lakehouse/folder emptiness and lease
		// guards. This path is never used for Notebook or Environment.
		if err := r.backend.Remove(ctx, entry, true); err != nil {
			return err
		}
	}
	if err := r.confirmManagedAbsent(ctx, value); err != nil {
		return err
	}
	if restOnly {
		// Explicit test-only REST cleanup bypasses FS cache invalidation.
		// Let the finite catalog cache expire before empty-parent rmdir.
		if err := waitUntil(ctx, time.Now().Add(cacheTTL)); err != nil {
			return err
		}
	}
	status := "backend-cleanup-confirmed-404"
	if restOnly {
		status = "explicit-owned-rest-delete-confirmed-404"
	} else if throughMount {
		status = "fuse-rmdir-confirmed-404"
	}
	return r.setManagedStatus(kind, status, "")
}

func (r *runner) deleteManagedFixtures() error {
	if err := r.doc.refuseLegacyOverlayCleanup(); err != nil {
		r.doc.Cleanup.Managed = "retained-unsupported-legacy-overlay-cleanup"
		return err
	}
	for _, kind := range []string{"Notebook", "Environment"} {
		if err := r.measure("managed.explicit-owned-rest-cleanup."+kind, func() (int, error) {
			return 0, r.removeManaged(r.ctx, kind, false)
		}); err != nil {
			return err
		}
	}
	for _, kind := range []string{"Lakehouse", "Folder"} {
		if err := r.measure("managed.empty-rmdir."+kind, func() (int, error) {
			return 0, r.removeManaged(r.ctx, kind, true)
		}); err != nil {
			return err
		}
	}
	r.doc.Cleanup.Managed = "explicit-owned-rest-and-guarded-fuse-cleanup-confirmed-absent"
	return r.save()
}

func (r *runner) managedNotebookUpdate() error {
	local, exists := r.managedLocations["Notebook"]
	if !exists {
		return fail("FUSE-created notebook has no owned descriptor")
	}
	receipt, err := r.verifyManagedIdentity(r.ctx, "Notebook", false)
	if err != nil {
		return err
	}
	children, err := r.backend.ReadDir(r.ctx, local.entry)
	if err != nil {
		return err
	}
	content, _, err := notebookContentLocations(r.ctx, r.backend.Lookup, local, children)
	if err != nil {
		return err
	}
	original, err := r.readSmall(content.path, maxNotebookBytes)
	if err != nil {
		return err
	}
	definition, err := r.freshNotebookDefinition(content, original)
	if err != nil {
		return err
	}
	marker := r.doc.Plan.OwnershipMarker + ":fuse-created"
	updated, err := setNotebookMarker(original, marker)
	if err != nil {
		return err
	}
	if err := r.guard.permitManagedNotebookUpdate(receipt.ID, true); err != nil {
		return err
	}
	defer r.guard.permitManagedNotebookUpdate(receipt.ID, false)
	before := r.counter.snapshot()
	if err := r.measure("managed.notebook-content-write-sync-close", func() (int, error) {
		file, err := os.OpenFile(content.path, os.O_RDWR, 0)
		if err != nil {
			return 0, err
		}
		r.track(file)
		defer r.closeFile(file)
		if err := writeAllAt(file, updated, 0); err != nil {
			return 0, err
		}
		if err := file.Truncate(int64(len(updated))); err != nil {
			return 0, err
		}
		if err := r.syncTwice(file, "managed.notebook"); err != nil {
			return 0, err
		}
		if err := r.closeFile(file); err != nil {
			return 0, err
		}
		return len(updated), nil
	}); err != nil {
		return err
	}
	if countDelta(before, r.counter.snapshot())["fabric|POST|definition-update"] == 0 {
		return fail("FUSE-created notebook update did not reach its owned remote definition")
	}
	if err := r.measure("managed.notebook-content-read-saved-marker", func() (int, error) {
		data, err := r.readSmall(content.path, maxNotebookBytes)
		if err != nil {
			return 0, err
		}
		return len(data), hasNotebookMarker(data, marker)
	}); err != nil {
		return err
	}
	return r.measure("managed.notebook-fresh-definition-part-preservation", func() (int, error) {
		return 0, r.verifySavedNotebookDefinition(content, definition, marker)
	})
}

func (r *runner) assertUnsupportedManagedRmdir(kind string) error {
	if kind != "Notebook" && kind != "Environment" {
		return fail("unsupported-rmdir probe is restricted to the two protected item types")
	}
	local, exists := r.managedLocations[kind]
	if !exists {
		return fail("unsupported-rmdir probe has no owned fixture descriptor")
	}
	if _, err := r.verifyManagedIdentity(r.ctx, kind, false); err != nil {
		return err
	}
	return r.measure("managed.rmdir-must-be-unsupported."+kind, func() (int, error) {
		before := r.counter.snapshot()
		_, deniedBefore := r.guard.state()
		err := os.Remove(local.path)
		_, deniedAfter := r.guard.state()
		delta := countDelta(before, r.counter.snapshot())
		if !errors.Is(err, syscall.ENOTSUP) || mutationCount(delta) != 0 ||
			delta["fabric|POST|definition-read"] != 0 || deniedAfter != deniedBefore {
			return 0, fail("Notebook/Environment rmdir must reject without deletion or definition-empty probing")
		}
		if _, err := r.verifyManagedIdentity(r.ctx, kind, false); err != nil {
			return 0, err
		}
		return 0, nil
	})
}

func (r *runner) cleanupManagedFixtures(ctx context.Context) error {
	if err := r.doc.refuseLegacyOverlayCleanup(); err != nil {
		r.doc.Cleanup.Managed = "retained-unsupported-legacy-overlay-cleanup"
		return err
	}
	snapshot := r.guard.managedEvidenceSnapshot()
	if len(snapshot) == 0 {
		r.doc.Cleanup.Managed = "not-created"
		return nil
	}
	if r.backend == nil {
		return fail("managed cleanup has no owning backend instance")
	}
	ctx = backendCleanupContext(ctx)
	var failures []error
	for _, kind := range []string{"Environment", "Lakehouse", "Notebook"} {
		if _, attempted := snapshot[kind]; !attempted {
			continue
		}
		if _, known := r.guard.managedReceipt(kind); !known {
			failures = append(failures, fail("managed create outcome has no confirmed ID; preserving its parent"))
			continue
		}
		if err := r.removeManaged(ctx, kind, false); err != nil {
			failures = append(failures, err)
			r.setManagedStatus(kind, "cleanup-failed-owned-id-preserved", "")
		}
	}
	if len(failures) == 0 {
		if _, exists := r.guard.managedReceipt("Folder"); exists {
			if err := r.removeManaged(ctx, "Folder", false); err != nil {
				failures = append(failures, err)
			}
		} else {
			failures = append(failures, fail("Fabric folder creation outcome remains unverified"))
		}
	}
	r.doc.Cleanup.Managed = "all-owned-managed-ids-confirmed-absent"
	if len(failures) != 0 {
		r.doc.Cleanup.Managed = "retained-after-incomplete-managed-cleanup"
	}
	return errors.Join(append(failures, r.save())...)
}
