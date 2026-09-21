//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/workspacefs"
)

type location struct {
	entry  workspacefs.Entry
	path   string
	parent string
}

type locations struct {
	root, workspace                                 location
	notebook, lakehouse, files, tables, environment location
	notebookFile, notebookIdentity                  location
	folder                                          location
}

type identityMetadata struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	DisplayName    string `json:"displayName"`
	WorkspaceID    string `json:"workspaceId"`
	FolderID       string `json:"folderId"`
	ParentFolderID string `json:"parentFolderId"`
	RemotePartPath string `json:"remotePartPath"`
}

func childLocation(parent location, entry workspacefs.Entry) (location, error) {
	if !validComponent(entry.Name) {
		return location{}, fail("backend returned an unsafe local namespace component")
	}
	return location{
		entry: entry, path: filepath.Join(parent.path, entry.Name),
		parent: parent.path,
	}, nil
}

func (r *runner) findChild(ctx context.Context, parent location, match func(workspacefs.Entry) bool) (location, error) {
	children, err := r.backend.ReadDir(ctx, parent.entry)
	if err != nil {
		return location{}, err
	}
	var result location
	found := false
	for _, entry := range children {
		if !match(entry) {
			continue
		}
		if found {
			return location{}, fail("immutable identity matched more than one local entry")
		}
		result, err = childLocation(parent, entry)
		if err != nil {
			return location{}, err
		}
		found = true
	}
	if !found {
		return location{}, fs.ErrNotExist
	}
	return result, nil
}

// Traverse only Fabric catalog folders, never notebook, Environment, Lakehouse,
// OneLake or injected bundle contents. Display names and duplicate-name suffixes always
// come from the backend, while immutable IDs select the requested target items.
func catalogLocations(
	ctx context.Context,
	readDir func(context.Context, workspacefs.Entry) ([]workspacefs.Entry, error),
	lookup func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error),
	workspace location,
	wanted map[string]workspacefs.Kind,
) (map[string]location, error) {
	type directory struct {
		location location
		depth    int
	}
	queue := []directory{{location: workspace}}
	seen := make(map[string]bool)
	found := make(map[string]location)
	entries := 0
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i >= 1024 || queue[i].depth > 64 {
			return nil, fail("Fabric folder discovery exceeded its bounded catalog walk")
		}
		parent := queue[i]
		children, err := readDir(ctx, parent.location.entry)
		if err != nil {
			return nil, err
		}
		entries += len(children)
		if entries > 100000 {
			return nil, fail("Fabric catalog discovery exceeded its entry bound")
		}
		for _, child := range children {
			if child.Kind == workspacefs.FabricFolder {
				id := strings.ToLower(child.Folder.ID)
				if fabric.ValidateID(id) != nil || seen[id] || !child.Directory ||
					!strings.EqualFold(child.Workspace, workspace.entry.Workspace) {
					return nil, fail("Fabric folder catalog contains an invalid or repeated identity")
				}
				seen[id] = true
				local, err := childLocation(parent.location, child)
				if err != nil {
					return nil, err
				}
				if len(queue) >= 1024 {
					return nil, fail("Fabric folder discovery exceeded its directory bound")
				}
				queue = append(queue, directory{location: local, depth: parent.depth + 1})
				continue
			}
			id := strings.ToLower(child.Item.ID)
			kind, needed := wanted[id]
			if !needed {
				continue
			}
			if child.Kind != kind || !child.Directory || !strings.EqualFold(child.Workspace, workspace.entry.Workspace) {
				return nil, fail("selected item changed its catalog type or workspace")
			}
			if _, duplicate := found[id]; duplicate {
				return nil, fail("selected immutable item identity appeared more than once")
			}
			local, err := childLocation(parent.location, child)
			if err != nil {
				return nil, err
			}
			resolved, err := lookup(ctx, parent.location.entry, child.Name)
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(resolved.Item.ID, id) || resolved.Kind != kind ||
				!strings.EqualFold(resolved.Workspace, workspace.entry.Workspace) {
				return nil, fail("item lookup did not preserve the selected immutable identity")
			}
			found[id] = local
		}
		if len(found) == len(wanted) {
			return found, nil
		}
	}
	return nil, fs.ErrNotExist
}

// SDK creation omits FolderID, so this fixture must be found directly at the
// workspace root. Do not fall back to a same-name item or scan item contents.
func rootNotebookLocation(
	ctx context.Context,
	readDir func(context.Context, workspacefs.Entry) ([]workspacefs.Entry, error),
	lookup func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error),
	workspace location,
	id string,
) (location, location, error) {
	if err := ctx.Err(); err != nil {
		return location{}, location{}, err
	}
	children, err := readDir(ctx, workspace.entry)
	if err != nil {
		return location{}, location{}, err
	}
	var notebook, folder location
	for _, child := range children {
		if child.Kind == workspacefs.FabricFolder && folder.path == "" {
			if fabric.ValidateID(child.Folder.ID) != nil || !child.Directory ||
				!strings.EqualFold(child.Workspace, workspace.entry.Workspace) {
				return location{}, location{}, fail("workspace root exposed an invalid Fabric folder")
			}
			folder, err = childLocation(workspace, child)
			if err != nil {
				return location{}, location{}, err
			}
		}
		if !strings.EqualFold(child.Item.ID, id) {
			continue
		}
		if notebook.path != "" || child.Kind != workspacefs.Notebook || !child.Directory ||
			child.Item.Type != "Notebook" || child.Item.FolderID != "" ||
			!strings.EqualFold(child.Workspace, workspace.entry.Workspace) {
			return location{}, location{}, fail("new notebook did not have its exact workspace-root identity")
		}
		notebook, err = childLocation(workspace, child)
		if err != nil {
			return location{}, location{}, err
		}
		resolved, err := lookup(ctx, workspace.entry, child.Name)
		if err != nil {
			return location{}, location{}, err
		}
		if resolved.Kind != workspacefs.Notebook || resolved.Item.FolderID != "" ||
			!strings.EqualFold(resolved.Item.ID, id) || !strings.EqualFold(resolved.Workspace, workspace.entry.Workspace) {
			return location{}, location{}, fail("root notebook lookup changed the fixture identity")
		}
	}
	if notebook.path == "" {
		return location{}, folder, fs.ErrNotExist
	}
	return notebook, folder, nil
}

func sameItemChild(parent, child workspacefs.Entry) bool {
	return strings.EqualFold(child.Workspace, parent.Workspace) &&
		strings.EqualFold(child.Item.ID, parent.Item.ID) && child.Item.Type == parent.Item.Type &&
		strings.EqualFold(child.Item.FolderID, parent.Item.FolderID)
}

// Fixed roots are descriptors, not proof that a resource provider is enabled.
// Environment definition exports may additionally reveal optional parts.
func fixedItemLocations(parent location, children []workspacefs.Entry) (map[string]location, error) {
	type shape struct {
		kind workspacefs.Kind
		dir  bool
	}
	expected := map[string]shape{".fabric.json": {workspacefs.IdentityFile, false}}
	kind, err := managedEntryKind(parent.entry.Item.Type)
	if err != nil || kind != parent.entry.Kind || !parent.entry.Directory ||
		fabric.ValidateID(parent.entry.Workspace) != nil || fabric.ValidateID(parent.entry.Item.ID) != nil {
		return nil, fail("fixed item-root check requires its immutable typed owner")
	}
	switch parent.entry.Kind {
	case workspacefs.Notebook:
		expected[namespace.NotebookContentFileName(entry.Item.DisplayName)] = shape{workspacefs.NotebookContent, false}
		expected["builtin"] = shape{workspacefs.ResourceDirectory, true}
	case workspacefs.Environment:
		expected["Libraries"] = shape{workspacefs.DefinitionDirectory, true}
		expected["Setting"] = shape{workspacefs.DefinitionDirectory, true}
		expected["resources"] = shape{workspacefs.ResourceDirectory, true}
	case workspacefs.Lakehouse:
		expected["Files"] = shape{workspacefs.LakeDirectory, true}
		expected["Tables"] = shape{workspacefs.LakeDirectory, true}
	default:
		return nil, fail("fixed item-root check requires a typed item descriptor")
	}
	found := make(map[string]location)
	for _, child := range children {
		if !sameItemChild(parent.entry, child) {
			return nil, fail("fixed item-root descriptor changed its immutable owner")
		}
		local, err := childLocation(parent, child)
		if err != nil {
			return nil, err
		}
		if _, duplicate := found[child.Name]; duplicate {
			return nil, fail("fixed item root exposed duplicate namespace entries")
		}
		want, fixed := expected[child.Name]
		if !fixed {
			if parent.entry.Kind != workspacefs.Environment || child.Part == "" ||
				child.Name == ".platform" || child.Part == ".platform" || strings.HasPrefix(child.Part, ".platform/") ||
				(child.Kind != workspacefs.DefinitionFile && child.Kind != workspacefs.DefinitionDirectory) ||
				child.Directory != (child.Kind == workspacefs.DefinitionDirectory) {
				return nil, fail("item root exposed a non-contract entry")
			}
		} else {
			if child.Kind != want.kind || child.Directory != want.dir {
				return nil, fail("fixed item-root entry has the wrong typed descriptor")
			}
			switch child.Kind {
			case workspacefs.IdentityFile:
				if child.Part != ".fabric.json" {
					return nil, fail("generated identity descriptor has the wrong binding")
				}
			case workspacefs.DefinitionDirectory:
				if !child.Fixed || child.Part != child.Name {
					return nil, fail("fixed Environment category lost its definition binding")
				}
			case workspacefs.ResourceDirectory:
				target := child.Resource.Target
				if !child.Fixed || child.Resource.Relative != "" || target.Kind != parent.entry.Item.Type ||
					!strings.EqualFold(target.WorkspaceID, parent.entry.Workspace) ||
					!strings.EqualFold(target.ItemID, parent.entry.Item.ID) {
					return nil, fail("fixed resource root lost its typed immutable target")
				}
			case workspacefs.LakeDirectory:
				if child.Remote != child.Name {
					return nil, fail("fixed Lakehouse root lost its exact OneLake path")
				}
			}
		}
		found[child.Name] = local
	}
	for name := range expected {
		if _, exists := found[name]; !exists {
			return nil, fail("item root omitted a required fixed descriptor")
		}
	}
	return found, nil
}

func notebookContentLocations(
	ctx context.Context,
	lookup func(context.Context, workspacefs.Entry, string) (workspacefs.Entry, error),
	parent location,
	children []workspacefs.Entry,
) (location, location, error) {
	if parent.entry.Kind != workspacefs.Notebook || parent.entry.Item.Type != "Notebook" {
		return location{}, location{}, fail("notebook content discovery requires its typed owner")
	}
	fixed, err := fixedItemLocations(parent, children)
	if err != nil {
		return location{}, location{}, err
	}
	contentName := namespace.NotebookContentFileName(parent.entry.Item.DisplayName)
	content := fixed[contentName]
	// Readdir intentionally leaves Size=-1 and Part empty. Only exact Lookup
	// (or Stat) can bind the body to the decoded remote snapshot.
	resolved, err := lookup(ctx, parent.entry, content.entry.Name)
	if err != nil {
		return location{}, location{}, err
	}
	if !sameItemChild(parent.entry, resolved) || resolved.Kind != workspacefs.NotebookContent ||
		resolved.Directory || resolved.Name != content.entry.Name || resolved.Size < 0 ||
		!strings.HasSuffix(strings.ToLower(resolved.Part), ".ipynb") {
		return location{}, location{}, fail("notebook lookup did not resolve its actual body size and remote part")
	}
	content.entry = resolved
	return content, fixed[".fabric.json"], nil
}

func (r *runner) discover() error {
	ctx, cancel := context.WithTimeout(r.ctx, time.Minute)
	defer cancel()
	p := &r.paths
	p.root = location{entry: r.backend.Root(), path: r.doc.Mountpoint}
	var err error
	p.workspace, err = r.findChild(ctx, p.root, func(e workspacefs.Entry) bool {
		return e.Kind == workspacefs.Workspace && strings.EqualFold(e.Workspace, r.opts.Workspace)
	})
	if err != nil {
		return err
	}
	lookup, err := r.backend.Lookup(ctx, p.root.entry, p.workspace.entry.Name)
	if err != nil || lookup.Kind != workspacefs.Workspace || !lookup.Directory ||
		!strings.EqualFold(lookup.Workspace, r.opts.Workspace) {
		return fail("workspace lookup did not preserve the selected immutable identity")
	}
	for {
		p.notebook, p.folder, err = rootNotebookLocation(ctx, r.backend.ReadDir, r.backend.Lookup, p.workspace, r.fixture.ownedID)
		if !errors.Is(err, fs.ErrNotExist) {
			break
		}
		if err := waitRetryAfter(ctx, "5"); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	if err := r.fixture.validateIdentity(p.notebook.entry.Item, r.fixture.ownedID, false); err != nil {
		return err
	}
	if p.folder.path == "" {
		r.doc.Notes = append(r.doc.Notes, "No Fabric folder exists at the workspace root; folder metadata/boundary probes are not applicable.")
	}
	wanted := map[string]workspacefs.Kind{r.opts.Lakehouse: workspacefs.Lakehouse}
	if r.environment != "" {
		wanted[r.environment] = workspacefs.Environment
	}
	var selected map[string]location
	for {
		selected, err = catalogLocations(ctx, r.backend.ReadDir, r.backend.Lookup, p.workspace, wanted)
		if !errors.Is(err, fs.ErrNotExist) {
			break
		}
		// ListItems may lag item creation. Let its bounded cache expire; never
		// guess a display-name path or claim a similarly named existing item.
		if err := waitRetryAfter(ctx, "5"); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	p.lakehouse = selected[r.opts.Lakehouse]
	p.environment = selected[r.environment]
	children, err := r.backend.ReadDir(ctx, p.lakehouse.entry)
	if err != nil {
		return err
	}
	fixed, err := fixedItemLocations(p.lakehouse, children)
	if err != nil {
		return err
	}
	p.files, p.tables = fixed["Files"], fixed["Tables"]
	return nil
}

func (r *runner) readSmall(path string, limit int64) (data []byte, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r.track(file)
	defer func() { err = errors.Join(err, r.closeFile(file)) }()
	data, err = io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fail("mounted file exceeded the smoke read bound")
	}
	return data, nil
}

func (r *runner) notebookReads() error {
	var original []byte
	err := r.measure("notebook.cold-discovery-and-read", func() (int, error) {
		children, err := r.backend.ReadDir(r.ctx, r.paths.notebook.entry)
		if err != nil {
			return 0, err
		}
		r.paths.notebookFile, r.paths.notebookIdentity, err = notebookContentLocations(r.ctx, r.backend.Lookup, r.paths.notebook, children)
		if err != nil {
			return 0, err
		}
		original, err = r.readSmall(r.paths.notebookFile.path, maxNotebookBytes)
		if err != nil {
			return 0, err
		}
		if _, _, err := notebookDocument(original); err != nil {
			return 0, err
		}
		return len(original), nil
	})
	if err != nil {
		return err
	}
	before := r.counter.snapshot()
	started := time.Now()
	for i := range 2 {
		if err := r.measure(fmt.Sprintf("notebook.warm-ls-stat-read-%d", i+1), func() (int, error) {
			if err := r.runListing("ls", []string{"-la", "--", r.paths.notebook.path}); err != nil {
				return 0, err
			}
			if _, err := os.Stat(r.paths.notebookFile.path); err != nil {
				return 0, err
			}
			data, err := r.readSmall(r.paths.notebookFile.path, maxNotebookBytes)
			if err != nil {
				return 0, err
			}
			if !bytes.Equal(original, data) {
				return 0, fail("immediate cached notebook read changed unexpectedly")
			}
			return len(data), nil
		}); err != nil {
			return err
		}
	}
	if time.Since(started) >= cacheTTL {
		return fail("warm notebook observations exceeded the five-second cache window")
	}
	if countDelta(before, r.counter.snapshot())["fabric|POST|definition-read"] != 0 {
		return fail("immediate warm notebook reads started additional definition exports")
	}
	return nil
}

func (r *runner) runListing(command string, args []string) error {
	cmd := exec.CommandContext(r.ctx, command, args...)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Run(); err != nil {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}
		return fail("ordinary mounted listing command failed; output deliberately not logged")
	}
	return nil
}

func (r *runner) listings() error {
	for _, target := range []struct {
		role string
		path string
	}{
		{"mount", r.paths.root.path}, {"workspace", r.paths.workspace.path},
		{"notebook", r.paths.notebook.path}, {"lakehouse", r.paths.lakehouse.path},
		{"files", r.paths.files.path},
	} {
		for _, command := range []struct {
			name string
			mode string
			args []string
		}{
			{"ls", "plain", []string{"--", target.path}},
			{"ls", "long-all", []string{"-la", "--", target.path}},
			{"find", "depth-one", []string{target.path, "-maxdepth", "1", "-print"}},
		} {
			if err := r.measure(command.name+"."+command.mode+"."+target.role, func() (int, error) {
				return 0, r.runListing(command.name, command.args)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *runner) syncTwice(file *os.File, label string) error {
	if err := file.Sync(); err != nil {
		return err
	}
	before := r.counter.snapshot()
	if err := r.measure(label+".second-sync", func() (int, error) { return 0, file.Sync() }); err != nil {
		return err
	}
	delta := countDelta(before, r.counter.snapshot())
	if delta["onelake|PATCH|append"] != 0 || delta["onelake|PUT|rename"] != 0 ||
		delta["fabric|POST|definition-update"] != 0 {
		return fail("second Sync repeated a remote append, rename, or definition update")
	}
	return nil
}

func writeAllAt(file *os.File, data []byte, offset int64) error {
	n, err := file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (r *runner) freshNotebookDefinition(content location, original []byte) (fabric.Definition, error) {
	entry := content.entry
	if entry.Kind != workspacefs.NotebookContent || entry.Item.Type != "Notebook" ||
		!strings.EqualFold(entry.Workspace, r.opts.Workspace) || entry.Part == "" || entry.Size < 0 {
		return fabric.Definition{}, fail("definition verification requires the selected notebook body")
	}
	// Use the raw guarded client, not the cached FS snapshot. This observation
	// does not replace the backend's own fresh pre-commit conflict comparison.
	def, err := r.fab.GetDefinition(r.ctx, entry.Workspace, entry.Item.ID, "Notebook", "ipynb")
	if err != nil {
		return fabric.Definition{}, err
	}
	body, err := notebookDefinitionBody(def, entry.Part)
	if err != nil {
		return fabric.Definition{}, err
	}
	if !bytes.Equal(body, original) {
		return fabric.Definition{}, fail("mounted notebook body differs from its fresh remote definition")
	}
	return def, nil
}

func (r *runner) verifySavedNotebookDefinition(content location, before fabric.Definition, marker string) error {
	if _, err := notebookDefinitionBody(before, content.entry.Part); err != nil {
		return err
	}
	after, err := r.fab.GetDefinition(r.ctx, content.entry.Workspace, content.entry.Item.ID, "Notebook", "ipynb")
	if err != nil {
		return err
	}
	return verifyNotebookDefinitionRoundTrip(before, after, content.entry.Part, marker)
}

func (r *runner) notebookUpdate() error {
	if r.fixture.ownedID == "" || !strings.EqualFold(r.paths.notebookFile.entry.Item.ID, r.fixture.ownedID) {
		return fail("refusing to update a notebook other than the verified fixture")
	}
	original, err := r.readSmall(r.paths.notebookFile.path, maxNotebookBytes)
	if err != nil {
		return err
	}
	definition, err := r.freshNotebookDefinition(r.paths.notebookFile, original)
	if err != nil {
		return err
	}
	updated, err := setNotebookMarker(original, r.doc.Plan.OwnershipMarker)
	if err != nil {
		return err
	}
	beforeWrite := r.counter.snapshot()
	if err := r.measure("notebook.write-truncate-sync-close", func() (int, error) {
		file, err := os.OpenFile(r.paths.notebookFile.path, os.O_RDWR, 0)
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
		if err := r.syncTwice(file, "notebook"); err != nil {
			return 0, err
		}
		return len(updated), r.closeFile(file)
	}); err != nil {
		return err
	}
	if countDelta(beforeWrite, r.counter.snapshot())["fabric|POST|definition-update"] == 0 {
		return fail("mounted notebook save did not issue a remote definition update")
	}
	if err := r.measure("notebook.read-saved-semantic-marker", func() (int, error) {
		data, err := r.readSmall(r.paths.notebookFile.path, maxNotebookBytes)
		if err != nil {
			return 0, err
		}
		return len(data), hasNotebookMarker(data, r.doc.Plan.OwnershipMarker)
	}); err != nil {
		return err
	}
	return r.measure("notebook.fresh-definition-part-preservation", func() (int, error) {
		return 0, r.verifySavedNotebookDefinition(r.paths.notebookFile, definition, r.doc.Plan.OwnershipMarker)
	})
}

func (r *runner) createOwnedFile(directory, name string) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	r.track(file)
	if err := r.rememberPath(name, false); err != nil {
		return nil, errors.Join(err, r.closeFile(file))
	}
	return file, nil
}

func verifyEntry(directory, name string, present, isDirectory bool, size int64) error {
	children, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	found := false
	for _, child := range children {
		if child.Name() == name {
			found = true
			if child.IsDir() != isDirectory {
				return fail("post-mutation directory entry type is stale")
			}
		}
	}
	if found != present {
		return fail("post-mutation readdir did not reflect the mutation immediately")
	}
	info, err := os.Stat(filepath.Join(directory, name))
	if !present {
		if !errors.Is(err, fs.ErrNotExist) {
			return fail("removed or renamed source still exists, or absence could not be verified")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() != isDirectory || (!isDirectory && info.Size() != size) {
		return fail("post-mutation stat did not reflect the new type or size")
	}
	return nil
}

func (r *runner) expectBytes(path string, expected []byte) (int, error) {
	data, err := r.readSmall(path, maxNotebookBytes)
	if err != nil {
		return 0, err
	}
	if !bytes.Equal(data, expected) {
		return 0, fail("owned mounted file bytes did not match the write/truncate result")
	}
	return len(data), nil
}

func (r *runner) lakeCRUD() error {
	directory := filepath.Join(r.paths.files.path, r.doc.Plan.Stem)
	if err := r.measure("lake.mkdir-create-new", func() (int, error) {
		r.dirAttempted = true
		if err := os.Mkdir(directory, 0700); err != nil {
			return 0, err
		}
		owned, _ := r.guard.state()
		if !owned {
			return 0, fail("new directory did not receive a conditional-create ownership receipt")
		}
		return 0, verifyEntry(r.paths.files.path, r.doc.Plan.Stem, true, true, 0)
	}); err != nil {
		return err
	}
	if _, err := os.ReadDir(directory); err != nil {
		return err
	}
	if err := r.measure("lake.empty-file-create-read", func() (int, error) {
		file, err := r.createOwnedFile(directory, "empty.txt")
		if err != nil {
			return 0, err
		}
		defer r.closeFile(file)
		if err := file.Sync(); err != nil {
			return 0, err
		}
		if err := r.closeFile(file); err != nil {
			return 0, err
		}
		if err := verifyEntry(directory, "empty.txt", true, false, 0); err != nil {
			return 0, err
		}
		return r.expectBytes(filepath.Join(directory, "empty.txt"), []byte{})
	}); err != nil {
		return err
	}
	payload := filepath.Join(directory, "payload.bin")
	expected := []byte{0, 0, 0, 0, 'h', 'e', 0, 0, 0, 0}
	beforePayload := r.counter.snapshot()
	if err := r.measure("lake.offset-write-shrink-zero-extend", func() (int, error) {
		file, err := r.createOwnedFile(directory, "payload.bin")
		if err != nil {
			return 0, err
		}
		defer r.closeFile(file)
		if err := writeAllAt(file, []byte("hello"), 4); err != nil {
			return 0, err
		}
		if err := file.Truncate(6); err != nil {
			return 0, err
		}
		if err := file.Truncate(10); err != nil {
			return 0, err
		}
		if err := r.syncTwice(file, "lake.payload"); err != nil {
			return 0, err
		}
		if err := r.closeFile(file); err != nil {
			return 0, err
		}
		return len(expected), verifyEntry(directory, "payload.bin", true, false, int64(len(expected)))
	}); err != nil {
		return err
	}
	payloadWrites := countDelta(beforePayload, r.counter.snapshot())
	if payloadWrites["onelake|PATCH|append"] == 0 || payloadWrites["onelake|PUT|rename"] == 0 {
		return fail("mounted nonempty file commit did not append and conditionally publish its bytes")
	}
	if err := r.measure("lake.cold-read", func() (int, error) { return r.expectBytes(payload, expected) }); err != nil {
		return err
	}
	if err := r.measure("lake.warm-read", func() (int, error) { return r.expectBytes(payload, expected) }); err != nil {
		return err
	}
	updated := []byte("updated-" + r.doc.Plan.Stem)
	if err := r.measure("lake.reopen-truncate-update", func() (int, error) {
		if _, owned := r.ownedPaths[r.lakePath("payload.bin").Relative]; !owned {
			return 0, fail("refusing O_TRUNC of an unowned file")
		}
		file, err := os.OpenFile(payload, os.O_WRONLY|os.O_TRUNC, 0)
		if err != nil {
			return 0, err
		}
		r.track(file)
		defer r.closeFile(file)
		if err := writeAllAt(file, updated, 0); err != nil {
			return 0, err
		}
		if err := r.syncTwice(file, "lake.truncated"); err != nil {
			return 0, err
		}
		if err := r.closeFile(file); err != nil {
			return 0, err
		}
		if err := verifyEntry(directory, "payload.bin", true, false, int64(len(updated))); err != nil {
			return 0, err
		}
		return r.expectBytes(payload, updated)
	}); err != nil {
		return err
	}
	if err := r.measure("lake.nested-mkdir-rmdir", func() (int, error) {
		nested := filepath.Join(directory, "nested")
		if err := os.Mkdir(nested, 0700); err != nil {
			return 0, err
		}
		if err := r.rememberPath("nested", true); err != nil {
			return 0, err
		}
		if err := verifyEntry(directory, "nested", true, true, 0); err != nil {
			return 0, err
		}
		if err := os.Remove(nested); err != nil {
			return 0, err
		}
		return 0, verifyEntry(directory, "nested", false, true, 0)
	}); err != nil {
		return err
	}
	if err := r.measure("lake.create-own-replacement-destination", func() (int, error) {
		file, err := r.createOwnedFile(directory, "replace.bin")
		if err != nil {
			return 0, err
		}
		defer r.closeFile(file)
		data := []byte("owned-replacement-" + r.doc.Plan.Stem)
		if err := writeAllAt(file, data, 0); err != nil {
			return 0, err
		}
		if err := file.Sync(); err != nil {
			return 0, err
		}
		if err := r.closeFile(file); err != nil {
			return 0, err
		}
		return len(data), verifyEntry(directory, "replace.bin", true, false, int64(len(data)))
	}); err != nil {
		return err
	}
	if err := r.measure("lake.conditional-replace-rename", func() (int, error) {
		for _, name := range []string{"payload.bin", "replace.bin"} {
			if _, owned := r.ownedPaths[r.lakePath(name).Relative]; !owned {
				return 0, fail("rename replacement is restricted to two confirmed owned files")
			}
		}
		if err := os.Rename(payload, filepath.Join(directory, "replace.bin")); err != nil {
			return 0, err
		}
		if err := verifyEntry(directory, "payload.bin", false, false, 0); err != nil {
			return 0, err
		}
		if err := verifyEntry(directory, "replace.bin", true, false, int64(len(updated))); err != nil {
			return 0, err
		}
		return r.expectBytes(filepath.Join(directory, "replace.bin"), updated)
	}); err != nil {
		return err
	}
	return r.measure("lake.remove-exact-files-and-empty-directory", func() (int, error) {
		for _, name := range []string{"empty.txt", "replace.bin"} {
			if err := os.Remove(filepath.Join(directory, name)); err != nil {
				return 0, err
			}
			if err := verifyEntry(directory, name, false, false, 0); err != nil {
				return 0, err
			}
		}
		children, err := os.ReadDir(directory)
		if err != nil {
			return 0, err
		}
		if len(children) != 0 {
			return 0, fail("unexpected fixture leftovers; empty-directory removal refused")
		}
		if err := os.Remove(directory); err != nil {
			return 0, err
		}
		return 0, verifyEntry(r.paths.files.path, r.doc.Plan.Stem, false, true, 0)
	})
}

func (r *runner) readonlyCall(label string, strictEROFS, allowReads bool, call func() error) error {
	return r.measure("readonly."+label, func() (int, error) {
		before := r.counter.snapshot()
		_, rejectedBefore := r.guard.state()
		err := call()
		expected := errors.Is(err, syscall.EROFS)
		if !strictEROFS {
			expected = expected || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM)
		}
		if !expected {
			var code syscall.Errno
			_ = errors.As(err, &code)
			return 0, fail(fmt.Sprintf("readonly mounted operation did not return the required local denial (errno=%d)", code))
		}
		_, rejectedAfter := r.guard.state()
		if rejectedAfter != rejectedBefore {
			return 0, fail("readonly operation reached the outbound mutation guard instead of rejecting locally")
		}
		delta := countDelta(before, r.counter.snapshot())
		for key, count := range delta {
			read := strings.Contains(key, "|GET|") || strings.Contains(key, "|HEAD|") || key == "fabric|POST|definition-read"
			if count != 0 && (!allowReads || !read) {
				return 0, fail("readonly operation issued unexpected HTTP requests")
			}
		}
		return 0, nil
	})
}

func (r *runner) rejectOpen(path string, flags int) error {
	file, err := os.OpenFile(path, flags, 0600)
	if file != nil {
		r.track(file)
		r.closeFile(file)
	}
	return err
}

func (r *runner) checkIdentityReadonly(role string, item location) error {
	var expectedPart string
	if item.entry.Kind == workspacefs.Notebook {
		children, err := r.backend.ReadDir(r.ctx, item.entry)
		if err != nil {
			return err
		}
		body, _, err := notebookContentLocations(r.ctx, r.backend.Lookup, item, children)
		if err != nil {
			return err
		}
		expectedPart = body.entry.Part
	}
	metadata, err := r.backend.Lookup(r.ctx, item.entry, ".fabric.json")
	if err != nil || metadata.Kind != workspacefs.IdentityFile {
		return fail("generated identity metadata could not be looked up locally")
	}
	local, err := childLocation(item, metadata)
	if err != nil {
		return err
	}
	beforeHTTP := r.counter.snapshot()
	original, err := r.readSmall(local.path, 64<<10)
	if err != nil {
		return err
	}
	if countDelta(beforeHTTP, r.counter.snapshot())["fabric|POST|definition-read"] != 0 {
		return fail("generated metadata read downloaded a definition")
	}
	var identity identityMetadata
	expectedID, expectedType := item.entry.Item.ID, item.entry.Item.Type
	expectedName, expectedWorkspace := item.entry.Item.DisplayName, item.entry.Workspace
	expectedFolder, expectedParentFolder := item.entry.Item.FolderID, ""
	if item.entry.Kind == workspacefs.Workspace {
		expectedID, expectedType = item.entry.Workspace, "Workspace"
		expectedName, expectedWorkspace = item.entry.Label, ""
	} else if item.entry.Kind == workspacefs.FabricFolder {
		expectedID, expectedType = item.entry.Folder.ID, "Folder"
		expectedName = item.entry.Folder.DisplayName
		expectedParentFolder = item.entry.Folder.ParentFolderID
	}
	if json.Unmarshal(original, &identity) != nil || !strings.EqualFold(identity.ID, expectedID) ||
		identity.Type != expectedType || identity.DisplayName != expectedName ||
		!strings.EqualFold(identity.WorkspaceID, expectedWorkspace) ||
		!strings.EqualFold(identity.FolderID, expectedFolder) ||
		!strings.EqualFold(identity.ParentFolderID, expectedParentFolder) ||
		(identity.RemotePartPath != "" && identity.RemotePartPath != expectedPart) {
		return fail("generated metadata did not preserve immutable identity")
	}
	// A lookup may refresh catalog/definition reads after their TTL expires;
	// those are not writes. The mutation guard and unchanged bytes remain strict.
	if err := r.readonlyCall("metadata."+role, false, true, func() error {
		return r.rejectOpen(local.path, os.O_WRONLY|os.O_TRUNC)
	}); err != nil {
		return err
	}
	current, err := r.readSmall(local.path, 64<<10)
	if err != nil {
		return err
	}
	return unchangedIdentityMetadata(original, current, expectedPart)
}

func unchangedIdentityMetadata(original, current []byte, expectedPart string) error {
	var stable [2][]byte
	for i, data := range [][]byte{original, current} {
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil || fields == nil {
			return fail("generated metadata is not a valid identity object")
		}
		if raw, exists := fields["remotePartPath"]; exists {
			var part string
			if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &part) != nil ||
				(part != "" && part != expectedPart) {
				return fail("generated metadata acquired an unverified remote notebook part")
			}
		}
		// Only this optional snapshot-derived field may disappear when its TTL
		// expires. All other fields, including unknown ones, must be unchanged.
		delete(fields, "remotePartPath")
		var err error
		stable[i], err = json.Marshal(fields)
		if err != nil {
			return fail("generated metadata could not be compared")
		}
	}
	if !bytes.Equal(stable[0], stable[1]) {
		return fail("generated metadata changed during a rejected write")
	}
	return nil
}

func (r *runner) checkFixedItemRoot(role string, item location) error {
	return r.measure("namespace.fixed-root."+role, func() (int, error) {
		var resourceRoots []location
		if _, err := r.checkReadTraffic(false, func() (int, error) {
			children, err := r.backend.ReadDir(r.ctx, item.entry)
			if err != nil {
				return 0, err
			}
			fixed, err := fixedItemLocations(item, children)
			if err != nil {
				return 0, err
			}
			for _, child := range fixed {
				if child.entry.Kind != workspacefs.ResourceDirectory {
					continue
				}
				resolved, err := r.backend.Lookup(r.ctx, item.entry, child.entry.Name)
				if err != nil {
					return 0, err
				}
				if resolved.Kind != workspacefs.ResourceDirectory || !resolved.Directory ||
					!resolved.Fixed || !sameItemChild(item.entry, resolved) || resolved.Resource != child.entry.Resource {
					return 0, fail("resource-root lookup did not preserve its fixed descriptor")
				}
				// This harness deliberately has no resource provider. Presence
				// alone must never be reported as live builtin/resources support.
				if _, err := r.backend.ReadDir(r.ctx, resolved); !errors.Is(err, fserrors.ErrUnsupported) {
					return 0, fail("disabled resource provider did not report unsupported")
				}
				resourceRoots = append(resourceRoots, child)
			}
			return 0, nil
		}); err != nil {
			return 0, err
		}
		return r.checkReadTraffic(true, func() (int, error) {
			for _, child := range resourceRoots {
				if _, err := os.ReadDir(child.path); !errors.Is(err, syscall.ENOTSUP) {
					return 0, fail("mounted disabled resource root did not report ENOTSUP")
				}
			}
			return 0, nil
		})
	})
}

func (r *runner) readonlyChecks() error {
	r.guard.setReadOnly(true)
	defer r.guard.setReadOnly(false)
	for _, item := range []struct {
		role string
		node location
	}{
		{"workspace", r.paths.workspace}, {"notebook", r.paths.notebook}, {"lakehouse", r.paths.lakehouse},
		{"environment", r.paths.environment},
		{"fabric-folder", r.paths.folder},
	} {
		if item.node.path == "" {
			continue
		}
		if item.node.entry.Kind == workspacefs.Notebook || item.node.entry.Kind == workspacefs.Environment ||
			item.node.entry.Kind == workspacefs.Lakehouse {
			if err := r.checkFixedItemRoot(item.role, item.node); err != nil {
				return err
			}
		}
		if err := r.checkIdentityReadonly(item.role, item.node); err != nil {
			return err
		}
	}
	for _, kind := range managedKinds {
		if local, exists := r.managedLocations[kind]; exists {
			if err := r.checkFixedItemRoot("managed-"+kind, local); err != nil {
				return err
			}
			if err := r.checkIdentityReadonly("managed-"+kind, local); err != nil {
				return err
			}
		}
	}
	for _, kind := range []string{"Notebook", "Environment"} {
		if err := r.assertUnsupportedManagedRmdir(kind); err != nil {
			return err
		}
	}
	// Workspace and Fabric-folder children now support real mkdir. Only the
	// mount root is synthetic and creation-protected.
	for _, target := range []struct {
		role string
		node location
	}{{"mount-root", r.paths.root}} {
		probe := filepath.Join(target.node.path, r.doc.Plan.Stem+"-protected")
		if err := r.namespaceDenial("create."+target.role, false, func() error {
			return r.rejectOpen(probe, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
		}); err != nil {
			return err
		}
		if err := r.namespaceDenial("mkdir."+target.role, false, func() error {
			return os.Mkdir(probe, 0700)
		}); err != nil {
			return err
		}
	}
	for _, kind := range []string{"Notebook", "Lakehouse", "Environment", "Folder"} {
		local, exists := r.managedLocations[kind]
		if !exists {
			continue
		}
		destination := filepath.Join(local.parent, r.doc.Plan.Stem+"-unsupported-rename-"+kind)
		if err := r.namespaceDenial("remote-rename."+kind, true, func() error {
			return os.Rename(local.path, destination)
		}); err != nil {
			return err
		}
	}
	if _, err := os.Stat(r.paths.tables.path); err != nil {
		return err
	}
	tableProbe := filepath.Join(r.paths.tables.path, r.doc.Plan.Stem+"-readonly")
	// Kernel lookup may HEAD a new Tables name before the mutation callback.
	// default_permissions may return EACCES/EPERM before the backend's EROFS.
	// GET/HEAD discovery is allowed; every remote mutation remains a failure.
	if err := r.readonlyCall("tables-create", false, true, func() error {
		return r.rejectOpen(tableProbe, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	}); err != nil {
		return err
	}
	if err := r.readonlyCall("tables-mkdir", false, true, func() error { return os.Mkdir(tableProbe, 0700) }); err != nil {
		return err
	}
	return r.readonlyCall("tables-root-rename", false, true, func() error {
		return os.Rename(r.paths.tables.path, filepath.Join(r.paths.lakehouse.path, r.doc.Plan.Stem+"-readonly-tables"))
	})
}

func (r *runner) namespaceDenial(label string, requireUnsupported bool, call func() error) error {
	return r.measure("namespace."+label, func() (int, error) {
		before := r.counter.snapshot()
		_, deniedBefore := r.guard.state()
		err := call()
		expected := errors.Is(err, syscall.ENOTSUP)
		if !requireUnsupported {
			expected = expected || errors.Is(err, syscall.EROFS) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM)
		}
		_, deniedAfter := r.guard.state()
		if !expected || mutationCount(countDelta(before, r.counter.snapshot())) != 0 || deniedAfter != deniedBefore {
			return 0, fail("protected or unsupported namespace operation did not reject locally")
		}
		return 0, nil
	})
}
