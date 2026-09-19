//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fusefs"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/workspacefs"
)

type runner struct {
	opts                   options
	ctx                    context.Context
	output                 io.Writer
	doc                    *report
	journal                *evidenceFile
	counter                *requestCounter
	guard                  *scopeGuard
	nativeHTTP             *http.Transport
	fab                    *fabric.Client
	lake                   *onelake.Client
	fixture                *fixtureClient
	backend                *workspacefs.FS
	server                 fusefs.Server
	logFile                *os.File
	parentInfo             os.FileInfo
	handles                map[*os.File]bool
	ownedPaths             map[string]bool
	dirAttempted           bool
	environment            string
	paths                  locations
	managedLocations       map[string]location
	managedCreateAttempted bool
}

func prepareSignals() { signal.Ignore(syscall.SIGPIPE) }

func syncEvidenceDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func run(opts options, output io.Writer) (runErr error) {
	if err := validateEvidenceTarget(opts.Evidence); err != nil {
		return err
	}
	doc, err := initialReport(opts)
	if err != nil {
		return err
	}
	journal, err := openEvidence(opts.Evidence, doc)
	if err != nil {
		return err
	}
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithTimeout(signalCtx, runTimeout)
	r := &runner{
		opts: opts, ctx: ctx, output: output, doc: doc, journal: journal,
		counter: &requestCounter{}, guard: newScopeGuard(opts, doc.Plan),
		handles: make(map[*os.File]bool), ownedPaths: make(map[string]bool),
		managedLocations: make(map[string]location),
	}
	defer func() {
		if recover() != nil {
			runErr = fail("unexpected smoke-runner panic; details omitted")
		}
		cancel()
		runErr = r.cleanup(runErr)
	}()

	if err := r.stage("local-preflight", r.preflight); err != nil {
		return err
	}
	if err := r.stage("initialize-private-tree-and-clients", r.initialize); err != nil {
		return err
	}
	if err := r.stage("verify-selected-workspace-and-lakehouse", r.verifySelection); err != nil {
		return err
	}
	if err := r.stage("create-and-verify-owned-notebook", func() error { return r.fixture.create(r.ctx) }); err != nil {
		return err
	}
	if err := r.stage("construct-cached-backend-and-mount", r.mount); err != nil {
		return err
	}
	if err := r.stage("discover-paths-by-immutable-identity", r.discover); err != nil {
		return err
	}
	if err := r.stage("notebook-cold-warm-read", r.notebookReads); err != nil {
		return err
	}
	if err := r.stage("ordinary-ls-and-shallow-find", r.listings); err != nil {
		return err
	}
	if err := r.stage("owned-notebook-metadata-update", r.notebookUpdate); err != nil {
		return err
	}
	if err := r.stage("owned-lakehouse-files-crud", r.lakeCRUD); err != nil {
		return err
	}
	if err := r.stage("fuse-create-owned-fabric-folder-and-typed-items", r.createManagedFixtures); err != nil {
		return err
	}
	if err := r.stage("fuse-created-notebook-content-read-update", r.managedNotebookUpdate); err != nil {
		return err
	}
	if err := r.stage("root-injected-agents-bundle-readonly", r.injectedBundleChecks); err != nil {
		return err
	}
	if err := r.stage("readonly-boundaries-on-new-mount", r.readonlyChecks); err != nil {
		return err
	}
	if err := r.stage("explicit-owned-item-rest-cleanup-and-guarded-fuse-rmdir", r.deleteManagedFixtures); err != nil {
		return err
	}
	if _, denied := r.guard.state(); denied != 0 {
		return fail("smoke encountered outbound scope rejections; refusing to claim success")
	}
	return nil
}

// Do not let an evidence argument accidentally write through a pre-existing
// FUSE mount. Reject symlink ancestors instead of following them into a mount.
func validateEvidenceTarget(target string) error {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return usageError("cannot verify that the evidence destination is outside existing FUSE mounts")
	}
	mounts, parseErr := readMounts(file)
	closeErr := file.Close()
	if errors.Join(parseErr, closeErr) != nil || insideFuseMount(target, mounts) {
		return usageError("evidence must be a new native file outside all existing FUSE mounts")
	}
	parent := filepath.Dir(target)
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(parent, "/"), "/") {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		if insideFuseMount(current, mounts) {
			return usageError("evidence parent is inside an existing FUSE mount")
		}
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return usageError("evidence parent must already exist without symlink ancestors")
		}
	}
	return nil
}

func (r *runner) save() error {
	r.doc.HTTP = r.counter.snapshot()
	r.doc.LakeDirectoryOwned, r.doc.DeniedHTTP = r.guard.state()
	if r.backend != nil {
		r.doc.BackendCache = r.backend.CacheStats()
	}
	for kind, snapshot := range r.guard.managedEvidenceSnapshot() {
		previous := r.doc.ManagedResources[kind]
		if previous.Status != "" && previous.ID == snapshot.ID {
			snapshot.Status = previous.Status
			snapshot.LocalPath = previous.LocalPath
		}
		r.doc.ManagedResources[kind] = snapshot
	}
	return r.journal.save(r.doc)
}

func (r *runner) stage(name string, work func() error) error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	before := r.counter.snapshot()
	_, denied := r.guard.state()
	started := time.Now()
	index := len(r.doc.Stages)
	r.doc.Status = "running"
	r.doc.Stages = append(r.doc.Stages, stageResult{Name: name, Status: "running", StartedAt: started.UTC()})
	if err := r.save(); err != nil {
		return err
	}
	err := work()
	result := &r.doc.Stages[index]
	result.Duration = time.Since(started)
	result.HTTP = countDelta(before, r.counter.snapshot())
	_, finalDenied := r.guard.state()
	result.DeniedHTTP = finalDenied - denied
	result.Status = "passed"
	if err != nil {
		result.Status, result.Error = "failed", safeError(err)
	}
	return errors.Join(err, r.save())
}

func (r *runner) measure(name string, work func() (int, error)) error {
	index := len(r.doc.Measurements)
	r.doc.Measurements = append(r.doc.Measurements, measurement{Name: name, Detail: "running"})
	if err := r.save(); err != nil {
		return err
	}
	before := r.counter.snapshot()
	_, denied := r.guard.state()
	started := time.Now()
	size, err := work()
	_, finalDenied := r.guard.state()
	detail := "passed"
	if err != nil {
		detail = "failed: " + safeError(err)
	}
	r.doc.Measurements[index] = measurement{
		Name: name, Duration: time.Since(started), HTTP: countDelta(before, r.counter.snapshot()),
		Bytes: size, DeniedHTTP: finalDenied - denied, Detail: detail,
	}
	return errors.Join(err, r.save())
}

func (r *runner) preflight() error {
	for _, command := range []string{"ls", "find"} {
		if _, err := exec.LookPath(command); err != nil {
			return fail("required native ls/find command is unavailable")
		}
	}
	if info, err := os.Stat("/dev/fuse"); err != nil || info.Mode()&os.ModeDevice == 0 {
		return fail("native Linux /dev/fuse is unavailable")
	}
	info, err := os.Lstat("/tmp")
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fail("native /tmp must be an actual directory, not a symlink")
	}
	return nil
}

func (r *runner) initialize() error {
	parent, err := os.MkdirTemp("/tmp", "fabric-workspace-fs-test-live-")
	if err != nil {
		return fail("cannot create the native private smoke parent")
	}
	r.doc.PrivateParent = parent
	if _, err := fmt.Fprintf(r.output, "private_parent=%s\n", parent); err != nil {
		return fail("cannot report the newly created private parent")
	}
	info, err := os.Lstat(parent)
	if err != nil || !validNativeParent(parent) || !info.IsDir() || info.Mode().Perm() != 0700 {
		return fail("new private parent did not satisfy native ownership checks")
	}
	r.parentInfo = info
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil || resolved != parent {
		return fail("new private parent is not its exact native canonical path")
	}
	r.doc.Mountpoint = filepath.Join(parent, "mount")
	r.doc.SpoolDirectory = filepath.Join(parent, "spool")
	r.doc.LogFile = filepath.Join(parent, "fuse.log")
	for _, directory := range []string{r.doc.Mountpoint, r.doc.SpoolDirectory} {
		if err := os.Mkdir(directory, 0700); err != nil {
			return fail("cannot create the private mount/spool directories")
		}
	}
	r.logFile, err = os.OpenFile(r.doc.LogFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fail("cannot create the private FUSE log")
	}
	if err := r.save(); err != nil {
		return err
	}
	tokens, err := auth.NewDefault()
	if err != nil {
		return err
	}
	r.nativeHTTP = http.DefaultTransport.(*http.Transport).Clone()
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &countedTransport{
			base: r.nativeHTTP, counter: r.counter, guard: r.guard,
		},
	}
	newTransport := func(base, scope string) (*transport.Client, error) {
		return transport.New(transport.Options{
			BaseURL: base, Scope: scope, Tokens: tokens, HTTPClient: httpClient,
			MaxRetries: 3, RetryDelay: time.Second, MaxRetryDelay: 30 * time.Second,
		})
	}
	fabricHTTP, err := newTransport(fabric.BaseURL, auth.FabricScope)
	if err != nil {
		return err
	}
	lakeHTTP, err := newTransport(onelake.Endpoint, auth.OneLakeScope)
	if err != nil {
		return err
	}
	r.fab = fabric.New(fabricHTTP, fabric.Options{
		OperationTimeout: operationTimeout, MaxDefinitionBytes: 64 << 20, PollInterval: time.Second,
	})
	r.lake = onelake.New(lakeHTTP, onelake.Options{ChunkSize: 4 << 20, MaxPages: 1000})
	r.fixture = &fixtureClient{http: fabricHTTP, workspace: r.opts.Workspace, plan: r.doc.Plan}
	r.fixture.onChange = func() error {
		r.doc.ReturnedFixtureID = r.fixture.returnedID
		r.doc.CreateOperationID = r.fixture.operationID
		r.doc.DeleteOperationID = r.fixture.deleteOperationID
		r.doc.FixtureID = r.fixture.ownedID
		if r.fixture.returnedID != "" {
			r.doc.NotebookItemURL = fabric.BaseURL + "/v1/workspaces/" + r.opts.Workspace + "/items/" + r.fixture.returnedID
			r.doc.Plan.RemotePaths["notebook-item"] = r.doc.NotebookItemURL
			r.doc.Plan.RemotePaths["notebook-delete"] = fabric.BaseURL + "/v1/workspaces/" + r.opts.Workspace + "/notebooks/" + r.fixture.returnedID
		}
		if r.fixture.ownedID != "" {
			r.guard.setNotebook(r.fixture.ownedID)
		}
		return r.save()
	}
	return nil
}

func (r *runner) verifySelection() error {
	workspace, err := r.fab.GetWorkspace(r.ctx, r.opts.Workspace)
	if err != nil {
		return err
	}
	if !strings.EqualFold(workspace.ID, r.opts.Workspace) {
		return fail("selected workspace identity mismatch")
	}
	items, err := r.fab.ListItems(r.ctx, r.opts.Workspace)
	if err != nil {
		return err
	}
	found := false
	for _, item := range items {
		if strings.EqualFold(item.ID, r.opts.Lakehouse) {
			if item.Type != "Lakehouse" {
				return fail("selected item is not a Lakehouse")
			}
			found = true
		}
		if r.environment == "" && item.Type == "Environment" {
			r.environment = strings.ToLower(item.ID)
		}
	}
	if !found {
		return fail("selected Lakehouse was not found in the selected workspace")
	}
	folders, err := r.fab.ListFolders(r.ctx, r.opts.Workspace)
	if err != nil {
		return err
	}
	for _, folder := range folders {
		if folder.ParentFolderID == "" && folder.DisplayName == r.doc.Plan.Managed.FolderName {
			return fail("planned managed fixture folder already exists; refusing to claim it")
		}
	}
	if r.environment == "" {
		r.doc.Notes = append(r.doc.Notes, "No existing Environment was listed; its generated identity metadata check is not applicable.")
	}
	_, err = r.lake.Stat(r.ctx, r.lakePath(""))
	if err == nil {
		return fail("planned new Lakehouse fixture directory already exists; no ownership claimed")
	}
	if !isNotFound(err) {
		return err
	}
	return nil
}

func (r *runner) mount() error {
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs = []string{r.opts.Workspace}
	opts.SpoolDirectory = r.doc.SpoolDirectory
	opts.CacheTTL = cacheTTL
	backend, err := workspacefs.New(
		boundedFabric{api: r.fab, run: r.ctx, guard: r.guard}, boundedLake{api: r.lake, run: r.ctx}, opts,
	)
	if err != nil {
		return err
	}
	r.backend = backend
	if err := r.assertUnmounted(); err != nil {
		return err
	}
	r.server, err = fusefs.Mount(r.doc.Mountpoint, backend, fusefs.Options{
		Logger: log.New(r.logFile, "", log.LstdFlags),
	})
	if err != nil {
		return err
	}
	if err := boundedWait(r.ctx, r.server.WaitMount); err != nil {
		return err
	}
	r.doc.Cleanup.Mount = "mounted-own-server"
	return nil
}

func boundedWait(ctx context.Context, call func() error) error {
	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stopOwnedServer(ctx context.Context, server fusefs.Server) error {
	return boundedWait(ctx, func() error {
		unmountErr := server.Unmount()
		server.Wait()
		return unmountErr
	})
}

func closeOwnedBackend(stopped bool, closeBackend func() error) (string, error) {
	if closeBackend == nil {
		return "not-created", nil
	}
	if !stopped {
		return "retained-until-own-server-stops", nil
	}
	if err := closeBackend(); err != nil {
		return "close-failed-private-parent-retained", err
	}
	return "closed-after-owned-server-and-managed-cleanup", nil
}

func (r *runner) lakePath(name string) onelake.Path {
	relative := r.doc.Plan.LakeDirectory
	if name != "" {
		relative += "/" + name
	}
	return onelake.Path{Workspace: r.opts.Workspace, Item: r.opts.Lakehouse, Relative: relative}
}

func (r *runner) rememberPath(name string, directory bool) error {
	if name != "empty.txt" && name != "payload.bin" && name != "replace.bin" && name != "nested" {
		return fail("refusing to register an unplanned fixture child")
	}
	relative := r.lakePath(name).Relative
	r.ownedPaths[relative] = directory
	kind := "file"
	if directory {
		kind = "directory"
	}
	r.doc.CreatedPaths[relative] = kind
	return r.save()
}

func (r *runner) track(file *os.File) *os.File {
	r.handles[file] = true
	return file
}

func (r *runner) closeFile(file *os.File) error {
	if !r.handles[file] {
		return nil
	}
	delete(r.handles, file)
	return file.Close()
}

func (r *runner) assertUnmounted() error {
	if r.doc.PrivateParent == "" || !validNativeParent(r.doc.PrivateParent) {
		return fail("private parent identity unavailable; cleanup refused")
	}
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return fail("cannot open mountinfo; private parent retained")
	}
	mounted, parseErr := subtreeMounted(file, r.doc.PrivateParent)
	closeErr := file.Close()
	if errors.Join(parseErr, closeErr) != nil {
		return fail("cannot verify complete mountinfo; private parent retained")
	}
	if mounted {
		return fail("private parent or subtree remains mounted; recursive local removal refused")
	}
	return nil
}

func (r *runner) removeLakeFixtures(ctx context.Context) error {
	owned, _ := r.guard.state()
	if !owned {
		if !r.dirAttempted || r.lake == nil {
			r.doc.Cleanup.LakeDirectory = "not-created"
			return nil
		}
		_, err := r.lake.Stat(ctx, r.lakePath(""))
		if isNotFound(err) {
			r.doc.Cleanup.LakeDirectory = "confirmed-absent"
			return nil
		}
		r.doc.Cleanup.LakeDirectory = "unverified-ownership-preserved"
		return fail("directory create outcome is unverified; exact planned directory preserved")
	}
	root := r.lakePath("")
	info, err := r.lake.Stat(ctx, root)
	if isNotFound(err) {
		r.doc.Cleanup.LakeDirectory = "confirmed-absent"
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir || info.Path != root.Relative {
		return fail("owned fixture directory identity changed")
	}
	children, err := r.lake.List(ctx, root)
	if err != nil {
		return err
	}
	for _, child := range children {
		directory, known := r.ownedPaths[child.Path]
		if !known || directory != child.IsDir {
			r.doc.Cleanup.UnexpectedItems++
		}
	}
	if r.doc.Cleanup.UnexpectedItems != 0 {
		r.doc.Cleanup.LakeDirectory = "unexpected-children-preserved"
		return fail("unexpected children in exact fixture directory; no cleanup scope expansion")
	}
	for relative, directory := range r.ownedPaths {
		if !directory {
			continue
		}
		p := root
		p.Relative = relative
		nested, err := r.lake.List(ctx, p)
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if len(nested) != 0 {
			r.doc.Cleanup.UnexpectedItems += len(nested)
			r.doc.Cleanup.LakeDirectory = "unexpected-nested-children-preserved"
			return fail("unexpected nested fixture children; directory preserved")
		}
	}
	keys := make([]string, 0, len(r.ownedPaths))
	for relative := range r.ownedPaths {
		keys = append(keys, relative)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if r.ownedPaths[a] != r.ownedPaths[b] {
			if r.ownedPaths[a] {
				return 1
			}
			return -1
		}
		return strings.Compare(a, b)
	})
	for _, relative := range keys {
		if !below(root.Relative, relative) || relative == root.Relative {
			return fail("cleanup child escaped the exact owned directory")
		}
		p := root
		p.Relative = relative
		info, err := r.lake.Stat(ctx, p)
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Path != p.Relative || info.IsDir != r.ownedPaths[relative] {
			return fail("cleanup child identity changed")
		}
		if err := r.lake.Remove(ctx, p, info.IsDir, info.ETag); err != nil {
			return err
		}
		if err := r.confirmLakeAbsent(ctx, p); err != nil {
			return err
		}
	}
	children, err = r.lake.List(ctx, root)
	if err != nil {
		return err
	}
	if len(children) != 0 {
		r.doc.Cleanup.UnexpectedItems += len(children)
		r.doc.Cleanup.LakeDirectory = "unexpected-children-preserved"
		return fail("fixture directory is not empty; nonrecursive cleanup stopped")
	}
	info, err = r.lake.Stat(ctx, root)
	if err != nil {
		return err
	}
	if err := r.lake.Remove(ctx, root, true, info.ETag); err != nil {
		return err
	}
	if err := r.confirmLakeAbsent(ctx, root); err != nil {
		return err
	}
	r.doc.Cleanup.LakeDirectory = "deleted-and-confirmed-absent"
	return nil
}

func (r *runner) confirmLakeAbsent(ctx context.Context, p onelake.Path) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		_, err := r.lake.Stat(ctx, p)
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

func (r *runner) cleanup(runErr error) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	var cleanupErrors []error
	record := func(err error) {
		if err != nil {
			cleanupErrors = append(cleanupErrors, err)
			r.doc.Cleanup.Errors = append(r.doc.Cleanup.Errors, safeError(err))
		}
	}
	r.doc.Status = "cleaning-up"
	legacyOverlayErr := r.doc.refuseLegacyOverlayCleanup()
	record(legacyOverlayErr)
	record(r.save())
	r.doc.Cleanup.Handles = "closed"
	for file := range r.handles {
		if err := r.closeFile(file); err != nil {
			r.doc.Cleanup.Handles = "close-error"
			record(err)
		}
	}
	stopped := true
	if r.server != nil {
		shutdownCtx, stop := context.WithTimeout(ctx, time.Minute)
		err := stopOwnedServer(shutdownCtx, r.server)
		stop()
		if err != nil {
			stopped = false
			r.doc.Cleanup.Mount = "shutdown-failed-private-parent-retained"
			record(err)
		} else {
			r.doc.Cleanup.Mount = "own-server-unmounted-and-waited"
		}
	}
	if r.logFile != nil && stopped {
		record(errors.Join(r.logFile.Sync(), r.logFile.Close()))
		r.logFile = nil
	}
	r.guard.setReadOnly(false)
	record(r.save())
	if stopped {
		if legacyOverlayErr == nil {
			record(r.cleanupManagedFixtures(ctx))
		} else {
			r.doc.Cleanup.Managed = "retained-unsupported-legacy-overlay-cleanup"
		}
		if err := r.removeLakeFixtures(ctx); err != nil {
			if r.doc.Cleanup.LakeDirectory == "not-created" {
				r.doc.Cleanup.LakeDirectory = "cleanup-failed-preserved"
			}
			record(err)
		}
		if r.fixture != nil && r.fixture.createSent && r.counter.snapshot()["fabric|POST|notebook-create"] != 0 {
			reconcileErr := r.fixture.reconcile(ctx)
			if reconcileErr != nil {
				record(reconcileErr)
			}
			if r.fixture.ownedID != "" {
				r.guard.setNotebook(r.fixture.ownedID)
				if err := r.fixture.remove(ctx); err != nil {
					r.doc.Cleanup.Notebook = "cleanup-failed-owned-id-preserved"
					record(err)
				} else {
					r.doc.Cleanup.Notebook = "same-id-confirmed-404"
				}
			} else {
				r.doc.Cleanup.Notebook = "unverified-ownership-preserved"
				if reconcileErr == nil {
					record(fail("notebook create outcome remained unverified"))
				}
			}
		}
	} else {
		r.doc.Cleanup.Managed = "preserved-until-own-server-stops"
		r.doc.Cleanup.Notebook = "preserved-until-own-server-stops"
		r.doc.Cleanup.LakeDirectory = "preserved-until-own-server-stops"
	}
	var closeBackend func() error
	if r.backend != nil {
		closeBackend = r.backend.Close
	}
	backendStatus, backendErr := closeOwnedBackend(stopped, closeBackend)
	r.doc.Cleanup.Backend = backendStatus
	record(backendErr)
	if r.nativeHTTP != nil {
		r.nativeHTTP.CloseIdleConnections()
	}
	record(r.save())
	if r.doc.PrivateParent != "" {
		safe := stopped
		if err := r.assertUnmounted(); err != nil {
			safe = false
			record(err)
		}
		info, err := os.Lstat(r.doc.PrivateParent)
		if err != nil || r.parentInfo == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			!os.SameFile(r.parentInfo, info) {
			safe = false
			record(fail("native private parent identity changed; removal refused"))
		}
		if safe && runErr == nil && len(cleanupErrors) == 0 {
			if err := os.RemoveAll(r.doc.PrivateParent); err != nil {
				record(fail("cannot remove the exact verified-unmounted private parent"))
				r.doc.Cleanup.LocalParent = "retained"
			} else {
				r.doc.Cleanup.LocalParent = "removed-after-mountinfo-verification"
			}
		} else {
			r.doc.Cleanup.LocalParent = "retained-after-failure"
		}
		if r.doc.Cleanup.LocalParent != "removed-after-mountinfo-verification" {
			fmt.Fprintf(r.output, "private_parent_retained=%s\n", r.doc.PrivateParent)
		}
	}
	result := errors.Join(append([]error{runErr}, cleanupErrors...)...)
	r.doc.FinishedAt = time.Now().UTC()
	r.doc.Status = "passed"
	if result != nil {
		r.doc.Status, r.doc.Error = "failed", safeError(result)
	}
	return errors.Join(result, r.save())
}
