//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/workspacefs"
)

func newOfflineBackendRunner(t *testing.T, route roundTripFunc) *runner {
	t.Helper()
	if route == nil {
		route = func(*http.Request) (*http.Response, error) {
			t.Fatal("local-only backend check attempted HTTP")
			return nil, fail("unexpected offline request")
		}
	}
	fixture, counter, guard := newFixtureHarness(t, route)
	fixture.ownedID = testNotebook
	guard.setNotebook(testNotebook)
	lakeHTTP, err := transport.New(transport.Options{
		BaseURL: onelake.Endpoint, Scope: auth.OneLakeScope, Tokens: offlineTokens{},
		HTTPClient: &http.Client{Transport: &countedTransport{base: route, counter: counter, guard: guard}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fab := fabric.New(fixture.http, fabric.Options{})
	lake := onelake.New(lakeHTTP, onelake.Options{})
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs, opts.SpoolDirectory, opts.CacheTTL = []string{testWorkspace}, t.TempDir(), cacheTTL
	backend, err := workspacefs.New(
		boundedFabric{api: fab, run: context.Background(), guard: guard},
		boundedLake{api: lake, run: context.Background()}, opts,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
	})
	doc, err := initialReport(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	doc.Plan = fixture.plan
	doc.SpoolDirectory = opts.SpoolDirectory
	// This is only a descriptor path. No mount or live runner is started.
	doc.Mountpoint = filepath.Join(t.TempDir(), "unmounted-descriptor")
	r := &runner{
		opts: testOptions(), ctx: context.Background(), doc: doc, fixture: fixture,
		fab: fab, lake: lake, counter: counter, guard: guard, backend: backend,
		handles: make(map[*os.File]bool), ownedPaths: make(map[string]bool),
		managedLocations: make(map[string]location),
	}
	r.paths.root = location{entry: backend.Root(), path: doc.Mountpoint}
	return r
}

func mockDefinitionResponse(t *testing.T, def fabric.Definition) *http.Response {
	t.Helper()
	data, err := json.Marshal(struct {
		Definition fabric.Definition `json:"definition"`
	}{def})
	if err != nil {
		t.Fatal(err)
	}
	return response(http.StatusOK, string(data), nil)
}

func readBackendBytes(t *testing.T, r *runner, entry workspacefs.Entry) []byte {
	t.Helper()
	handle, err := r.backend.Open(r.ctx, entry, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, int(handle.Size()))
	n, readErr := handle.ReadAt(r.ctx, data, 0)
	closeErr := handle.Close()
	if n != len(data) || readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
		t.Fatal("backend read did not return the exact bounded bytes")
	}
	return data
}

func TestInjectedBundleChecksUseTypedEntriesWithoutHTTPOrCleanupOwnership(t *testing.T) {
	r := newOfflineBackendRunner(t, nil)
	var nodes []bundleNode
	size, err := r.checkReadTraffic(false, func() (int, error) {
		var size int
		var err error
		nodes, size, err = r.directInjectedBundle()
		return size, err
	})
	if err != nil || size <= 0 || len(nodes) < 4 {
		t.Fatal("direct generated-bundle checks failed:", safeError(err))
	}
	for _, node := range nodes {
		if !validBundleEntry(node.entry) || r.backend.Writable(node.entry) {
			t.Fatal("bundle probe accepted a remote, overlay, or writable descriptor")
		}
	}
	if totalCounts(r.counter.snapshot()) != 0 || len(r.doc.CreatedOverlayPaths) != 0 ||
		r.doc.OverlayDirectory != "" || r.doc.Cleanup.Overlay != "" ||
		len(r.guard.managedEvidenceSnapshot()) != 0 {
		t.Fatal("injected bundle checks issued HTTP or granted fixture cleanup ownership")
	}
	if len(r.handles) != 0 {
		t.Fatal("bundle check leaked a mounted file handle")
	}
}

func TestBundleTrafficAccountingIsStrictForDirectAndMountedChecks(t *testing.T) {
	for _, tt := range []struct {
		name       string
		allowReads bool
		key        string
		deny       bool
		wantError  bool
	}{
		{"direct local", false, "", false, false},
		{"direct catalog read", false, "fabric|GET|workspace", false, true},
		{"direct definition read", false, "fabric|POST|definition-read", false, true},
		{"mounted traversal read", true, "fabric|GET|workspace", false, false},
		{"mounted definition read", true, "fabric|POST|definition-read", false, false},
		{"mounted mutation", true, "fabric|POST|managed-folder-create", false, true},
		{"mounted denied mutation", true, "", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := newOfflineBackendRunner(t, nil)
			_, err := r.checkReadTraffic(tt.allowReads, func() (int, error) {
				if tt.key != "" {
					r.counter.add(tt.key)
				}
				if tt.deny {
					_, _ = r.guard.authorize(managedCreateRequest(t, r.doc.Plan.Managed.FolderCreateURL, ".agents", "", ""))
				}
				return 0, nil
			})
			if (err != nil) != tt.wantError {
				t.Fatal("unexpected direct/mounted request accounting result")
			}
		})
	}
}

func TestBundleDescriptorRejectsNameOnlyAndLegacyOverlayEntries(t *testing.T) {
	for _, entry := range []workspacefs.Entry{
		{Name: ".agents", Kind: workspacefs.OverlayDirectory, Directory: true},
		{Name: ".agents", Kind: workspacefs.FabricFolder, Directory: true},
		{Name: "AGENT.md", Kind: workspacefs.AgentFile, Workspace: testWorkspace},
		{Name: "AGENT.md", Kind: workspacefs.AgentFile, Item: fabric.Item{ID: testNotebook}},
		{Name: ".agents", Kind: workspacefs.AgentDirectory, Directory: true},
	} {
		if validBundleEntry(entry) {
			t.Fatal("name-only or legacy local data was accepted as injected content")
		}
	}
}

func TestRunnerDiscoversSelectedWorkspaceDirectlyAtRootByIdentity(t *testing.T) {
	for _, displayName := range []string{"Workspace display", "Workspaces", ".agents"} {
		t.Run(displayName, func(t *testing.T) {
			plan := testPlan(t)
			base := "/v1/workspaces/" + testWorkspace
			r := newOfflineBackendRunner(t, func(req *http.Request) (*http.Response, error) {
				switch req.Method + " " + req.URL.Path {
				case "GET " + base:
					data, _ := json.Marshal(fabric.Workspace{ID: testWorkspace, DisplayName: displayName})
					return response(200, string(data), nil), nil
				case "GET " + base + "/items":
					return response(200, `{"value":[`+fixtureItemJSON(t, plan)+`,{"id":"`+testLakehouse+`","displayName":"Lake","type":"Lakehouse"}]}`, nil), nil
				case "GET " + base + "/folders":
					return response(200, `{"value":[]}`, nil), nil
				default:
					t.Fatal("catalog-only discovery attempted an unrelated request")
					return nil, fail("unexpected offline request")
				}
			})
			if err := r.discover(); err != nil {
				t.Fatal("flattened namespace discovery failed:", safeError(err))
			}
			if r.paths.workspace.parent != r.doc.Mountpoint ||
				r.paths.workspace.path != filepath.Join(r.doc.Mountpoint, r.paths.workspace.entry.Name) ||
				r.paths.workspace.entry.Workspace != testWorkspace || r.paths.workspace.entry.Label != displayName ||
				r.paths.notebook.parent != r.paths.workspace.path {
				t.Fatal("workspace paths still depend on a wrapper or guessed display name")
			}
			children, err := r.backend.ReadDir(r.ctx, r.backend.Root())
			if err != nil || len(children) != 2 {
				t.Fatal("root did not list the selected workspace and injected bundle")
			}
			for _, child := range children {
				if child.Kind != workspacefs.Workspace && child.Kind != workspacefs.AgentDirectory {
					t.Fatal("root exposed a synthetic workspace container")
				}
			}
		})
	}
}

func TestNotebookRootIsLazyAndBuiltinPresenceIsNotProviderSupport(t *testing.T) {
	r := newOfflineBackendRunner(t, func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/workspaces/"+testWorkspace+"/notebooks/"+testNotebook+"/getDefinition" {
			t.Fatal("notebook lookup escaped its exact mock definition endpoint")
		}
		return mockDefinitionResponse(t, definitionForTest()), nil
	})
	parent := notebookLocationForTest()
	children, err := r.backend.ReadDir(r.ctx, parent.entry)
	if err != nil {
		t.Fatal(err)
	}
	fixed, err := fixedItemLocations(parent, children)
	if err != nil || len(fixed) != 3 || fixed["content.ipynb"].entry.Size != -1 || fixed["content.ipynb"].entry.Part != "" {
		t.Fatal("notebook root did not expose its exact lazy fixed descriptors")
	}
	coldIdentity := readBackendBytes(t, r, fixed[".fabric.json"].entry)
	var identity identityMetadata
	if json.Unmarshal(coldIdentity, &identity) != nil || identity.ID != testNotebook || identity.RemotePartPath != "" {
		t.Fatal("cold identity metadata guessed a remote part")
	}
	if _, err := r.backend.ReadDir(r.ctx, fixed["builtin"].entry); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("disabled builtin provider was reported as supported")
	}
	if totalCounts(r.counter.snapshot()) != 0 {
		t.Fatal("lazy root, identity, or disabled provider exported a definition")
	}
	content, _, err := notebookContentLocations(r.ctx, r.backend.Lookup, parent, children)
	if err != nil || content.entry.Part != testRemoteNotebookPart || content.entry.Size != int64(len(initialNotebook())) {
		t.Fatal("actual snapshot did not resolve the lazy body", safeError(err))
	}
	metadata, err := r.backend.Lookup(r.ctx, parent.entry, ".fabric.json")
	if err != nil {
		t.Fatal(err)
	}
	if data := readBackendBytes(t, r, metadata); json.Unmarshal(data, &identity) != nil || identity.RemotePartPath != testRemoteNotebookPart {
		t.Fatal("warm generated metadata did not reflect the resolved remote body")
	}
	if r.counter.snapshot()["fabric|POST|definition-read"] != 1 {
		t.Fatal("identity read started an additional definition export")
	}
}

func recordOfflineManagedResource(t *testing.T, r *runner, kind, id string) managedResource {
	t.Helper()
	folder := managedResource{ID: testOperation, Type: "Folder", DisplayName: r.doc.Plan.Managed.FolderName}
	folderRequest := managedCreateRequest(t, r.doc.Plan.Managed.FolderCreateURL, folder.DisplayName, "", "")
	if _, err := r.guard.authorize(folderRequest); err != nil {
		t.Fatal(err)
	}
	if r.guard.noteManagedCreation(folder) != nil || r.guard.confirmManagedIdentity(folder) != nil {
		t.Fatal("offline folder receipt setup failed")
	}
	spec, ok := r.guard.plannedItem(kind)
	if !ok {
		t.Fatal("unplanned offline resource")
	}
	if _, err := r.guard.authorize(managedCreateRequest(t, spec.CreateURL, spec.DisplayName, folder.ID, "")); err != nil {
		t.Fatal(err)
	}
	item := managedResource{ID: id, Type: kind, DisplayName: spec.DisplayName, FolderID: folder.ID}
	if r.guard.noteManagedCreation(item) != nil || r.guard.confirmManagedIdentity(item) != nil {
		t.Fatal("offline managed receipt setup failed")
	}
	return item
}

func TestEnvironmentFixedRootsAndOptionalPartsUseExactDefinitionLookup(t *testing.T) {
	r := newOfflineBackendRunner(t, func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/workspaces/"+testWorkspace+"/environments/"+testDeleteOp+"/getDefinition" {
			t.Fatal("Environment lookup escaped the exact owned mock definition endpoint")
		}
		def := definitionForTest()
		def.Format = ""
		def.Parts[0].Path = "Libraries/PublicLibraries/environment.yml"
		def.Parts[2].Path = "optional-definition.json"
		return mockDefinitionResponse(t, def), nil
	})
	receipt := recordOfflineManagedResource(t, r, "Environment", testDeleteOp)
	entry, err := r.managedEntry(receipt)
	if err != nil {
		t.Fatal(err)
	}
	parent := location{entry: entry, path: filepath.Join(r.doc.Mountpoint, entry.Name)}
	children, err := r.backend.ReadDir(r.ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	fixed, err := fixedItemLocations(parent, children)
	if err != nil || len(fixed) != 4 {
		t.Fatal("cold Environment root omitted a fixed category")
	}
	if _, err := r.backend.ReadDir(r.ctx, fixed["resources"].entry); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("disabled Environment resources were treated as a live provider")
	}
	libraries, err := r.backend.Lookup(r.ctx, entry, "Libraries")
	if err != nil {
		t.Fatal(err)
	}
	public, err := r.backend.Lookup(r.ctx, libraries, "PublicLibraries")
	if err != nil || totalCounts(r.counter.snapshot()) != 0 {
		t.Fatal("fixed Environment descriptor fetched a definition")
	}
	optional, err := r.backend.Lookup(r.ctx, public, "environment.yml")
	if err != nil || optional.Kind != workspacefs.DefinitionFile ||
		optional.Part != "Libraries/PublicLibraries/environment.yml" ||
		r.counter.snapshot()["fabric|POST|definition-read"] != 1 {
		t.Fatal("optional Environment file was not discovered through exact definition lookup")
	}
	children, err = r.backend.ReadDir(r.ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	fixed, err = fixedItemLocations(parent, children)
	if err != nil || len(fixed) != 5 || fixed["optional-definition.json"].entry.Kind != workspacefs.DefinitionFile {
		t.Fatal("cached optional definitions hid fixed roots or exposed .platform")
	}
}

func TestLakehouseRootIsFilesTablesAndGeneratedIdentity(t *testing.T) {
	r := newOfflineBackendRunner(t, nil)
	parent := location{
		entry: workspacefs.Entry{Kind: workspacefs.Lakehouse, Directory: true, Workspace: testWorkspace,
			Item: fabric.Item{ID: testLakehouse, Type: "Lakehouse", DisplayName: "Lake"}},
		path: filepath.Join(r.doc.Mountpoint, "Lake.Lakehouse"),
	}
	children, err := r.backend.ReadDir(r.ctx, parent.entry)
	if err != nil {
		t.Fatal(err)
	}
	fixed, err := fixedItemLocations(parent, children)
	if err != nil || len(fixed) != 3 || fixed["Files"].entry.Remote != "Files" || fixed["Tables"].entry.Remote != "Tables" ||
		fixed[".fabric.json"].entry.Kind != workspacefs.IdentityFile || totalCounts(r.counter.snapshot()) != 0 {
		t.Fatal("Lakehouse fixed roots required a provider or lost identity")
	}
}

func TestRetiredOverlayOptionsFailWithoutOpeningDataOrHTTP(t *testing.T) {
	r := newOfflineBackendRunner(t, nil)
	for _, configure := range []func(*workspacefs.Options){
		func(o *workspacefs.Options) {
			o.OverlayDirectory = filepath.Join(r.doc.Mountpoint, "unopened-old-overlay")
		},
		func(o *workspacefs.Options) { o.OverlayMaxFileSize = 1 },
		func(o *workspacefs.Options) { o.OverlayMaxBytes = 1 },
		func(o *workspacefs.Options) { o.OverlayMaxEntries = 1 },
	} {
		opts := workspacefs.DefaultOptions()
		opts.WorkspaceIDs, opts.ReadOnly = []string{testWorkspace}, true
		configure(&opts)
		backend, err := workspacefs.New(r.fab, r.lake, opts)
		if backend != nil {
			_ = backend.Close()
		}
		if backend != nil || !errors.Is(err, fserrors.ErrUnsupported) {
			t.Fatal("retired overlay configuration was not explicitly rejected")
		}
	}
	if totalCounts(r.counter.snapshot()) != 0 {
		t.Fatal("retired overlay options issued HTTP")
	}
}

func TestDotDirectoriesNeverCreateArbitraryLocalOverlays(t *testing.T) {
	r := newOfflineBackendRunner(t, func(req *http.Request) (*http.Response, error) {
		switch req.Method + " " + req.URL.Path {
		case "GET /v1/workspaces/" + testWorkspace + "/items":
			return response(200, `{"value":[]}`, nil), nil
		case "GET /v1/workspaces/" + testWorkspace + "/folders":
			return response(200, `{"value":[{"id":"`+testOperation+`","displayName":"Existing catalog folder"}]}`, nil), nil
		default:
			t.Fatal("dot mkdir attempted anything other than a catalog read")
			return nil, fail("unexpected offline request")
		}
	})
	for _, kind := range []workspacefs.Kind{
		workspacefs.Root, workspacefs.Workspace, workspacefs.FabricFolder,
		workspacefs.Notebook, workspacefs.Environment, workspacefs.Lakehouse,
	} {
		parent := workspacefs.Entry{Kind: kind, Directory: true, Workspace: testWorkspace}
		if kind == workspacefs.FabricFolder {
			parent.Folder = fabric.Folder{ID: testOperation, DisplayName: "Existing catalog folder"}
		}
		for _, name := range []string{".agents", ".other-dot-directory"} {
			_, err := r.backend.Mkdir(r.ctx, parent, name)
			if !errors.Is(err, fs.ErrInvalid) && !errors.Is(err, fserrors.ErrReadOnly) {
				t.Fatal("dot mkdir was not rejected locally")
			}
			if kind == workspacefs.Workspace || kind == workspacefs.FabricFolder {
				if _, err := r.backend.Lookup(r.ctx, parent, name); !errors.Is(err, fs.ErrNotExist) {
					t.Fatal("rejected dot mkdir exposed a local overlay")
				}
			}
		}
	}
	if _, denied := r.guard.state(); denied != 0 || mutationCount(r.counter.snapshot()) != 0 {
		t.Fatal("arbitrary dot-directory creation reached an HTTP mutation or scope denial")
	}
}

func TestFreshDefinitionVerificationBypassesWarmSnapshots(t *testing.T) {
	calls := 0
	r := newOfflineBackendRunner(t, func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodPost || !strings.HasSuffix(req.URL.Path, "/getDefinition") {
			t.Fatal("definition verification attempted a mutation")
		}
		if calls < 3 {
			return mockDefinitionResponse(t, definitionForTest()), nil
		}
		return mockDefinitionResponse(t, savedDefinitionForTest(t, "saved-marker")), nil
	})
	parent := notebookLocationForTest()
	children, err := r.backend.ReadDir(r.ctx, parent.entry)
	if err != nil {
		t.Fatal(err)
	}
	content, _, err := notebookContentLocations(r.ctx, r.backend.Lookup, parent, children)
	if err != nil {
		t.Fatal(err)
	}
	before, err := r.freshNotebookDefinition(content, initialNotebook())
	if err != nil || calls != 2 {
		t.Fatal("fresh definition comparison reused the cached body snapshot")
	}
	if err := r.verifySavedNotebookDefinition(content, before, "saved-marker"); err != nil || calls != 3 {
		t.Fatal("saved definition verification was not a single fresh read")
	}
	if mutationCount(r.counter.snapshot()) != 0 {
		t.Fatal("definition verification mutated the mock service")
	}
}

func TestSmokeKeepsShortTTLInsteadOfChangingProductDefault(t *testing.T) {
	if workspacefs.DefaultOptions().CacheTTL != 2*time.Minute || cacheTTL <= 0 || cacheTTL >= 2*time.Minute {
		t.Fatal("smoke override no longer distinguishes the two-minute product default")
	}
}

func TestDefinitionVerificationRequiresResolvedBodyBeforeHTTP(t *testing.T) {
	r := newOfflineBackendRunner(t, nil)
	parent := notebookLocationForTest()
	content := location{entry: notebookChildrenForTest(parent)[0]}
	if _, err := r.freshNotebookDefinition(content, initialNotebook()); err == nil {
		t.Fatal("unresolved listing descriptor was accepted as a definition body")
	}
	if err := r.verifySavedNotebookDefinition(content, definitionForTest(), "marker"); err == nil {
		t.Fatal("unresolved descriptor initiated saved-definition verification")
	}
	if totalCounts(r.counter.snapshot()) != 0 {
		t.Fatal("unresolved descriptor initiated HTTP")
	}
}

func TestSavedDefinitionMismatchIsNotRetried(t *testing.T) {
	calls := 0
	r := newOfflineBackendRunner(t, func(req *http.Request) (*http.Response, error) {
		calls++
		return mockDefinitionResponse(t, definitionForTest()), nil
	})
	content := location{entry: notebookChildrenForTest(notebookLocationForTest())[0]}
	content.entry.Part, content.entry.Size = testRemoteNotebookPart, int64(len(initialNotebook()))
	if err := r.verifySavedNotebookDefinition(content, definitionForTest(), "missing-marker"); err == nil || calls != 1 {
		t.Fatal("failed save verification was retried or incorrectly reported as successful")
	}
}
