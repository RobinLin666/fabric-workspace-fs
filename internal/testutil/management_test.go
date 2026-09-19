package testutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
)

func requireManagementStatus(t *testing.T, err error, status int, code string) {
	t.Helper()
	var actual *transport.HTTPError
	if !errors.As(err, &actual) || actual.StatusCode != status || actual.Code != code {
		t.Fatalf("unexpected management error: status=%d expected-code=%s err=%v", status, code, err)
	}
}

func managementTestTransport(t *testing.T, service *Service, scope string) *transport.Client {
	t.Helper()
	client, err := transport.New(transport.Options{
		BaseURL: service.Server.URL, Scope: scope, Tokens: tokens{}, HTTPClient: service.Server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func managementTestBody(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal("unable to encode management fixture request")
	}
	return body
}

func managementDefinitionsEqual(t *testing.T, a, b fabric.Definition) bool {
	t.Helper()
	return bytes.Equal(managementTestBody(t, a), managementTestBody(t, b))
}

func TestManagementCreateListReadDeleteLifecycle(t *testing.T) {
	service := New(t)
	fab, lake := service.Clients()
	ctx := context.Background()
	if service.Counts() != (Counts{}) {
		t.Fatal("management changed initial fixture counts")
	}
	parent, err := fab.CreateFolder(ctx, WorkspaceID, "ordinary parent", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := fab.CreateFolder(ctx, WorkspaceID, "ordinary child", parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, folder := range []fabric.Folder{parent, child} {
		got, err := fab.GetFolder(ctx, WorkspaceID, folder.ID)
		if err != nil || got != folder {
			t.Fatal("created folder identity was not retained", err)
		}
	}
	seen := map[string]bool{parent.ID: true, child.ID: true}
	var created []fabric.Item
	for _, kind := range []string{"Notebook", "Lakehouse", "Environment"} {
		item, err := fab.CreateItem(ctx, WorkspaceID, kind, "Managed_"+kind, child.ID)
		if err != nil {
			t.Fatal(err)
		}
		if fabric.ValidateID(item.ID) != nil || seen[item.ID] || item.Type != kind || item.FolderID != child.ID {
			t.Fatal("management allocated an invalid, duplicate, or incorrectly parented identity")
		}
		seen[item.ID] = true
		got, err := fab.GetItem(ctx, WorkspaceID, item.ID)
		if err != nil || got != item {
			t.Fatal("created item identity was not retained", err)
		}
		created = append(created, item)
		switch kind {
		case "Notebook", "Environment":
			definition, err := fab.GetDefinition(ctx, WorkspaceID, item.ID, kind, "")
			if err != nil {
				t.Fatal("created item has no readable definition", err)
			}
			var paths []string
			for _, part := range definition.Parts {
				paths = append(paths, part.Path)
				content, err := part.Decode()
				if err != nil {
					t.Fatal("managed definition part is not valid base64", err)
				}
				if part.Path == ".platform" {
					var platform struct {
						Metadata struct{ Type, DisplayName string }
						Config   struct{ LogicalID string }
					}
					if json.Unmarshal(content, &platform) != nil || platform.Metadata.Type != kind ||
						platform.Metadata.DisplayName != item.DisplayName || platform.Config.LogicalID != item.ID {
						t.Fatal("generated platform metadata does not identify the created item")
					}
				}
			}
			if len(paths) != 2 {
				t.Fatal("default definition fabricated user content or omitted metadata")
			}
			if kind == "Notebook" && (!managementNotebook(definition) || paths[0] != "notebook-content.ipynb") {
				t.Fatal("notebook create did not retain its initial valid ipynb definition")
			}
			if kind == "Environment" && (paths[0] != "Setting/Sparkcompute.yml" || paths[1] != ".platform") {
				t.Fatal("default environment did not expose its compute and platform snapshot")
			}
		case "Lakehouse":
			for _, root := range []string{"Files", "Tables"} {
				path := onelake.Path{Workspace: WorkspaceID, Item: item.ID, Relative: root}
				info, err := lake.Stat(ctx, path)
				if err != nil || !info.IsDir || info.Size != 0 || info.ETag == "" || info.ModTime.IsZero() {
					t.Fatal("new lakehouse root lacks real directory metadata", err)
				}
				entries, err := lake.List(ctx, path)
				if err != nil || entries == nil || len(entries) != 0 {
					t.Fatal("new lakehouse root is not an explicitly empty list", err)
				}
			}
		}
	}
	items, err := fab.ListItems(ctx, WorkspaceID)
	if err != nil || len(items) != 7 {
		t.Fatal("new items were not included in catalog discovery", err)
	}
	folders, err := fab.ListFolders(ctx, WorkspaceID)
	if err != nil || len(folders) != 2 || folders[1].ParentFolderID != parent.ID {
		t.Fatal("new nested folders were not included in catalog discovery", err)
	}
	requireManagementStatus(t, fab.DeleteFolder(ctx, WorkspaceID, parent.ID), http.StatusConflict, "FolderNotEmpty")
	requireManagementStatus(t, fab.DeleteFolder(ctx, WorkspaceID, child.ID), http.StatusConflict, "FolderNotEmpty")
	for _, item := range created {
		if err := fab.DeleteItem(ctx, WorkspaceID, item.ID); err != nil {
			t.Fatal(err)
		}
		_, err := fab.GetItem(ctx, WorkspaceID, item.ID)
		requireManagementStatus(t, err, http.StatusNotFound, "ItemNotFound")
		if item.Type != "Lakehouse" {
			_, err = fab.GetDefinition(ctx, WorkspaceID, item.ID, item.Type, "")
			requireManagementStatus(t, err, http.StatusNotFound, "ItemNotFound")
		} else {
			path := onelake.Path{Workspace: WorkspaceID, Item: item.ID, Relative: "Files"}
			_, err = lake.Stat(ctx, path)
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("deleted lakehouse still exposes Files metadata", err)
			}
			if _, err = lake.List(ctx, path); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("deleted lakehouse still exposes a Files listing", err)
			}
		}
	}
	if err := fab.DeleteFolder(ctx, WorkspaceID, child.ID); err != nil {
		t.Fatal(err)
	}
	if err := fab.DeleteFolder(ctx, WorkspaceID, parent.ID); err != nil {
		t.Fatal(err)
	}
	_, err = fab.GetFolder(ctx, WorkspaceID, child.ID)
	requireManagementStatus(t, err, http.StatusNotFound, "FolderNotFound")
	items, err = fab.ListItems(ctx, WorkspaceID)
	if err != nil || len(items) != 4 {
		t.Fatal("management deletion removed original fixture items or retained new items", err)
	}
	folders, err = fab.ListFolders(ctx, WorkspaceID)
	if err != nil || folders == nil || len(folders) != 0 {
		t.Fatal("deleted folders are still listed", err)
	}
	counts := service.Counts()
	if counts.ManagedCreates != 5 || counts.ManagedDeletes != 5 || counts.DefinitionReads != 4 ||
		counts.StorageStats != 3 || counts.StorageLists != 3 || counts.CatalogReads == 0 {
		t.Fatal("management operations were not counted correctly")
	}
}

func TestManagementNotebookDefinitionsPreserveFieldsAndOriginalFailures(t *testing.T) {
	service := New(t)
	fab, _ := service.Clients()
	ctx := context.Background()
	original := service.Definition()
	service.FailNotebookUpdates(1)
	service.UseRegionalDefinitionPolling()
	item, err := fab.CreateItem(ctx, WorkspaceID, "Notebook", "New notebook", "")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := fab.GetDefinition(ctx, WorkspaceID, item.ID, "Notebook", "")
	if err != nil {
		t.Fatal(err)
	}
	definition.Extra["futureEnvelope"] = json.RawMessage(`{"preserve":true}`)
	for i := range definition.Parts {
		if definition.Parts[i].Path == "notebook-content.ipynb" {
			definition.Parts[i].SetData([]byte(`{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"tag":"updated"}}`))
			definition.Parts[i].Extra["futureContent"] = json.RawMessage(`{"preserve":[1,2,3]}`)
		}
	}
	definition.Parts = append(definition.Parts, part("Extras/opaque.bin", "preserve dynamic unknown bytes"))
	if err := fab.UpdateNotebook(ctx, WorkspaceID, item.ID, definition); err != nil {
		t.Fatal("dynamic notebook consumed the original notebook's injected failure", err)
	}
	got, err := fab.GetDefinition(ctx, WorkspaceID, item.ID, "Notebook", "")
	if err != nil || !managementDefinitionsEqual(t, got, definition) {
		t.Fatal("dynamic notebook definition was not preserved deeply", err)
	}
	if !reflect.DeepEqual(service.Definition(), original) {
		t.Fatal("dynamic update changed the original notebook definition")
	}
	err = fab.UpdateNotebook(ctx, WorkspaceID, NotebookID, original)
	requireManagementStatus(t, err, http.StatusInternalServerError, "InjectedWriteFailure")
	if !reflect.DeepEqual(service.Definition(), original) {
		t.Fatal("original injected update failure no longer preserves its definition")
	}
	originalRead, err := fab.GetDefinition(ctx, WorkspaceID, NotebookID, "Notebook", "")
	original.Format = "ipynb"
	if err != nil || !managementDefinitionsEqual(t, originalRead, original) {
		t.Fatal("management interception broke the original regional definition flow", err)
	}
	counts := service.Counts()
	if counts.NotebookAttempts != 2 || counts.NotebookUpdates != 1 ||
		counts.ManagedCreates != 1 || counts.ManagedDeletes != 0 {
		t.Fatal("dynamic and original notebook counters interfered")
	}
}

func TestManagementEnvironmentDefinitionUpdate(t *testing.T) {
	service := New(t)
	fab, _ := service.Clients()
	ctx := context.Background()
	original, err := fab.GetDefinition(ctx, WorkspaceID, EnvironmentID, "Environment", "")
	if err != nil {
		t.Fatal(err)
	}
	item, err := fab.CreateItem(ctx, WorkspaceID, "Environment", "New environment", "")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := fab.GetDefinition(ctx, WorkspaceID, item.ID, "Environment", "")
	if err != nil {
		t.Fatal(err)
	}
	definition.Extra["futureEnvelope"] = json.RawMessage(`{"version":1}`)
	definition.Parts = append(definition.Parts, part("Libraries/CustomLibraries/probe.py", "# offline library\n"))
	client := managementTestTransport(t, service, "https://api.fabric.microsoft.com/.default")
	body := managementTestBody(t, map[string]any{"definition": definition})
	response, err := client.Request(ctx, http.MethodPost,
		client.URL("/v1/workspaces/"+WorkspaceID+"/environments/"+item.ID+"/updateDefinition", nil),
		http.Header{"Content-Type": {"application/json"}}, body, false)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("environment update returned the wrong status")
	}
	got, err := fab.GetDefinition(ctx, WorkspaceID, item.ID, "Environment", "")
	if err != nil || !managementDefinitionsEqual(t, got, definition) {
		t.Fatal("dynamic environment update discarded definition parts or fields", err)
	}
	unchanged, err := fab.GetDefinition(ctx, WorkspaceID, EnvironmentID, "Environment", "")
	if err != nil || !managementDefinitionsEqual(t, unchanged, original) {
		t.Fatal("dynamic environment update changed the original environment", err)
	}
}

func TestManagementEmptyFolderIncludesUnsupportedItemsAndSeededParents(t *testing.T) {
	const seededID = "77777777-7777-7777-7777-777777777777"
	const unsupportedID = "55555555-5555-5555-5555-555555555555"
	service := New(t)
	service.SetFolders([]fabric.Folder{{ID: seededID, DisplayName: "seeded parent"}})
	fab, _ := service.Clients()
	ctx := context.Background()
	child, err := fab.CreateFolder(ctx, WorkspaceID, "managed child", seededID)
	if err != nil {
		t.Fatal("existing SetFolders state is unavailable to management", err)
	}
	service.SetItemFolder(unsupportedID, child.ID)
	requireManagementStatus(t, fab.DeleteFolder(ctx, WorkspaceID, child.ID), http.StatusConflict, "FolderNotEmpty")
	requireManagementStatus(t, fab.DeleteFolder(ctx, WorkspaceID, seededID), http.StatusConflict, "FolderNotEmpty")
	got, err := fab.GetItem(ctx, WorkspaceID, unsupportedID)
	if err != nil || got.Type != "Warehouse" || got.FolderID != child.ID {
		t.Fatal("management changed SetItemFolder or hid an unsupported item", err)
	}
	service.SetItemFolder(unsupportedID, "")
	if err := fab.DeleteFolder(ctx, WorkspaceID, child.ID); err != nil {
		t.Fatal("empty child folder could not be removed", err)
	}
	if service.Counts().ManagedDeletes != 1 {
		t.Fatal("rejected nonempty-folder deletes were counted as successful")
	}
}

func TestManagementIDsDoNotCollideOrRecycle(t *testing.T) {
	const occupied = "90000000-0000-0000-0000-000000000001"
	service := New(t)
	service.SetFolders([]fabric.Folder{{ID: occupied, DisplayName: "seeded identity"}})
	fab, _ := service.Clients()
	ctx := context.Background()
	first, err := fab.CreateFolder(ctx, WorkspaceID, "first", "")
	if err != nil || first.ID != "90000000-0000-0000-0000-000000000002" {
		t.Fatal("deterministic allocation did not skip an occupied UUID", err)
	}
	item, err := fab.CreateItem(ctx, WorkspaceID, "Notebook", "managed", "")
	if err != nil || item.ID != "90000000-0000-0000-0000-000000000003" {
		t.Fatal("item and folder allocation did not share a unique UUID sequence", err)
	}
	if err := fab.DeleteFolder(ctx, WorkspaceID, first.ID); err != nil {
		t.Fatal(err)
	}
	next, err := fab.CreateFolder(ctx, WorkspaceID, "first", "")
	if err != nil || next.ID != "90000000-0000-0000-0000-000000000004" {
		t.Fatal("a deleted immutable identity was reused", err)
	}
}

func TestManagementNewLakehousesDoNotShareOriginalStorage(t *testing.T) {
	service := New(t)
	service.SetFile("Files/original.txt", []byte("original lakehouse only"))
	fab, lake := service.Clients()
	ctx := context.Background()
	newLakehouse, err := fab.CreateItem(ctx, WorkspaceID, "Lakehouse", "New_lakehouse", "")
	if err != nil {
		t.Fatal(err)
	}
	path := onelake.Path{Workspace: WorkspaceID, Item: newLakehouse.ID, Relative: "Files"}
	entries, err := lake.List(ctx, path)
	if err != nil || len(entries) != 0 {
		t.Fatal("new lakehouse inherited the original lakehouse's files", err)
	}
	path.Relative = "Files/original.txt"
	if _, err := lake.Stat(ctx, path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("new lakehouse exposed an original file", err)
	}
	client := managementTestTransport(t, service, onelake.Scope)
	_, err = client.Request(ctx, http.MethodPut,
		client.URL("/"+WorkspaceID+"/"+newLakehouse.ID+"/Files/not-supported.txt", nil),
		nil, nil, false)
	requireManagementStatus(t, err, http.StatusForbidden, "ManagedLakehouseReadOnly")
	if err := fab.DeleteItem(ctx, WorkspaceID, newLakehouse.ID); err != nil {
		t.Fatal(err)
	}
	path.Item = LakehouseID
	info, err := lake.Stat(ctx, path)
	if err != nil || info.Size != int64(len("original lakehouse only")) {
		t.Fatal("management corrupted original lakehouse metadata", err)
	}
	data := make([]byte, info.Size)
	n, err := lake.Read(ctx, path, 0, data, info.ETag)
	if (err != nil && !errors.Is(err, io.EOF)) || string(data[:n]) != "original lakehouse only" {
		t.Fatal("management corrupted original lakehouse data", err)
	}
	service.DenyTables(true)
	other, err := fab.CreateItem(ctx, WorkspaceID, "Lakehouse", "Other_lakehouse", "")
	if err != nil {
		t.Fatal(err)
	}
	path = onelake.Path{Workspace: WorkspaceID, Item: other.ID, Relative: "Tables"}
	if _, err := lake.List(ctx, path); !errors.Is(err, fs.ErrPermission) {
		t.Fatal("new lakehouse listing bypassed the Tables-denial fixture", err)
	}
	if _, err := lake.Stat(ctx, path); !errors.Is(err, fs.ErrPermission) {
		t.Fatal("new lakehouse stat bypassed the Tables-denial fixture", err)
	}
}

func TestManagementCreateFailuresHaveNoSideEffects(t *testing.T) {
	service := New(t)
	fab, _ := service.Clients()
	ctx := context.Background()
	client := managementTestTransport(t, service, "https://api.fabric.microsoft.com/.default")
	endpoint := client.URL("/v1/workspaces/"+WorkspaceID+"/notebooks", nil)
	for _, definition := range []*fabric.Definition{
		nil,
		{Format: "fabricGitSource", Parts: []fabric.Part{part("notebook-content.ipynb", InitialNotebook)}},
		{Format: "ipynb", Parts: []fabric.Part{part("notebook-content.ipynb", `{}`)}},
		{Format: "ipynb", Parts: []fabric.Part{part("notebook-content.py", InitialNotebook)}},
		{Format: "ipynb", Parts: []fabric.Part{{Path: "notebook-content.ipynb", PayloadType: "InlineBase64", Payload: "not-base64"}}},
	} {
		body := managementTestBody(t, map[string]any{"displayName": "invalid probe", "definition": definition})
		_, err := client.Request(ctx, http.MethodPost, endpoint,
			http.Header{"Content-Type": {"application/json"}}, body, false)
		requireManagementStatus(t, err, http.StatusBadRequest, "CorruptedPayload")
	}
	const absentFolder = "88888888-8888-8888-8888-888888888888"
	_, err := fab.CreateItem(ctx, WorkspaceID, "Notebook", "no parent", absentFolder)
	requireManagementStatus(t, err, http.StatusNotFound, "FolderNotFound")
	_, err = fab.CreateFolder(ctx, WorkspaceID, "no parent", absentFolder)
	requireManagementStatus(t, err, http.StatusNotFound, "FolderNotFound")
	_, err = fab.CreateItem(ctx, WorkspaceID, "Notebook", "Sample notebook", "")
	requireManagementStatus(t, err, http.StatusConflict, "ItemDisplayNameAlreadyInUse")
	requireManagementStatus(t, fab.DeleteItem(ctx, WorkspaceID, NotebookID), http.StatusForbidden, "FixtureItemNotOwned")
	items, err := fab.ListItems(ctx, WorkspaceID)
	if err != nil || len(items) != 4 || service.Counts().ManagedCreates != 0 || service.Counts().ManagedDeletes != 0 {
		t.Fatal("failed management operations changed fixture state", err)
	}
	for _, item := range items {
		if strings.HasPrefix(item.ID, "90000000-") {
			t.Fatal("rejected create left a phantom item")
		}
	}
}
