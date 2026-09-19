package testutil

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"fabric-workspace-fs/internal/fabric"
)

type managementState struct {
	nextID uint64
	items  map[string]*managedItem
}

type managedItem struct {
	kind       string
	definition fabric.Definition
	deleted    bool
}

// serveManagement runs under serve's mutex and authentication check. Existing
// fixed-ID definition routes deliberately fall through to their original fault
// injection and regional-operation handlers.
func (s *Service) serveManagement(w http.ResponseWriter, r *http.Request) bool {
	path, ok := strings.CutPrefix(r.URL.Path, "/v1/workspaces/"+WorkspaceID+"/")
	if !ok {
		return false
	}
	segments := strings.Split(path, "/")
	collection := segments[0]
	if len(segments) == 1 && r.Method == http.MethodPost {
		switch collection {
		case "notebooks", "lakehouses", "environments":
			s.createManagedItem(w, r, collection)
			return true
		case "folders":
			s.createManagedFolder(w, r)
			return true
		}
	}
	if len(segments) == 2 && (collection == "items" || collection == "folders") {
		id := segments[1]
		if fabric.ValidateID(id) != nil {
			s.fail(w, http.StatusBadRequest, "InvalidItemId")
			return true
		}
		switch r.Method {
		case http.MethodGet:
			s.counts.CatalogReads++
			if collection == "items" {
				if i := s.itemIndex(id); i >= 0 {
					s.managedItemResponse(w, http.StatusOK, s.items[i])
				} else {
					s.fail(w, http.StatusNotFound, "ItemNotFound")
				}
			} else if i := s.folderIndex(id); i >= 0 {
				s.managedFolderResponse(w, http.StatusOK, s.folders[i])
			} else {
				s.fail(w, http.StatusNotFound, "FolderNotFound")
			}
			return true
		case http.MethodDelete:
			if r.URL.RawQuery != "" {
				s.fail(w, http.StatusBadRequest, "UnexpectedDeleteQuery")
			} else if collection == "items" {
				s.deleteManagedItem(w, id)
			} else {
				s.deleteManagedFolder(w, id)
			}
			return true
		}
	}
	if len(segments) == 3 && (collection == "notebooks" || collection == "environments") &&
		(segments[2] == "getDefinition" || segments[2] == "updateDefinition") &&
		segments[1] != NotebookID && segments[1] != EnvironmentID {
		s.serveManagedDefinition(w, r, collection, segments[1], segments[2])
		return true
	}
	return false
}

func (s *Service) nextManagementID() string {
	if s.management == nil {
		s.management = &managementState{items: make(map[string]*managedItem)}
	}
	for {
		s.management.nextID++
		id := fmt.Sprintf("90000000-0000-0000-0000-%012x", s.management.nextID)
		if s.itemIndex(id) < 0 && s.folderIndex(id) < 0 {
			return id
		}
	}
}

func (s *Service) itemIndex(id string) int {
	for i := range s.items {
		if strings.EqualFold(s.items[i].ID, id) {
			return i
		}
	}
	return -1
}

func (s *Service) folderIndex(id string) int {
	for i := range s.folders {
		if strings.EqualFold(s.folders[i].ID, id) {
			return i
		}
	}
	return -1
}

func (s *Service) managedItem(id string) *managedItem {
	if s.management == nil {
		return nil
	}
	return s.management.items[strings.ToLower(id)]
}

func decodeManagementBody(r *http.Request, target any) bool {
	const limit = 32 << 20
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	return err == nil && len(body) <= limit && utf8.Valid(body) && json.Unmarshal(body, target) == nil
}

func managementName(name string) bool {
	if !utf8.ValidString(name) || strings.TrimSpace(name) == "" || name == "." || name == ".." {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (s *Service) createManagedItem(w http.ResponseWriter, r *http.Request, collection string) {
	var body struct {
		DisplayName string             `json:"displayName"`
		FolderID    string             `json:"folderId"`
		Definition  *fabric.Definition `json:"definition"`
	}
	if !decodeManagementBody(r, &body) || !managementName(body.DisplayName) {
		s.fail(w, http.StatusBadRequest, "InvalidDefinition")
		return
	}
	if body.FolderID != "" && (fabric.ValidateID(body.FolderID) != nil || s.folderIndex(body.FolderID) < 0) {
		s.fail(w, http.StatusNotFound, "FolderNotFound")
		return
	}
	kind := map[string]string{"notebooks": "Notebook", "lakehouses": "Lakehouse", "environments": "Environment"}[collection]
	if kind == "Notebook" && (body.Definition == nil || !managementNotebook(*body.Definition)) {
		s.fail(w, http.StatusBadRequest, "CorruptedPayload")
		return
	}
	for _, item := range s.items {
		if item.Type == kind && item.DisplayName == body.DisplayName {
			s.fail(w, http.StatusConflict, "ItemDisplayNameAlreadyInUse")
			return
		}
	}
	item := fabric.Item{ID: s.nextManagementID(), DisplayName: body.DisplayName, Type: kind, FolderID: body.FolderID}
	managed := &managedItem{kind: kind}
	if body.Definition != nil {
		managed.definition = cloneDefinition(*body.Definition)
	}
	if kind == "Environment" && body.Definition == nil {
		// A deterministic fixture snapshot, not a promise that an unpublished
		// production environment exports immediately. Public definition parts:
		// https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/environment-definition
		managed.definition.Parts = []fabric.Part{part("Setting/Sparkcompute.yml",
			"enable_native_execution_engine: false\ninstance_pool_id: null\ndriver_cores: 4\ndriver_memory: 28g\n"+
				"executor_cores: 4\nexecutor_memory: 28g\ndynamic_executor_allocation:\n  enabled: false\n"+
				"  min_executors: 1\n  max_executors: 1\nspark_conf: {}\nruntime_version: '1.3'\n")}
	}
	if kind != "Lakehouse" {
		hasPlatform := false
		for _, part := range managed.definition.Parts {
			hasPlatform = hasPlatform || part.Path == ".platform"
		}
		if !hasPlatform {
			managed.definition.Parts = append(managed.definition.Parts, managementPlatform(item))
		}
	}
	s.items = append(s.items, item)
	s.management.items[item.ID] = managed
	s.counts.ManagedCreates++
	s.managedItemResponse(w, http.StatusCreated, item)
}

func managementPlatform(item fabric.Item) fabric.Part {
	body, _ := json.Marshal(map[string]any{
		"$schema":  "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
		"metadata": map[string]string{"type": item.Type, "displayName": item.DisplayName},
		"config":   map[string]string{"version": "2.0", "logicalId": item.ID},
	})
	return part(".platform", string(body))
}

func managementNotebook(def fabric.Definition) bool {
	if def.Format != "ipynb" {
		return false
	}
	contentParts := 0
	for _, part := range def.Parts {
		if !strings.HasSuffix(part.Path, ".ipynb") {
			continue
		}
		contentParts++
		content, err := part.Decode()
		var notebook struct {
			Format   int                        `json:"nbformat"`
			Minor    *int                       `json:"nbformat_minor"`
			Cells    []json.RawMessage          `json:"cells"`
			Metadata map[string]json.RawMessage `json:"metadata"`
		}
		if err != nil || json.Unmarshal(content, &notebook) != nil || notebook.Format != 4 ||
			notebook.Minor == nil || *notebook.Minor < 0 || notebook.Cells == nil || notebook.Metadata == nil {
			return false
		}
	}
	return contentParts == 1
}

func (s *Service) createManagedFolder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DisplayName    string `json:"displayName"`
		ParentFolderID string `json:"parentFolderId"`
	}
	if !decodeManagementBody(r, &body) || !managementName(body.DisplayName) {
		s.fail(w, http.StatusBadRequest, "InvalidFolderDisplayName")
		return
	}
	if body.ParentFolderID != "" && (fabric.ValidateID(body.ParentFolderID) != nil || s.folderIndex(body.ParentFolderID) < 0) {
		s.fail(w, http.StatusNotFound, "FolderNotFound")
		return
	}
	for _, folder := range s.folders {
		if strings.EqualFold(folder.ParentFolderID, body.ParentFolderID) && folder.DisplayName == body.DisplayName {
			s.fail(w, http.StatusConflict, "FolderDisplayNameAlreadyInUse")
			return
		}
	}
	folder := fabric.Folder{ID: s.nextManagementID(), DisplayName: body.DisplayName, ParentFolderID: body.ParentFolderID}
	s.folders = append(s.folders, folder)
	s.counts.ManagedCreates++
	s.managedFolderResponse(w, http.StatusCreated, folder)
}

func (s *Service) managedItemResponse(w http.ResponseWriter, status int, item fabric.Item) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		fabric.Item
		WorkspaceID string `json:"workspaceId"`
	}{Item: item, WorkspaceID: WorkspaceID})
}

func (s *Service) managedFolderResponse(w http.ResponseWriter, status int, folder fabric.Folder) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		fabric.Folder
		WorkspaceID string `json:"workspaceId"`
	}{Folder: folder, WorkspaceID: WorkspaceID})
}

func (s *Service) deleteManagedItem(w http.ResponseWriter, id string) {
	i := s.itemIndex(id)
	if i < 0 {
		s.fail(w, http.StatusNotFound, "ItemNotFound")
		return
	}
	managed := s.managedItem(id)
	if managed == nil || managed.deleted {
		s.fail(w, http.StatusForbidden, "FixtureItemNotOwned")
		return
	}
	s.items = append(s.items[:i], s.items[i+1:]...)
	// Retain the ID tombstone so subsequent definition/storage requests return
	// 404 instead of falling into the fixed-ID fixture's unexpected-route checks.
	managed.deleted = true
	managed.definition = fabric.Definition{}
	s.counts.ManagedDeletes++
	w.WriteHeader(http.StatusOK)
}

func (s *Service) deleteManagedFolder(w http.ResponseWriter, id string) {
	i := s.folderIndex(id)
	if i < 0 {
		s.fail(w, http.StatusNotFound, "FolderNotFound")
		return
	}
	// The public Folder Delete contract rejects both items and nested folders,
	// including item types that the mount does not expose.
	for _, item := range s.items {
		if strings.EqualFold(item.FolderID, id) {
			s.fail(w, http.StatusConflict, "FolderNotEmpty")
			return
		}
	}
	for _, folder := range s.folders {
		if strings.EqualFold(folder.ParentFolderID, id) {
			s.fail(w, http.StatusConflict, "FolderNotEmpty")
			return
		}
	}
	s.folders = append(s.folders[:i], s.folders[i+1:]...)
	s.counts.ManagedDeletes++
	w.WriteHeader(http.StatusOK)
}

func (s *Service) serveManagedDefinition(w http.ResponseWriter, r *http.Request, collection, id, action string) {
	if r.Method == http.MethodPost && action == "getDefinition" {
		s.counts.DefinitionReads++
	}
	managed := s.managedItem(id)
	expected := map[string]string{"notebooks": "Notebook", "environments": "Environment"}[collection]
	if managed == nil || managed.deleted || managed.kind != expected || s.itemIndex(id) < 0 {
		s.fail(w, http.StatusNotFound, "ItemNotFound")
		return
	}
	if r.Method != http.MethodPost {
		s.fail(w, http.StatusMethodNotAllowed, "UnsupportedMethod")
		return
	}
	if action == "getDefinition" {
		if expected == "Notebook" && r.URL.Query().Get("format") != "ipynb" {
			s.fail(w, http.StatusBadRequest, "InvalidFormat")
			return
		}
		s.json(w, map[string]any{"definition": managed.definition})
		return
	}
	if expected == "Notebook" {
		s.counts.NotebookAttempts++
	}
	var body struct {
		Definition *fabric.Definition `json:"definition"`
	}
	if !decodeManagementBody(r, &body) || body.Definition == nil ||
		(expected == "Notebook" && !managementNotebook(*body.Definition)) {
		s.fail(w, http.StatusBadRequest, "InvalidDefinition")
		return
	}
	if r.URL.Query().Get("updateMetadata") == "true" {
		s.fail(w, http.StatusBadRequest, "MetadataWriteForbidden")
		return
	}
	managed.definition = cloneDefinition(*body.Definition)
	if expected == "Notebook" {
		s.counts.NotebookUpdates++
	}
	w.WriteHeader(http.StatusOK)
}

// New lakehouses expose only empty Files/Tables roots. Never repoint objects:
// that map belongs exclusively to the original full-CRUD Lakehouse fixture.
func (s *Service) serveManagedLake(w http.ResponseWriter, r *http.Request) bool {
	list := r.Method == http.MethodGet && r.URL.Path == "/"+WorkspaceID && r.URL.Query().Get("resource") == "filesystem"
	var id, relative string
	if list {
		id, relative, _ = strings.Cut(r.URL.Query().Get("directory"), "/")
	} else {
		path, ok := strings.CutPrefix(r.URL.Path, "/"+WorkspaceID+"/")
		if !ok {
			return false
		}
		id, relative, _ = strings.Cut(path, "/")
	}
	managed := s.managedItem(id)
	if managed == nil || managed.kind != "Lakehouse" {
		return false
	}
	switch {
	case list:
		s.counts.StorageLists++
	case r.Method == http.MethodHead:
		s.counts.StorageStats++
	case r.Method == http.MethodGet:
		s.counts.StorageReads++
	}
	if managed.deleted || s.itemIndex(id) < 0 {
		s.fail(w, http.StatusNotFound, "PathNotFound")
		return true
	}
	if relative == "Tables" || strings.HasPrefix(relative, "Tables/") {
		s.counts.TablesRequests++
		if s.denyTables {
			s.fail(w, http.StatusForbidden, "AuthorizationPermissionMismatch")
			return true
		}
	}
	if r.Method != http.MethodHead && r.Method != http.MethodGet {
		s.fail(w, http.StatusForbidden, "ManagedLakehouseReadOnly")
		return true
	}
	if relative != "Files" && relative != "Tables" {
		s.fail(w, http.StatusNotFound, "PathNotFound")
		return true
	}
	if list {
		if r.URL.Query().Get("recursive") != "false" {
			s.fail(w, http.StatusBadRequest, "InvalidDirectory")
		} else {
			s.json(w, map[string]any{"paths": []any{}})
		}
		return true
	}
	if r.Method != http.MethodHead {
		s.fail(w, http.StatusNotFound, "PathNotFound")
		return true
	}
	root := object{dir: true, etag: fmt.Sprintf(`"managed-%s-%s"`, strings.ToLower(id), relative)}
	if s.conditions(w, r, root, true) {
		s.properties(w, root)
		w.WriteHeader(http.StatusOK)
	}
	return true
}
