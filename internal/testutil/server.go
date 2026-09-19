// Package testutil provides an offline Fabric/ADLS contract fixture. It is not
// imported by the executable and never contacts Microsoft services.
package testutil

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
)

const (
	WorkspaceID     = "11111111-1111-1111-1111-111111111111"
	NotebookID      = "22222222-2222-2222-2222-222222222222"
	LakehouseID     = "33333333-3333-3333-3333-333333333333"
	EnvironmentID   = "44444444-4444-4444-4444-444444444444"
	InitialNotebook = `{"cells":[],"metadata":{"tag":"initial"},"nbformat":4,"nbformat_minor":5}`
)

type object struct {
	data       []byte
	dir        bool
	etag       string
	properties string
}

type Counts struct {
	NotebookAttempts int
	NotebookUpdates  int
	Renames          int
	Appends          int
	TablesRequests   int
	CatalogReads     int
	DefinitionReads  int
	StorageStats     int
	StorageLists     int
	StorageReads     int
	ManagedCreates   int
	ManagedDeletes   int
}

type Service struct {
	mu                  sync.Mutex
	t                   testing.TB
	Server              *httptest.Server
	notebook            fabric.Definition
	environment         fabric.Definition
	folders             []fabric.Folder
	items               []fabric.Item
	objects             map[string]object
	version             int
	counts              Counts
	failNotebook        int
	failStorage         int
	denyTables          bool
	regionalDefinitions bool
	management          *managementState
}

func part(path, data string) fabric.Part {
	return fabric.Part{Path: path, Payload: base64.StdEncoding.EncodeToString([]byte(data)), PayloadType: "InlineBase64"}
}

func New(t testing.TB) *Service {
	t.Helper()
	service := &Service{t: t, objects: make(map[string]object), folders: []fabric.Folder{}, items: []fabric.Item{
		{ID: NotebookID, Type: "Notebook", DisplayName: "Sample notebook"},
		{ID: LakehouseID, Type: "Lakehouse", DisplayName: "Sample lakehouse"},
		{ID: EnvironmentID, Type: "Environment", DisplayName: "Sample environment"},
		{ID: "55555555-5555-5555-5555-555555555555", Type: "Warehouse", DisplayName: "Not exposed"},
	}}
	service.notebook = fabric.Definition{
		Parts: []fabric.Part{
			part(".platform", `{"metadata":{"displayName":"Sample notebook"},"config":{"logicalId":"keep-me"}}`),
			part("notebook-content.ipynb", InitialNotebook),
			part("Extras/preserve.bin", "opaque unknown part"),
		},
		Extra: map[string]json.RawMessage{"futureDefinitionField": json.RawMessage(`{"keep":true}`)},
	}
	service.notebook.Parts[1].Extra = map[string]json.RawMessage{"futurePartField": json.RawMessage(`{"version":7}`)}
	service.environment = fabric.Definition{Parts: []fabric.Part{
		part(".platform", `{"metadata":{"displayName":"Sample environment"}}`),
		part("Libraries/PublicLibraries/environment.yml", "dependencies: []\n"),
		part("Libraries/CustomLibraries/example.whl", "mock package bytes"),
		part("Setting/Sparkcompute.yml", "spark_conf: {}\n"),
	}}
	service.setObject("Files", object{dir: true})
	service.setObject("Files/demo.txt", object{data: []byte("0123456789")})
	service.setObject("Tables", object{dir: true})
	service.setObject("Tables/table", object{dir: true})
	service.setObject("Tables/table/part.parquet", object{data: []byte("read-only table bytes")})
	service.Server = httptest.NewServer(http.HandlerFunc(service.serve))
	t.Cleanup(service.Server.Close)
	return service
}

type tokens struct{}

func (tokens) Token(_ context.Context, scope string) (string, error) {
	switch scope {
	case "https://api.fabric.microsoft.com/.default":
		return "fabric-test-token", nil
	case "https://storage.azure.com/.default":
		return "storage-test-token", nil
	default:
		return "", fmt.Errorf("wrong token scope")
	}
}

func (s *Service) Clients() (*fabric.Client, *onelake.Client) {
	s.t.Helper()
	makeTransport := func(scope string) *transport.Client {
		client, err := transport.New(transport.Options{
			BaseURL: s.Server.URL, Scope: scope, Tokens: tokens{},
			HTTPClient: s.Server.Client(), MaxRetries: 0,
			RetryDelay: time.Millisecond, MaxRetryDelay: 100 * time.Millisecond,
		})
		if err != nil {
			s.t.Fatal(err)
		}
		return client
	}
	fab := fabric.New(makeTransport("https://api.fabric.microsoft.com/.default"), fabric.Options{
		OperationTimeout: 2 * time.Second, PollInterval: time.Millisecond, MaxDefinitionBytes: 32 << 20,
	})
	lake := onelake.New(makeTransport("https://storage.azure.com/.default"), onelake.Options{ChunkSize: 4, MaxPages: 20})
	return fab, lake
}

func cloneDefinition(def fabric.Definition) fabric.Definition {
	clone := def
	clone.Parts = append([]fabric.Part(nil), def.Parts...)
	clone.Extra = cloneRaw(def.Extra)
	for i := range clone.Parts {
		clone.Parts[i].Extra = cloneRaw(def.Parts[i].Extra)
	}
	return clone
}

func cloneRaw(input map[string]json.RawMessage) map[string]json.RawMessage {
	if input == nil {
		return nil
	}
	output := make(map[string]json.RawMessage, len(input))
	for key, data := range input {
		output[key] = append(json.RawMessage(nil), data...)
	}
	return output
}

func (s *Service) Definition() fabric.Definition {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneDefinition(s.notebook)
}

func (s *Service) SetDefinition(def fabric.Definition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notebook = cloneDefinition(def)
}

func (s *Service) SetEnvironment(def fabric.Definition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.environment = cloneDefinition(def)
}

func (s *Service) SetFolders(folders []fabric.Folder) {
	s.mu.Lock()
	s.folders = append([]fabric.Folder{}, folders...)
	s.mu.Unlock()
}

func (s *Service) SetItemFolder(id, folder string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		if s.items[i].ID == id {
			s.items[i].FolderID = folder
			return
		}
	}
	s.t.Fatalf("unknown fixture item for folder assignment")
}

func (s *Service) UseRegionalDefinitionPolling() {
	s.mu.Lock()
	s.regionalDefinitions = true
	s.mu.Unlock()
}

func (s *Service) SetFile(path string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setObject(path, object{data: append([]byte(nil), data...)})
}

func (s *Service) File(path string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, found := s.objects[path]
	return append([]byte(nil), obj.data...), found
}

func (s *Service) Counts() Counts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts
}

func (s *Service) FailNotebookUpdates(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNotebook = count
}

func (s *Service) FailStorageCommits(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failStorage = count
}

func (s *Service) DenyTables(deny bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.denyTables = deny
}

func (s *Service) setObject(path string, obj object) {
	s.version++
	obj.etag = fmt.Sprintf(`"%d"`, s.version)
	s.objects[path] = obj
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("x-ms-request-id", "mock-request-id")
	w.Header().Set("requestId", "mock-fabric-request-id")
	fabricRequest := strings.HasPrefix(r.URL.Path, "/v1/")
	token := "Bearer storage-test-token"
	if fabricRequest {
		token = "Bearer fabric-test-token"
	}
	if r.Header.Get("Authorization") != token {
		s.t.Error("request used wrong audience or omitted authorization")
		s.fail(w, http.StatusForbidden, "AuthorizationPermissionMismatch")
		return
	}
	if fabricRequest {
		s.serveFabric(w, r)
		return
	}
	s.serveLake(w, r)
}

func (s *Service) fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": "offline fixture failure"}})
}

func (s *Service) json(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Service) serveFabric(w http.ResponseWriter, r *http.Request) {
	if s.serveManagement(w, r) {
		return
	}
	workspace := fabric.Workspace{ID: WorkspaceID, DisplayName: "Sample workspace"}
	base := "/v1/workspaces/" + WorkspaceID
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/workspaces":
		s.counts.CatalogReads++
		s.json(w, map[string]any{"value": []fabric.Workspace{workspace}})
	case r.Method == http.MethodGet && r.URL.Path == base:
		s.counts.CatalogReads++
		s.json(w, workspace)
	case r.Method == http.MethodGet && r.URL.Path == base+"/items":
		s.counts.CatalogReads++
		s.json(w, map[string]any{"value": s.items})
	case r.Method == http.MethodGet && r.URL.Path == base+"/folders":
		s.counts.CatalogReads++
		if r.URL.Query().Get("recursive") != "true" {
			s.t.Error("folder discovery is not recursive")
		}
		s.json(w, map[string]any{"value": s.folders})
	case r.Method == http.MethodPost && r.URL.Path == base+"/notebooks/"+NotebookID+"/getDefinition":
		s.counts.DefinitionReads++
		if r.URL.Query().Get("format") != "ipynb" {
			s.t.Error("notebook getDefinition omitted ipynb format")
			s.fail(w, 400, "InvalidFormat")
			return
		}
		if s.regionalDefinitions {
			const operationID = "66666666-6666-6666-6666-666666666666"
			w.Header().Set("Location", "https://region-redirect.analysis.windows.net/v1/operations/"+operationID)
			w.Header().Set("x-ms-operation-id", operationID)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(202)
			return
		}
		s.json(w, map[string]any{"definition": s.notebook})
	case r.Method == http.MethodPost && r.URL.Path == base+"/environments/"+EnvironmentID+"/getDefinition":
		s.counts.DefinitionReads++
		s.json(w, map[string]any{"definition": s.environment})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/66666666-6666-6666-6666-666666666666":
		w.Header().Set("Location", "https://region-redirect.analysis.windows.net/v1/operations/66666666-6666-6666-6666-666666666666/result")
		s.json(w, map[string]string{"status": "Succeeded"})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/66666666-6666-6666-6666-666666666666/result":
		s.json(w, map[string]any{"definition": s.notebook})
	case r.Method == http.MethodPost && r.URL.Path == base+"/notebooks/"+NotebookID+"/updateDefinition":
		s.counts.NotebookAttempts++
		if s.failNotebook > 0 {
			s.failNotebook--
			s.fail(w, 500, "InjectedWriteFailure")
			return
		}
		if r.URL.Query().Get("updateMetadata") == "true" {
			s.t.Error("ordinary notebook update requested metadata mutation")
			s.fail(w, 400, "MetadataWriteForbidden")
			return
		}
		var body struct {
			Definition fabric.Definition `json:"definition"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Definition.Format != "ipynb" {
			s.t.Error("invalid notebook update definition/format")
			s.fail(w, 400, "InvalidDefinition")
			return
		}
		s.notebook = body.Definition
		s.counts.NotebookUpdates++
		w.WriteHeader(http.StatusOK)
	default:
		s.t.Errorf("unexpected Fabric request %s %s", r.Method, r.URL.EscapedPath())
		s.fail(w, 404, "ItemNotFound")
	}
}

func (s *Service) properties(w http.ResponseWriter, obj object) {
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", "Fri, 18 Sep 2026 07:00:00 GMT")
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	resource := "file"
	if obj.dir {
		resource = "directory"
	}
	w.Header().Set("x-ms-resource-type", resource)
	w.Header().Set("x-ms-properties", obj.properties)
}

func (s *Service) conditions(w http.ResponseWriter, r *http.Request, obj object, found bool) bool {
	if value := r.Header.Get("If-Match"); value != "" && (!found || value != obj.etag) {
		s.fail(w, 412, "ConditionNotMet")
		return false
	}
	if r.Header.Get("If-None-Match") == "*" && found {
		s.fail(w, 412, "ConditionNotMet")
		return false
	}
	return true
}

func (s *Service) serveLake(w http.ResponseWriter, r *http.Request) {
	if s.serveManagedLake(w, r) {
		return
	}
	if r.URL.Path == "/"+WorkspaceID && r.Method == http.MethodGet && r.URL.Query().Get("resource") == "filesystem" {
		s.list(w, r)
		return
	}
	prefix := "/" + WorkspaceID + "/" + LakehouseID + "/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		s.t.Errorf("unexpected OneLake URL %s", r.URL.EscapedPath())
		s.fail(w, 404, "PathNotFound")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if path == "Tables" || strings.HasPrefix(path, "Tables/") {
		s.counts.TablesRequests++
		if s.denyTables {
			s.fail(w, 403, "AuthorizationPermissionMismatch")
			return
		}
	}
	obj, found := s.objects[path]
	if r.Method != http.MethodHead && r.Method != http.MethodGet &&
		(path == "Files" || path == "Tables" || strings.HasPrefix(path, "Tables/")) {
		s.t.Error("mutation reached a protected OneLake path")
		s.fail(w, 403, "ProtectedPath")
		return
	}
	if !s.conditions(w, r, obj, found) {
		return
	}
	switch r.Method {
	case http.MethodHead:
		s.counts.StorageStats++
		if !found {
			s.fail(w, 404, "PathNotFound")
			return
		}
		s.properties(w, obj)
		w.WriteHeader(200)
	case http.MethodGet:
		s.counts.StorageReads++
		if !found || obj.dir {
			s.fail(w, 404, "PathNotFound")
			return
		}
		var start, end int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil ||
			start < 0 || end < start || start >= len(obj.data) {
			s.fail(w, 416, "InvalidRange")
			return
		}
		end = min(end, len(obj.data)-1)
		s.properties(w, obj)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.WriteHeader(206)
		_, _ = w.Write(obj.data[start : end+1])
	case http.MethodPut:
		if source := r.Header.Get("x-ms-rename-source"); source != "" {
			s.rename(w, r, path, source)
			return
		}
		resource := r.URL.Query().Get("resource")
		if resource != "file" && resource != "directory" {
			s.fail(w, 400, "MissingResourceType")
			return
		}
		if r.Header.Get("If-None-Match") != "*" {
			s.t.Error("create omitted no-overwrite condition")
			s.fail(w, 400, "ConditionRequired")
			return
		}
		parent := path[:strings.LastIndex(path, "/")]
		if p, ok := s.objects[parent]; !ok || !p.dir {
			s.fail(w, 404, "ParentNotFound")
			return
		}
		s.setObject(path, object{dir: resource == "directory", properties: r.Header.Get("x-ms-properties")})
		s.properties(w, s.objects[path])
		w.WriteHeader(201)
	case http.MethodPatch:
		if !found || obj.dir {
			s.fail(w, 404, "PathNotFound")
			return
		}
		position, err := strconv.Atoi(r.URL.Query().Get("position"))
		if err != nil || position != len(obj.data) {
			s.fail(w, 400, "InvalidPosition")
			return
		}
		switch r.URL.Query().Get("action") {
		case "append":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				s.fail(w, 400, "InvalidBody")
				return
			}
			obj.data = append(obj.data, data...)
			// ADLS append stages uncommitted bytes; flush updates the ETag.
			s.objects[path] = obj
			s.counts.Appends++
			w.WriteHeader(202)
		case "flush":
			s.setObject(path, obj)
			s.properties(w, s.objects[path])
			// A flush returns properties, not the file content.
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(200)
		default:
			s.fail(w, 400, "InvalidAction")
		}
	case http.MethodDelete:
		if !found {
			s.fail(w, 404, "PathNotFound")
			return
		}
		if r.Header.Get("If-Match") == "" {
			s.t.Error("delete omitted ETag condition")
			s.fail(w, 400, "ConditionRequired")
			return
		}
		if obj.dir {
			if r.URL.Query().Get("recursive") != "false" {
				s.t.Error("rmdir attempted recursive delete")
				s.fail(w, 400, "RecursiveDeleteForbidden")
				return
			}
			for child := range s.objects {
				if strings.HasPrefix(child, path+"/") {
					s.fail(w, 409, "DirectoryNotEmpty")
					return
				}
			}
		}
		delete(s.objects, path)
		w.WriteHeader(200)
	default:
		s.fail(w, 405, "UnsupportedMethod")
	}
}

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	s.counts.StorageLists++
	directory := r.URL.Query().Get("directory")
	if !strings.HasPrefix(directory, LakehouseID+"/") || r.URL.Query().Get("recursive") != "false" {
		s.t.Error("list paths must be GUID-addressed and nonrecursive")
		s.fail(w, 400, "InvalidDirectory")
		return
	}
	parent := strings.TrimPrefix(directory, LakehouseID+"/")
	if parent == "Tables" || strings.HasPrefix(parent, "Tables/") {
		s.counts.TablesRequests++
		if s.denyTables {
			s.fail(w, 403, "AuthorizationPermissionMismatch")
			return
		}
	}
	obj, found := s.objects[parent]
	if !found || !obj.dir {
		s.fail(w, 404, "PathNotFound")
		return
	}
	var names []string
	for path := range s.objects {
		if raw, ok := strings.CutPrefix(path, parent+"/"); ok && !strings.Contains(raw, "/") {
			names = append(names, path)
		}
	}
	sort.Strings(names)
	paths := make([]map[string]any, 0, len(names))
	for _, path := range names {
		obj := s.objects[path]
		paths = append(paths, map[string]any{
			"name": LakehouseID + "/" + path, "isDirectory": obj.dir,
			"contentLength": strconv.Itoa(len(obj.data)), "etag": obj.etag,
			"lastModified": "Fri, 18 Sep 2026 07:00:00 GMT",
		})
	}
	s.json(w, map[string]any{"paths": paths})
}

func (s *Service) rename(w http.ResponseWriter, r *http.Request, destination, header string) {
	if s.failStorage > 0 {
		s.failStorage--
		s.fail(w, 500, "InjectedCommitFailure")
		return
	}
	source, err := url.PathUnescape(header)
	prefix := "/" + WorkspaceID + "/" + LakehouseID + "/"
	if err != nil || !strings.HasPrefix(source, prefix) || r.URL.Query().Get("mode") != "posix" {
		s.fail(w, 400, "InvalidRenameSource")
		return
	}
	source = strings.TrimPrefix(source, prefix)
	obj, found := s.objects[source]
	if !found {
		s.fail(w, 404, "SourcePathNotFound")
		return
	}
	if value := r.Header.Get("x-ms-source-if-match"); value == "" || value != obj.etag {
		s.fail(w, 412, "SourceConditionNotMet")
		return
	}
	if r.Header.Get("If-Match") == "" && r.Header.Get("If-None-Match") != "*" {
		s.t.Error("rename omitted destination condition")
		s.fail(w, 400, "ConditionRequired")
		return
	}
	parent := destination[:strings.LastIndex(destination, "/")]
	if p, ok := s.objects[parent]; !ok || !p.dir {
		s.fail(w, 404, "ParentNotFound")
		return
	}
	var descendants []string
	for path := range s.objects {
		if strings.HasPrefix(path, source+"/") {
			descendants = append(descendants, path)
		}
	}
	for _, path := range descendants {
		s.objects[destination+strings.TrimPrefix(path, source)] = s.objects[path]
		delete(s.objects, path)
	}
	delete(s.objects, source)
	if _, present := r.Header["X-Ms-Properties"]; present {
		obj.properties = r.Header.Get("x-ms-properties")
	}
	s.setObject(destination, obj)
	s.counts.Renames++
	s.properties(w, s.objects[destination])
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(201)
}
