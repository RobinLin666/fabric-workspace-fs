package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"fabric-workspace-fs/internal/fabric"
)

var managedKinds = []string{"Notebook", "Lakehouse", "Environment"}

type managedItemPlan struct {
	Type        string `json:"type"`
	DisplayName string `json:"displayName"`
	LocalName   string `json:"localName"`
	CreateURL   string `json:"createURL"`
}

type managedPlan struct {
	FolderName      string            `json:"folderName"`
	FolderCreateURL string            `json:"folderCreateURL"`
	OverlayName     string            `json:"overlayName,omitempty"` // Legacy receipts only; never a cleanup target.
	Items           []managedItemPlan `json:"items"`
}

type managedResource struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	DisplayName string `json:"displayName"`
	FolderID    string `json:"folderId,omitempty"`
}

type managedEvidence struct {
	managedResource
	Status      string `json:"status"`
	LocalPath   string `json:"localPath,omitempty"`
	GetURL      string `json:"getURL"`
	DeleteURL   string `json:"deleteURL"`
	OperationID string `json:"operationID,omitempty"`
}

func mutationCount(values counts) uint64 {
	var result uint64
	for key, n := range values {
		if strings.Contains(key, "|GET|") || strings.Contains(key, "|HEAD|") ||
			key == "fabric|POST|definition-read" {
			continue
		}
		result += n
	}
	return result
}

func (g *scopeGuard) plannedItem(kind string) (managedItemPlan, bool) {
	for _, item := range g.plan.Managed.Items {
		if item.Type == kind {
			return item, true
		}
	}
	return managedItemPlan{}, false
}

// The HTTP layer sees only buffered requests produced by our transport.
// Inspect a fresh body reader, never consume or log the actual outgoing body.
func createRequestIdentity(req *http.Request) (name, folder, parent, kind, description string, ok bool) {
	if req.GetBody == nil || req.ContentLength > sdkFixtureLimit {
		return "", "", "", "", "", false
	}
	body, err := req.GetBody()
	if err != nil {
		return "", "", "", "", "", false
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, sdkFixtureLimit+1))
	if err != nil || len(data) > sdkFixtureLimit {
		return "", "", "", "", "", false
	}
	var value struct {
		DisplayName    string `json:"displayName"`
		FolderID       string `json:"folderId"`
		ParentFolderID string `json:"parentFolderId"`
		Type           string `json:"type"`
		Description    string `json:"description"`
	}
	if json.Unmarshal(data, &value) != nil || value.DisplayName == "" {
		return "", "", "", "", "", false
	}
	return value.DisplayName, value.FolderID, value.ParentFolderID, value.Type, value.Description, true
}

func (g *scopeGuard) allowCreateLocked(req *http.Request) (string, bool) {
	if g.readOnly || req.Method != http.MethodPost || req.URL.RawQuery != "" || req.URL.ForceQuery {
		return "", false
	}
	base := "/v1/workspaces/" + g.workspace
	name, folder, parent, bodyKind, description, ok := createRequestIdentity(req)
	if !ok {
		return "", false
	}
	if req.URL.Path == base+"/notebooks" && name == g.plan.NotebookDisplayName &&
		description == g.plan.NotebookDescription && folder == "" && parent == "" && !g.createSent {
		g.createSent = true
		return "fabric|POST|notebook-create", true
	}
	if req.URL.Path == base+"/folders" && name == g.plan.Managed.FolderName &&
		folder == "" && parent == "" && !g.managedAttempts["Folder"] {
		g.managedAttempts["Folder"] = true
		return "fabric|POST|managed-folder-create", true
	}
	root, verified := g.managedVerified["Folder"]
	if !verified || folder == "" || !strings.EqualFold(folder, root.ID) || parent != "" {
		return "", false
	}
	for _, item := range g.plan.Managed.Items {
		if req.URL.Path == base+"/"+strings.ToLower(item.Type)+"s" &&
			name == item.DisplayName && (bodyKind == "" || bodyKind == item.Type) &&
			!g.managedAttempts[item.Type] {
			g.managedAttempts[item.Type] = true
			return "fabric|POST|managed-item-create", true
		}
	}
	return "", false
}

// Called only after the authenticated production Create API returned its
// validated immutable identity. A name-only catalog match cannot grant delete.
func (g *scopeGuard) noteManagedCreation(value managedResource) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if fabric.ValidateID(value.ID) != nil || !g.managedAttempts[value.Type] {
		return fail("managed creation had no approved request or valid returned ID")
	}
	value.ID = strings.ToLower(value.ID)
	value.FolderID = strings.ToLower(value.FolderID)
	if value.Type == "Folder" {
		if value.DisplayName != g.plan.Managed.FolderName || value.FolderID != "" {
			return fail("new Fabric folder did not match its exact plan")
		}
	} else {
		spec, exists := g.plannedItem(value.Type)
		folder, verified := g.managedVerified["Folder"]
		if !exists || !verified || value.DisplayName != spec.DisplayName || value.FolderID != folder.ID {
			return fail("new managed item did not match its exact type, name, and owned parent")
		}
	}
	if previous, exists := g.managedReceipts[value.Type]; exists && previous != value {
		return fail("managed creation returned more than one immutable identity")
	}
	g.managedReceipts[value.Type] = value
	return nil
}

func (g *scopeGuard) confirmManagedIdentity(value managedResource) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	value.ID = strings.ToLower(value.ID)
	value.FolderID = strings.ToLower(value.FolderID)
	receipt, exists := g.managedReceipts[value.Type]
	if !exists || receipt != value {
		return fail("fresh managed identity did not match the authenticated creation receipt")
	}
	g.managedVerified[value.Type] = value
	return nil
}

func (g *scopeGuard) managedReceipt(kind string) (managedResource, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	value, exists := g.managedReceipts[kind]
	return value, exists
}

func (g *scopeGuard) managedEvidenceSnapshot() map[string]managedEvidence {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := make(map[string]managedEvidence)
	for kind := range g.managedAttempts {
		resource, received := g.managedReceipts[kind]
		status := "authenticated-create-receipt"
		if !received {
			status = "create-attempted-unverified"
			resource.Type = kind
			if kind == "Folder" {
				resource.DisplayName = g.plan.Managed.FolderName
			} else if spec, exists := g.plannedItem(kind); exists {
				resource.DisplayName = spec.DisplayName
				resource.FolderID = g.managedVerified["Folder"].ID
			}
		}
		evidence := managedEvidence{managedResource: resource, Status: status, OperationID: g.managedOperations[kind]}
		if received {
			collection := "items"
			if kind == "Folder" {
				collection = "folders"
			}
			evidence.GetURL = fabric.BaseURL + "/v1/workspaces/" + g.workspace + "/" + collection + "/" + resource.ID
			evidence.DeleteURL = evidence.GetURL
		}
		result[kind] = evidence
	}
	return result
}

func (g *scopeGuard) pendingManagedCreateLocked() bool {
	for kind, attempted := range g.managedAttempts {
		if _, received := g.managedReceipts[kind]; attempted && !received {
			return true
		}
	}
	return false
}

func (g *scopeGuard) permitManagedDelete(id string, permit bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	id = strings.ToLower(id)
	if !permit {
		delete(g.managedDeletes, id)
		return nil
	}
	for _, value := range g.managedVerified {
		if value.ID == id {
			if value.Type == "Folder" && g.pendingManagedCreateLocked() {
				return fail("owned folder retained while a managed creation outcome is unverified")
			}
			g.managedDeletes[id] = true
			return nil
		}
	}
	return fail("managed deletion requires an exact verified creation receipt")
}

func (g *scopeGuard) permitManagedNotebookUpdate(id string, permit bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !permit {
		g.managedNotebookUpdate = false
		return nil
	}
	value, verified := g.managedVerified["Notebook"]
	if !verified || !strings.EqualFold(value.ID, id) {
		return fail("managed notebook update requires its verified creation receipt")
	}
	g.managedNotebookUpdate = true
	return nil
}

func (g *scopeGuard) allowManagedRequestLocked(req *http.Request) (string, bool) {
	base := "/v1/workspaces/" + g.workspace
	for _, value := range g.managedReceipts {
		if req.Method == http.MethodPost && (value.Type == "Notebook" || value.Type == "Environment") &&
			req.URL.Path == base+"/"+strings.ToLower(value.Type)+"s/"+value.ID+"/getDefinition" {
			return "fabric|POST|definition-read", true
		}
		if value.Type == "Notebook" && g.managedNotebookUpdate && !g.readOnly &&
			g.managedVerified["Notebook"].ID == value.ID && req.Method == http.MethodPost &&
			req.URL.Path == base+"/notebooks/"+value.ID+"/updateDefinition" &&
			req.URL.RawQuery == "" && !req.URL.ForceQuery {
			return "fabric|POST|definition-update", true
		}
		if req.Method != http.MethodDelete || g.readOnly || !g.managedDeletes[value.ID] ||
			req.URL.RawQuery != "" || req.URL.ForceQuery {
			continue
		}
		collection := "items"
		if value.Type == "Folder" {
			collection = "folders"
		}
		if req.URL.Path == base+"/"+collection+"/"+value.ID {
			return "fabric|DELETE|managed-delete", true
		}
	}
	return "", false
}

func (g *scopeGuard) allowManagedLakeReadLocked(req *http.Request) (string, bool) {
	value, exists := g.managedReceipts["Lakehouse"]
	if !exists || (req.Method != http.MethodHead && req.Method != http.MethodGet) {
		return "", false
	}
	base := "/" + g.workspace + "/" + value.ID + "/"
	if req.Method == http.MethodGet && req.URL.Path == "/"+g.workspace {
		query := req.URL.Query()
		directory := query.Get("directory")
		if query.Get("resource") == "filesystem" && query.Get("recursive") == "false" &&
			(directory == value.ID+"/Files" || directory == value.ID+"/Tables") {
			return "onelake|GET|managed-empty-list", true
		}
	}
	if strings.HasPrefix(req.URL.Path, base) {
		relative := strings.TrimPrefix(req.URL.Path, base)
		if relative == "Files" || relative == "Tables" {
			if req.Method == http.MethodHead {
				return "onelake|HEAD|managed-empty-stat", true
			}
		}
	}
	return "", false
}
