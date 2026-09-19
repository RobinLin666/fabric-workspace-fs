package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func managedCreateRequest(t *testing.T, target, name, folder, parent string) *http.Request {
	t.Helper()
	body, err := json.Marshal(struct {
		DisplayName, FolderID, ParentFolderID string
	}{DisplayName: name, FolderID: folder, ParentFolderID: parent})
	if err != nil {
		t.Fatal("cannot encode offline creation identity")
	}
	req, err := http.NewRequest("POST", target, bytes.NewReader(body))
	if err != nil {
		t.Fatal("cannot construct offline creation request")
	}
	return req
}

func managedFolderGuard(t *testing.T) (*scopeGuard, managedResource) {
	t.Helper()
	plan := testPlan(t)
	g := newScopeGuard(testOptions(), plan)
	req := managedCreateRequest(t, plan.Managed.FolderCreateURL, plan.Managed.FolderName, "", "")
	if _, err := g.authorize(req); err != nil {
		t.Fatal("planned root folder request was rejected")
	}
	folder := managedResource{ID: testOperation, Type: "Folder", DisplayName: plan.Managed.FolderName}
	if g.noteManagedCreation(folder) != nil || g.confirmManagedIdentity(folder) != nil {
		t.Fatal("matching authenticated folder receipt was rejected")
	}
	return g, folder
}

func TestManagedPlanUsesUniqueLakehouseSafeNames(t *testing.T) {
	plan := testPlan(t)
	if len(plan.Managed.Items) != 3 || plan.Managed.OverlayName != "" {
		t.Fatal("managed fixture plan is incomplete or still claims an overlay")
	}
	prefix := strings.ReplaceAll(plan.Stem, "-", "_")
	for _, spec := range plan.Managed.Items {
		if !strings.HasPrefix(spec.DisplayName, prefix+"_") || spec.LocalName != spec.DisplayName+"."+spec.Type {
			t.Fatal("typed fixture name lost the shared unique prefix")
		}
		if spec.Type == "Lakehouse" {
			if len(spec.DisplayName) > 123 || spec.DisplayName[0] < 'a' || spec.DisplayName[0] > 'z' {
				t.Fatal("planned Lakehouse violates its length/start-character rules")
			}
			for _, ch := range spec.DisplayName {
				if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
					t.Fatal("planned Lakehouse contains a forbidden name character")
				}
			}
		}
	}
}

func TestManagedCreationRequiresExactPlanAndVerifiedParent(t *testing.T) {
	plan := testPlan(t)
	spec := plan.Managed.Items[0]
	withoutParent := newScopeGuard(testOptions(), plan)
	if _, err := withoutParent.authorize(managedCreateRequest(t, spec.CreateURL, spec.DisplayName, testOperation, "")); err == nil {
		t.Fatal("typed item creation was allowed before parent ownership verification")
	}
	for _, bad := range []struct{ name, folder, parent string }{
		{"existing-user-item", testOperation, ""},
		{spec.DisplayName, testLakehouse, ""},
		{spec.DisplayName, "", ""},
		{spec.DisplayName, testOperation, testDeleteOp},
	} {
		g, _ := managedFolderGuard(t)
		if _, err := g.authorize(managedCreateRequest(t, spec.CreateURL, bad.name, bad.folder, bad.parent)); err == nil {
			t.Fatal("unplanned name or parent was accepted")
		}
	}
	g, folder := managedFolderGuard(t)
	req := managedCreateRequest(t, spec.CreateURL, spec.DisplayName, folder.ID, "")
	if _, err := g.authorize(req); err != nil {
		t.Fatal("exact typed request under the owned folder was rejected")
	}
	if _, err := g.authorize(req); err == nil {
		t.Fatal("managed creation could be retried")
	}
	receipt := managedResource{ID: testNotebook, Type: spec.Type, DisplayName: spec.DisplayName, FolderID: folder.ID}
	if err := g.noteManagedCreation(receipt); err != nil {
		t.Fatal("matching created-item response was not recorded")
	}
	mismatch := receipt
	mismatch.FolderID = testDeleteOp
	if g.confirmManagedIdentity(mismatch) == nil {
		t.Fatal("fresh identity changed its parent without detection")
	}
}

func TestManagedDeletionRequiresReceiptVerificationAndExplicitPermit(t *testing.T) {
	g, folder := managedFolderGuard(t)
	spec := g.plan.Managed.Items[0]
	if _, err := g.authorize(managedCreateRequest(t, spec.CreateURL, spec.DisplayName, folder.ID, "")); err != nil {
		t.Fatal("create setup failed")
	}
	item := managedResource{ID: testNotebook, Type: spec.Type, DisplayName: spec.DisplayName, FolderID: folder.ID}
	if g.noteManagedCreation(item) != nil {
		t.Fatal("receipt setup failed")
	}
	target := "https://api.fabric.microsoft.com/v1/workspaces/" + testWorkspace + "/items/" + item.ID
	if g.permitManagedDelete(item.ID, true) == nil {
		t.Fatal("a creation response alone authorized deletion")
	}
	if g.confirmManagedIdentity(item) != nil {
		t.Fatal("fresh identity setup failed")
	}
	if _, err := g.authorize(request(t, "DELETE", target, nil)); err == nil {
		t.Fatal("verified identity alone bypassed the empty-delete entry point")
	}
	if g.permitManagedDelete(item.ID, true) != nil {
		t.Fatal("explicit owned deletion permit was rejected")
	}
	if _, err := g.authorize(request(t, "DELETE", target, nil)); err != nil {
		t.Fatal("exact owned delete was rejected")
	}
	if _, err := g.authorize(request(t, "DELETE", target+"?hardDelete=true", nil)); err == nil {
		t.Fatal("hard deletion was allowed")
	}
	if _, err := g.authorize(request(t, "DELETE", strings.Replace(target, item.ID, testLakehouse, 1), nil)); err == nil {
		t.Fatal("delete permit covered an existing/unowned item")
	}
	g.permitManagedDelete(item.ID, false)
	if _, err := g.authorize(request(t, "DELETE", target, nil)); err == nil {
		t.Fatal("delete permit was not revoked")
	}
}

func TestPendingManagedCreatePreservesItsParent(t *testing.T) {
	g, folder := managedFolderGuard(t)
	spec := g.plan.Managed.Items[0]
	req := managedCreateRequest(t, spec.CreateURL, spec.DisplayName, folder.ID, "")
	if _, err := g.authorize(req); err != nil {
		t.Fatal("pending create setup failed")
	}
	g.observe(req, response(202, "", http.Header{"X-Ms-Operation-Id": {testDeleteOp}}))
	snapshot := g.managedEvidenceSnapshot()[spec.Type]
	if snapshot.ID != "" || snapshot.OperationID != testDeleteOp || snapshot.Status != "create-attempted-unverified" {
		t.Fatal("ambiguous create outcome was not preserved in evidence")
	}
	if g.permitManagedDelete(folder.ID, true) == nil {
		t.Fatal("parent deletion was allowed while a child create outcome was unknown")
	}
}

func TestManagedReceiptsCannotBeInventedFromNames(t *testing.T) {
	g := newScopeGuard(testOptions(), testPlan(t))
	folder := managedResource{ID: testOperation, Type: "Folder", DisplayName: g.plan.Managed.FolderName}
	if g.noteManagedCreation(folder) == nil || g.confirmManagedIdentity(folder) == nil {
		t.Fatal("a matching catalog name invented creation ownership")
	}
}

func TestInjectedAgentsCannotIssueFabricMutations(t *testing.T) {
	g, folder := managedFolderGuard(t)
	target := g.plan.Managed.FolderCreateURL
	if _, err := g.authorize(managedCreateRequest(t, target, ".agents", "", folder.ID)); err == nil {
		t.Fatal(".agents was authorized as a remote Fabric folder")
	}
	g.setReadOnly(true)
	spec := g.plan.Managed.Items[0]
	if _, err := g.authorize(managedCreateRequest(t, spec.CreateURL, spec.DisplayName, folder.ID, "")); err == nil {
		t.Fatal("readonly injected-bundle phase authorized an HTTP mutation")
	}
	if mutationCount(counts{"fabric|GET|folder": 2, "fabric|POST|definition-read": 1, "onelake|HEAD|stat": 1}) != 0 {
		t.Fatal("read-only API traffic was counted as mutation")
	}
	if mutationCount(counts{"fabric|POST|managed-item-create": 1, "fabric|DELETE|managed-delete": 1}) != 2 {
		t.Fatal("managed resource mutation traffic was omitted")
	}
}

func TestNewOwnedLakehouseOnlyAllowsEmptyInspection(t *testing.T) {
	g, folder := managedFolderGuard(t)
	spec, _ := g.plannedItem("Lakehouse")
	if _, err := g.authorize(managedCreateRequest(t, spec.CreateURL, spec.DisplayName, folder.ID, "")); err != nil {
		t.Fatal("Lakehouse request setup failed")
	}

	item := managedResource{ID: testDeleteOp, Type: "Lakehouse", DisplayName: spec.DisplayName, FolderID: folder.ID}
	if g.noteManagedCreation(item) != nil {
		t.Fatal("Lakehouse receipt setup failed")
	}
	base := "https://onelake.dfs.fabric.microsoft.com/" + testWorkspace
	for _, root := range []string{"Files", "Tables"} {
		list := base + "?resource=filesystem&recursive=false&directory=" + item.ID + "/" + root
		if _, err := g.authorize(request(t, "GET", list, nil)); err != nil {
			t.Fatal("owned Lakehouse empty inspection was rejected")
		}
		if _, err := g.authorize(request(t, "PUT", base+"/"+item.ID+"/"+root+"/unplanned", map[string]string{"If-None-Match": "*"})); err == nil {
			t.Fatal("empty-inspection scope allowed a write")
		}
	}
}
