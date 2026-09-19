package main

import "testing"

func TestManagedNotebookUpdateRequiresVerifiedScopedPermit(t *testing.T) {
	g, folder := managedFolderGuard(t)
	spec, _ := g.plannedItem("Notebook")
	if _, err := g.authorize(managedCreateRequest(t, spec.CreateURL, spec.DisplayName, folder.ID, "")); err != nil {
		t.Fatal("Notebook request setup failed")
	}
	item := managedResource{ID: testNotebook, Type: "Notebook", DisplayName: spec.DisplayName, FolderID: folder.ID}
	if g.noteManagedCreation(item) != nil {
		t.Fatal("Notebook receipt setup failed")
	}
	if g.permitManagedNotebookUpdate(item.ID, true) == nil {
		t.Fatal("unverified notebook received an update permit")
	}
	if g.confirmManagedIdentity(item) != nil {
		t.Fatal("Notebook fresh-identity setup failed")
	}
	target := "https://api.fabric.microsoft.com/v1/workspaces/" + testWorkspace + "/notebooks/" + item.ID + "/updateDefinition"
	if _, err := g.authorize(request(t, "POST", target, nil)); err == nil {
		t.Fatal("creation ownership alone enabled arbitrary updates")
	}
	if g.permitManagedNotebookUpdate(testLakehouse, true) == nil {
		t.Fatal("a different notebook ID received an update permit")
	}
	if g.permitManagedNotebookUpdate(item.ID, true) != nil {
		t.Fatal("explicit owned update permit failed")
	}
	if _, err := g.authorize(request(t, "POST", target, nil)); err != nil {
		t.Fatal("owned notebook update was rejected")
	}
	g.setReadOnly(true)
	if _, err := g.authorize(request(t, "POST", target, nil)); err == nil {
		t.Fatal("rmdir/read-only probe phase permitted an update")
	}
	g.setReadOnly(false)
	g.permitManagedNotebookUpdate(item.ID, false)
	if _, err := g.authorize(request(t, "POST", target, nil)); err == nil {
		t.Fatal("owned update permit was not revoked")
	}
}
