package namespace

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestCatalogConflictsArePredictableWithoutExposingIDs(t *testing.T) {
	labels := []Label{
		{ID: "33333333-3333-3333-3333-333333333333", DisplayName: "Data"},
		{ID: "11111111-1111-1111-1111-111111111111", DisplayName: "Data"},
		{ID: "22222222-2222-2222-2222-222222222222", DisplayName: "Data (2)"},
	}
	var catalog Catalog
	names, err := catalog.Names("items", labels)
	if err != nil || !reflect.DeepEqual(names, []string{"Data (3)", "Data", "Data (2)"}) {
		t.Fatalf("unexpected collision strategy: %v %v", names, err)
	}
	for i, name := range names {
		if strings.Contains(name, labels[i].ID) {
			t.Fatal("identity leaked into a name", name)
		}
	}
	reversed := []Label{labels[2], labels[1], labels[0]}
	var fresh Catalog
	other, err := fresh.Names("items", reversed)
	if err != nil || !reflect.DeepEqual(other, []string{"Data (2)", "Data", "Data (3)"}) {
		t.Fatal("listing order changed names", other, err)
	}
}

func TestCatalogNeverReassignsAnOldPathToANewIdentity(t *testing.T) {
	a := Label{ID: "22222222-2222-2222-2222-222222222222", DisplayName: "Notebook"}
	b := Label{ID: "11111111-1111-1111-1111-111111111111", DisplayName: "Notebook"}
	var catalog Catalog
	names, _ := catalog.Names("items", []Label{a})
	if names[0] != "Notebook" {
		t.Fatal(names)
	}
	names, err := catalog.Names("items", []Label{b, a})
	if err != nil || !reflect.DeepEqual(names, []string{"Notebook (2)", "Notebook"}) {
		t.Fatal("new lower ID stole an existing path", names, err)
	}
	names, _ = catalog.Names("items", []Label{b})
	if names[0] != "Notebook (2)" {
		t.Fatal("deletion reassigned another item's path", names)
	}
	independent, _ := catalog.Names("other-workspace", []Label{b})
	if independent[0] != "Notebook" {
		t.Fatal("independent directory names interfered", independent)
	}
}

func TestCatalogRejectsDuplicateIDsAndHandlesConcurrentReads(t *testing.T) {
	label := Label{ID: "11111111-1111-1111-1111-111111111111", DisplayName: "Display"}
	var catalog Catalog
	if _, err := catalog.Names("items", []Label{label, label}); err == nil {
		t.Fatal("duplicate IDs silently accepted")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			names, err := catalog.Names("items", []Label{label})
			if err != nil || len(names) != 1 || names[0] != "Display" {
				t.Error(names, err)
			}
		})
	}
	wg.Wait()
}
