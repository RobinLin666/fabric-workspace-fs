package main

import (
	"testing"
)

func TestRealEmptyExportWithoutCellsCanReceiveFixtureMarker(t *testing.T) {
	for _, body := range []string{`{"nbformat":4,"metadata":{}}`, `{"nbformat":4,"cells":null,"metadata":{}}`} {
		updated, err := setNotebookMarker([]byte(body), "only-owned-fixture")
		if err != nil {
			t.Fatal(err)
		}
		if err := hasNotebookMarker(updated, "only-owned-fixture"); err != nil {
			t.Fatal(err)
		}
	}
}
