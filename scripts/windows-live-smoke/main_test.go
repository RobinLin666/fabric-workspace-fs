//go:build windows

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSelectDriveRejectsInvalidPaths(t *testing.T) {
	for _, path := range []string{"C:", "m:", "M:\\", "AA:", "D:\\child", "1:", "Z"} {
		if _, err := selectDrive(path); err == nil {
			t.Errorf("accepted invalid mountpoint %q", path)
		}
	}
}

func TestSyncedNotebookSavesPreserveDistinctCellMarkers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "content.ipynb")
	for _, marker := range []string{"first-owned-revision-longer", "second", "node-revision"} {
		data := ownedNotebookContent(marker)
		var notebook struct {
			Cells []struct {
				CellType string   `json:"cell_type"`
				Source   []string `json:"source"`
			} `json:"cells"`
		}
		if err := json.Unmarshal(data, &notebook); err != nil {
			t.Fatal(err)
		}
		if len(notebook.Cells) != 1 || notebook.Cells[0].CellType != "markdown" ||
			len(notebook.Cells[0].Source) != 1 || notebook.Cells[0].Source[0] != marker {
			t.Fatal("owned fixture must contain one non-executable revision marker")
		}
		if err := saveNotebookSynced(path, data); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("saved bytes differ: %v", err)
		}
	}
	if err := saveNotebookSynced(filepath.Join(t.TempDir(), "missing", "content.ipynb"), nil); err == nil {
		t.Fatal("missing directory error was swallowed")
	}
}
