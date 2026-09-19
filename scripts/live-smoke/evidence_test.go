package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestEvidenceIsExclusiveDurableAndUpdated(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "evidence.json")
	doc, err := initialReport(testOptions())
	if err != nil {
		t.Fatal("cannot construct evidence")
	}
	journal, err := openEvidence(path, doc)
	if err != nil {
		t.Fatalf("initial private evidence failed: %s", safeError(err))
	}
	if _, err := openEvidence(path, doc); err == nil {
		t.Fatal("existing evidence was overwritten")
	}
	for range 3 {
		doc.Status = "running"
		doc.ReturnedFixtureID = testNotebook
		doc.Plan.RemotePaths["notebook-item"] = "https://api.fabric.microsoft.com/v1/workspaces/" + testWorkspace + "/items/" + testNotebook
		if err := journal.save(doc); err != nil {
			t.Fatalf("evidence replacement failed: %s", safeError(err))
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("evidence did not persist")
	}
	var saved report
	if json.Unmarshal(data, &saved) != nil || saved.Status != "running" || saved.ReturnedFixtureID != testNotebook ||
		len(saved.Plan.RemotePaths) == 0 || saved.UpdatedAt.IsZero() {
		t.Fatal("durable evidence omitted lifecycle/identity/plan fields")
	}
	info, err := os.Stat(path)
	if err != nil || (runtime.GOOS == "linux" && info.Mode().Perm() != 0600) {
		t.Fatal("evidence file is not private")
	}
	children, err := os.ReadDir(directory)
	if err != nil || len(children) != 1 || children[0].Name() != "evidence.json" {
		t.Fatal("successful evidence replacement left temporary files")
	}
}

func TestEvidenceRefusesExistingAndReplacedUserFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "evidence.json")
	doc, _ := initialReport(testOptions())
	const original = "user-owned-content"
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal("test setup failed")
	}
	if _, err := openEvidence(path, doc); err == nil {
		t.Fatal("existing user file was accepted")
	} else {
		var own *problem
		if !errors.As(err, &own) || own.code != 2 {
			t.Fatal("existing evidence should fail preflight with exit 2")
		}
	}
	data, _ := os.ReadFile(path)
	if string(data) != original {
		t.Fatal("existing user file changed")
	}
	path = filepath.Join(directory, "owned.json")
	journal, err := openEvidence(path, doc)
	if err != nil {
		t.Fatal("private evidence setup failed")
	}
	replacement := filepath.Join(directory, "replacement.json")
	if err := os.WriteFile(replacement, []byte(original), 0600); err != nil {
		t.Fatal("replacement setup failed")
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal("test replacement failed")
	}
	if journal.save(doc) == nil {
		t.Fatal("externally replaced evidence was overwritten")
	}
	data, _ = os.ReadFile(path)
	if string(data) != original {
		t.Fatal("replacement user file changed")
	}
}

func TestEvidenceMissingParentIsPreflightError(t *testing.T) {
	doc, _ := initialReport(testOptions())
	_, err := openEvidence(filepath.Join(t.TempDir(), "absent", "evidence.json"), doc)
	var own *problem
	if !errors.As(err, &own) || own.code != 2 {
		t.Fatal("missing evidence parent did not fail before remote work")
	}
}

func TestNewEvidenceHasNoOverlayLifecycle(t *testing.T) {
	doc, err := initialReport(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{`"overlayDirectory"`, `"createdOverlayPaths"`, `"overlayName"`, `"overlay"`} {
		if strings.Contains(string(data), retired) {
			t.Fatalf("new receipt still records an overlay lifecycle: %s", retired)
		}
	}
	if err := doc.refuseLegacyOverlayCleanup(); err != nil || doc.Cleanup.Overlay != "" {
		t.Fatal("new run needs no overlay cleanup or fabricated completion status")
	}
}

func TestLegacyOverlayReceiptsRemainParseableButNeverAuthorizeCleanup(t *testing.T) {
	for _, input := range []string{
		`{"overlayDirectory":"unopened-retired-store"}`,
		`{"createdOverlayPaths":{".agents/source.txt":false}}`,
		`{"cleanup":{"overlay":"retained-after-incomplete-local-cleanup"}}`,
		`{"overlayDirectory":"unopened-retired-store","cleanup":{"overlay":"owned-local-paths-confirmed-absent"}}`,
		`{"createdOverlayPaths":{".agents":true},"cleanup":{"overlay":"local-crud-complete-zero-http-mutations"}}`,
	} {
		var doc report
		if err := json.Unmarshal([]byte(input), &doc); err != nil {
			t.Fatal("old receipt is not parseable", err)
		}
		directory := doc.OverlayDirectory
		paths, _ := json.Marshal(doc.CreatedOverlayPaths)
		for range 2 {
			err := doc.refuseLegacyOverlayCleanup()
			if err == nil || !strings.Contains(safeError(err), "unsupported") ||
				doc.Cleanup.Overlay != "unsupported-legacy-overlay-preserved" {
				t.Fatal("retired overlay cleanup was silently treated as complete")
			}
			current, _ := json.Marshal(doc.CreatedOverlayPaths)
			if doc.OverlayDirectory != directory || string(current) != string(paths) {
				t.Fatal("legacy path evidence was discarded or rewritten")
			}
		}
	}
	var planned report
	if err := json.Unmarshal([]byte(`{"plan":{"managed":{"overlayName":".agents"}},"cleanup":{"overlay":"not-created"}}`), &planned); err != nil {
		t.Fatal(err)
	}
	if err := planned.refuseLegacyOverlayCleanup(); err != nil || planned.Cleanup.Overlay != "not-created" {
		t.Fatal("an unattempted legacy plan was treated as created or cleaned")
	}
}
