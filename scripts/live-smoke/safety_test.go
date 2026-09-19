package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/transport"
)

const (
	testWorkspace = "11111111-1111-4111-8111-111111111111"
	testLakehouse = "22222222-2222-4222-8222-222222222222"
	testNotebook  = "33333333-3333-4333-8333-333333333333"
	testOperation = "44444444-4444-4444-8444-444444444444"
	testDeleteOp  = "55555555-5555-4555-8555-555555555555"
)

func testOptions() options {
	return options{Workspace: testWorkspace, Lakehouse: testLakehouse, Evidence: "unused-offline.json"}
}

func testPlan(t *testing.T) fixturePlan {
	t.Helper()
	plan, err := newPlan(testOptions(), time.Date(2026, 9, 18, 10, 20, 30, 0, time.UTC),
		bytes.NewReader(bytes.Repeat([]byte{0x6a}, 12)))
	if err != nil {
		t.Fatal("cannot construct offline fixture plan")
	}
	return plan
}

func validArgs(evidence string) []string {
	return []string{"--allow-writes", "--workspace", testWorkspace, "--lakehouse", testLakehouse, "--evidence", evidence}
}

func TestRequiredFlagsRejectBeforeRun(t *testing.T) {
	args := validArgs(filepath.Join(t.TempDir(), "new.json"))
	tests := [][]string{
		nil,
		{"--allow-writes"},
		{"--workspace", testWorkspace, "--lakehouse", testLakehouse, "--evidence", "new.json"},
		{"--allow-writes=false", "--workspace", testWorkspace, "--lakehouse", testLakehouse, "--evidence", "new.json"},
		{"--allow-writes", "--workspace", "not-a-uuid", "--lakehouse", testLakehouse, "--evidence", "new.json"},
		{"--allow-writes", "--workspace", testWorkspace, "--lakehouse", "https://untrusted.invalid", "--evidence", "new.json"},
		append(append([]string{}, args...), "extra"),
		append(append([]string{}, args...), "--endpoint=https://untrusted.invalid"),
		append(append([]string{}, args...), "--overlay-dir=retired-user-overlay"),
		append(append([]string{}, args...), "--overlay-max-file-size=1"),
		append(append([]string{}, args...), "--overlay-max-bytes=1"),
		append(append([]string{}, args...), "--overlay-max-entries=1"),
		{"--allow-writes", "--workspace", testWorkspace, "--lakehouse", testLakehouse, "--evidence", ""},
		{"--allow-writes", "--workspace", testWorkspace, "--lakehouse", testLakehouse, "--evidence", " "},
		{"--help"},
	}
	for i, candidate := range tests {
		var stdout, stderr bytes.Buffer
		if code := execute(candidate, &stdout, &stderr); code != 2 {
			t.Fatalf("invalid flags case %d exited %d, want 2", i, code)
		}
		if stdout.Len() != 0 {
			t.Fatalf("invalid flags case %d reached the runner", i)
		}
	}
	opts, err := parseOptions(args)
	if err != nil || opts.Workspace != testWorkspace || opts.Lakehouse != testLakehouse || !filepath.IsAbs(opts.Evidence) {
		t.Fatal("valid explicit flags were not preserved")
	}
	if _, err := os.Stat(opts.Evidence); !os.IsNotExist(err) {
		t.Fatal("flag parsing created or changed the evidence file")
	}
}

func TestNonLinuxIsExplicitlyUnsupported(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("the live Linux runner is never invoked by offline tests")
	}
	file := filepath.Join(t.TempDir(), "must-not-be-created.json")
	var stdout, stderr bytes.Buffer
	if code := execute(validArgs(file), &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "unsupported") {
		t.Fatal("non-Linux execution was not explicitly unsupported")
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatal("unsupported execution created evidence or reached initialization")
	}
}

func TestPlanAndNotebookPayloadAreUniqueAndBounded(t *testing.T) {
	plan := testPlan(t)
	if !strings.HasPrefix(plan.Stem, "fabricfs-e2e-20260918T102030Z-") || !validComponent(plan.Stem) {
		t.Fatal("fixture name is not the expected safe UTC/random form")
	}
	if plan.NotebookDescription != plan.OwnershipMarker || plan.LakeDirectory != "Files/"+plan.Stem {
		t.Fatal("fixture plan did not bind ownership and directory name")
	}
	for name, path := range plan.RemotePaths {
		if !strings.HasPrefix(path, "https://onelake.dfs.fabric.microsoft.com/"+testWorkspace+"/"+testLakehouse+"/") {
			t.Fatalf("unexpected planned origin for %s", name)
		}
	}
	if _, err := newPlan(testOptions(), time.Now(), bytes.NewReader(nil)); err == nil {
		t.Fatal("missing cryptographic entropy was accepted")
	}
	body, err := fixtureBody(plan)
	if err != nil {
		t.Fatal("fixture request body could not be encoded")
	}
	var request struct {
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
		Definition  struct {
			Format string `json:"format"`
			Parts  []struct {
				Path, Payload, PayloadType string
			} `json:"parts"`
		} `json:"definition"`
	}
	if json.Unmarshal(body, &request) != nil || request.DisplayName != plan.Stem ||
		request.Description != plan.OwnershipMarker || request.Definition.Format != "ipynb" || len(request.Definition.Parts) != 1 {
		t.Fatal("fixture request does not match the notebook create contract")
	}
	part := request.Definition.Parts[0]
	data, err := base64.StdEncoding.DecodeString(part.Payload)
	if err != nil || part.Path != "notebook-content.ipynb" || part.PayloadType != "InlineBase64" {
		t.Fatal("fixture definition part does not use the documented inline ipynb schema")
	}
	doc, metadata, err := notebookDocument(data)
	if err != nil || string(doc["cells"]) != "[]" || metadata["kernelspec"] == nil {
		t.Fatal("initial notebook is not a valid empty nbformat-4 Python notebook")
	}
}

func TestNotebookEditOnlyChangesTargetMetadata(t *testing.T) {
	input := []byte(`{"nbformat":4,"nbformat_minor":5,"cells":[{"cell_type":"code","id":"owned-cell","source":["print(1)"],"metadata":{"x":1},"execution_count":null,"outputs":[]}],"metadata":{"kernelspec":{"name":"python3","language":"python"},"other":{"large":9007199254740993}},"custom":{"preserve":true}}`)
	updated, err := setNotebookMarker(input, "owned-marker")
	if err != nil || hasNotebookMarker(updated, "owned-marker") != nil {
		t.Fatal("notebook marker did not round-trip")
	}
	before, beforeMeta, _ := notebookDocument(input)
	after, afterMeta, _ := notebookDocument(updated)
	delete(afterMeta, "fabricfs_test")
	if !reflect.DeepEqual(beforeMeta, afterMeta) {
		t.Fatal("edit changed unrelated notebook metadata")
	}
	delete(before, "metadata")
	delete(after, "metadata")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("edit changed cells or unrelated top-level notebook fields")
	}
	for _, invalid := range [][]byte{
		[]byte(`null`), []byte(`[]`), []byte(`{"nbformat":3,"cells":[]}`),
		[]byte(`{"nbformat":4,"cells":{}}`), []byte(`{"nbformat":4,"cells":[],"metadata":[]}`),
		bytes.Repeat([]byte{'x'}, maxNotebookBytes+1),
	} {
		if _, err := setNotebookMarker(invalid, "owned"); err == nil {
			t.Fatal("malformed or oversized notebook was accepted")
		}
	}
	if hasNotebookMarker(updated, "different") == nil {
		t.Fatal("a different marker was accepted")
	}
}

func TestMountinfoCleanupGuard(t *testing.T) {
	parent := "/tmp/fabric-workspace-fs-test-live-owned"
	root := "20 1 0:1 / / rw - ext4 /dev/root rw\n"
	line := func(point string) string { return "30 20 0:2 / " + point + " rw - fuse.fabricfs fabricfs rw\n" }
	tests := []struct {
		name    string
		text    string
		mounted bool
		bad     bool
	}{
		{"root only", root, false, false},
		{"own parent", root + line(parent), true, false},
		{"own mount", root + line(parent+"/mount"), true, false},
		{"nested mount", root + line(parent+"/mount/child"), true, false},
		{"same prefix other directory", root + line(parent+"-other/mount"), false, false},
		{"unrelated mount", root + line("/mnt/unrelated"), false, false},
		{"malformed after match", root + line(parent) + "broken\n", false, true},
		{"empty", "", false, true},
		{"bad escape", root + line(parent+`/bad\099`), false, true},
		{"traversal", root + line(parent+"/../other"), false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mounted, err := subtreeMounted(strings.NewReader(tt.text), parent)
			if mounted != tt.mounted || (err != nil) != tt.bad {
				t.Fatalf("guard result mounted=%v err=%v", mounted, err != nil)
			}
		})
	}
	spaceParent := parent + " space"
	if mounted, err := subtreeMounted(strings.NewReader(root+line(parent+`\040space/mount`)), spaceParent); err != nil || !mounted {
		t.Fatal("escaped mountpoint was not detected")
	}
	for _, invalid := range []string{"/tmp", "/home/elsewhere", parent + "/child", "/tmp/fabric-workspace-fs-test-live-"} {
		if _, err := subtreeMounted(strings.NewReader(root), invalid); err == nil {
			t.Fatal("unrecognized cleanup parent was accepted")
		}
	}
	if _, err := subtreeMounted(errorReader{}, parent); err == nil {
		t.Fatal("unreadable mountinfo was treated as safe")
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestPathBoundariesAndComponents(t *testing.T) {
	if !below("Files/owned", "Files/owned/child") || below("Files/owned", "Files/owned-other") {
		t.Fatal("owned directory boundary is not exact")
	}
	for _, value := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b", "a\nb"} {
		if validComponent(value) {
			t.Fatal("unsafe local component was accepted")
		}
	}
	if !validComponent("display name (duplicate)") {
		t.Fatal("safe display-name component was rejected")
	}
}

func TestEvidenceMountGuardNeverSelectsExistingFuseMount(t *testing.T) {
	mounts, err := readMounts(strings.NewReader(
		"20 1 0:1 / / rw - ext4 /dev/root rw\n" +
			"30 20 0:2 / /mnt/existing rw - fuse.fabricfs fabricfs rw\n"))
	if err != nil {
		t.Fatal("valid mount table was rejected")
	}
	for _, candidate := range []string{"/mnt/existing", "/mnt/existing/new.json", "/mnt/existing/sub/new.json"} {
		if !insideFuseMount(candidate, mounts) {
			t.Fatal("evidence guard failed to reject an existing mount subtree")
		}
	}
	if insideFuseMount("/mnt/existing-other/new.json", mounts) || insideFuseMount("/home/user/new.json", mounts) {
		t.Fatal("evidence guard confused a sibling or native directory with a mount")
	}
}

func TestSanitizedErrorsNeverExposeBodiesOrTokens(t *testing.T) {
	secret := "test-secret-must-not-be-logged"
	httpErr := &transport.HTTPError{
		StatusCode: 403, Method: "GET", Path: "/v1/workspaces?token=" + secret,
		Message: secret, Code: secret, RequestID: secret,
	}
	message := safeError(httpErr)
	if !strings.Contains(message, "HTTP 403") || strings.Contains(message, secret) {
		t.Fatal("HTTP error sanitization leaked response fields or query data")
	}
	if strings.Contains(safeError(errors.New(secret)), secret) {
		t.Fatal("arbitrary SDK error details were exposed")
	}
	if safeError(context.Canceled) != "operation canceled" {
		t.Fatal("context cancellation was not classified safely")
	}
}

func TestBoundedJSONResponses(t *testing.T) {
	var target map[string]any
	for _, body := range []string{`{"ok":true} {}`, `{"partial":`, strings.Repeat("x", 101)} {
		if err := decodeSmall(strings.NewReader(body), 100, &target); err == nil {
			t.Fatal("malformed, trailing, or oversized response was accepted")
		}
	}
	if decodeSmall(strings.NewReader(`{"ok":true}`), 100, &target) != nil {
		t.Fatal("valid small response was rejected")
	}
	if decodeSmall(errorReader{}, 100, &target) == nil {
		t.Fatal("response read error was ignored")
	}
	if !isNotFound(&transport.HTTPError{StatusCode: http.StatusNotFound}) || isNotFound(&transport.HTTPError{StatusCode: 403}) {
		t.Fatal("absence detection treated a service failure as absence")
	}
}
