//go:build linux

package fusefs

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"
)

func TestMountedFabricFolderHierarchyAndContentFilename(t *testing.T) {
	m := mountFixture(t, false)
	const parentID = "77777777-7777-7777-7777-777777777777"
	const childID = "88888888-8888-8888-8888-888888888888"
	m.service.SetFolders([]fabric.Folder{
		{ID: childID, DisplayName: "Nested", ParentFolderID: parentID},
		{ID: parentID, DisplayName: "Projects"},
	})
	m.service.SetItemFolder(testutil.NotebookID, childID)
	m.service.UseRegionalDefinitionPolling()
	workspace := m.workspace
	notebook := filepath.Join(workspace, "Projects", "Nested", "Sample notebook.Notebook")
	for _, directory := range []string{workspace, filepath.Join(workspace, "Projects"), filepath.Join(workspace, "Projects", "Nested"), notebook} {
		for _, args := range [][]string{{"ls", directory}, {"ls", "-la", directory}, {"find", directory, "-maxdepth", "1", "-print"}} {
			if output, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
				t.Fatalf("%v: %s %v", args, output, err)
			}
		}
	}
	readFile(t, filepath.Join(notebook, "Sample notebook.ipynb"), testutil.InitialNotebook)
	data, err := os.ReadFile(filepath.Join(notebook, ".fabric.json"))
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]string
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["folderId"] != childID || metadata["remotePartPath"] != "notebook-content.ipynb" {
		t.Fatal("missing bound folder/part identity", metadata)
	}
	for _, group := range []string{"Notebooks", "Lakehouses", "Environments"} {
		if _, err := os.Stat(filepath.Join(workspace, group)); !os.IsNotExist(err) {
			t.Fatal("old grouped layout remains", group, err)
		}
	}
}

func TestMountedInjectedAgentBundleIsReadonlyAndHTTPFree(t *testing.T) {
	m := mountFixtureOptions(t, false, func(o *workspacefs.Options) {
		o.CacheTTL = time.Hour
		o.FNTKExecutable = "/opt/fntk/bin/fntk"
	})
	before := m.service.Counts()
	root := m.root
	if entries, err := os.ReadDir(root); err != nil || len(entries) == 0 {
		t.Fatal("injected instructions are missing", entries, err)
	}
	file := filepath.Join(root, "AGENTS.md")
	if data, err := os.ReadFile(file); err != nil || len(data) == 0 {
		t.Fatal("injected instructions are empty", err)
	} else if !bytes.Contains(data, []byte("/opt/fntk/bin/fntk")) ||
		!bytes.Contains(data, []byte("fabric-notebook-workflow")) {
		t.Fatal("injected instructions do not advertise the notebook workflow")
	}
	skills := 0
	err := filepath.WalkDir(filepath.Join(root, ".agents", "skills"), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err == nil && len(data) != 0 {
			skills++
		}
		return err
	})
	if err != nil || skills == 0 {
		t.Fatal("nested injected skills are missing or unreadable", err)
	}
	if info, err := os.Stat(file); err != nil || info.Mode().Perm() != 0444 {
		t.Fatal("injected instructions are not read-only", info, err)
	}
	for _, operation := range []func() error{
		func() error { return os.WriteFile(file, []byte("not allowed"), 0644) },
		func() error { return os.Truncate(file, 0) },
		func() error { return os.WriteFile(filepath.Join(root, "new"), nil, 0644) },
		func() error { return os.Mkdir(filepath.Join(root, "new-directory"), 0755) },
		func() error { return os.Remove(file) },
		func() error { return os.Remove(filepath.Join(root, ".agents")) },
		func() error { return os.Rename(file, filepath.Join(root, "renamed")) },
	} {
		if err := operation(); !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) {
			t.Fatal("injected instructions allowed a mutation", err)
		}
	}
	if after := m.service.Counts(); after != before {
		t.Fatalf("injected instructions made Fabric HTTP calls: before=%+v after=%+v", before, after)
	}
	// Resolving a destination outside .agents may discover the root's remote
	// workspaces before the kernel rejects the read-only parent mutation.
	if err := os.Rename(root, filepath.Join(m.root, ".agents-renamed")); !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) {
		t.Fatal("injected root was renamed", err)
	}
	if _, err := os.Stat(m.workspace); err != nil {
		t.Fatal(err)
	}
	requireMissing(t, filepath.Join(m.root, "Workspaces"))
}
