//go:build linux

package fusefs

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/testutil"
)

func TestMountedDeepLSFindAndIdentityMetadata(t *testing.T) {
	m := mountFixture(t, false)
	m.service.UseRegionalDefinitionPolling()
	workspace := m.workspace
	lake := filepath.Dir(m.files)
	for _, directory := range []string{m.root, workspace, m.notebookDir, lake, m.environment, m.files, m.tables} {
		for _, command := range [][]string{{"ls", directory}, {"ls", "-la", directory}, {"find", directory, "-maxdepth", "1", "-print"}} {
			output, err := exec.Command(command[0], command[1:]...).CombinedOutput()
			if err != nil {
				t.Fatalf("ordinary %v failed: %s %v", command, output, err)
			}
			if strings.Contains(string(output), "~"+testutil.WorkspaceID) || strings.Contains(string(output), "~"+testutil.NotebookID) {
				t.Fatalf("ID-suffixed path still exposed: %s", output)
			}
		}
	}
	for _, test := range []struct{ directory, id, kind string }{
		{workspace, testutil.WorkspaceID, "Workspace"},
		{m.notebookDir, testutil.NotebookID, "Notebook"},
		{lake, testutil.LakehouseID, "Lakehouse"},
		{m.environment, testutil.EnvironmentID, "Environment"},
	} {
		path := filepath.Join(test.directory, ".fabric.json")
		data, err := os.ReadFile(path)
		var metadata map[string]string
		if err != nil || json.Unmarshal(data, &metadata) != nil || metadata["id"] != test.id ||
			metadata["type"] != test.kind || metadata["displayName"] == "" {
			t.Fatalf("bad identity metadata %s: %s %v", path, data, err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0444 || info.Size() != int64(len(data)) {
			t.Fatal("metadata stat/mode incorrect", info, err)
		}
		for _, operation := range []func() error{
			func() error { return os.WriteFile(path, []byte("not allowed"), 0644) },
			func() error { return os.Truncate(path, 0) },
			func() error { return os.Remove(path) },
			func() error { return os.Rename(path, filepath.Join(m.files, "stolen-meta")) },
			func() error { return os.Rename(filepath.Join(m.files, "demo.txt"), path) },
		} {
			if err := operation(); err == nil {
				t.Fatalf("identity metadata mutation succeeded: %s", path)
			}
		}
	}
	readFile(t, m.notebook, testutil.InitialNotebook)
}
