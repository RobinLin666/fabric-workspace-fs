//go:build linux

package fusefs

import (
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"
)

func TestMountedBuiltinCRUDAndReadonlyEnvironmentResources(t *testing.T) {
	store := testutil.NewResources()
	m := mountFixtureOptions(t, false, func(o *workspacefs.Options) { o.ResourceBackend = store })
	def := m.service.Definition()
	def.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(`{"nbformat":4,"cells":[],"metadata":{"dependencies":{"environment":{"environmentId":"` + testutil.EnvironmentID + `"}}}}`))
	m.service.SetDefinition(def)
	envTarget := resources.Target{WorkspaceID: testutil.WorkspaceID, ItemID: testutil.EnvironmentID, Kind: "Environment"}
	store.Seed(resources.Path{Target: envTarget, Relative: "shared.bin"}, []byte{0xff, 0, 1})
	builtin := filepath.Join(m.notebookDir, "builtin")
	dir := filepath.Join(builtin, "owned")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "binary")
	h, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt([]byte{0xff, 0, 2, 3}, 2); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(5); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	_, writes := store.Counts()
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	closeFile(t, h)
	_, after := store.Counts()
	if after != writes {
		t.Fatal("duplicate resource upload")
	}
	readFile(t, path, string([]byte{0, 0, 0xff, 0, 2}))
	dest := filepath.Join(dir, "renamed")
	if err := os.Rename(path, dest); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("ls", "-la", dir).CombinedOutput(); err != nil {
		t.Fatal("builtin ls", string(output), err)
	}
	if err := os.Remove(dir); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal("builtin rmdir recursed", err)
	}
	if err := os.Remove(dest); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	for _, hidden := range []string{filepath.Join(m.notebookDir, "env"), filepath.Join(m.notebookDir, ".platform"), filepath.Join(m.environment, ".platform")} {
		if _, err := os.Stat(hidden); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("hidden export/alias is visible: %s: %v", hidden, err)
		}
	}
	for _, root := range []string{filepath.Join(m.environment, "resources")} {
		readFile(t, filepath.Join(root, "shared.bin"), string([]byte{0xff, 0, 1}))
		for _, operation := range []func() error{
			func() error { return os.WriteFile(filepath.Join(root, "shared.bin"), nil, 0644) },
			func() error { return os.Truncate(filepath.Join(root, "shared.bin"), 0) },
			func() error { return os.Mkdir(filepath.Join(root, "new"), 0755) },
			func() error { return os.Remove(filepath.Join(root, "shared.bin")) },
			func() error { return os.Rename(filepath.Join(root, "shared.bin"), filepath.Join(builtin, "stolen")) },
		} {
			if err := operation(); !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) {
				t.Fatal("Environment resource readonly bypass", err)
			}
		}
	}
	if err := os.Remove(builtin); err == nil {
		t.Fatal("builtin root removed")
	}
}
