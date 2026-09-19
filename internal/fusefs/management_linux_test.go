//go:build linux

package fusefs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"fabric-workspace-fs/internal/workspacefs"
)

func TestMountedManagedMkdirAndGuardedRmdir(t *testing.T) {
	m := mountFixtureOptions(t, false, func(o *workspacefs.Options) { o.CacheTTL = time.Hour })
	workspace := m.workspace
	folder := filepath.Join(workspace, "Created")
	if err := os.Mkdir(folder, 0755); err != nil {
		t.Fatal("ordinary folder create", err)
	}
	folderMeta := readIdentity(t, folder)
	if folderMeta["type"] != "Folder" {
		t.Fatal("unknown suffix did not create a Fabric folder", folderMeta)
	}
	var created []string
	for _, kind := range []string{"Notebook", "Lakehouse", "Environment"} {
		path := filepath.Join(folder, "Default."+kind)
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatal("typed item mkdir", kind, err)
		}
		created = append(created, path)
		metadata := readIdentity(t, path)
		if metadata["type"] != kind || metadata["folderId"] != folderMeta["id"] {
			t.Fatal("item creation lost parent/type", metadata)
		}
		if output, err := exec.Command("ls", "-la", path).CombinedOutput(); err != nil {
			t.Fatalf("new item ls: %s %v", output, err)
		}
		if err := os.Mkdir(path, 0755); !errors.Is(err, syscall.EEXIST) {
			t.Fatal("duplicate item mkdir", err)
		}
		if err := os.Rename(path, path+"-renamed"); !errors.Is(err, syscall.ENOTSUP) {
			t.Fatal("remote rename must not be faked", err)
		}
	}
	if err := os.Remove(folder); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal("nonempty Fabric folder deletion", err)
	}
	if _, err := os.ReadDir(folder); err != nil {
		t.Fatal(err)
	}
	before := m.service.Counts()
	dotFolder := filepath.Join(folder, ".agents")
	// Dot paths now follow Fabric name validation, never an implicit local
	// overlay. The real management client rejects this folder display name.
	if err := os.Mkdir(dotFolder, 0700); !errors.Is(err, syscall.EINVAL) {
		t.Fatal("dot directory bypassed remote name validation", err)
	}
	requireMissing(t, dotFolder)
	if after := m.service.Counts(); after.ManagedCreates != before.ManagedCreates || after.ManagedDeletes != before.ManagedDeletes {
		t.Fatal("rejected dot folder made a remote mutation", before, after)
	}
	for _, path := range created {
		meta := readIdentity(t, path)
		if meta["type"] == "Lakehouse" {
			if err := os.Remove(path); err != nil {
				t.Fatal("empty Lakehouse removal", err)
			}
		} else {
			before := m.service.Counts().ManagedDeletes
			if err := os.Remove(path); !errors.Is(err, syscall.ENOTSUP) {
				t.Fatal("item with unverifiable hidden resources was deleted", err)
			}
			if m.service.Counts().ManagedDeletes != before {
				t.Fatal("ENOTSUP path sent DELETE")
			}
			client, _ := m.service.Clients()
			if err := client.DeleteItem(context.Background(), meta["workspaceId"], meta["id"]); err != nil {
				t.Fatal("owned mock fixture REST cleanup", err)
			}
		}
		if meta["type"] == "Lakehouse" {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("deleted identity still visible", err)
			}
		}
	}
	if err := os.Remove(folder); err != nil {
		t.Fatal("empty folder removal", err)
	}
	if _, err := os.Stat(folder); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("removed folder remained cached", err)
	}
}

func readIdentity(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(path, ".fabric.json"))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]string
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	if value["id"] == "" || value["displayName"] == "" {
		t.Fatal("missing identity metadata", value)
	}
	return value
}

func TestMountedReadOnlyRejectsEveryCreationRoute(t *testing.T) {
	m := mountFixture(t, true)
	root := m.workspace
	for _, name := range []string{"New.Notebook", "New.Lakehouse", "New.Environment", "NewFolder", ".agents", ".Notebook"} {
		err := os.Mkdir(filepath.Join(root, name), 0755)
		if !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) {
			t.Fatal("readonly creation", name, err)
		}
	}
}
