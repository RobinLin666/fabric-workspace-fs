package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyOverlayFlagsAreRejectedWithoutFilesystemChanges(t *testing.T) {
	data := t.TempDir()
	t.Setenv("XDG_DATA_HOME", data)
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	existing := filepath.Join(data, "fabric-workspace-fs", "overlays")
	if err := os.MkdirAll(existing, 0700); err != nil {
		t.Fatal(err)
	}
	retained := filepath.Join(existing, "retained")
	if err := os.WriteFile(retained, []byte("old data must remain"), 0600); err != nil {
		t.Fatal(err)
	}
	unused := filepath.Join(data, "must-not-create")
	for _, test := range []struct {
		flag  string
		value string
	}{
		{"overlay-dir", existing},
		{"overlay-dir", unused},
		{"overlay-dir", ""},
		{"overlay-max-file-size", "0"},
		{"overlay-max-file-size", "1073741824"},
		{"overlay-max-bytes", "0"},
		{"overlay-max-bytes", "4294967296"},
		{"overlay-max-entries", "0"},
		{"overlay-max-entries", "10000"},
	} {
		for _, flags := range [][]string{
			{"--" + test.flag, test.value},
			{"--" + test.flag + "=" + test.value},
			{"-" + test.flag + "=" + test.value},
		} {
			args := append([]string{"mount", "--all-workspaces"}, flags...)
			args = append(args, "point")
			var output bytes.Buffer
			if status := Run(context.Background(), args, &output, &output, "test"); status != 2 ||
				!strings.Contains(output.String(), "flag provided but not defined: -"+test.flag) {
				t.Fatalf("%v = %d, %s", args, status, &output)
			}
		}
	}
	if _, err := os.Lstat(unused); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed overlay path was created: %v", err)
	}
	if got, err := os.ReadFile(retained); err != nil || string(got) != "old data must remain" {
		t.Fatalf("old overlay data changed: %q, %v", got, err)
	}
}

func TestStorageRootsCannotResolveInsideMount(t *testing.T) {
	base := t.TempDir()
	mount := filepath.Join(base, "mount")
	if err := os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	if outsideMount(mount, mount) || outsideMount(mount, filepath.Join(mount, "new", "spool")) {
		t.Fatal("inside mount accepted")
	}
	if !outsideMount(mount, filepath.Join(base, "data", "spool")) {
		t.Fatal("outside mount rejected")
	}
	link := filepath.Join(base, "alias")
	if err := os.Symlink(mount, link); err != nil {
		t.Skipf("symlink privilege unavailable: %v", err)
	}
	if outsideMount(mount, filepath.Join(link, "new", "spool")) {
		t.Fatal("symlink ancestor bypassed mount boundary")
	}
}
