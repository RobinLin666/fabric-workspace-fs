package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/fusefs"
)

func TestHelpAndVersionNeverAuthenticate(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	for _, args := range [][]string{{"--help"}, {"mount", "--help"}, {"workspaces", "--help"}, {"version"}} {
		var stdout, stderr bytes.Buffer
		code := Run(context.Background(), args, &stdout, &stderr, "test-version")
		if code != 0 || stdout.Len()+stderr.Len() == 0 {
			t.Fatalf("%v returned %d %s%s", args, code, &stdout, &stderr)
		}
	}
}

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		nil, {"unknown"}, {"workspaces", "extra"}, {"mount"},
		{"mount", "--workspace", "not-a-uuid", "point"},
		{"mount", "point"},
		{"mount", "--all-workspaces", "--workspace", "11111111-1111-1111-1111-111111111111", "point"},
		{"mount", "--all-workspaces", "--max-file-size", "-1", "point"},
		{"mount", "--all-workspaces", "--max-writers", "100", "--max-open-handles", "1", "point"},
		{"mount", "--all-workspaces", "--http-timeout", "0s", "point"},
		{"mount", "--all-workspaces", "--max-definition-size", "100", "point"},
	} {
		var output bytes.Buffer
		if code := Run(context.Background(), args, &output, &output, "test"); code != 2 {
			t.Fatalf("%v = %d: %s", args, code, &output)
		}
	}
}

func TestUnavailableMountDoesNotRequestCredentials(t *testing.T) {
	if fusefs.Supported() && fusefs.CheckPrerequisites() == nil {
		t.Skip("current platform has an available mount runtime")
	}
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	var output bytes.Buffer
	if code := Run(context.Background(), []string{"mount", "--all-workspaces", "mountpoint"}, &output, &output, "test"); code != 1 ||
		strings.Contains(output.String(), "Azure Identity") {
		t.Fatalf("unavailable mount requested credentials: %d %s", code, &output)
	}
}

func TestPrivateSpoolDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	got, err := spoolDirectory(dir)
	info, statErr := os.Stat(dir)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if runtime.GOOS == "linux" && info.Mode().Perm()&0077 != 0 {
		// DrvFS can ignore mode 0700. Production must reject that directory,
		// not claim privacy that its backing filesystem cannot provide.
		if err == nil {
			t.Fatal("insecure backing filesystem accepted for spool")
		}
	} else if err != nil || got != dir {
		t.Fatalf("spool: %q %v", got, err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := spoolDirectory(file); err == nil {
		t.Fatal("file accepted as spool directory")
	}
	if runtime.GOOS == "linux" {
		if err := os.Chmod(dir, 0777); err != nil {
			t.Fatal(err)
		}
		if _, err := spoolDirectory(dir); err == nil {
			t.Fatal("world-readable spool accepted")
		}
	}
}
