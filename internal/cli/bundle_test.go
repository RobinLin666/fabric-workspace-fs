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

func TestFNTKRejectsRelativePathsBeforeCredentials(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	for _, path := range []string{"fntk", filepath.Join("bin", "fntk"), "."} {
		var output bytes.Buffer
		code := Run(context.Background(), []string{"mount", "--all-workspaces", "--fntk", path, "point"}, &output, &output, "v4-test")
		if code != 2 || !strings.Contains(output.String(), "--fntk must be an absolute native executable path") {
			t.Fatalf("relative fntk path %q = %d, %s", path, code, &output)
		}
	}
}

func TestFNTKRejectsUnavailableAndNonFilePaths(t *testing.T) {
	for _, path := range []string{
		filepath.Join(t.TempDir(), "missing-fntk"),
		t.TempDir(),
	} {
		if err := validateFNTKExecutable(path); err == nil {
			t.Fatalf("invalid fntk path accepted: %q", path)
		}
	}
}

func TestFNTKAcceptsAbsoluteNativePathAtPlatformBoundary(t *testing.T) {
	if fusefs.Supported() && fusefs.CheckPrerequisites() == nil {
		t.Skip("current platform has an available mount runtime; validation is covered directly")
	}
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	path := filepath.Join(t.TempDir(), "fntk.exe")
	if err := os.WriteFile(path, []byte("external fntk test executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := Run(context.Background(), []string{"mount", "--all-workspaces", "--fntk", path, "point"}, &output, &output, "v4-test")
	if code != 1 || (!strings.Contains(output.String(), "not supported") &&
		!strings.Contains(output.String(), "without CGO") && !strings.Contains(output.String(), "WinFsp")) {
		t.Fatalf("absolute fntk path %q = %d, %s", path, code, &output)
	}
}

func TestValidateFNTKExecutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fntk")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validateFNTKExecutable(path); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o111 != 0 {
			t.Skip("test filesystem does not persist Unix executable mode changes")
		}
		if err := validateFNTKExecutable(path); err == nil || !strings.Contains(err.Error(), "not executable") {
			t.Fatalf("non-executable fntk accepted: %v", err)
		}
	}
}

func TestHelpDescribesDirectWorkspaceRootAndReadonlyBundle(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	for _, args := range [][]string{{"--help"}, {"mount", "--help"}} {
		var output bytes.Buffer
		if code := Run(context.Background(), args, &output, &output, "v4-test"); code != 0 {
			t.Fatalf("help %v = %d, %s", args, code, &output)
		}
		if !strings.Contains(output.String(), "workspace display-name directories") &&
			!strings.Contains(output.String(), "Workspace display-name directories") {
			t.Fatalf("missing direct workspace root in help: %s", &output)
		}
		if !strings.Contains(output.String(), "read-only /.agents") ||
			strings.Contains(output.String(), "/Workspaces") ||
			strings.Contains(output.String(), "persistent local overlays") {
			t.Fatalf("stale mount layout in help: %s", &output)
		}
		if len(args) > 1 && (!strings.Contains(output.String(), "-fntk") || strings.Contains(output.String(), "-exec-helper")) {
			t.Fatalf("missing external fntk option or stale helper option in mount help: %s", &output)
		}
		for _, legacy := range []string{"-overlay-dir", "-overlay-max-file-size", "-overlay-max-bytes", "-overlay-max-entries"} {
			if strings.Contains(output.String(), legacy) {
				t.Fatalf("help advertises removed %s flag: %s", legacy, &output)
			}
		}
	}
}
