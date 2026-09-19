package workspacefs

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/testutil"
)

type rootWorkspaceAPI struct {
	FabricAPI
	workspaces []fabric.Workspace
}

func (a *rootWorkspaceAPI) ListWorkspaces(context.Context) ([]fabric.Workspace, error) {
	return append([]fabric.Workspace(nil), a.workspaces...), nil
}

func TestMountRootContainsWorkspacesWithoutWrapperAndReservesAgents(t *testing.T) {
	api := &rootWorkspaceAPI{workspaces: []fabric.Workspace{
		{ID: testutil.WorkspaceID, DisplayName: ".agents"},
		{ID: "55555555-5555-5555-5555-555555555555", DisplayName: ".agents (2)"},
	}}
	opts := DefaultOptions()
	opts.AllWorkspaces, opts.SpoolDirectory, opts.Version = true, t.TempDir(), "test-version"
	s, err := New(api, &emptyManagementLake{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.ReadDir(context.Background(), s.Root())
	if err != nil || len(entries) != 3 {
		t.Fatal(entries, err)
	}
	bundle := lookup(t, s, s.Root(), ".agents")
	if bundle.Kind != AgentDirectory {
		t.Fatal("a workspace shadowed the reserved instruction root")
	}
	first := lookup(t, s, s.Root(), ".agents (3)")
	second := lookup(t, s, s.Root(), ".agents (2)")
	if first.Kind != Workspace || first.Workspace != testutil.WorkspaceID || second.Workspace != api.workspaces[1].ID {
		t.Fatal("a reserved-name collision hid or rebound a workspace", first, second)
	}
	if _, err := s.Lookup(context.Background(), s.Root(), "Workspaces"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("legacy Workspaces wrapper still exposed", err)
	}
	if e := lookup(t, s, bundle, "AGENT.md"); e.Kind != AgentFile || s.Writable(e) || e.Size == 0 {
		t.Fatal("injected instructions are absent or mutable", e)
	}
}

func TestWorkspaceDotNamesNeverRouteToLocalOverlay(t *testing.T) {
	s, remote := newTestFS(t, nil)
	workspace := workspaceRoot(t, s)
	before := remote.Counts()
	if _, err := s.Mkdir(context.Background(), workspace, ".agents"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal("dot folder bypassed Fabric's actual name validation", err)
	}
	if remote.Counts().ManagedCreates != before.ManagedCreates {
		t.Fatal("invalid Fabric folder name reached a remote create")
	}
	if _, err := s.Lookup(context.Background(), workspace, ".agents"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("phantom local overlay appeared after rejected create", err)
	}
}
