package cli

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/namespace"
)

func TestWorkspaceListingReservesAgentBundleName(t *testing.T) {
	items := []fabric.Workspace{
		{ID: "22222222-2222-2222-2222-222222222222", DisplayName: "Analytics"},
		{ID: "11111111-1111-1111-1111-111111111111", DisplayName: namespace.AgentRootName},
	}
	labels := make([]namespace.Label, len(items))
	for i, item := range items {
		labels[i] = namespace.Label{ID: item.ID, DisplayName: item.DisplayName}
	}
	var rootCatalog namespace.Catalog
	if err := rootCatalog.ReserveName("workspaces", namespace.AgentRootName); err != nil {
		t.Fatal(err)
	}
	rootNames, err := rootCatalog.Names("workspaces", labels)
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[string]string, len(items))
	for i, item := range items {
		expected[item.ID] = rootNames[i]
	}

	var output bytes.Buffer
	if err := writeWorkspaceListing(&output, items); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != len(items)+1 || lines[0] != "WORKSPACE UUID\tDISPLAY NAME\tDIRECTORY" {
		t.Fatalf("workspace listing lost a workspace: %s", &output)
	}
	for _, line := range lines[1:] {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Fatalf("unexpected row: %q", line)
		}
		directory, err := strconv.Unquote(fields[2])
		if err != nil || directory != expected[fields[0]] || directory == namespace.AgentRootName {
			t.Fatalf("workspace directory differs from reserved root catalog: %q, %v", line, err)
		}
	}
	if expected["22222222-2222-2222-2222-222222222222"] != "Analytics" {
		t.Fatal("unrelated workspace name changed")
	}
	reordered := []fabric.Workspace{items[1], items[0]}
	var repeated bytes.Buffer
	if err := writeWorkspaceListing(&repeated, reordered); err != nil {
		t.Fatal(err)
	}
	if repeated.String() != output.String() {
		t.Fatalf("workspace listing changed with discovery order:\n%s\n%s", &output, &repeated)
	}
}

func TestEmptyWorkspaceListingDoesNotInventAgentWorkspace(t *testing.T) {
	var output bytes.Buffer
	if err := writeWorkspaceListing(&output, nil); err != nil {
		t.Fatal(err)
	}
	if output.String() != "WORKSPACE UUID\tDISPLAY NAME\tDIRECTORY\n" {
		t.Fatalf("synthetic bundle appeared as a workspace: %s", &output)
	}
}
