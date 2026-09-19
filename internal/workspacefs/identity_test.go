package workspacefs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/testutil"
)

func TestIdentityMetadataForEveryWorkspaceAndItem(t *testing.T) {
	backend, remote := newTestFS(t, nil)
	ctx := context.Background()
	workspace := lookup(t, backend, backend.Root(), "Sample workspace")
	nb, _ := notebook(t, backend)
	lake, files, _ := lakeRoots(t, backend)
	env := item(t, backend, "Environments", "Sample environment", testutil.EnvironmentID)
	for _, test := range []struct {
		parent          Entry
		id, kind, label string
	}{
		{workspace, testutil.WorkspaceID, "Workspace", "Sample workspace"},
		{nb, testutil.NotebookID, "Notebook", "Sample notebook"},
		{lake, testutil.LakehouseID, "Lakehouse", "Sample lakehouse"},
		{env, testutil.EnvironmentID, "Environment", "Sample environment"},
	} {
		t.Run(test.kind, func(t *testing.T) {
			before := remote.Counts()
			e := lookup(t, backend, test.parent, ".fabric.json")
			stat, err := backend.Stat(ctx, e)
			if err != nil || stat.Directory || backend.Writable(e) {
				t.Fatal(stat, err)
			}
			handle, err := backend.Open(ctx, e, os.O_RDONLY)
			if err != nil {
				t.Fatal(err)
			}
			data := read(t, handle)
			_ = handle.Close()
			var value map[string]string
			if err := json.Unmarshal([]byte(data), &value); err != nil || value["id"] != test.id ||
				value["type"] != test.kind || value["displayName"] != test.label || stat.Size != int64(len(data)) {
				t.Fatalf("identity metadata: %s %v", data, err)
			}
			if _, err := backend.Open(ctx, e, os.O_WRONLY|os.O_TRUNC); !errors.Is(err, fserrors.ErrReadOnly) {
				t.Fatal("metadata write allowed", err)
			}
			if err := backend.Truncate(ctx, e, 0); !errors.Is(err, fserrors.ErrReadOnly) {
				t.Fatal("metadata truncate allowed", err)
			}
			if err := backend.Remove(ctx, e, false); !errors.Is(err, fserrors.ErrReadOnly) {
				t.Fatal("metadata deletion allowed", err)
			}
			if _, err := backend.Rename(ctx, e, files, "copied", false); !errors.Is(err, fserrors.ErrReadOnly) {
				t.Fatal("metadata rename allowed", err)
			}
			if _, _, err := backend.Create(ctx, test.parent, ".fabric.json", os.O_WRONLY); !errors.Is(err, fserrors.ErrReadOnly) {
				t.Fatal("metadata create allowed", err)
			}
			after := remote.Counts()
			if after != before {
				t.Fatal("identity metadata invoked an unexpected remote API", before, after)
			}
		})
	}
}

func TestRemoteDefinitionCannotShadowIdentityMetadata(t *testing.T) {
	backend, remote := newTestFS(t, func(o *Options) { o.CacheTTL = time.Minute })
	remote.SetEnvironment(fabric.Definition{Parts: []fabric.Part{{
		Path: ".fabric.json", PayloadType: "InlineBase64",
		Payload: base64.StdEncoding.EncodeToString([]byte("actual remote definition part")),
	}}})
	env := item(t, backend, "Environments", "Sample environment", testutil.EnvironmentID)
	e := lookup(t, backend, env, "%2Efabric.json")
	children, err := backend.ReadDir(context.Background(), env)
	if err != nil || len(children) != 5 || children[0].Name != "%2Efabric.json" || children[1].Name != ".fabric.json" {
		t.Fatal("reserved metadata collided with a discovered real part", children, err)
	}
	h, err := backend.Open(context.Background(), e, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if got := read(t, h); got != "actual remote definition part" {
		t.Fatal("remote part was hidden or rewritten", got)
	}
}
