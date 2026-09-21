package workspacefs

import (
	"context"
	"encoding/base64"
	"os"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/testutil"
)

type delayedDefinitionAPI struct {
	FabricAPI
	first   sync.Once
	started chan struct{}
	resume  chan struct{}
}

func (d *delayedDefinitionAPI) GetDefinition(ctx context.Context, ws, item, kind, format string) (fabric.Definition, error) {
	def, err := d.FabricAPI.GetDefinition(ctx, ws, item, kind, format)
	if err != nil {
		return def, err
	}
	block := false
	d.first.Do(func() {
		block = true
		close(d.started)
	})
	if block {
		select {
		case <-ctx.Done():
			return fabric.Definition{}, ctx.Err()
		case <-d.resume:
		}
	}
	return def, nil
}

func TestInvalidatedDefinitionFlightCannotRepublishOldSnapshot(t *testing.T) {
	remote := testutil.New(t)
	fab, lake := remote.Clients()
	delayed := &delayedDefinitionAPI{FabricAPI: fab, started: make(chan struct{}), resume: make(chan struct{})}
	opts := DefaultOptions()
	opts.WorkspaceIDs, opts.SpoolDirectory = []string{testutil.WorkspaceID}, t.TempDir()
	s, err := New(delayed, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nb := directNotebook()
	oldResult := make(chan error, 1)
	go func() {
		_, err := s.Lookup(ctx, nb, "Sample notebook.ipynb")
		oldResult <- err
	}()
	select {
	case <-delayed.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	s.invalidateDefinition(nb)
	current := `{"nbformat":4,"cells":[],"metadata":{"new":"generation"}}`
	def := remote.Definition()
	def.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(current))
	remote.SetDefinition(def)
	entry, err := s.Lookup(ctx, nb, "Sample notebook.ipynb")
	close(delayed.resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-oldResult; err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(ctx, entry, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if read(t, h) != current || remote.Counts().DefinitionReads != 2 || s.SnapshotStats().Decodes != 2 {
		t.Fatal("detached old flight repopulated the decoded snapshot cache")
	}
}
