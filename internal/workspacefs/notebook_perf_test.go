package workspacefs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/fabric"
)

type failedExportAPI struct{ FabricAPI }

func (failedExportAPI) GetDefinition(context.Context, string, string, string, string) (fabric.Definition, error) {
	return fabric.Definition{}, errors.New("PRIVATE response body and credential")
}

func TestNotebookDiagnosticsRecordFailedPreflightWithoutPayload(t *testing.T) {
	s, _ := newTestFS(t, nil)
	var events []NotebookEvent
	s.opts.LogNotebookEvent = func(event NotebookEvent) { events = append(events, event) }
	_, e := notebook(t, s)
	h, err := s.Open(context.Background(), e, os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt(context.Background(), []byte(" "), h.Size()); err != nil {
		t.Fatal(err)
	}
	s.fabric.FabricAPI = failedExportAPI{FabricAPI: s.fabric.FabricAPI}
	if err := h.Flush(context.Background()); err == nil {
		t.Fatal("failed preflight reported success")
	}
	if err := h.Close(); err == nil {
		t.Fatal("failed save discarded recovery state")
	}
	stats := s.NotebookStats()
	if stats["save_preflight"].Calls != 1 || stats["save_preflight"].Failures != 1 ||
		stats["definition_export"].Failures != 1 || stats["save_update"].Calls != 0 {
		t.Fatal("incomplete failed-save diagnostics", stats)
	}
	for _, event := range events {
		if strings.Contains(fmt.Sprint(event), "PRIVATE") || event.Elapsed < 0 {
			t.Fatal("event exposed payload or invalid duration", event)
		}
	}
	// A caller cannot mutate the internal fixed-size counters through a snapshot.
	delete(stats, "definition_export")
	if len(s.NotebookStats()) != len(notebookStages) {
		t.Fatal("stats were not isolated")
	}
}
