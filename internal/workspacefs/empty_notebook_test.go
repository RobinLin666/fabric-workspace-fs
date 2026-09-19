package workspacefs

import (
	"context"
	"encoding/base64"
	"os"
	"testing"
)

func TestFabricEmptyNotebookExportCanBeSavedWithoutInventingCells(t *testing.T) {
	for _, original := range []string{`{"nbformat":4,"nbformat_minor":5,"metadata":{}}`, `{"nbformat":4,"cells":null,"metadata":{}}`} {
		s, remote := newTestFS(t, nil)
		def := remote.Definition()
		def.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(original))
		remote.SetDefinition(def)
		_, entry := notebook(t, s)
		h, err := s.Open(context.Background(), entry, os.O_RDWR)
		if err != nil {
			t.Fatal(err)
		}
		if read(t, h) != original {
			t.Fatal("empty export was silently rewritten")
		}
		save(t, h, original+" ")
		if err := h.Close(); err != nil {
			t.Fatal("actual empty export could not be saved", err)
		}
	}
}
