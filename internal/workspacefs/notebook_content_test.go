package workspacefs

import (
	"context"
	"sync"
	"testing"
)

type fakeNotebookContentAPI struct {
	mu   sync.Mutex
	data []byte
	gets int
	puts int
}

func (f *fakeNotebookContentAPI) GetNotebookContent(_ context.Context, _, _ string) ([]byte, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	return append([]byte(nil), f.data...), `"notebook-content"`, nil
}

func (f *fakeNotebookContentAPI) PutNotebookContent(_ context.Context, _, _ string, data []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	f.data = append([]byte(nil), data...)
	return `"updated-content"`, nil
}

func TestNotebookContentAPIDrivesNotebookReadAndWrite(t *testing.T) {
	content := &fakeNotebookContentAPI{data: []byte(`{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[]}`)}
	s, _ := newTestFS(t, func(opts *Options) { opts.NotebookContentAPI = content })
	_, e := notebook(t, s)
	h, err := s.Open(context.Background(), e, 2)
	if err != nil {
		t.Fatal(err)
	}
	updated := []byte(`{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"cell_type":"markdown","metadata":{},"source":["saved through MWC"]}]}`)
	if _, err := h.WriteAt(context.Background(), updated, 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(context.Background(), int64(len(updated))); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	content.mu.Lock()
	defer content.mu.Unlock()
	if content.gets != 2 || content.puts != 1 || string(content.data) != string(updated) {
		t.Fatalf("content API calls=%d/%d data=%q", content.gets, content.puts, content.data)
	}
}
