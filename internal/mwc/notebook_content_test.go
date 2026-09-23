package mwc

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestNotebookContentUsesFNTKMWCContract(t *testing.T) {
	w := newWire(t)
	notebook := []byte(`{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[]}`)
	w.hook = func(r *http.Request, body []byte) (*http.Response, error, bool) {
		if !strings.HasSuffix(r.URL.Path, "/content") {
			return nil, nil, false
		}
		resp := response(http.StatusOK, nil)
		resp.Header.Set("ETag", `"content-version"`)
		switch r.Method {
		case http.MethodGet:
			resp = response(http.StatusOK, notebook)
			resp.Header.Set("Content-Type", "application/json")
			resp.Header.Set("ETag", `"content-version"`)
			return resp, nil, true
		case http.MethodPut:
			if !bytes.Equal(body, notebook) {
				t.Fatalf("PUT payload = %q", body)
			}
			return resp, nil, true
		default:
			return response(http.StatusMethodNotAllowed, nil), nil, true
		}
	}
	c := w.client()
	got, etag, err := c.GetNotebookContent(context.Background(), testWorkspace, testItem)
	if err != nil || !bytes.Equal(got, notebook) || etag != `"content-version"` {
		t.Fatalf("GetNotebookContent = %q, %q, %v", got, etag, err)
	}
	etag, err = c.PutNotebookContent(context.Background(), testWorkspace, testItem, notebook)
	if err != nil || etag != `"content-version"` {
		t.Fatalf("PutNotebookContent = %q, %v", etag, err)
	}
	wantPath := "/webapi/capacities/" + testCapacity +
		"/workloads/Notebook/Data/Automatic/api/workspaces/" + testWorkspace +
		"/artifacts/" + testItem + "/content"
	var contentRequests []recordedRequest
	for _, request := range w.recorded() {
		if request.path == wantPath {
			contentRequests = append(contentRequests, request)
		}
	}
	if len(contentRequests) != 2 {
		t.Fatalf("content requests = %+v", contentRequests)
	}
	for _, request := range contentRequests {
		if request.host != "resources.mwc.test" || request.headers.Get("Authorization") != "MwcToken "+testMWC {
			t.Fatalf("request did not use bound MWC grant: %+v", request)
		}
	}
	if contentRequests[1].headers.Get("If-Match") != "" || contentRequests[1].headers.Get("Content-Type") != "application/json" {
		t.Fatal("PUT did not preserve fntk's unconditional JSON contract")
	}
}

func TestNotebookContentBoundsResponse(t *testing.T) {
	w := newWire(t)
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "/content") {
			return response(http.StatusOK, bytes.Repeat([]byte("x"), 9)), nil, true
		}
		return nil, nil, false
	}
	c := w.client(func(o *Options) { o.MaxFileSize = 8 })
	if _, _, err := c.GetNotebookContent(context.Background(), testWorkspace, testItem); err == nil {
		t.Fatal("oversized Notebook content was accepted")
	}
}

func TestNotebookContentUnwrapsJSONStringResponse(t *testing.T) {
	w := newWire(t)
	notebook := []byte(`{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[]}`)
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "/content") && r.Method == http.MethodGet {
			wrapped, err := json.Marshal(string(notebook))
			if err != nil {
				t.Fatal(err)
			}
			return response(http.StatusOK, wrapped), nil, true
		}
		return nil, nil, false
	}
	got, _, err := w.client().GetNotebookContent(context.Background(), testWorkspace, testItem)
	if err != nil || !bytes.Equal(got, notebook) {
		t.Fatalf("GetNotebookContent = %q, %v", got, err)
	}
}
