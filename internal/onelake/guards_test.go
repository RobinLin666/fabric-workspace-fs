package onelake

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

type guardToken struct{}

func (guardToken) Token(context.Context, string) (string, error) { return "guard-offline-token", nil }

func guardsClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	httpClient, err := transport.New(transport.Options{BaseURL: server.URL, Scope: Scope, Tokens: guardToken{}, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return New(httpClient, Options{})
}

func TestIgnoredSafetyHeadersAreNotSuccess(t *testing.T) {
	client := guardsClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-rejected-headers", "If-None-Match")
		w.WriteHeader(201)
	})
	if err := client.Mkdir(context.Background(), testPath("Files/dir")); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("ignored no-overwrite condition was reported successful", err)
	}
	client = guardsClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-rejected-headers", "x-ms-other, if-match")
		w.Header().Set("Content-Range", "bytes 0-0/1")
		w.Header().Set("ETag", `"old"`)
		w.WriteHeader(206)
		fmt.Fprint(w, "x")
	})
	if _, err := client.Read(context.Background(), testPath("Files/a"), 0, make([]byte, 1), `"old"`); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("ignored read condition was reported successful", err)
	}
}

func TestReadRequiresETagForSnapshot(t *testing.T) {
	client := guardsClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/1")
		w.WriteHeader(206)
		fmt.Fprint(w, "x")
	})
	if _, err := client.Read(context.Background(), testPath("Files/a"), 0, make([]byte, 1), `"expected"`); err == nil {
		t.Fatal("unverifiable snapshot read accepted")
	}
}

func TestAggregateListingIsBounded(t *testing.T) {
	var calls atomic.Int64
	path := testPath("Files")
	client := guardsClient(t, func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		w.Header().Set("x-ms-continuation", fmt.Sprintf("page-%d", call))
		fmt.Fprintf(w, `{"paths":[{"name":"%s/Files/%d","contentLength":"1","etag":"tag"}]}`, path.Item, call)
	})
	client.maxEntries = 2
	if _, err := client.List(context.Background(), path); !errors.Is(err, fserrors.ErrTooLarge) || calls.Load() != 3 {
		t.Fatalf("aggregate listing limit: requests=%d error=%v", calls.Load(), err)
	}
}
