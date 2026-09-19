package transport

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestMWCSchemeStillRequiresExactOriginBeforeToken(t *testing.T) {
	var authCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "MwcToken opaque-test-grant" {
			t.Error("wrong MWC authorization scheme")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	client, err := New(Options{BaseURL: server.URL, Scope: "bound-resource", AuthorizationScheme: "MwcToken",
		Tokens: tokenFunc(func(context.Context, string) (string, error) { authCalls.Add(1); return "opaque-test-grant", nil })})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Request(context.Background(), "GET", "https://other.example/steal", nil, nil, true); !errors.Is(err, fs.ErrInvalid) || authCalls.Load() != 0 {
		t.Fatal("foreign MWC target reached credentials", err)
	}
	response, err := client.Request(context.Background(), "GET", "/resource", http.Header{"Authorization": {"Bearer caller-value"}}, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if authCalls.Load() != 1 {
		t.Fatal("unexpected authorization count")
	}
}

func TestAuthorizationSchemeIsNotArbitrary(t *testing.T) {
	for _, scheme := range []string{"Basic", "Bearer injected", "MwcToken\r\nHeader: injected", "mwctoken"} {
		opts := options("https://origin.example")
		opts.AuthorizationScheme = scheme
		if _, err := New(opts); !errors.Is(err, fs.ErrInvalid) {
			t.Fatal("arbitrary authorization scheme accepted", scheme, err)
		}
	}
}
