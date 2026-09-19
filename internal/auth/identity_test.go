package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fabric-workspace-fs/internal/transport"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type fakeCredential struct {
	get func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error)
}

func (f fakeCredential) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return f.get(ctx, opts)
}

func TestTokenSourceCachesEachAudience(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "context-preserved")
	var scopes []string
	source := &tokenSource{credential: fakeCredential{get: func(got context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
		if got.Value(key{}) != "context-preserved" || len(opts.Scopes) != 1 {
			t.Error("credential options/context not preserved")
		}
		scopes = append(scopes, opts.Scopes...)
		return azcore.AccessToken{Token: fmt.Sprintf("token-%d", len(scopes)), ExpiresOn: time.Now().Add(time.Hour)}, nil
	}}}
	want := []string{FabricScope, OneLakeScope, FabricScope}
	for i, scope := range want {
		token, err := source.Token(ctx, scope)
		if err != nil || token != fmt.Sprintf("token-%d", i%2+1) {
			t.Fatalf("unexpected token: %q, %v", token, err)
		}
	}
	if !reflect.DeepEqual(scopes, want[:2]) {
		t.Fatal(scopes)
	}
}

func TestDualAudienceTransportUsesSeparateTokens(t *testing.T) {
	var scopes []string
	source := &tokenSource{credential: fakeCredential{get: func(_ context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
		scopes = append(scopes, opts.Scopes[0])
		if opts.Scopes[0] == FabricScope {
			return azcore.AccessToken{Token: "fabric-offline-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
		}
		return azcore.AccessToken{Token: "storage-offline-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
	}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "Bearer fabric-offline-token"
		if r.URL.Path == "/storage" {
			want = "Bearer storage-offline-token"
		}
		if r.Header.Get("Authorization") != want {
			t.Errorf("wrong audience token on %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	for _, test := range []struct{ scope, path string }{{FabricScope, "/fabric"}, {OneLakeScope, "/storage"}, {FabricScope, "/fabric"}} {
		c, err := transport.New(transport.Options{BaseURL: server.URL, Scope: test.scope, Tokens: source})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.Request(context.Background(), "GET", test.path, nil, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if !reflect.DeepEqual(scopes, []string{FabricScope, OneLakeScope}) {
		t.Fatal(scopes)
	}
}

func TestRefreshBeforeExpiryAndFailureNeverReturnsStaleToken(t *testing.T) {
	now := time.Now()
	calls, fail := 0, false
	source := &tokenSource{now: func() time.Time { return now }, credential: fakeCredential{get: func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
		calls++
		if fail {
			return azcore.AccessToken{}, errors.New("refresh failed with secret")
		}
		return azcore.AccessToken{Token: fmt.Sprint(calls), ExpiresOn: now.Add(time.Hour)}, nil
	}}}
	ctx := context.Background()
	if token, err := source.Token(ctx, FabricScope); token != "1" || err != nil {
		t.Fatal(token, err)
	}
	now = now.Add(59 * time.Minute)
	if token, err := source.Token(ctx, FabricScope); token != "2" || err != nil {
		t.Fatal("near-expiry token was not refreshed", token, err)
	}
	now = now.Add(59 * time.Minute)
	fail = true
	if token, err := source.Token(ctx, FabricScope); token != "" || err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("failed refresh returned stale token or sensitive error", token, err)
	}
	now = now.Add(time.Hour)
	if token, err := source.Token(ctx, FabricScope); token != "" || err == nil {
		t.Fatal("expired token was returned", token, err)
	}
}

func TestConcurrentRefreshAndCanceledWaiter(t *testing.T) {
	var calls atomic.Int32
	started, finish := make(chan struct{}), make(chan struct{})
	source := &tokenSource{credential: fakeCredential{get: func(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
		calls.Add(1)
		if opts.Scopes[0] == FabricScope {
			close(started)
			select {
			case <-ctx.Done():
				return azcore.AccessToken{}, ctx.Err()
			case <-finish:
			}
		}
		return azcore.AccessToken{Token: "memory-only", ExpiresOn: time.Now().Add(time.Hour)}, nil
	}}}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if _, err := source.Token(context.Background(), FabricScope); err != nil {
				t.Error(err)
			}
		})
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := source.Token(ctx, FabricScope); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("refresh waiter not cancelable", err)
	}
	if _, err := source.Token(context.Background(), OneLakeScope); err != nil {
		t.Fatal("one audience blocked the other", err)
	}
	close(finish)
	wg.Wait()
	if calls.Load() != 2 {
		t.Fatalf("expected one credential request per audience, got %d", calls.Load())
	}
}

func TestInvalidAudienceAndContextNeverReachIdentity(t *testing.T) {
	calls := 0
	source := &tokenSource{credential: fakeCredential{get: func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
		calls++
		return azcore.AccessToken{}, errors.New("unexpected identity call")
	}}}
	for _, scope := range []string{"", "https://evil.test/.default", FabricScope + " " + OneLakeScope, "https://storage.azure.com/", strings.ToUpper(FabricScope)} {
		_, err := source.Token(context.Background(), scope)
		if !errors.Is(err, fs.ErrInvalid) || (scope != "" && strings.Contains(err.Error(), scope)) {
			t.Errorf("invalid audience error = %v", err)
		}
	}
	if _, err := source.Token(nil, FabricScope); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Token(ctx, FabricScope); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("identity called %d times", calls)
	}
}

func TestCredentialErrorsAreSanitizedAndUnwrap(t *testing.T) {
	for _, cause := range []error{errors.New("secret token from identity"), context.Canceled, context.DeadlineExceeded} {
		source := &tokenSource{credential: fakeCredential{get: func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
			return azcore.AccessToken{}, cause
		}}}
		token, err := source.Token(context.Background(), FabricScope)
		if token != "" || !errors.Is(err, cause) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("credential error = %q, %v", token, err)
		}
	}
	source := &tokenSource{credential: fakeCredential{get: func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
		return azcore.AccessToken{}, nil
	}}}
	if _, err := source.Token(context.Background(), FabricScope); err == nil {
		t.Fatal("empty access token accepted")
	}
}

func TestNewDefaultConstructsWithoutAuthentication(t *testing.T) {
	// Construction doesn't invoke the CLI or contact Entra ID.
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "AzureCLICredential")
	source, err := NewDefault()
	if err != nil || source == nil {
		t.Fatalf("default credential construction failed: %v", err)
	}
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-secret-credential")
	_, err = NewDefault()
	if err == nil || strings.Contains(err.Error(), "invalid-secret-credential") {
		t.Fatalf("unsanitized constructor error: %v", err)
	}
}
