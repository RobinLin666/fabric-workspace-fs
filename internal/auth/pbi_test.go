package auth

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

func TestPowerBIAudienceIsIsolatedAndCached(t *testing.T) {
	calls := map[string]int{}
	source := &tokenSource{credential: fakeCredential{get: func(_ context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
		scope := opts.Scopes[0]
		calls[scope]++
		return azcore.AccessToken{Token: scope, ExpiresOn: time.Now().Add(time.Hour)}, nil
	}}}
	for range 3 {
		for _, scope := range []string{FabricScope, OneLakeScope, PowerBIScope} {
			value, err := source.Token(context.Background(), scope)
			if err != nil || value != scope {
				t.Fatal("audiences mixed", scope, err)
			}
		}
	}
	for scope, count := range calls {
		if count != 1 {
			t.Fatal("audience cache miss", scope, count)
		}
	}
	if len(calls) != 3 {
		t.Fatal("missing dedicated Power BI audience")
	}
}
