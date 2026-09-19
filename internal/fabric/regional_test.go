package fabric

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestRegionalLocationUsesOperationIDOnConfiguredOrigin(t *testing.T) {
	polls, results := 0, 0
	client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "https://region-redirect.analysis.windows.net/v1/operations/"+operationID)
			w.Header().Set("x-ms-operation-id", operationID)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(202)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/result") {
			results++
			fmt.Fprint(w, definitionEnvelope())
			return
		}
		polls++
		w.Header().Set("Location", "https://region-redirect.analysis.windows.net/v1/operations/"+operationID+"/result")
		fmt.Fprint(w, `{"status":"Succeeded"}`)
	}, Options{})
	def, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb")
	if err != nil || len(def.Parts) != 3 || polls != 1 || results != 1 || tokens.calls.Load() != 3 {
		t.Fatalf("public-origin operation-ID flow failed: polls=%d results=%d tokens=%d error=%v", polls, results, tokens.calls.Load(), err)
	}
}

func TestRegionalOperationFallbackRequiresSafeMatchingIdentity(t *testing.T) {
	client, tokens := api(t, func(http.ResponseWriter, *http.Request) { t.Fatal("unsafe target requested") }, Options{})
	for _, test := range []struct{ location, id string }{
		{"https://region-redirect.analysis.windows.net/v1/operations/" + operationID, ""},
		{"https://region-redirect.analysis.windows.net/v1/operations/" + operationID, secondID},
		{"//region-redirect.analysis.windows.net/v1/operations/" + operationID, operationID},
		{"http://region-redirect.analysis.windows.net/v1/operations/" + operationID, operationID},
		{"https://region-redirect.analysis.windows.net:443/v1/operations/" + operationID, operationID},
		{"https://user@region-redirect.analysis.windows.net/v1/operations/" + operationID, operationID},
		{"https://region-redirect.analysis.windows.net/v1/operations/" + operationID + "#fragment", operationID},
		{"https://region-redirect.analysis.windows.net/v1/operations/" + operationID + "#", operationID},
		{"https://region-redirect.analysis.windows.net/v1/operations/" + operationID + "?secret=ignored", operationID},
		{"https://region-redirect.analysis.windows.net/operations/" + operationID, operationID},
		{"https://region-redirect.analysis.windows.net/v1/a/../operations/" + operationID, operationID},
		{"https://region-redirect.analysis.windows.net/v1/%2e%2e/operations/" + operationID, operationID},
		{"https://region-redirect.analysis.windows.net.evil.example/v1/operations/" + operationID, operationID},
		{"https://evil.example/v1/operations/" + operationID, operationID},
	} {
		if _, err := client.operationTarget(test.location, test.id); err == nil {
			t.Errorf("unsafe regional target accepted: %s", test.location)
		}
	}
	if tokens.calls.Load() != 0 {
		t.Fatal("unsafe operation location reached credentials")
	}
}
