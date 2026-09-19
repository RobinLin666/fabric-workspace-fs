package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	fabricsdk "github.com/microsoft/fabric-sdk-go/fabric"
	"github.com/microsoft/fabric-sdk-go/fabric/core"
	"github.com/microsoft/fabric-sdk-go/fabric/notebook"
)

func TestSDKDefinitionModelCannotReplaceLosslessRoundTrip(t *testing.T) {
	input := `{"format":"ipynb","future":{"keep":true},"parts":[{"path":"notebook-content.ipynb","payload":"eA==","payloadType":"InlineBase64","futurePart":{"keep":true}}]}`
	var model notebook.Definition
	if err := json.Unmarshal([]byte(input), &model); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"future"`) || strings.Contains(string(encoded), `"futurePart"`) {
		t.Fatal("SDK behavior changed: re-evaluate whether lossless definitions are now supported")
	}
	var lossless Definition
	if err := json.Unmarshal([]byte(input), &lossless); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(lossless)
	if err != nil || !strings.Contains(string(encoded), `"future"`) || !strings.Contains(string(encoded), `"futurePart"`) {
		t.Fatal("production definitions lost fields that the SDK omits", err)
	}
}

func TestSDKNativePagerDoesNotFollowTokenOnlyContinuation(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		fmt.Fprint(w, `{"value":[],"continuationToken":"next-token-only"}`)
	}))
	defer server.Close()
	endpoint := server.URL
	client, err := fabricsdk.NewClient(nil, &endpoint, &fabricsdk.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: server.Client(), Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	pager := core.NewClientFactoryWithClient(*client).NewWorkspacesClient().NewListWorkspacesPager(nil)
	page, err := pager.NextPage(context.Background())
	if err != nil || page.ContinuationToken == nil || *page.ContinuationToken != "next-token-only" {
		t.Fatal("unexpected SDK page shape", err)
	}
	if pager.More() || requests != 1 {
		t.Fatal("SDK behavior changed: re-evaluate native pager compatibility")
	}
	// The production ListWorkspaces contract tests independently require the
	// second token-only page, origin checks, bounded bodies and cycle detection.
}
