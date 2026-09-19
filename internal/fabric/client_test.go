package fabric

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

const (
	workspaceID = "11111111-1111-1111-1111-111111111111"
	itemID      = "22222222-2222-2222-2222-222222222222"
	operationID = "33333333-3333-3333-3333-333333333333"
	secondID    = "44444444-4444-4444-4444-444444444444"
)

type testTokens struct{ calls atomic.Int64 }

func (t *testTokens) Token(_ context.Context, scope string) (string, error) {
	if scope != "https://api.fabric.microsoft.com/.default" {
		return "", errors.New("wrong Fabric audience")
	}
	t.calls.Add(1)
	return "fabric-offline-token", nil
}

func api(t *testing.T, handler http.HandlerFunc, opts Options) (*Client, *testTokens) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fabric-offline-token" {
			t.Error("missing or incorrect Fabric bearer")
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	tokens := &testTokens{}
	client, err := transport.New(transport.Options{
		BaseURL: server.URL, Scope: "https://api.fabric.microsoft.com/.default", Tokens: tokens,
		HTTPClient: server.Client(), MaxRetries: 2, RetryDelay: time.Millisecond, MaxRetryDelay: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Millisecond
	}
	if opts.OperationTimeout == 0 {
		opts.OperationTimeout = time.Second
	}
	return New(client, opts), tokens
}

func definitionEnvelope() string {
	return `{"definition":{"future":{"nested":[1,2,3]},"parts":[{"path":".platform","payload":"eA==","payloadType":"InlineBase64"},{"path":"notebook-content.ipynb","payload":"eyJjZWxscyI6W10sIm5iZm9ybWF0Ijo0fQ==","payloadType":"InlineBase64","futurePart":{"keep":true}},{"path":"opaque/future.bin","payload":"not-base64-reference","payloadType":"FutureEncoding"}]}}`
}

func TestDiscoverWorkspacesItemsFoldersAndContinuation(t *testing.T) {
	tests := []struct {
		name, path, first, second string
		invoke                    func(context.Context, *Client) (int, error)
	}{
		{"workspaces", "/v1/workspaces", `{"id":"` + workspaceID + `","displayName":"a"}`, `{"id":"` + secondID + `","displayName":"b"}`,
			func(ctx context.Context, c *Client) (int, error) {
				items, err := c.ListWorkspaces(ctx)
				return len(items), err
			}},
		{"items", "/v1/workspaces/" + workspaceID + "/items", `{"id":"` + itemID + `","displayName":"n","type":"Notebook"}`, `{"id":"` + secondID + `","displayName":"nested","type":"Lakehouse","folderId":"` + operationID + `"}`,
			func(ctx context.Context, c *Client) (int, error) {
				items, err := c.ListItems(ctx, workspaceID)
				return len(items), err
			}},
		{"folders", "/v1/workspaces/" + workspaceID + "/folders", `{"id":"` + itemID + `","displayName":"parent"}`, `{"id":"` + secondID + `","displayName":"child","parentFolderId":"` + itemID + `"}`,
			func(ctx context.Context, c *Client) (int, error) {
				items, err := c.ListFolders(ctx, workspaceID)
				return len(items), err
			}},
	}
	for _, test := range tests {
		for _, useURI := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/URI=%v", test.name, useURI), func(t *testing.T) {
				calls := 0
				client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
					calls++
					if r.URL.Path != test.path || r.Method != http.MethodGet {
						t.Errorf("wrong discovery request %s %s", r.Method, r.URL)
					}
					if test.name != "workspaces" && r.URL.Query().Get("recursive") != "true" {
						t.Error("nested workspace items were not discovered")
					}
					if calls == 1 {
						if useURI {
							fmt.Fprintf(w, `{"value":[%s],"continuationUri":%q}`, test.first, "?continuationToken=a%2B%2F%3D%25")
						} else {
							fmt.Fprintf(w, `{"value":[%s],"continuationToken":"a+/=%%"}`, test.first)
						}
						return
					}
					if r.URL.Query().Get("continuationToken") != "a+/=%" {
						t.Error("continuation was not encoded exactly once", r.URL.RawQuery)
					}
					fmt.Fprintf(w, `{"value":[%s]}`, test.second)
				}, Options{})
				count, err := test.invoke(context.Background(), client)
				if err != nil || count != 2 || calls != 2 {
					t.Fatalf("discovery: count=%d requests=%d err=%v", count, calls, err)
				}
			})
		}
	}
}

func TestWorkspaceResponseIDAndInvalidRequests(t *testing.T) {
	client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"id":%q,"displayName":"different workspace"}`, secondID)
	}, Options{})
	if _, err := client.GetWorkspace(context.Background(), workspaceID); err == nil {
		t.Fatal("mismatched workspace ID accepted")
	}
	before := tokens.calls.Load()
	for _, id := range []string{"../escape", "", "named-workspace", workspaceID + "/items", strings.ReplaceAll(workspaceID, "-", "")} {
		if _, err := client.ListItems(context.Background(), id); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("invalid identity accepted: %q %v", id, err)
		}
	}
	if _, err := client.ListWorkspaces(nil); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal("nil context", err)
	}
	if tokens.calls.Load() != before {
		t.Fatal("invalid IDs reached authentication")
	}
}

func TestPaginationDoesNotHideMalformedBodiesOrForeignURIs(t *testing.T) {
	for _, body := range []string{
		`{}`, `{"value":null}`, `{"value":[{"id":"bad","displayName":"a"}]}`, `{"value":[]} trailing`,
		`{"value":[],"continuationUri":"https://foreign.example/secret"}`,
		`{"value":[],"continuationUri":"/v1/workspaces/../operations"}`,
		`{"value":[],"continuationUri":"/v1/operations/` + operationID + `"}`,
		`{"value":[],"continuationToken":"otherwise-safe","continuationUri":"https://foreign.example/secret"}`,
	} {
		t.Run(body, func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }, Options{})
			if _, err := client.ListWorkspaces(context.Background()); err == nil {
				t.Fatal("invalid pagination succeeded")
			}
			if tokens.calls.Load() != 1 {
				t.Fatal("untrusted next target reached credential retrieval")
			}
		})
	}
}

func TestPaginationCyclesAndLimits(t *testing.T) {
	for _, mode := range []string{"token-cycle", "uri-cycle", "page-limit", "missing-recursive"} {
		t.Run(mode, func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "token-cycle":
					fmt.Fprint(w, `{"value":[],"continuationToken":"again"}`)
				case "uri-cycle":
					fmt.Fprintf(w, `{"value":[],"continuationUri":%q}`, r.URL.RequestURI())
				case "page-limit":
					fmt.Fprintf(w, `{"value":[],"continuationToken":%q}`, r.URL.Query().Get("continuationToken")+"next")
				case "missing-recursive":
					fmt.Fprintf(w, `{"value":[],"continuationUri":%q}`, "/v1/workspaces/"+workspaceID+"/items?recursive=false")
				}
			}, Options{})
			client.maxPages = 3
			var err error
			if mode == "missing-recursive" {
				_, err = client.ListItems(context.Background(), workspaceID)
			} else {
				_, err = client.ListWorkspaces(context.Background())
			}
			if err == nil || tokens.calls.Load() > 3 {
				t.Fatalf("pagination did not stop: calls=%d err=%v", tokens.calls.Load(), err)
			}
		})
	}
}

func TestDefinitionRoundTripUnknownFieldsAndEmptyUpdate(t *testing.T) {
	updates := 0
	client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
		base := "/v1/workspaces/" + workspaceID + "/notebooks/" + itemID
		if r.Method != http.MethodPost || r.Header.Get("If-Match") != "" || r.URL.Query().Get("updateMetadata") != "" {
			t.Error("wrong notebook protocol or fabricated CAS")
		}
		switch r.URL.Path {
		case base + "/getDefinition":
			if r.URL.Query().Get("format") != "ipynb" {
				t.Error("getDefinition did not request ipynb")
			}
			fmt.Fprint(w, definitionEnvelope())
		case base + "/updateDefinition":
			var body struct{ Definition Definition }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			def := body.Definition
			if def.Format != "ipynb" || len(def.Parts) != 3 ||
				string(def.Extra["future"]) != `{"nested":[1,2,3]}` ||
				string(def.Parts[1].Extra["futurePart"]) != `{"keep":true}` ||
				def.Parts[2].PayloadType != "FutureEncoding" {
				t.Errorf("unknown definition data changed: %+v", def)
			}
			content, err := def.Parts[1].Decode()
			if err != nil || string(content) != `{"cells":[],"nbformat":4,"metadata":{"changed":true}}` {
				t.Error("content update lost", err)
			}
			updates++
			w.WriteHeader(200)
		default:
			t.Error("invalid endpoint", r.URL.Path)
			w.WriteHeader(404)
		}
	}, Options{})
	def, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb")
	if err != nil {
		t.Fatal(err)
	}
	if def.Format != "ipynb" {
		t.Fatal("omitted response format was not normalized")
	}
	if _, err := def.Parts[2].Decode(); !errors.Is(err, fserrors.ErrUnsupported) {
		t.Fatal("unknown encoding must be preserved but not fabricated", err)
	}
	def.Parts[1].SetData([]byte(`{"cells":[],"nbformat":4,"metadata":{"changed":true}}`))
	if err := client.UpdateNotebook(context.Background(), workspaceID, itemID, def); err != nil {
		t.Fatal("empty 200 update treated as missing definition", err)
	}
	if updates != 1 {
		t.Fatal("update was lost or repeated")
	}
}

func TestEnvironmentDefinitionAndLimits(t *testing.T) {
	client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/workspaces/"+workspaceID+"/environments/"+itemID+"/getDefinition" ||
			r.Method != http.MethodPost || r.URL.RawQuery != "" {
			t.Error("invented environment query or endpoint", r.URL)
		}
		fmt.Fprint(w, `{"definition":{"parts":[{"path":"Libraries/PublicLibraries/environment.yml","payload":"eA==","payloadType":"InlineBase64"}]}}`)
	}, Options{})
	if def, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Environment", ""); err != nil || len(def.Parts) != 1 {
		t.Fatal(def, err)
	}
	client.maxDefinitionBytes = 20
	if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Environment", ""); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal("oversized definition accepted", err)
	}
	def := Definition{Parts: []Part{{Path: "artifact.content.ipynb", PayloadType: "InlineBase64", Payload: base64.StdEncoding.EncodeToString(make([]byte, 128))}}}
	if err := client.UpdateNotebook(context.Background(), workspaceID, itemID, def); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal("base64 envelope growth was not limited", err)
	}
}

func TestLongRunningDefinitionAndUpdate(t *testing.T) {
	for _, useLocation := range []bool{false, true} {
		for _, write := range []bool{false, true} {
			t.Run(fmt.Sprintf("location=%v/write=%v", useLocation, write), func(t *testing.T) {
				polls, results, posts := 0, 0, 0
				client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodPost:
						posts++
						if useLocation {
							w.Header().Set("Location", "/v1/operations/"+operationID)
						} else {
							w.Header().Set("x-ms-operation-id", operationID)
						}
						w.Header().Set("Retry-After", "0")
						w.WriteHeader(202)
					case r.URL.Path == "/v1/operations/"+operationID:
						polls++
						w.Header().Set("Retry-After", "0")
						if polls == 1 {
							w.WriteHeader(202)
							fmt.Fprint(w, `{"status":"Running"}`)
							return
						}
						if useLocation {
							w.Header().Set("Location", "/v1/operations/"+operationID+"/result")
						}
						fmt.Fprint(w, `{"status":"Succeeded"}`)
					case r.URL.Path == "/v1/operations/"+operationID+"/result":
						results++
						fmt.Fprint(w, definitionEnvelope())
					default:
						t.Error("unexpected LRO URL", r.URL)
						w.WriteHeader(404)
					}
				}, Options{})
				ctx := context.Background()
				if write {
					def := Definition{Parts: []Part{{Path: "notebook-content.ipynb", Payload: "eA==", PayloadType: "InlineBase64"}}}
					if err := client.UpdateNotebook(ctx, workspaceID, itemID, def); err != nil {
						t.Fatal(err)
					}
				} else {
					def, err := client.GetDefinition(ctx, workspaceID, itemID, "Notebook", "ipynb")
					if err != nil || len(def.Parts) != 3 {
						t.Fatal(def, err)
					}
				}
				wantResults := 1
				if write {
					wantResults = 0
				}
				if polls != 2 || results != wantResults || posts != 1 {
					t.Fatalf("LRO protocol polls=%d results=%d posts=%d", polls, results, posts)
				}
			})
		}
	}
}

func TestLongRunningFailureCancellationAndMissingStatus(t *testing.T) {
	for _, state := range []string{"Failed", "Cancelled", "Canceled", "unknown", ""} {
		t.Run(state, func(t *testing.T) {
			client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.Header().Set("Location", "/v1/operations/"+operationID)
					w.WriteHeader(202)
					return
				}
				fmt.Fprintf(w, `{"status":%q,"error":{"errorCode":"ExportFailed","requestId":"operation-request"}}`, state)
			}, Options{})
			_, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb")
			if err == nil {
				t.Fatal("failed/invalid operation was accepted")
			}
			if state == "Failed" || state == "Cancelled" || state == "Canceled" {
				var operation *OperationError
				if !errors.As(err, &operation) || operation.Code != "ExportFailed" || operation.RequestID != "operation-request" {
					t.Fatalf("operation details lost: %v", err)
				}
			}
		})
	}
}

func TestLongRunningTargetsAreOriginAndOperationBound(t *testing.T) {
	for _, location := range []string{
		"https://foreign.example/v1/operations/" + operationID,
		"/v1/workspaces/" + workspaceID,
		"/v1/operations/../workspaces",
		"/v1/operations/" + secondID,
		"/v1/operations/" + operationID + "/result",
	} {
		t.Run(location, func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", location)
				w.Header().Set("x-ms-operation-id", operationID)
				w.WriteHeader(202)
			}, Options{})
			if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb"); err == nil {
				t.Fatal("invalid LRO accepted")
			}
			if tokens.calls.Load() != 1 {
				t.Fatal("invalid LRO target reached credential provider")
			}
		})
	}
	for _, header := range []string{"", "not-a-uuid"} {
		client, _ := api(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("x-ms-operation-id", header)
			w.WriteHeader(202)
		}, Options{})
		if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb"); err == nil {
			t.Fatal("missing polling identity accepted")
		}
	}
}

func TestLongRunningTimeoutAndRetryAfter(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelRequest), func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", "/v1/operations/"+operationID)
				w.Header().Set("Retry-After", "2")
				w.WriteHeader(202)
			}, Options{OperationTimeout: 20 * time.Millisecond})
			ctx := context.Background()
			var cancel context.CancelFunc
			if cancelRequest {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err := client.GetDefinition(ctx, workspaceID, itemID, "Notebook", "ipynb")
			expected := context.DeadlineExceeded
			if cancelRequest {
				expected = context.Canceled
			}
			if !errors.Is(err, expected) || tokens.calls.Load() > 1 {
				t.Fatalf("timeout/Retry-After violated: calls=%d %v", tokens.calls.Load(), err)
			}
		})
	}
	var polls atomic.Int64
	client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "/v1/operations/"+operationID)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(202)
			return
		}
		polls.Add(1)
		if strings.HasSuffix(r.URL.Path, "/result") {
			fmt.Fprint(w, definitionEnvelope())
		} else {
			fmt.Fprint(w, `{"status":"Succeeded"}`)
		}
	}, Options{OperationTimeout: 2 * time.Second})
	start := time.Now()
	if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < time.Second || polls.Load() != 2 {
		t.Fatal("LRO polled before server Retry-After")
	}
}

func TestReadDefinition429RetriesButUpdatesDoNot(t *testing.T) {
	gets, updates := 0, 0
	client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/getDefinition") {
			gets++
			if gets == 3 {
				fmt.Fprint(w, definitionEnvelope())
				return
			}
		} else {
			updates++
		}
		w.Header().Set("Retry-After", "0")
		w.Header().Set("requestId", "retained-request-id")
		w.WriteHeader(429)
		fmt.Fprint(w, `{"errorCode":"TooManyRequests","message":"secret response omitted"}`)
	}, Options{})
	def, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb")
	if err != nil || gets != 3 {
		t.Fatal("safe definition POST not retried", gets, err)
	}
	err = client.UpdateNotebook(context.Background(), workspaceID, itemID, def)
	var response *transport.HTTPError
	if !errors.As(err, &response) || response.StatusCode != 429 || response.RequestID != "retained-request-id" || updates != 1 {
		t.Fatal("write retry or request ID loss", updates, err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("raw service error leaked")
	}
}

func TestDefinitionValidationAndSynchronousResults(t *testing.T) {
	for _, body := range []string{
		``, `{}`, `{"definition":null}`, `{"definition":{"parts":[]}}`,
		`{"definition":{"parts":[{"path":"../escape","payload":"eA==","payloadType":"InlineBase64"}]}}`,
		`{"definition":{"parts":[{"path":"a.ipynb","payload":"eA==","payloadType":"InlineBase64"},{"path":"a.ipynb","payload":"eA==","payloadType":"InlineBase64"}]}}`,
	} {
		client, _ := api(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }, Options{})
		if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb"); err == nil {
			t.Error("invalid/empty definition accepted", body)
		}
	}
	for _, status := range []int{200, 201} {
		client, _ := api(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			fmt.Fprint(w, definitionEnvelope())
		}, Options{})
		if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb"); err != nil {
			t.Fatal(status, err)
		}
	}
	for _, format := range []string{"fabricGitSource", "unsupported"} {
		client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) { t.Error("invalid format requested") }, Options{})
		if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", format); !errors.Is(err, fserrors.ErrUnsupported) || tokens.calls.Load() != 0 {
			t.Fatal("invalid format accepted", err)
		}
	}
}

func TestDefinitionJSONDoesNotLoseZeroOrOpaqueMetadata(t *testing.T) {
	raw := `{"format":"ipynb","parts":[{"path":"a.ipynb","payload":"","payloadType":"InlineBase64","extra":[false,null,0]}],"extra":{"zero":0,"nil":null}}`
	var def Definition
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	var before, after any
	_ = json.Unmarshal([]byte(raw), &before)
	_ = json.Unmarshal(data, &after)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("definition metadata lost: %s", data)
	}
	reader := io.NopCloser(strings.NewReader("unbounded-response"))
	response := &http.Response{StatusCode: 200, Body: reader, ContentLength: -1}
	if _, err := readResponse(response, 3); !errors.Is(err, fserrors.ErrTooLarge) {
		t.Fatal("chunked oversized responses not bounded", err)
	}
}
