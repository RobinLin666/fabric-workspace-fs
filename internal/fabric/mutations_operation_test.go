package fabric

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

func TestMutationCreateWaitsForGuardedOperationResult(t *testing.T) {
	for _, kind := range []string{"Notebook", "Lakehouse", "Environment"} {
		for _, mode := range []string{"public", "regional", "id-only"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				resultBody := mutationItemBody(t, kind, "Probe", secondID)
				pollPath := "/v1/operations/" + operationID
				location := pollPath
				if mode == "regional" {
					location = "https://region-redirect.analysis.windows.net" + pollPath
				}
				var posts, polls, results atomic.Int64
				client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
					if r.URL.RawQuery != "" {
						t.Error("create operation introduced a query")
					}
					if r.Method == http.MethodPost {
						posts.Add(1)
						if r.URL.Path != "/v1/workspaces/"+workspaceID+"/"+strings.ToLower(kind)+"s" {
							t.Error("create used the wrong typed endpoint")
						}
						if mode != "id-only" {
							w.Header().Set("Location", location)
						}
						w.Header().Set("x-ms-operation-id", operationID)
						w.Header().Set("Retry-After", "0")
						w.WriteHeader(http.StatusAccepted)
						return
					}
					if r.Method != http.MethodGet {
						t.Error("poll or result was not a safe GET")
					}
					switch r.URL.Path {
					case pollPath:
						n := polls.Add(1)
						w.Header().Set("x-ms-operation-id", operationID)
						w.Header().Set("Retry-After", "0")
						if n == 1 {
							w.Header().Set("Location", location)
							w.WriteHeader(http.StatusAccepted)
							return
						}
						if n == 2 {
							w.Header().Set("Location", location)
							fmt.Fprint(w, `{"status":"Running"}`)
							return
						}
						w.Header().Set("Location", location+"/result")
						fmt.Fprint(w, `{"status":"Succeeded"}`)
					case pollPath + "/result":
						results.Add(1)
						if polls.Load() != 3 {
							t.Error("result fetched before a terminal operation status")
						}
						w.WriteHeader(http.StatusCreated)
						fmt.Fprint(w, resultBody)
					default:
						t.Error("operation escaped its immutable ID")
						w.WriteHeader(http.StatusNotFound)
					}
				}, Options{})
				item, err := client.CreateItem(context.Background(), workspaceID, kind, "Probe", secondID)
				if err != nil || item.ID != itemID || item.Type != kind || item.FolderID != secondID ||
					posts.Load() != 1 || polls.Load() != 3 || results.Load() != 1 || tokens.calls.Load() != 5 {
					t.Fatalf("guarded LRO create failed: posts=%d polls=%d results=%d err=%v", posts.Load(), polls.Load(), results.Load(), err)
				}
			})
		}
	}
}

func TestMutationCreateRejectsUntrustedOperationTargets(t *testing.T) {
	base := "https://region-redirect.analysis.windows.net/v1/operations/" + operationID
	for i, location := range []string{
		"https://foreign.example/v1/operations/" + operationID,
		base + "?secret=location-private-value",
		base + "?",
		base + "#location-private-value",
		base + "#",
		"https://user:location-private-value@region-redirect.analysis.windows.net/v1/operations/" + operationID,
		"https://region-redirect.analysis.windows.net:443/v1/operations/" + operationID,
		"http://region-redirect.analysis.windows.net/v1/operations/" + operationID,
		"//region-redirect.analysis.windows.net/v1/operations/" + operationID,
		"https://region-redirect.analysis.windows.net.evil.example/v1/operations/" + operationID,
		"https://region-redirect.analysis.windows.net/v1/operations/" + secondID,
		"https://region-redirect.analysis.windows.net/v1/operations/" + operationID + "/extra",
		"https://region-redirect.analysis.windows.net/v1/workspaces/" + operationID,
		"https://region-redirect.analysis.windows.net/v1/operations/%2e%2e/" + operationID,
	} {
		for _, phase := range []string{"accepted", "poll"} {
			t.Run(fmt.Sprintf("%s/target-%d", phase, i), func(t *testing.T) {
				var calls atomic.Int64
				client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
					n := calls.Add(1)
					w.Header().Set("x-ms-operation-id", operationID)
					if phase == "poll" && n == 1 {
						w.Header().Set("Location", "/v1/operations/"+operationID)
						w.WriteHeader(http.StatusAccepted)
						return
					}
					w.Header().Set("Location", location)
					if r.Method == http.MethodPost {
						w.WriteHeader(http.StatusAccepted)
					} else {
						fmt.Fprint(w, `{"status":"Succeeded"}`)
					}
				}, Options{})
				item, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
				expectedCalls := int64(1)
				if phase == "poll" {
					expectedCalls++
				}
				if err == nil || item != (Item{}) || calls.Load() != expectedCalls || tokens.calls.Load() != expectedCalls {
					t.Fatal("untrusted operation location reached authentication or created a phantom item")
				}
				if err != nil && strings.Contains(err.Error(), "location-private-value") {
					t.Fatal("operation location leaked through an error")
				}
			})
		}
	}
}

func TestMutationCreateAcceptedIsNotAResource(t *testing.T) {
	for _, test := range []struct{ name, location, id string }{
		{"missing-target", "", ""},
		{"invalid-header-id", "", "not-an-operation-id"},
		{"changed-header-id", "/v1/operations/" + operationID, secondID},
		{"direct-result", "/v1/operations/" + operationID + "/result", operationID},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := mutationItemBody(t, "Notebook", "Probe", "")
			client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", test.location)
				w.Header().Set("x-ms-operation-id", test.id)
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprint(w, body)
			}, Options{})
			item, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
			if err == nil || item != (Item{}) || tokens.calls.Load() != 1 {
				t.Fatal("accepted response body was treated as a created resource")
			}
		})
	}
	client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/v1/operations/"+operationID)
		w.Header().Set("x-ms-operation-id", operationID)
		w.WriteHeader(http.StatusAccepted)
	}, Options{})
	folder, err := client.CreateFolder(context.Background(), workspaceID, "Probe", "")
	var status *transport.HTTPError
	if !errors.As(err, &status) || status.StatusCode != http.StatusAccepted || folder != (Folder{}) || tokens.calls.Load() != 1 {
		t.Fatal("folder create followed an undocumented accepted-operation contract", err)
	}
}

func TestMutationCreateTerminalFailures(t *testing.T) {
	for _, state := range []string{"Failed", "Cancelled", "Canceled"} {
		t.Run(state, func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.Header().Set("Location", "/v1/operations/"+operationID)
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if r.URL.Path != "/v1/operations/"+operationID {
					t.Error("failed create attempted to retrieve a result")
				}
				fmt.Fprintf(w, `{"status":%q,"error":{"errorCode":"ProvisioningFailed","requestId":%q,"message":"private-operation-message"}}`, state, secondID)
			}, Options{})
			item, err := client.CreateItem(context.Background(), workspaceID, "Environment", "Probe", "")
			var operationErr *OperationError
			if !errors.As(err, &operationErr) || operationErr.Status != state || operationErr.Code != "ProvisioningFailed" ||
				operationErr.RequestID != secondID || item != (Item{}) || tokens.calls.Load() != 2 {
				t.Fatal("failed create lost its operation error or returned a phantom item", err)
			}
			if strings.Contains(err.Error(), "private-operation-message") {
				t.Fatal("operation body leaked into its error")
			}
		})
	}
}

func TestMutationCreateRejectsMalformedOperationState(t *testing.T) {
	for _, test := range []struct {
		name, body, location, id string
		status                   int
	}{
		{"empty", "", "", "", 200},
		{"malformed", "{", "", "", 200},
		{"missing-status", "{}", "", "", 200},
		{"unknown", `{"status":"MaybeSucceeded"}`, "", "", 200},
		{"accepted-success", `{"status":"Succeeded"}`, "", "", 202},
		{"pending-result", `{"status":"Running"}`, "/v1/operations/" + operationID + "/result", "", 200},
		{"changed-id", `{"status":"Running"}`, "", secondID, 200},
		{"changed-location-id", `{"status":"Succeeded"}`, "/v1/operations/" + secondID + "/result", "", 200},
		{"invalid-status-code", "", "", "", 204},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.Header().Set("Location", "/v1/operations/"+operationID)
					w.Header().Set("x-ms-operation-id", operationID)
					w.WriteHeader(http.StatusAccepted)
					return
				}
				w.Header().Set("Location", test.location)
				w.Header().Set("x-ms-operation-id", test.id)
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}, Options{})
			item, err := client.CreateItem(context.Background(), workspaceID, "Lakehouse", "Probe", "")
			if err == nil || item != (Item{}) || tokens.calls.Load() != 2 {
				t.Fatal("malformed operation state became a successful create")
			}
		})
	}
}

func TestMutationCreateResultMustBeFinalAndMatchRequest(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
	}{
		{"missing", "", 200},
		{"malformed", "{}", 201},
		{"wrong-kind", mutationItemBody(t, "Environment", "Probe", ""), 200},
		{"wrong-name", mutationItemBody(t, "Notebook", "Other", ""), 200},
		{"wrong-folder", mutationItemBody(t, "Notebook", "Probe", secondID), 200},
		{"still-accepted", mutationItemBody(t, "Notebook", "Probe", ""), 202},
		{"no-content", "", 204},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.Header().Set("x-ms-operation-id", operationID)
					w.WriteHeader(http.StatusAccepted)
				} else if r.URL.Path == "/v1/operations/"+operationID {
					fmt.Fprint(w, `{"status":"Succeeded"}`)
				} else {
					w.WriteHeader(test.status)
					fmt.Fprint(w, test.body)
				}
			}, Options{})
			item, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
			if err == nil || item != (Item{}) || tokens.calls.Load() != 3 {
				t.Fatal("missing, mismatched, or nonfinal create result succeeded")
			}
		})
	}
}

func TestMutationCreatePollingTimeoutAndCancellation(t *testing.T) {
	for _, kind := range []string{"Notebook", "Lakehouse", "Environment"} {
		t.Run(kind+"/timeout", func(t *testing.T) {
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Error("operation ignored its server retry delay")
				}
				w.Header().Set("x-ms-operation-id", operationID)
				w.Header().Set("Retry-After", "600")
				w.WriteHeader(http.StatusAccepted)
			}, Options{OperationTimeout: 100 * time.Millisecond})
			item, err := client.CreateItem(context.Background(), workspaceID, kind, "Probe", "")
			if !errors.Is(err, context.DeadlineExceeded) || item != (Item{}) || tokens.calls.Load() != 1 {
				t.Fatal("create did not obey the operation deadline", err)
			}
		})
		t.Run(kind+"/cancel", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.Header().Set("x-ms-operation-id", operationID)
					w.WriteHeader(http.StatusAccepted)
					return
				}
				cancel()
				fmt.Fprint(w, `{"status":"Running"}`)
			}, Options{})
			item, err := client.CreateItem(ctx, workspaceID, kind, "Probe", "")
			if !errors.Is(err, context.Canceled) || item != (Item{}) || tokens.calls.Load() != 2 {
				t.Fatal("create cancellation became success or continued polling", err)
			}
		})
	}
}

func TestMutationCreateRetriesOnlySafePollsAndResultReads(t *testing.T) {
	var posts, polls, results atomic.Int64
	body := mutationItemBody(t, "Notebook", "Probe", "")
	client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			posts.Add(1)
			w.Header().Set("x-ms-operation-id", operationID)
			w.WriteHeader(http.StatusAccepted)
		case r.URL.Path == "/v1/operations/"+operationID:
			if polls.Add(1) == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
			} else {
				fmt.Fprint(w, `{"status":"Succeeded"}`)
			}
		case r.URL.Path == "/v1/operations/"+operationID+"/result":
			if results.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
			} else {
				fmt.Fprint(w, body)
			}
		default:
			t.Error("unexpected retry target")
			w.WriteHeader(http.StatusNotFound)
		}
	}, Options{})
	item, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
	if err != nil || item.ID != itemID || posts.Load() != 1 || polls.Load() != 2 || results.Load() != 2 {
		t.Fatal("retry policy failed to distinguish create from safe reads", err)
	}
}

func TestMutationCreatePollingServiceErrorIsNotEmptySuccess(t *testing.T) {
	for _, atResult := range []bool{false, true} {
		t.Run(fmt.Sprintf("result=%t", atResult), func(t *testing.T) {
			client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.Header().Set("x-ms-operation-id", operationID)
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if atResult && !strings.HasSuffix(r.URL.Path, "/result") {
					fmt.Fprint(w, `{"status":"Succeeded"}`)
					return
				}
				w.Header().Set("x-ms-request-id", secondID)
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `{"errorCode":"ProvisioningAccessDenied"}`)
			}, Options{})
			item, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
			var status *transport.HTTPError
			if !errors.As(err, &status) || status.StatusCode != http.StatusForbidden ||
				status.Code != "ProvisioningAccessDenied" || status.RequestID != secondID || item != (Item{}) {
				t.Fatal("operation read service failure was lost", err)
			}
		})
	}
}

func TestMutationCreateOperationBodiesAreBounded(t *testing.T) {
	for _, stage := range []string{"accepted", "poll", "result"} {
		t.Run(stage, func(t *testing.T) {
			client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
				current := "poll"
				if r.Method == http.MethodPost {
					current = "accepted"
					w.Header().Set("x-ms-operation-id", operationID)
					w.WriteHeader(http.StatusAccepted)
				} else if strings.HasSuffix(r.URL.Path, "/result") {
					current = "result"
				}
				if current == stage {
					w.(http.Flusher).Flush()
					fmt.Fprint(w, strings.Repeat("x", 129))
				} else if current == "poll" {
					fmt.Fprint(w, `{"status":"Succeeded"}`)
				}
			}, Options{MaxDefinitionBytes: 128})
			item, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
			if !errors.Is(err, fserrors.ErrTooLarge) || item != (Item{}) {
				t.Fatal("operation body exceeded its configured bound", err)
			}
		})
	}
}

func TestMutationErrorsDoNotRetainReflectedTokens(t *testing.T) {
	for _, call := range mutationCalls() {
		t.Run(call.name, func(t *testing.T) {
			client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
				token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				w.Header().Set("x-ms-request-id", token)
				w.WriteHeader(http.StatusNoContent)
			}, Options{})
			err := call.invoke(context.Background(), client, workspaceID, secondID)
			var status *transport.HTTPError
			if !errors.As(err, &status) || status.RequestID != "" {
				t.Fatal("unexpected status retained a reflected bearer value")
			}
		})
	}
	client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("x-ms-operation-id", operationID)
			w.WriteHeader(http.StatusAccepted)
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		fmt.Fprintf(w, `{"status":"Failed","error":{"errorCode":%q,"requestId":%q}}`, token, token)
	}, Options{})
	_, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
	var operationErr *OperationError
	if !errors.As(err, &operationErr) || operationErr.Code != "" || operationErr.RequestID != "" {
		t.Fatal("operation error retained a reflected bearer value")
	}
}
