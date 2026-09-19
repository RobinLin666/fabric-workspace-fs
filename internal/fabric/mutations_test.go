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

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

func mutationItemBody(t *testing.T, kind, name, folder string) string {
	t.Helper()
	wire := map[string]any{
		"id": itemID, "workspaceId": workspaceID, "type": kind, "displayName": name,
	}
	if folder != "" {
		wire["folderId"] = folder
	}
	return mutationJSON(t, wire)
}

func mutationFolderBody(t *testing.T, name, parent string) string {
	t.Helper()
	wire := map[string]any{"id": itemID, "workspaceId": workspaceID, "displayName": name}
	if parent != "" {
		wire["parentFolderId"] = parent
	}
	return mutationJSON(t, wire)
}

func mutationJSON(t *testing.T, wire any) string {
	t.Helper()
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatal("unable to encode test fixture")
	}
	return string(body)
}

func TestMutationCreateItemSDKPayloads(t *testing.T) {
	for _, test := range []struct{ kind, collection, name string }{
		{"Notebook", "notebooks", "Sales/2026 [分析].ipynb"},
		{"Lakehouse", "lakehouses", "Sales_2026"},
		{"Environment", "environments", "Sales environment"},
	} {
		for _, parent := range []string{"", secondID} {
			for _, status := range []int{http.StatusCreated, http.StatusOK} {
				t.Run(fmt.Sprintf("%s/nested=%t/status=%d", test.kind, parent != "", status), func(t *testing.T) {
					response := mutationItemBody(t, test.kind, test.name, parent)
					var calls atomic.Int64
					client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						if r.Method != http.MethodPost || r.URL.Path != "/v1/workspaces/"+workspaceID+"/"+test.collection ||
							r.URL.RawQuery != "" || r.Header.Get("If-Match") != "" ||
							r.Header.Get("Content-Type") != "application/json" {
							t.Error("incorrect create request method, path, query, or headers")
						}
						var body map[string]any
						if json.NewDecoder(r.Body).Decode(&body) != nil {
							t.Error("invalid create JSON")
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						expectedFields := 1
						if test.kind == "Notebook" {
							expectedFields++
							assertMutationNotebookDefinition(t, body["definition"])
						}
						if parent != "" {
							expectedFields++
							if body["folderId"] != parent {
								t.Error("item create did not use folderId")
							}
						} else if _, found := body["folderId"]; found {
							t.Error("root create must omit folderId")
						}
						if body["displayName"] != test.name || len(body) != expectedFields {
							t.Error("create changed the decoded name or invented optional payload fields")
						}
						if _, found := body["parentFolderId"]; found {
							t.Error("item create used the folder parent field")
						}
						if test.kind != "Notebook" {
							if _, found := body["definition"]; found {
								t.Error("default item create fabricated a definition")
							}
							if _, found := body["creationPayload"]; found {
								t.Error("default item create overrode service defaults")
							}
						}
						w.WriteHeader(status)
						fmt.Fprint(w, response)
					}, Options{})
					item, err := client.CreateItem(context.Background(), workspaceID, test.kind, test.name, parent)
					if err != nil || item.ID != itemID || item.Type != test.kind ||
						item.DisplayName != test.name || item.FolderID != parent ||
						calls.Load() != 1 || tokens.calls.Load() != 1 {
						t.Fatalf("item create contract failed: calls=%d err=%v", calls.Load(), err)
					}
				})
			}
		}
	}
}

func assertMutationNotebookDefinition(t *testing.T, value any) {
	t.Helper()
	definition, ok := value.(map[string]any)
	if !ok || len(definition) != 2 || definition["format"] != "ipynb" {
		t.Error("notebook create did not select ipynb")
		return
	}
	parts, ok := definition["parts"].([]any)
	if !ok || len(parts) != 1 {
		t.Error("notebook create must send only its content, not a fabricated platform part")
		return
	}
	part, ok := parts[0].(map[string]any)
	if !ok || len(part) != 3 || part["path"] != "notebook-content.ipynb" || part["payloadType"] != "InlineBase64" {
		t.Error("notebook part contract changed")
		return
	}
	payload, ok := part["payload"].(string)
	if !ok {
		t.Error("notebook payload is not a string")
		return
	}
	content, err := base64.StdEncoding.Strict().DecodeString(payload)
	if err != nil {
		t.Error("notebook payload is not valid base64")
		return
	}
	var actual, expected any
	if json.Unmarshal(content, &actual) != nil ||
		json.Unmarshal([]byte(`{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"kernelspec":{"name":"python3","language":"python","display_name":"Python 3"},"language_info":{"name":"python"}}}`), &expected) != nil ||
		!reflect.DeepEqual(actual, expected) {
		t.Error("notebook content is not the minimal valid Python ipynb")
	}
}

func TestMutationCreateFolderSDKPayload(t *testing.T) {
	for _, parent := range []string{"", secondID} {
		for _, status := range []int{http.StatusCreated, http.StatusOK} {
			t.Run(fmt.Sprintf("nested=%t/status=%d", parent != "", status), func(t *testing.T) {
				response := mutationFolderBody(t, "ordinary folder", parent)
				client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost || r.URL.Path != "/v1/workspaces/"+workspaceID+"/folders" ||
						r.URL.RawQuery != "" || r.Header.Get("If-Match") != "" {
						t.Error("folder create used an incorrect route or conditional write")
					}
					var actual map[string]any
					if json.NewDecoder(r.Body).Decode(&actual) != nil {
						t.Error("invalid folder create JSON")
					}
					expected := map[string]any{"displayName": "ordinary folder"}
					if parent != "" {
						expected["parentFolderId"] = parent
					}
					if !reflect.DeepEqual(actual, expected) {
						t.Error("folder create must use parentFolderId, not folderId or item definition")
					}
					w.WriteHeader(status)
					fmt.Fprint(w, response)
				}, Options{})
				folder, err := client.CreateFolder(context.Background(), workspaceID, "ordinary folder", parent)
				if err != nil || folder.ID != itemID || folder.DisplayName != "ordinary folder" ||
					folder.ParentFolderID != parent || tokens.calls.Load() != 1 {
					t.Fatal("folder create failed its identity contract", err)
				}
			})
		}
	}
}

func TestMutationInvalidNamesBeforeAuthentication(t *testing.T) {
	client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("invalid name reached the server")
		w.WriteHeader(http.StatusBadRequest)
	}, Options{})
	common := []string{"", "   ", ".", "..", string([]byte{0xff}), "a\x00b", "a\x1fb", "a\x7fb", "a\u0085b"}
	for i, name := range common {
		for _, kind := range []string{"Notebook", "Lakehouse", "Environment", "Folder"} {
			t.Run(fmt.Sprintf("%s/invalid-%d", kind, i), func(t *testing.T) {
				var err error
				if kind == "Folder" {
					_, err = client.CreateFolder(context.Background(), workspaceID, name, "")
				} else {
					_, err = client.CreateItem(context.Background(), workspaceID, kind, name, "")
				}
				if !errors.Is(err, fs.ErrInvalid) {
					t.Fatal("invalid display name was accepted", err)
				}
			})
		}
	}
	for i, name := range []string{"1starts_with_digit", "_starts_with_underscore", "has space", "has/slash", "has-hyphen", strings.Repeat("a", 124)} {
		t.Run(fmt.Sprintf("Lakehouse/rule-%d", i), func(t *testing.T) {
			if _, err := client.CreateItem(context.Background(), workspaceID, "Lakehouse", name, ""); !errors.Is(err, fs.ErrInvalid) {
				t.Fatal("documented lakehouse name rule was not enforced", err)
			}
		})
	}
	folderNames := []string{" leading", "trailing ", "$recycle.bin", "ReCyClEd", "RECYCLER", strings.Repeat("界", 256)}
	for _, r := range "~\"#.&*:<>?/{|}" {
		folderNames = append(folderNames, "before"+string(r)+"after")
	}
	for i, name := range folderNames {
		t.Run(fmt.Sprintf("Folder/rule-%d", i), func(t *testing.T) {
			if _, err := client.CreateFolder(context.Background(), workspaceID, name, ""); !errors.Is(err, fs.ErrInvalid) {
				t.Fatal("documented folder name rule was not enforced", err)
			}
		})
	}
	if tokens.calls.Load() != 0 {
		t.Fatal("invalid names acquired a credential")
	}
}

func TestMutationNamesDoNotApplyWindowsPathRules(t *testing.T) {
	for _, test := range []struct{ kind, name string }{
		{"Notebook", "CON"}, {"Notebook", "a/b"}, {"Notebook", `a\b`}, {"Notebook", "a:b?.txt"},
		{"Environment", "CON"}, {"Environment", "a/b"},
		{"Lakehouse", strings.Repeat("a", 123)},
		{"Folder", "CON"}, {"Folder", `a\b`}, {"Folder", "a%b"}, {"Folder", "unknownFoo"},
		{"Folder", strings.Repeat("界", 255)},
	} {
		t.Run(fmt.Sprintf("%s/length-%d", test.kind, len(test.name)), func(t *testing.T) {
			response := mutationItemBody(t, test.kind, test.name, "")
			if test.kind == "Folder" {
				response = mutationFolderBody(t, test.name, "")
			}
			client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if json.NewDecoder(r.Body).Decode(&body) != nil || body["displayName"] != test.name {
					t.Error("decoded display name was encoded or normalized")
				}
				w.WriteHeader(http.StatusCreated)
				fmt.Fprint(w, response)
			}, Options{})
			var err error
			if test.kind == "Folder" {
				_, err = client.CreateFolder(context.Background(), workspaceID, test.name, "")
			} else {
				_, err = client.CreateItem(context.Background(), workspaceID, test.kind, test.name, "")
			}
			if err != nil {
				t.Fatal("name was rejected by an unrelated filesystem naming rule", err)
			}
		})
	}
}

type mutationInvocation struct {
	name   string
	invoke func(context.Context, *Client, string, string) error
}

func mutationWrites() []mutationInvocation {
	return []mutationInvocation{
		{"create-notebook", func(ctx context.Context, c *Client, workspace, id string) error {
			_, err := c.CreateItem(ctx, workspace, "Notebook", "Probe", id)
			return err
		}},
		{"create-lakehouse", func(ctx context.Context, c *Client, workspace, id string) error {
			_, err := c.CreateItem(ctx, workspace, "Lakehouse", "Probe", id)
			return err
		}},
		{"create-environment", func(ctx context.Context, c *Client, workspace, id string) error {
			_, err := c.CreateItem(ctx, workspace, "Environment", "Probe", id)
			return err
		}},
		{"create-folder", func(ctx context.Context, c *Client, workspace, id string) error {
			_, err := c.CreateFolder(ctx, workspace, "Probe", id)
			return err
		}},
		{"delete-item", func(ctx context.Context, c *Client, workspace, id string) error {
			return c.DeleteItem(ctx, workspace, id)
		}},
		{"delete-folder", func(ctx context.Context, c *Client, workspace, id string) error {
			return c.DeleteFolder(ctx, workspace, id)
		}},
	}
}

func mutationCalls() []mutationInvocation {
	return append(mutationWrites(),
		mutationInvocation{"get-item", func(ctx context.Context, c *Client, workspace, id string) error {
			_, err := c.GetItem(ctx, workspace, id)
			return err
		}},
		mutationInvocation{"get-folder", func(ctx context.Context, c *Client, workspace, id string) error {
			_, err := c.GetFolder(ctx, workspace, id)
			return err
		}},
	)
}

func TestMutationInvalidIDsContextsAndKindsBeforeAuthentication(t *testing.T) {
	client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("invalid input reached a request")
		w.WriteHeader(http.StatusBadRequest)
	}, Options{})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, call := range mutationCalls() {
		t.Run(call.name, func(t *testing.T) {
			for _, id := range []string{"", "../escape", "human name", itemID + "/child", strings.ReplaceAll(itemID, "-", "")} {
				if err := call.invoke(context.Background(), client, id, secondID); !errors.Is(err, fs.ErrInvalid) {
					t.Fatal("invalid workspace ID accepted", err)
				}
				if id != "" || !strings.HasPrefix(call.name, "create-") {
					if err := call.invoke(context.Background(), client, workspaceID, id); !errors.Is(err, fs.ErrInvalid) {
						t.Fatal("invalid object or parent ID accepted", err)
					}
				}
			}
			if err := call.invoke(nil, client, workspaceID, secondID); !errors.Is(err, fs.ErrInvalid) {
				t.Fatal("nil context accepted", err)
			}
			if err := call.invoke(canceled, client, workspaceID, secondID); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled context accepted", err)
			}
			if err := call.invoke(context.Background(), New(nil, Options{}), workspaceID, secondID); !errors.Is(err, fs.ErrInvalid) {
				t.Fatal("invalid client configuration accepted", err)
			}
		})
	}
	for _, kind := range []string{"notebook", "Warehouse", "", ".Notebook", "Notebook/other"} {
		if _, err := client.CreateItem(context.Background(), workspaceID, kind, "Probe", ""); !errors.Is(err, fserrors.ErrUnsupported) {
			t.Fatal("unsupported item kind accepted", err)
		}
	}
	if tokens.calls.Load() != 0 {
		t.Fatal("invalid mutation inputs acquired tokens")
	}
}

func TestMutationCreateItemRejectsWrongIdentity(t *testing.T) {
	tests := []struct {
		name  string
		patch func(map[string]any)
	}{
		{"missing-id", func(m map[string]any) { delete(m, "id") }},
		{"invalid-id", func(m map[string]any) { m["id"] = "not-an-id" }},
		{"numeric-id", func(m map[string]any) { m["id"] = 42 }},
		{"missing-name", func(m map[string]any) { delete(m, "displayName") }},
		{"wrong-name", func(m map[string]any) { m["displayName"] = "Other" }},
		{"missing-type", func(m map[string]any) { delete(m, "type") }},
		{"wrong-type", func(m map[string]any) { m["type"] = "Lakehouse" }},
		{"wrong-type-case", func(m map[string]any) { m["type"] = "notebook" }},
		{"missing-parent", func(m map[string]any) { delete(m, "folderId") }},
		{"null-parent", func(m map[string]any) { m["folderId"] = nil }},
		{"wrong-parent", func(m map[string]any) { m["folderId"] = operationID }},
		{"invalid-parent", func(m map[string]any) { m["folderId"] = "folder-name" }},
		{"empty-parent", func(m map[string]any) { m["folderId"] = "" }},
		{"wrong-parent-field", func(m map[string]any) { delete(m, "folderId"); m["parentFolderId"] = secondID }},
		{"wrong-workspace", func(m map[string]any) { m["workspaceId"] = secondID }},
		{"invalid-workspace", func(m map[string]any) { m["workspaceId"] = "workspace-name" }},
		{"empty-workspace", func(m map[string]any) { m["workspaceId"] = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire := map[string]any{"id": itemID, "type": "Notebook", "displayName": "Probe", "folderId": secondID, "workspaceId": workspaceID}
			test.patch(wire)
			body := mutationJSON(t, wire)
			client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
				fmt.Fprint(w, body)
			}, Options{})
			item, err := client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", secondID)
			if err == nil || item != (Item{}) || tokens.calls.Load() != 1 {
				t.Fatal("incorrect create identity returned a phantom item")
			}
		})
	}
}

func TestMutationCreateFolderRejectsWrongIdentity(t *testing.T) {
	for _, test := range []struct {
		name  string
		patch func(map[string]any)
	}{
		{"missing-id", func(m map[string]any) { delete(m, "id") }},
		{"invalid-id", func(m map[string]any) { m["id"] = "folder-name" }},
		{"missing-name", func(m map[string]any) { delete(m, "displayName") }},
		{"wrong-name", func(m map[string]any) { m["displayName"] = "Other" }},
		{"missing-parent", func(m map[string]any) { delete(m, "parentFolderId") }},
		{"wrong-parent", func(m map[string]any) { m["parentFolderId"] = operationID }},
		{"invalid-parent", func(m map[string]any) { m["parentFolderId"] = "parent-name" }},
		{"wrong-parent-field", func(m map[string]any) { delete(m, "parentFolderId"); m["folderId"] = secondID }},
		{"own-parent", func(m map[string]any) { m["id"] = secondID }},
		{"wrong-workspace", func(m map[string]any) { m["workspaceId"] = secondID }},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := map[string]any{"id": itemID, "displayName": "Probe", "parentFolderId": secondID, "workspaceId": workspaceID}
			test.patch(wire)
			body := mutationJSON(t, wire)
			client, _ := api(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
				fmt.Fprint(w, body)
			}, Options{})
			folder, err := client.CreateFolder(context.Background(), workspaceID, "Probe", secondID)
			if err == nil || folder != (Folder{}) {
				t.Fatal("incorrect create identity returned a phantom folder")
			}
		})
	}
}

func TestMutationRootParentsMustBeAbsentOrNull(t *testing.T) {
	for _, isFolder := range []bool{false, true} {
		for _, parent := range []any{nil, secondID, ""} {
			t.Run(fmt.Sprintf("folder=%t/parent=%v", isFolder, parent), func(t *testing.T) {
				wire := map[string]any{"id": itemID, "displayName": "Probe", "type": "Notebook"}
				if isFolder {
					wire["parentFolderId"] = parent
				} else {
					wire["folderId"] = parent
				}
				body := mutationJSON(t, wire)
				client, _ := api(t, func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusCreated)
					fmt.Fprint(w, body)
				}, Options{})
				var err error
				if isFolder {
					_, err = client.CreateFolder(context.Background(), workspaceID, "Probe", "")
				} else {
					_, err = client.CreateItem(context.Background(), workspaceID, "Notebook", "Probe", "")
				}
				if (err == nil) != (parent == nil) {
					t.Fatal("root create accepted a non-root or malformed parent", err)
				}
			})
		}
	}
}

func TestMutationIdentityReadsAndMalformedResponses(t *testing.T) {
	for _, isFolder := range []bool{false, true} {
		t.Run(fmt.Sprintf("folder=%t", isFolder), func(t *testing.T) {
			body := mutationItemBody(t, "Notebook", "Probe", secondID)
			collection := "items"
			if isFolder {
				body = mutationFolderBody(t, "Probe", secondID)
				collection = "folders"
			}
			client, tokens := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/workspaces/"+workspaceID+"/"+collection+"/"+itemID ||
					r.URL.RawQuery != "" {
					t.Error("identity read used a name or wrong ID")
				}
				fmt.Fprint(w, body)
			}, Options{})
			if isFolder {
				folder, err := client.GetFolder(context.Background(), workspaceID, itemID)
				if err != nil || folder.ID != itemID || folder.ParentFolderID != secondID {
					t.Fatal("folder identity read failed", err)
				}
			} else {
				item, err := client.GetItem(context.Background(), workspaceID, itemID)
				if err != nil || item.ID != itemID || item.Type != "Notebook" || item.FolderID != secondID {
					t.Fatal("item identity read failed", err)
				}
			}
			if tokens.calls.Load() != 1 {
				t.Fatal("identity read fetched unexpected credentials")
			}
		})
	}
	for i, body := range []string{
		"", `{`, `{}`, `null`, `[]`, `{} trailing`,
		`{"id":"` + operationID + `","displayName":"Probe","type":"Notebook"}`,
		`{"id":"` + itemID + `","displayName":"Probe","type":"Notebook","workspaceId":"` + secondID + `"}`,
	} {
		for _, call := range mutationCalls() {
			if strings.HasPrefix(call.name, "delete-") {
				continue
			}
			t.Run(fmt.Sprintf("%s/invalid-body-%d", call.name, i), func(t *testing.T) {
				client, _ := api(t, func(w http.ResponseWriter, _ *http.Request) {
					fmt.Fprint(w, body)
				}, Options{})
				if err := call.invoke(context.Background(), client, workspaceID, secondID); err == nil {
					t.Fatal("missing, malformed, or mismatched response succeeded")
				}
			})
		}
	}
}

func TestMutationDeleteUsesExactIDAndDocumentedStatus(t *testing.T) {
	for _, collection := range []string{"items", "folders"} {
		for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent} {
			t.Run(fmt.Sprintf("%s/status=%d", collection, status), func(t *testing.T) {
				var calls atomic.Int64
				client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil || len(body) != 0 || r.Method != http.MethodDelete ||
						r.URL.Path != "/v1/workspaces/"+workspaceID+"/"+collection+"/"+itemID ||
						r.URL.RawQuery != "" || r.Header.Get("If-Match") != "" {
						t.Error("delete used a body, human name, conditional, hardDelete query, or wrong method")
					}
					w.Header().Set("Location", "/v1/operations/"+operationID)
					w.Header().Set("x-ms-operation-id", operationID)
					w.WriteHeader(status)
				}, Options{})
				var err error
				if collection == "items" {
					err = client.DeleteItem(context.Background(), workspaceID, itemID)
				} else {
					err = client.DeleteFolder(context.Background(), workspaceID, itemID)
				}
				if (err == nil) != (status == http.StatusOK) || calls.Load() != 1 {
					t.Fatal("delete accepted an undocumented success or issued a follow-up write", err)
				}
				if err != nil {
					var statusErr *transport.HTTPError
					if !errors.As(err, &statusErr) || statusErr.StatusCode != status || statusErr.Method != http.MethodDelete {
						t.Fatal("delete discarded the unexpected response status", err)
					}
				}
			})
		}
	}
}

func TestMutationFolderNotEmptyErrorIsPreserved(t *testing.T) {
	client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-ms-request-id", operationID)
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"errorCode":"FolderNotEmpty","message":"folder contains remote children"}`)
	}, Options{})
	err := client.DeleteFolder(context.Background(), workspaceID, itemID)
	var status *transport.HTTPError
	if !errors.As(err, &status) || status.StatusCode != http.StatusConflict ||
		status.Code != "FolderNotEmpty" || status.RequestID != operationID || tokens.calls.Load() != 1 {
		t.Fatal("server-side empty-folder enforcement was hidden", err)
	}
}

func TestMutationWritesNeverRetryAndKeepSafeHTTPError(t *testing.T) {
	for _, call := range mutationWrites() {
		for _, status := range []int{401, 403, 408, 409, 429, 500, 503} {
			t.Run(fmt.Sprintf("%s/status=%d", call.name, status), func(t *testing.T) {
				var calls atomic.Int64
				client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					w.Header().Set("Retry-After", "0")
					w.Header().Set("x-ms-request-id", operationID)
					w.WriteHeader(status)
					fmt.Fprint(w, `{"errorCode":"ServiceProbe","message":"private-service-message"}`)
				}, Options{})
				err := call.invoke(context.Background(), client, workspaceID, secondID)
				var statusErr *transport.HTTPError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != status ||
					statusErr.Code != "ServiceProbe" || statusErr.RequestID != operationID ||
					calls.Load() != 1 || tokens.calls.Load() != 1 {
					t.Fatalf("write retried or lost its typed error: calls=%d err=%v", calls.Load(), err)
				}
				if strings.Contains(err.Error(), "private-service-message") {
					t.Fatal("service body leaked through an error")
				}
			})
		}
	}
}

func TestMutationWritesNeverRetryLostResponse(t *testing.T) {
	for _, call := range mutationWrites() {
		t.Run(call.name, func(t *testing.T) {
			var calls atomic.Int64
			client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					t.Error("test server cannot simulate a lost response")
					return
				}
				conn, _, err := hijacker.Hijack()
				if err != nil {
					t.Error("unable to simulate a lost response")
					return
				}
				conn.Close()
			}, Options{})
			if err := call.invoke(context.Background(), client, workspaceID, secondID); err == nil ||
				calls.Load() != 1 || tokens.calls.Load() != 1 {
				t.Fatalf("ambiguous write was retried: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestMutationRequestsNeverFollowRedirects(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	for _, call := range mutationCalls() {
		for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
			t.Run(fmt.Sprintf("%s/status=%d", call.name, status), func(t *testing.T) {
				client, tokens := api(t, func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Location", target.URL)
					w.WriteHeader(status)
				}, Options{})
				err := call.invoke(context.Background(), client, workspaceID, secondID)
				var statusErr *transport.HTTPError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != status || tokens.calls.Load() != 1 {
					t.Fatal("mutation redirected, retried, or hid the redirect status", err)
				}
			})
		}
	}
	if redirected.Load() != 0 {
		t.Fatal("mutation credentials or writes reached a redirect target")
	}
}

func TestMutationResponseBounds(t *testing.T) {
	for _, call := range mutationCalls() {
		for _, chunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/chunked=%t", call.name, chunked), func(t *testing.T) {
				client, _ := api(t, func(w http.ResponseWriter, _ *http.Request) {
					if chunked {
						w.(http.Flusher).Flush()
					}
					fmt.Fprint(w, strings.Repeat("x", 129))
				}, Options{MaxDefinitionBytes: 128})
				if err := call.invoke(context.Background(), client, workspaceID, secondID); !errors.Is(err, fserrors.ErrTooLarge) {
					t.Fatal("mutation response was not bounded", err)
				}
			})
		}
	}
}
