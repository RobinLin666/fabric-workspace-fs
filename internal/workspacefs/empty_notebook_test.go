package workspacefs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestPrepareNotebookForFabricRestoresNotebookRunResult(t *testing.T) {
	const mime = "application/vnd.synapse.mssparkutilsrunmultiple-result+json"
	payload := map[string]any{
		"activities": []any{map[string]any{
			"notebook_name": "Notebook_pyspark", "status": "success",
			"progress": float64(100), "duration": float64(11269),
			"exit_value": "", "exception": "", "snapshot_status": "success",
		}},
		"limit": float64(50),
	}
	input := map[string]any{
		"cells": []any{map[string]any{
			"outputs": []any{map[string]any{
				"output_type": "display_data",
				"data":        map[string]any{"text/html": "<table>completed</table>"},
				"metadata": map[string]any{
					"keep": true,
					"fabric_jupyter": map[string]any{
						"generated_mime_types": []any{"text/html"},
						"restore_data":         map[string]any{mime: payload},
					},
				},
			}},
		}},
		"metadata": map[string]any{
			"synapse_widget": map[string]any{"state": map[string]any{"existing": map[string]any{"type": "Synapse.DataFrame"}}},
		},
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := prepareNotebookForFabric(raw)
	if err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	if err := json.Unmarshal(got, &saved); err != nil {
		t.Fatal(err)
	}
	output := saved["cells"].([]any)[0].(map[string]any)["outputs"].([]any)[0].(map[string]any)
	if output["output_type"] != "display_data" || !reflect.DeepEqual(output["data"], map[string]any{mime: payload}) {
		t.Fatalf("native run result changed: %v", output)
	}
	if !reflect.DeepEqual(output["metadata"], map[string]any{"keep": true}) {
		t.Fatalf("output metadata not restored: %v", output["metadata"])
	}
	if !reflect.DeepEqual(saved["metadata"], input["metadata"]) {
		t.Fatal("unrelated notebook widget state was changed")
	}
}

func TestPrepareNotebookForFabricRestoresInlineTables(t *testing.T) {
	for mime, payload := range map[string]any{
		"application/vnd.synapse-jupyter.display-view+json": map[string]any{
			"table": map[string]any{
				"schema": []any{map[string]any{"key": "name", "name": "Name"}},
				"rows":   []any{map[string]any{"name": "<value>"}},
			},
		},
		"application/vnd.synapse.sparksql-result+json": map[string]any{
			"schema": map[string]any{"fields": []any{map[string]any{"name": "Name"}}},
			"data":   []any{[]any{"<value>"}},
		},
		"application/vnd.jupyter.statement-meta+json": map[string]any{"state": "finished"},
	} {
		t.Run(mime, func(t *testing.T) {
			cellMetadata := map[string]any{
				"sqlViewState":            map[string]any{"type": "chart"},
				"jupyterDisplayViewState": map[string]any{"type": "details"},
			}
			input := map[string]any{
				"cells": []any{map[string]any{
					"metadata": cellMetadata,
					"outputs": []any{map[string]any{
						"output_type": "display_data",
						"data":        map[string]any{"text/html": "<table>preview</table>", "text/plain": "original"},
						"metadata": map[string]any{
							"keep": true,
							"fabric_jupyter": map[string]any{
								"generated_mime_types": []any{"text/html"},
								"restore_data":         map[string]any{mime: payload},
							},
						},
					}},
				}},
			}
			raw, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			got, err := prepareNotebookForFabric(raw)
			if err != nil {
				t.Fatal(err)
			}
			var saved map[string]any
			if err := json.Unmarshal(got, &saved); err != nil {
				t.Fatal(err)
			}
			cell := saved["cells"].([]any)[0].(map[string]any)
			output := cell["outputs"].([]any)[0].(map[string]any)
			if !reflect.DeepEqual(output["data"], map[string]any{mime: payload, "text/plain": "original"}) {
				t.Fatalf("native MIME not restored: %v", output)
			}
			if !reflect.DeepEqual(output["metadata"], map[string]any{"keep": true}) ||
				!reflect.DeepEqual(cell["metadata"], cellMetadata) {
				t.Fatalf("metadata changed: %v", cell)
			}
		})
	}
}

func TestFabricEmptyNotebookExportCanBeSavedWithoutInventingCells(t *testing.T) {
	for _, original := range []string{`{"nbformat":4,"nbformat_minor":5,"metadata":{}}`, `{"nbformat":4,"cells":null,"metadata":{}}`} {
		s, remote := newTestFS(t, nil)
		def := remote.Definition()
		def.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte(original))
		remote.SetDefinition(def)
		_, entry := notebook(t, s)
		h, err := s.Open(context.Background(), entry, os.O_RDWR)
		if err != nil {
			t.Fatal(err)
		}
		if read(t, h) != original {
			t.Fatal("empty export was silently rewritten")
		}
		save(t, h, original+" ")
		if err := h.Close(); err != nil {
			t.Fatal("actual empty export could not be saved", err)
		}
	}
}

func TestPrepareNotebookForFabricRestoresWidgetOutputAndMetadata(t *testing.T) {
	input := []byte(`{
			"nbformat":4,
			"cells":[{
				"cell_type":"code",
				"outputs":[{
					"output_type":"display_data",
					"data":{
						"application/vnd.synapse.widget-view+json":{
							"widget_id":"widget-1",
							"widget_type":"Synapse.DataFrame"
						},
						"text/plain":"SynapseWidget(Synapse.DataFrame, widget-1)",
						"text/html":"<table><tr><td>value</td></tr></table>",
						"application/vnd.dataresource+json":{"data":[{"name":"value"}]}
					},
					"metadata":{
						"fabric_jupyter":{
							"generated_mime_types":["application/vnd.dataresource+json","text/html"],
							"widget_id":"widget-1",
							"widget_state":{
								"type":"Synapse.DataFrame",
								"sync_state":{
									"table":{
										"schema":[{"key":"0","name":"name","type":"string"}],
										"rows":[{"0":"value"}],
										"truncated":false
									}
								}
							}
						}
					}
				}]
			}],
			"metadata":{
				"synapse_widget":{
					"version":"0.1",
					"state":{
						"widget-1":{"persist_state":{"view":{"pageSize":50}}},
						"orphan":{"type":"Synapse.DataFrame"}
					}
				}
			}
		}`)
	got, err := prepareNotebookForFabric(input)
	if err != nil {
		t.Fatal(err)
	}
	var notebook map[string]any
	if err := json.Unmarshal(got, &notebook); err != nil {
		t.Fatal(err)
	}
	output := notebook["cells"].([]any)[0].(map[string]any)["outputs"].([]any)[0].(map[string]any)
	data := output["data"].(map[string]any)
	if _, exists := data["text/html"]; exists {
		t.Fatal("generated HTML was persisted")
	}
	if _, exists := data["application/vnd.dataresource+json"]; exists {
		t.Fatal("generated data resource was persisted")
	}
	if _, exists := output["metadata"].(map[string]any)["fabric_jupyter"]; exists {
		t.Fatal("fabric-jupyter output metadata was persisted")
	}
	metadata := notebook["metadata"].(map[string]any)
	synapse := metadata["synapse_widget"].(map[string]any)
	states := synapse["state"].(map[string]any)
	if _, exists := states["orphan"]; exists {
		t.Fatal("orphaned widget state was retained")
	}
	state := states["widget-1"].(map[string]any)
	if state["type"] != "Synapse.DataFrame" {
		t.Fatalf("widget type = %v", state["type"])
	}
	persist := state["persist_state"].(map[string]any)
	view := persist["view"].(map[string]any)
	if view["pageSize"] != float64(50) {
		t.Fatalf("persisted view state lost: %v", view)
	}
}

func TestPrepareNotebookForFabricLeavesOrdinaryNotebookBytesUnchanged(t *testing.T) {
	input := []byte("{\n  \"nbformat\": 4,\n  \"cells\": [],\n  \"metadata\": {}\n}\n")
	got, err := prepareNotebookForFabric(input)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(input) {
		t.Fatalf("ordinary notebook rewritten:\n%s", got)
	}
}

func TestPrepareNotebookForFabricStripsStatementFallbackWithoutPruningWidgets(t *testing.T) {
	input := []byte(`{
		"nbformat":4,
		"cells":[{
			"outputs":[{
				"output_type":"display_data",
				"data":{
					"application/vnd.livy.statement-meta+json":{"state":"finished"},
					"text/plain":"StatementMeta(, session, 1, Finished, Available)",
					"text/html":"<span style=\"display:none\"></span>"
				},
				"metadata":{"fabric_jupyter":{"generated_mime_types":["text/html"]}}
			}]
		}],
		"metadata":{
			"synapse_widget":{
				"version":"0.1",
				"state":{"widget-1":{"type":"Synapse.DataFrame"}}
			}
		}
	}`)
	got, err := prepareNotebookForFabric(input)
	if err != nil {
		t.Fatal(err)
	}
	var notebook map[string]any
	if err := json.Unmarshal(got, &notebook); err != nil {
		t.Fatal(err)
	}
	output := notebook["cells"].([]any)[0].(map[string]any)["outputs"].([]any)[0].(map[string]any)
	if _, exists := output["data"].(map[string]any)["text/html"]; exists {
		t.Fatal("generated statement HTML was persisted")
	}
	states := notebook["metadata"].(map[string]any)["synapse_widget"].(map[string]any)["state"].(map[string]any)
	if _, exists := states["widget-1"]; !exists {
		t.Fatal("unrelated widget state was pruned")
	}
}
