package main

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"testing"

	"fabric-workspace-fs/internal/fabric"
)

const testRemoteNotebookPart = "original-server-notebook-part.ipynb"

func definitionForTest() fabric.Definition {
	return fabric.Definition{
		Format: "ipynb",
		Extra:  map[string]json.RawMessage{"serverMetadata": json.RawMessage(`{"large":9007199254740993}`)},
		Parts: []fabric.Part{
			{Path: testRemoteNotebookPart, PayloadType: "InlineBase64", Payload: base64.StdEncoding.EncodeToString(initialNotebook()),
				Extra: map[string]json.RawMessage{"bodyMetadata": json.RawMessage(`{"keep":true}`)}},
			{Path: ".platform", PayloadType: "InlineBase64", Payload: base64.StdEncoding.EncodeToString([]byte(`{"platform":"preserved"}`)),
				Extra: map[string]json.RawMessage{"platformMetadata": json.RawMessage(`{"keep":9007199254740993}`)}},
			{Path: "arbitrary/hidden-sidecar.json", PayloadType: "InlineBase64", Payload: base64.StdEncoding.EncodeToString([]byte(`{"unchanged":true}`))},
		},
	}
}

func savedDefinitionForTest(t *testing.T, marker string) fabric.Definition {
	t.Helper()
	after := definitionForTest()
	updated, err := setNotebookMarker(initialNotebook(), marker)
	if err != nil {
		t.Fatal(err)
	}
	after.Parts[0].Payload = base64.StdEncoding.EncodeToString(updated)
	return after
}

func TestDefinitionRoundTripPreservesHiddenPartsAndUnknownMetadata(t *testing.T) {
	before, after := definitionForTest(), savedDefinitionForTest(t, "owned-marker")
	original, _ := json.Marshal(before)
	for range 2 {
		if err := verifyNotebookDefinitionRoundTrip(before, after, testRemoteNotebookPart, "owned-marker"); err != nil {
			t.Fatal("valid definition roundtrip rejected:", safeError(err))
		}
		slices.Reverse(after.Parts)
	}
	unchanged, _ := json.Marshal(before)
	if string(original) != string(unchanged) {
		t.Fatal("verification modified the original definition snapshot")
	}
	if _, err := notebookDefinitionBody(before, "content.ipynb"); err == nil {
		t.Fatal("local content filename was treated as the remote body path")
	}
}

func TestDefinitionRoundTripRejectsPartOrMetadataLoss(t *testing.T) {
	for _, change := range []struct {
		name   string
		mutate func(*fabric.Definition)
	}{
		{"platform bytes", func(d *fabric.Definition) { d.Parts[1].Payload = base64.StdEncoding.EncodeToString([]byte("changed")) }},
		{"platform type", func(d *fabric.Definition) { d.Parts[1].PayloadType = "Changed" }},
		{"platform metadata", func(d *fabric.Definition) { d.Parts[1].Extra = nil }},
		{"sidecar bytes", func(d *fabric.Definition) { d.Parts[2].Payload = "" }},
		{"missing platform", func(d *fabric.Definition) { d.Parts = []fabric.Part{d.Parts[0], d.Parts[2]} }},
		{"extra part", func(d *fabric.Definition) {
			d.Parts = append(d.Parts, fabric.Part{Path: "new.txt", PayloadType: "InlineBase64"})
		}},
		{"renamed part", func(d *fabric.Definition) { d.Parts[2].Path = "other.txt" }},
		{"duplicate part", func(d *fabric.Definition) { d.Parts[2] = d.Parts[1] }},
		{"format", func(d *fabric.Definition) { d.Format = "changed" }},
		{"definition metadata", func(d *fabric.Definition) { d.Extra = nil }},
		{"body metadata", func(d *fabric.Definition) { d.Parts[0].Extra = nil }},
		{"body path", func(d *fabric.Definition) { d.Parts[0].Path = "content.ipynb" }},
		{"invalid body", func(d *fabric.Definition) { d.Parts[0].Payload = "not-base64" }},
		{"marker missing", func(d *fabric.Definition) { d.Parts[0].Payload = base64.StdEncoding.EncodeToString(initialNotebook()) }},
		{"second body", func(d *fabric.Definition) { d.Parts[2].Path = "second.ipynb" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			before, after := definitionForTest(), savedDefinitionForTest(t, "owned-marker")
			change.mutate(&after)
			if err := verifyNotebookDefinitionRoundTrip(before, after, testRemoteNotebookPart, "owned-marker"); err == nil {
				t.Fatal("unsafe definition roundtrip was accepted")
			}
		})
	}
}
