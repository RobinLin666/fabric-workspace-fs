package main

import (
	"bytes"
	"encoding/json"
	"strings"

	"fabric-workspace-fs/internal/fabric"
)

func notebookDefinitionBody(def fabric.Definition, remotePart string) ([]byte, error) {
	if remotePart == "" {
		return nil, fail("notebook definition verification requires a resolved remote part")
	}
	seen := make(map[string]bool)
	var body fabric.Part
	count := 0
	for _, part := range def.Parts {
		if part.Path == "" || seen[part.Path] {
			return nil, fail("notebook definition contains an ambiguous part path")
		}
		seen[part.Path] = true
		if strings.HasSuffix(strings.ToLower(part.Path), ".ipynb") {
			body = part
			count++
		}
	}
	if count != 1 || body.Path != remotePart || body.PayloadType != "InlineBase64" ||
		len(body.Payload) > (maxNotebookBytes/3+1)*4 {
		return nil, fail("notebook definition did not preserve its exact bounded body part")
	}
	data, err := body.Decode()
	if err != nil {
		return nil, fail("notebook definition body could not be decoded")
	}
	if _, _, err := notebookDocument(data); err != nil {
		return nil, err
	}
	return data, nil
}

// The hidden .platform and every other remote part remain part of the
// definition roundtrip. Never expose or mutate them through a guessed FUSE path.
func verifyNotebookDefinitionRoundTrip(before, after fabric.Definition, remotePart, marker string) error {
	if _, err := notebookDefinitionBody(before, remotePart); err != nil {
		return err
	}
	data, err := notebookDefinitionBody(after, remotePart)
	if err != nil {
		return err
	}
	if err := hasNotebookMarker(data, marker); err != nil {
		return err
	}
	if len(before.Parts) != len(after.Parts) {
		return fail("notebook update added or removed a remote definition part")
	}
	actualParts := make(map[string]fabric.Part, len(after.Parts))
	for _, part := range after.Parts {
		actualParts[part.Path] = part
	}
	expected := before
	expected.Parts = append([]fabric.Part(nil), before.Parts...)
	orderedAfter := after
	orderedAfter.Parts = make([]fabric.Part, len(before.Parts))
	for i, part := range expected.Parts {
		actual, exists := actualParts[part.Path]
		if !exists {
			return fail("notebook update renamed a remote definition part")
		}
		orderedAfter.Parts[i] = actual
		if part.Path == remotePart {
			expected.Parts[i].Payload = actual.Payload
		}
	}
	expectedJSON, expectedErr := json.Marshal(expected)
	actualJSON, actualErr := json.Marshal(orderedAfter)
	if expectedErr != nil || actualErr != nil || !bytes.Equal(expectedJSON, actualJSON) {
		return fail("notebook update changed a preserved remote part or definition metadata")
	}
	return nil
}
