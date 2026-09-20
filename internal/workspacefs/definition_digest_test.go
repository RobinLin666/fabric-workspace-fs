package workspacefs

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"strings"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
)

func checkDefinitionDigestEquality(tb testing.TB, a, b fabric.Definition) {
	tb.Helper()
	jsonA, jsonErrA := json.Marshal(a)
	jsonB, jsonErrB := json.Marshal(b)
	hashA, errA := digest(a)
	hashB, errB := digest(b)
	if (jsonErrA != nil) != (errA != nil) || (jsonErrB != nil) != (errB != nil) {
		tb.Fatalf("JSON/digest validity differs: JSON=(%v,%v) digest=(%v,%v)", jsonErrA, jsonErrB, errA, errB)
	}
	for _, err := range []error{errA, errB} {
		if err != nil && !errors.Is(err, fs.ErrInvalid) {
			tb.Fatalf("invalid definition lost fs.ErrInvalid: %v", err)
		}
	}
	if errA == nil && errB == nil && (bytes.Equal(jsonA, jsonB) != (hashA == hashB)) {
		tb.Fatalf("digest equality differs from marshaled equality:\na=%q\nb=%q", jsonA, jsonB)
	}
}

func digestTestDefinition() fabric.Definition {
	return fabric.Definition{
		Format: "ipynb",
		Extra:  map[string]json.RawMessage{"unknown": json.RawMessage(`{"version":1}`)},
		Parts: []fabric.Part{
			{Path: ".platform", Payload: "e30=", PayloadType: "InlineBase64"},
			{Path: "notebook.ipynb", Payload: "e30=", PayloadType: "InlineBase64",
				Extra: map[string]json.RawMessage{"unknown_part": json.RawMessage(`[1,true]`)}},
		},
	}
}

func TestDefinitionDigestJSONEqualityEdges(t *testing.T) {
	base := digestTestDefinition()
	tests := []struct {
		name   string
		mutate func(*fabric.Definition)
	}{
		{"Format", func(d *fabric.Definition) { d.Format = "" }},
		{"OrderedParts", func(d *fabric.Definition) { d.Parts[0], d.Parts[1] = d.Parts[1], d.Parts[0] }},
		{"PartPath", func(d *fabric.Definition) { d.Parts[0].Path = "other" }},
		{"PartPayload", func(d *fabric.Definition) { d.Parts[0].Payload = "W10=" }},
		{"PartEncoding", func(d *fabric.Definition) { d.Parts[0].PayloadType = "Other" }},
		{"DefinitionMetadata", func(d *fabric.Definition) { d.Extra["unknown"] = json.RawMessage(`{"version":2}`) }},
		{"PartMetadata", func(d *fabric.Definition) { d.Parts[1].Extra["unknown_part"] = json.RawMessage(`[2,true]`) }},
		{"IgnoredDefinitionCollision", func(d *fabric.Definition) {
			d.Extra["format"] = json.RawMessage(`invalid`)
			d.Extra["parts"] = json.RawMessage(`invalid`)
		}},
		{"IgnoredPartCollision", func(d *fabric.Definition) {
			d.Parts[1].Extra["path"] = json.RawMessage(`invalid`)
			d.Parts[1].Extra["payload"] = json.RawMessage(`invalid`)
			d.Parts[1].Extra["payloadType"] = json.RawMessage(`invalid`)
		}},
		{"RawWhitespace", func(d *fabric.Definition) { d.Extra["unknown"] = json.RawMessage(" \n{ \"version\" : 1 } \t") }},
		{"RawNumberSpelling", func(d *fabric.Definition) { d.Extra["unknown"] = json.RawMessage(`{"version":1.0}`) }},
		{"InvalidRaw", func(d *fabric.Definition) { d.Extra["unknown"] = json.RawMessage(`{"version":`) }},
		{"InvalidPartRaw", func(d *fabric.Definition) { d.Parts[1].Extra["unknown_part"] = json.RawMessage(`invalid`) }},
		{"EmptyRaw", func(d *fabric.Definition) { d.Extra["unknown"] = json.RawMessage{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			other := cloneDefinition(base)
			test.mutate(&other)
			checkDefinitionDigestEquality(t, base, other)
		})
	}
	t.Run("NilAndEmptyParts", func(t *testing.T) {
		checkDefinitionDigestEquality(t, fabric.Definition{}, fabric.Definition{Parts: []fabric.Part{}})
	})
	t.Run("NilAndEmptyExtra", func(t *testing.T) {
		checkDefinitionDigestEquality(t, fabric.Definition{}, fabric.Definition{Extra: map[string]json.RawMessage{}})
	})
	t.Run("OmittedFormatCollision", func(t *testing.T) {
		checkDefinitionDigestEquality(t, fabric.Definition{}, fabric.Definition{
			Extra: map[string]json.RawMessage{"format": json.RawMessage(`invalid`), "parts": json.RawMessage(`invalid`)},
		})
	})
	raws := []json.RawMessage{
		nil, json.RawMessage(`null`), json.RawMessage(" \nnull\t"), json.RawMessage{},
		json.RawMessage(`"<>&"`), json.RawMessage(`"\u003c\u003e\u0026"`),
		json.RawMessage(`"a"`), json.RawMessage(`"\u0061"`),
		json.RawMessage(`{"a":1,"b":2}`), json.RawMessage(`{ "a" : 1, "b" : 2 }`),
		json.RawMessage(`{"b":2,"a":1}`), json.RawMessage(`{"a":1,"a":2}`),
		json.RawMessage(`1`), json.RawMessage(`1.0`), json.RawMessage(`1e0`),
		json.RawMessage("\"\u2028\u2029\""), json.RawMessage(`"\u2028\u2029"`),
		json.RawMessage{'"', 0xff, '"'}, json.RawMessage(`"�"`),
	}
	for i, rawA := range raws {
		for j, rawB := range raws {
			a, b := cloneDefinition(base), cloneDefinition(base)
			a.Extra["unknown"], b.Extra["unknown"] = rawA, rawB
			a.Parts[1].Extra["unknown_part"], b.Parts[1].Extra["unknown_part"] = rawA, rawB
			t.Run("RawPairs/"+string(rune('A'+i))+"/"+string(rune('A'+j)), func(t *testing.T) {
				checkDefinitionDigestEquality(t, a, b)
			})
		}
	}
}

func TestDefinitionDigestStringNormalization(t *testing.T) {
	strings := []string{
		"", "ascii", "AA+/09==", "\"\\\n\r\t\b\f\x00\x1f\x7f", "<>&", "\u2028\u2029",
		"世界", "�", "\xff", "\xfe", "\xff\xfe", "��", "\xed\xa0\x80", "���",
	}

	for _, aString := range strings {
		for _, bString := range strings {
			a, b := digestTestDefinition(), digestTestDefinition()
			a.Format, b.Format = aString, bString
			for i := range a.Parts {
				a.Parts[i].Path, a.Parts[i].Payload, a.Parts[i].PayloadType = aString, aString, aString
				b.Parts[i].Path, b.Parts[i].Payload, b.Parts[i].PayloadType = bString, bString, bString
			}
			a.Extra[aString], b.Extra[bString] = json.RawMessage(`true`), json.RawMessage(`true`)
			checkDefinitionDigestEquality(t, a, b)
		}
	}
	// Distinct invalid UTF-8 map keys can normalize to duplicate JSON keys.
	// Sorting must use the original Go strings, just like encoding/json.
	a, b := digestTestDefinition(), digestTestDefinition()
	a.Extra = map[string]json.RawMessage{"\xff": json.RawMessage(`1`), "\xfe": json.RawMessage(`2`)}
	b.Extra = map[string]json.RawMessage{"\xff": json.RawMessage(`2`), "\xfe": json.RawMessage(`1`)}
	checkDefinitionDigestEquality(t, a, b)
}

func TestDefinitionDigestJSONNestingBoundary(t *testing.T) {
	for _, depth := range []int{9997, 9998, 9999, 10000, 10001} {
		raw := json.RawMessage(strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth))
		definition := digestTestDefinition()
		definition.Extra["deep"] = raw
		checkDefinitionDigestEquality(t, definition, definition)
		part := digestTestDefinition()
		part.Parts[1].Extra["deep"] = raw
		checkDefinitionDigestEquality(t, part, part)
	}
}

func TestDefinitionDigestEqualityProperty(t *testing.T) {
	random := rand.New(rand.NewSource(20260920))
	for range 500 {
		a := digestTestDefinition()
		data := make([]byte, random.Intn(256))
		if _, err := random.Read(data); err != nil {
			t.Fatal(err)
		}
		a.Parts[1].Payload = string(data)
		a.Extra[string(data)] = json.RawMessage(` { "opaque" : [1, true, null] } `)
		b := cloneDefinition(a)
		switch random.Intn(5) {
		case 0:
			b.Parts[1].Payload += "x"
		case 1:
			b.Extra[string(data)] = json.RawMessage(`{"opaque":[1,true,null]}`)
		case 2:
			b.Parts[1].Extra["payload"] = json.RawMessage("not JSON")
		case 3:
			b.Extra[string(data)] = json.RawMessage(`{"opaque":[1,false,null]}`)
		case 4:
			var normalized string
			encoded, err := json.Marshal(b.Parts[1].Payload)
			if err != nil || json.Unmarshal(encoded, &normalized) != nil {
				t.Fatal("string normalization failed", err)
			}
			b.Parts[1].Payload = normalized
		}
		checkDefinitionDigestEquality(t, a, b)
	}
}

func TestDefinitionDigestMetadataChangesStillConflict(t *testing.T) {
	for _, mutation := range []struct {
		name string
		edit func(*fabric.Definition)
	}{
		{"DefinitionExtra", func(d *fabric.Definition) { d.Extra["new"] = json.RawMessage(`true`) }},
		{"NotebookPartExtra", func(d *fabric.Definition) { d.Parts[1].Extra["new"] = json.RawMessage(`true`) }},
		{"PlatformPayload", func(d *fabric.Definition) { d.Parts[0].Payload = "W10=" }},
		{"UnknownPartPayload", func(d *fabric.Definition) { d.Parts[2].Payload = "W10=" }},
		{"UnknownPartExtra", func(d *fabric.Definition) { d.Parts[2].Extra["new"] = json.RawMessage(`true`) }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			h := newOfflineNotebookHarness(t, 1024)
			handle, err := h.fs.Open(h.ctx, h.entry, os.O_RDWR)
			if err != nil {
				t.Fatal(err)
			}
			defer handle.Close()
			h.api.mu.Lock()
			mutation.edit(&h.api.def)
			h.api.mu.Unlock()
			if _, err := handle.WriteAt(h.ctx, h.changed, 0); err != nil {
				t.Fatal(err)
			}
			before := h.counts()
			if err := handle.Flush(h.ctx); !errors.Is(err, fserrors.ErrConflict) {
				t.Fatalf("external metadata/content change did not conflict: %v", err)
			}
			if delta := h.counts().sub(before); delta.reads != 1 || delta.updates != 0 {
				t.Fatalf("conflict should preflight once without updating: %+v", delta)
			}
		})
	}
}

func FuzzDefinitionDigestJSONEquality(f *testing.F) {
	for _, seed := range []struct {
		text string
		raw  string
	}{
		{"e30=", ` { "unknown" : [1, true] } `},
		{"<>&\u2028\u2029", `"<>&"`},
		{"\xff\xfe", `"\u0061"`},
		{"\x00\"\\\n", `null`},
		{"", `invalid`},
		{"世界", `{"a":1,"a":2}`},
	} {
		for operation := uint8(0); operation < 5; operation++ {
			f.Add(seed.text, []byte(seed.raw), operation)
		}
	}
	f.Fuzz(func(t *testing.T, text string, raw []byte, operation uint8) {
		if len(text) > 8192 || len(raw) > 8192 {
			t.Skip()
		}
		a := digestTestDefinition()
		a.Parts[1].Payload = text
		a.Extra[text] = json.RawMessage(raw)
		a.Parts[1].Extra["unknown_part"] = json.RawMessage(raw)
		b := cloneDefinition(a)
		switch operation % 5 {
		case 0:
			b.Parts[1].Payload += "x"
		case 1:
			b.Extra["format"] = json.RawMessage(`invalid ignored collision`)
		case 2:
			encoded, err := json.Marshal(json.RawMessage(raw))
			if err == nil {
				b.Extra[text] = encoded
				b.Parts[1].Extra["unknown_part"] = encoded
			}
		case 3:
			var normalized string
			encoded, _ := json.Marshal(text)
			if err := json.Unmarshal(encoded, &normalized); err != nil {
				t.Fatal(err)
			}
			b.Parts[1].Payload = normalized
		case 4:
			b.Parts[1].Extra["new"] = json.RawMessage(`true`)
		}
		checkDefinitionDigestEquality(t, a, b)
	})
}
