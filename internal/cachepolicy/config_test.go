package cachepolicy_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
)

func TestValidTypeSurfaces(t *testing.T) {
	for kind, surfaces := range map[string][]string{
		"Notebook":    {"content", "builtin"},
		"Lakehouse":   {"Files", "Tables"},
		"Environment": {"definition", "resources"},
	} {
		for _, surface := range surfaces {
			t.Run(kind+"/"+surface, func(t *testing.T) {
				policy := mustParse(t, fmt.Sprintf(`{"types":{%q:{"surfaces":{%q:{"attr":"1.5s"}}}}}`, kind, surface))
				if got := policy.Resolve(cachepolicy.Selector{Type: kind, Surface: surface}).Attr; got != 1500*time.Millisecond {
					t.Fatalf("surface override = %s", got)
				}
			})
		}
	}
}

func TestUppercaseUUIDsAndWorkspaceScopedItems(t *testing.T) {
	policy := mustParse(t, fmt.Sprintf(`{"workspaces":{
		%q:{"catalog":"7s","items":{%q:{"type":"Environment","surfaces":{"resources":{"content":"8s"}}}}},
		%q:{"items":{%q:{"type":"Notebook","surfaces":{"builtin":{"content":"9s"}}}}}
	}}`, strings.ToUpper(workspaceID), strings.ToUpper(itemID), otherWSID, itemID))
	if got := policy.Resolve(cachepolicy.Selector{Workspace: workspaceID, Item: itemID, Surface: "resources"}); got.Content != 8*time.Second || got.Catalog != 7*time.Second {
		t.Fatalf("uppercase UUID policy = %+v", got)
	}
	if got := policy.Resolve(cachepolicy.Selector{Workspace: otherWSID, Item: itemID, Surface: "builtin"}); got.Content != 9*time.Second || got.Catalog != cachepolicy.DefaultTTL {
		t.Fatalf("other workspace's item policy = %+v", got)
	}
}

func TestInvalidJSONAndSchema(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{"empty", ""},
		{"root null", "null"},
		{"root array", "[]"},
		{"root string", `"policy"`},
		{"syntax", `{"defaults":`},
		{"trailing comma", `{"defaults":{},}`},
		{"trailing object", `{} {}`},
		{"trailing scalar", `{} true`},
		{"trailing null", `{} null`},
		{"trailing garbage", `{} trailing`},
		{"unknown root", `{"ttl":"2m"}`},
		{"case sensitive root", `{"Defaults":{}}`},
		{"defaults array", `{"defaults":[]}`},
		{"defaults null", `{"defaults":null}`},
		{"unknown layer", `{"defaults":{"ttl":"2m"}}`},
		{"case sensitive layer", `{"defaults":{"Attr":"2m"}}`},
		{"kernel misspelling", `{"defaults":{"kernelattr":"2m"}}`},
		{"unknown type", `{"types":{"Warehouse":{}}}`},
		{"case sensitive type", `{"types":{"notebook":{}}}`},
		{"types array", `{"types":[]}`},
		{"type null", `{"types":{"Notebook":null}}`},
		{"type string", `{"types":{"Notebook":"2m"}}`},
		{"unknown surface", `{"types":{"Notebook":{"surfaces":{"missing":{}}}}}`},
		{"wrong surface type", `{"types":{"Notebook":{"surfaces":{"Files":{}}}}}`},
		{"case sensitive surface", `{"types":{"Lakehouse":{"surfaces":{"files":{}}}}}`},
		{"cross type surface", `{"types":{"Environment":{"surfaces":{"builtin":{}}}}}`},
		{"surface array", `{"types":{"Notebook":{"surfaces":[]}}}`},
		{"surface null", `{"types":{"Notebook":{"surfaces":{"builtin":null}}}}`},
		{"nested surfaces", `{"types":{"Notebook":{"surfaces":{"builtin":{"surfaces":{}}}}}}`},
		{"defaults surfaces", `{"defaults":{"surfaces":{}}}`},
		{"root surfaces", `{"surfaces":{"builtin":{}}}`},
		{"root items", `{"items":{}}`},
		{"workspace display name", `{"workspaces":{"my workspace":{}}}`},
		{"workspace unhyphenated UUID", `{"workspaces":{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":{}}}`},
		{"workspace invalid hex", `{"workspaces":{"gggggggg-gggg-gggg-gggg-gggggggggggg":{}}}`},
		{"workspace array", `{"workspaces":[]}`},
		{"workspace null", fmt.Sprintf(`{"workspaces":{%q:null}}`, workspaceID)},
		{"workspace surface lacks type", fmt.Sprintf(`{"workspaces":{%q:{"surfaces":{}}}}`, workspaceID)},
		{"item display name", fmt.Sprintf(`{"workspaces":{%q:{"items":{"My Notebook":{"type":"Notebook"}}}}}`, workspaceID)},
		{"items array", fmt.Sprintf(`{"workspaces":{%q:{"items":[]}}}`, workspaceID)},
		{"item null", itemConfig("null")},
		{"missing item type", itemConfig(`{"attr":"1m"}`)},
		{"null item type", itemConfig(`{"type":null}`)},
		{"unknown item type", itemConfig(`{"type":"Warehouse"}`)},
		{"item type array", itemConfig(`{"type":["Notebook"]}`)},
		{"item unknown key", itemConfig(`{"type":"Notebook","displayName":"name"}`)},
		{"item wrong surface", itemConfig(`{"type":"Notebook","surfaces":{"resources":{}}}`)},
		{"duplicate root", `{"defaults":{},"defaults":{}}`},
		{"duplicate layer", `{"defaults":{"attr":"1s","attr":"2s"}}`},
		{"escaped duplicate layer", `{"defaults":{"attr":"1s","\u0061ttr":"2s"}}`},
		{"duplicate type", `{"types":{"Notebook":{},"Notebook":{}}}`},
		{"duplicate surface", `{"types":{"Notebook":{"surfaces":{"builtin":{},"builtin":{}}}}}`},
		{"duplicate item type", itemConfig(`{"type":"Notebook","type":"Environment"}`)},
		{"duplicate workspace UUID", fmt.Sprintf(`{"workspaces":{%q:{},%q:{}}}`, workspaceID, strings.ToUpper(workspaceID))},
		{"duplicate item UUID", fmt.Sprintf(`{"workspaces":{%q:{"items":{%q:{"type":"Notebook"},%q:{"type":"Notebook"}}}}}`, workspaceID, itemID, strings.ToUpper(itemID))},
		{"invalid UTF-8", "{\"\xff\":{}}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy, err := cachepolicy.Parse([]byte(test.data))
			if policy != nil || !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("Parse(%s) = %v, %v; want invalid", test.data, policy, err)
			}
		})
	}
}

func TestInvalidDurations(t *testing.T) {
	for _, value := range []string{
		`0`, `3.5`, `true`, `[]`, `{}`, `""`, `"2"`, `"forever"`, `"2 minutes"`,
		`" 2m"`, `"2m "`, `"-1s"`, `"-0s"`, `"-0.1ns"`, `"999999999999999999999h"`,
	} {
		t.Run(value, func(t *testing.T) {
			policy, err := cachepolicy.Parse([]byte(`{"defaults":{"attr":` + value + `}}`))
			if policy != nil || !errors.Is(err, fs.ErrInvalid) || !strings.Contains(err.Error(), "defaults.attr") {
				t.Fatalf("invalid duration %s = %v, %v", value, policy, err)
			}
		})
	}
}

func TestCatalogCannotSplitItemFolderGeneration(t *testing.T) {
	for name, wrap := range map[string]func(string) string{
		"type": func(value string) string {
			return `{"types":{"Notebook":{"catalog":` + value + `}}}`
		},
		"type surface": func(value string) string {
			return `{"types":{"Notebook":{"surfaces":{"content":{"catalog":` + value + `}}}}}`
		},
		"workspace type": func(value string) string {
			return fmt.Sprintf(`{"workspaces":{%q:{"types":{"Notebook":{"catalog":%s}}}}}`, workspaceID, value)
		},
		"workspace type surface": func(value string) string {
			return fmt.Sprintf(`{"workspaces":{%q:{"types":{"Notebook":{"surfaces":{"builtin":{"catalog":%s}}}}}}}`, workspaceID, value)
		},
		"item": func(value string) string {
			return itemConfig(`{"type":"Notebook","catalog":` + value + `}`)
		},
		"item surface": func(value string) string {
			return itemConfig(`{"type":"Notebook","surfaces":{"content":{"catalog":` + value + `}}}`)
		},
	} {
		for _, value := range []string{`"0s"`, "null"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				policy, err := cachepolicy.Parse([]byte(wrap(value)))
				if policy != nil || !errors.Is(err, fs.ErrInvalid) || !strings.Contains(err.Error(), "catalog is allowed only") {
					t.Fatalf("catalog override accepted: %v, %v", policy, err)
				}
			})
		}
	}
}

func TestLakehouseContentOverridesAreRejected(t *testing.T) {
	for name, wrap := range map[string]func(string) string{
		"type": func(value string) string {
			return `{"types":{"Lakehouse":{"content":` + value + `}}}`
		},
		"Files": func(value string) string {
			return `{"types":{"Lakehouse":{"surfaces":{"Files":{"content":` + value + `}}}}}`
		},
		"Tables": func(value string) string {
			return `{"types":{"Lakehouse":{"surfaces":{"Tables":{"content":` + value + `}}}}}`
		},
		"workspace type": func(value string) string {
			return fmt.Sprintf(`{"workspaces":{%q:{"types":{"Lakehouse":{"content":%s}}}}}`, workspaceID, value)
		},
		"workspace type surface": func(value string) string {
			return fmt.Sprintf(`{"workspaces":{%q:{"types":{"Lakehouse":{"surfaces":{"Files":{"content":%s}}}}}}}`, workspaceID, value)
		},
		"item": func(value string) string {
			return itemConfig(`{"type":"Lakehouse","content":` + value + `}`)
		},
		"item surface": func(value string) string {
			return itemConfig(`{"type":"Lakehouse","surfaces":{"Tables":{"content":` + value + `}}}`)
		},
	} {
		for _, value := range []string{`"0s"`, `"2m"`, "null"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				policy, err := cachepolicy.Parse([]byte(wrap(value)))
				if policy != nil || !errors.Is(err, fs.ErrInvalid) || !strings.Contains(err.Error(), "streaming") {
					t.Fatalf("Lakehouse content override accepted: %v, %v", policy, err)
				}
			})
		}
	}
	policy := mustParse(t, fmt.Sprintf(`{
		"defaults":{"content":"1s"},
		"workspaces":{%q:{"content":"3s"}},
		"types":{
			"Notebook":{"surfaces":{"content":{"content":"2s"},"builtin":{"content":"4s"}}},
			"Environment":{"surfaces":{"definition":{"content":"5s"},"resources":{"content":"6s"}}}
		}
	}`, workspaceID))
	for _, selector := range []cachepolicy.Selector{
		{Type: "Notebook", Surface: "content"},
		{Type: "Notebook", Surface: "builtin"},
		{Type: "Environment", Surface: "definition"},
		{Type: "Environment", Surface: "resources"},
	} {
		if policy.Resolve(selector).Content == cachepolicy.DefaultTTL {
			t.Fatalf("buffered content override not applied for %+v", selector)
		}
		selector.Workspace = workspaceID
		if policy.Resolve(selector).Content != 3*time.Second {
			t.Fatalf("workspace buffered content override not applied for %+v", selector)
		}
	}
}

func TestConfigSizeBoundAndLoadSnapshot(t *testing.T) {
	atLimit := append([]byte("{}"), bytes.Repeat([]byte(" "), cachepolicy.MaxConfigBytes-2)...)
	if _, err := cachepolicy.Parse(atLimit); err != nil {
		t.Fatalf("exactly one MiB rejected: %v", err)
	}
	overLimit := append(atLimit, ' ')
	if policy, err := cachepolicy.Parse(overLimit); policy != nil || !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("oversized policy = %v, %v", policy, err)
	}
	file := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(file, []byte(`{"defaults":{"attr":"0s"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := cachepolicy.Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, overLimit, 0600); err != nil {
		t.Fatal(err)
	}
	if policy.Resolve(cachepolicy.Selector{}).Attr != 0 {
		t.Fatal("mount-time snapshot changed after file rewrite")
	}
	if policy, err := cachepolicy.Load(file); policy != nil || !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("oversized file = %v, %v", policy, err)
	}
	if policy, err := cachepolicy.Load(file + ".missing"); policy != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file = %v, %v", policy, err)
	}
	if policy, err := cachepolicy.Load(filepath.Dir(file)); policy != nil || err == nil {
		t.Fatalf("directory accepted = %v, %v", policy, err)
	}
}

func itemConfig(item string) string {
	return fmt.Sprintf(`{"workspaces":{%q:{"items":{%q:%s}}}}`, workspaceID, itemID, item)
}

func FuzzParse(f *testing.F) {
	for _, data := range []string{
		`{}`, `null`, `{} {}`, `{"defaults":{"attr":"0s","content":null}}`,
		`{"defaults":{"attr":"1s","attr":"2s"}}`,
		`{"types":{"Lakehouse":{"surfaces":{"Files":{"content":"0s"}}}}}`,
		itemConfig(`{"type":"Notebook","surfaces":{"builtin":{"content":"3s"}}}`),
	} {
		f.Add([]byte(data))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		policy, err := cachepolicy.Parse(data)
		if err != nil {
			if policy != nil || !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("invalid policy = %v, %v", policy, err)
			}
			return
		}
		if policy == nil {
			t.Fatal("successful parse returned nil")
		}
		for _, selector := range []cachepolicy.Selector{
			{}, {Workspace: workspaceID, Item: itemID, Type: "Notebook", Surface: "builtin"},
			{Workspace: workspaceID, Type: "Lakehouse", Surface: "Files"},
			{Workspace: workspaceID, Item: itemID, Type: "Environment", Surface: "resources"},
		} {
			values := policy.Resolve(selector)
			if values.Catalog != policy.CatalogTTL(selector.Workspace) {
				t.Fatal("selector split the workspace catalog TTL")
			}
			for _, ttl := range []time.Duration{
				values.Catalog, values.Attr, values.Directory, values.Definition,
				values.Content, values.KernelAttr, values.KernelEntry, values.KernelNegative,
			} {
				if ttl < 0 {
					t.Fatalf("negative resolved TTL %s", ttl)
				}
			}
		}
	})
}
