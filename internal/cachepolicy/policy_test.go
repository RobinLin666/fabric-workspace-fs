package cachepolicy_test

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
)

const (
	workspaceID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	otherWSID   = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	itemID      = "cccccccc-cccc-cccc-cccc-cccccccccccc"
)

func allValues(ttl time.Duration) cachepolicy.Values {
	return cachepolicy.Values{
		Catalog: ttl, Attr: ttl, Directory: ttl, Definition: ttl, Content: ttl,
		KernelAttr: ttl, KernelEntry: ttl, KernelNegative: ttl,
	}
}

func mustParse(t *testing.T, data string) *cachepolicy.Policy {
	t.Helper()
	policy, err := cachepolicy.Parse([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestDefaultsAreExactlyTwoMinutes(t *testing.T) {
	if cachepolicy.DefaultTTL != 2*time.Minute {
		t.Fatalf("DefaultTTL = %s", cachepolicy.DefaultTTL)
	}
	for name, policy := range map[string]*cachepolicy.Policy{
		"default": cachepolicy.Default(),
		"nil":     nil,
		"zero":    {},
		"empty":   mustParse(t, `{}`),
		"null durations": mustParse(t, `{"defaults":{
			"catalog":null,"attr":null,"directory":null,"definition":null,"content":null,
			"kernelAttr":null,"kernelEntry":null,"kernelNegative":null
		}}`),
	} {
		t.Run(name, func(t *testing.T) {
			for _, selector := range []cachepolicy.Selector{
				{}, {Workspace: workspaceID},
				{Workspace: workspaceID, Item: itemID, Type: "Notebook", Surface: "content"},
				{Workspace: workspaceID, Item: itemID, Type: "Lakehouse", Surface: "Files"},
				{Workspace: workspaceID, Item: itemID, Type: "Environment", Surface: "resources"},
			} {
				if got := policy.Resolve(selector); got != allValues(2*time.Minute) {
					t.Fatalf("Resolve(%+v) = %+v", selector, got)
				}
			}
			for _, workspace := range []string{"", workspaceID} {
				if got := policy.CatalogTTL(workspace); got != 2*time.Minute {
					t.Fatalf("CatalogTTL(%q) = %s", workspace, got)
				}
			}
		})
	}
}

func TestUniformIncludesZeroAndKernelLayers(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Nanosecond, 17 * time.Minute, cachepolicy.DefaultTTL} {
		t.Run(ttl.String(), func(t *testing.T) {
			policy, err := cachepolicy.Uniform(ttl)
			if err != nil {
				t.Fatal(err)
			}
			for _, selector := range []cachepolicy.Selector{
				{}, {Workspace: workspaceID},
				{Workspace: workspaceID, Item: itemID, Type: "Notebook", Surface: "builtin"},
				{Workspace: workspaceID, Item: itemID, Type: "Lakehouse", Surface: "Tables"},
			} {
				if got := policy.Resolve(selector); got != allValues(ttl) {
					t.Fatalf("Resolve(%+v) = %+v, want all %s", selector, got, ttl)
				}
			}
			if policy.CatalogTTL("") != ttl || policy.CatalogTTL(workspaceID) != ttl {
				t.Fatal("uniform catalog did not match TTL")
			}
		})
	}
	if policy, err := cachepolicy.Uniform(-time.Nanosecond); policy != nil || !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("negative uniform TTL = %v, %v", policy, err)
	}
}

func TestResolvePrecedence(t *testing.T) {
	policy := mustParse(t, fmt.Sprintf(`{
		"defaults":{"catalog":"11s","attr":"1s","directory":"19s"},
		"types":{"Notebook":{
			"attr":"2s","surfaces":{"content":{"attr":"3s"},"builtin":{"attr":"3s"}}
		}},
		"workspaces":{
			%q:{
				"catalog":"0s","attr":"4s",
				"types":{"Notebook":{
					"attr":"5s","surfaces":{"content":{"attr":"6s"},"builtin":{"attr":"6s"}}
				}},
				"items":{%q:{"type":"Notebook","attr":"7s","surfaces":{"content":{"attr":"8s"}}}}
			},
			%q:{"attr":"4s"}
		}
	}`, workspaceID, itemID, otherWSID))
	for _, test := range []struct {
		name     string
		selector cachepolicy.Selector
		attr     time.Duration
	}{
		{"defaults", cachepolicy.Selector{}, time.Second},
		{"type", cachepolicy.Selector{Type: "Notebook"}, 2 * time.Second},
		{"type surface", cachepolicy.Selector{Type: "Notebook", Surface: "content"}, 3 * time.Second},
		{"workspace", cachepolicy.Selector{Workspace: workspaceID}, 4 * time.Second},
		{"workspace beats type surface", cachepolicy.Selector{Workspace: otherWSID, Type: "Notebook", Surface: "content"}, 4 * time.Second},
		{"workspace type", cachepolicy.Selector{Workspace: workspaceID, Type: "Notebook"}, 5 * time.Second},
		{"workspace type surface", cachepolicy.Selector{Workspace: workspaceID, Type: "Notebook", Surface: "content"}, 6 * time.Second},
		{"item", cachepolicy.Selector{Workspace: workspaceID, Item: itemID, Type: "Notebook"}, 7 * time.Second},
		{"item beats workspace type surface", cachepolicy.Selector{Workspace: workspaceID, Item: itemID, Type: "Notebook", Surface: "builtin"}, 7 * time.Second},
		{"item surface", cachepolicy.Selector{Workspace: workspaceID, Item: itemID, Type: "Notebook", Surface: "content"}, 8 * time.Second},
		{"item infers type", cachepolicy.Selector{Workspace: workspaceID, Item: itemID, Surface: "content"}, 8 * time.Second},
		{"item cannot cross workspace", cachepolicy.Selector{Workspace: otherWSID, Item: itemID, Type: "Notebook", Surface: "content"}, 4 * time.Second},
		{"item type mismatch", cachepolicy.Selector{Workspace: workspaceID, Item: itemID, Type: "Environment", Surface: "resources"}, 4 * time.Second},
		{"case insensitive UUIDs", cachepolicy.Selector{Workspace: strings.ToUpper(workspaceID), Item: strings.ToUpper(itemID), Type: "Notebook", Surface: "content"}, 8 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := allValues(cachepolicy.DefaultTTL)
			want.Attr, want.Directory = test.attr, 19*time.Second
			want.Catalog = 11 * time.Second
			if strings.EqualFold(test.selector.Workspace, workspaceID) {
				want.Catalog = 0
			}
			if got := policy.Resolve(test.selector); got != want {
				t.Fatalf("Resolve(%+v) = %+v, want %+v", test.selector, got, want)
			}
			if got := policy.CatalogTTL(test.selector.Workspace); got != want.Catalog {
				t.Fatalf("catalog = %s, want %s", got, want.Catalog)
			}
		})
	}
}

func TestIndependentLayersAndNullInheritance(t *testing.T) {
	policy := mustParse(t, fmt.Sprintf(`{
		"defaults":{
			"catalog":"1s","attr":"2s","directory":"3s","definition":"4s",
			"content":"5s","kernelAttr":"6s","kernelEntry":"7s","kernelNegative":"8s"
		},
		"types":{"Notebook":{"attr":"0s","directory":null,"definition":"10s"}},
		"workspaces":{%q:{
			"catalog":null,"attr":null,"content":"0s",
			"types":{"Notebook":{"kernelAttr":"0s","surfaces":{"content":{"kernelEntry":"0s"}}}},
			"items":{%q:{"type":"Notebook","definition":null,"surfaces":{"content":{
				"attr":null,"content":null,"kernelNegative":"0s"
			}}}}
		}}
	}`, workspaceID, itemID))
	want := cachepolicy.Values{
		Catalog: time.Second, Attr: 0, Directory: 3 * time.Second,
		Definition: 10 * time.Second, Content: 0,
		KernelAttr: 0, KernelEntry: 0, KernelNegative: 0,
	}
	if got := policy.Resolve(cachepolicy.Selector{Workspace: workspaceID, Item: itemID, Type: "Notebook", Surface: "content"}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestEveryLayerAcceptsExplicitZero(t *testing.T) {
	for _, field := range []struct {
		name string
		get  func(cachepolicy.Values) time.Duration
	}{
		{"catalog", func(v cachepolicy.Values) time.Duration { return v.Catalog }},
		{"attr", func(v cachepolicy.Values) time.Duration { return v.Attr }},
		{"directory", func(v cachepolicy.Values) time.Duration { return v.Directory }},
		{"definition", func(v cachepolicy.Values) time.Duration { return v.Definition }},
		{"content", func(v cachepolicy.Values) time.Duration { return v.Content }},
		{"kernelAttr", func(v cachepolicy.Values) time.Duration { return v.KernelAttr }},
		{"kernelEntry", func(v cachepolicy.Values) time.Duration { return v.KernelEntry }},
		{"kernelNegative", func(v cachepolicy.Values) time.Duration { return v.KernelNegative }},
	} {
		t.Run(field.name, func(t *testing.T) {
			policy := mustParse(t, fmt.Sprintf(`{"defaults":{%q:"0s"}}`, field.name))
			if got := field.get(policy.Resolve(cachepolicy.Selector{})); got != 0 {
				t.Fatalf("zero %s became %s", field.name, got)
			}
		})
	}
}

func TestPolicyOwnsParsedValuesAndSupportsConcurrentResolution(t *testing.T) {
	data := []byte(`{"types":{"Notebook":{"surfaces":{"builtin":{"content":"9s"}}}}}`)
	policy, err := cachepolicy.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	clear(data)
	selector := cachepolicy.Selector{Type: "Notebook", Surface: "builtin"}
	got := policy.Resolve(selector)
	got.Content = 0
	if policy.Resolve(selector).Content != 9*time.Second {
		t.Fatal("input or returned value mutation changed policy")
	}
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for range 100 {
				if got := policy.Resolve(selector).Content; got != 9*time.Second {
					t.Errorf("concurrent content TTL = %s", got)
				}
			}
		})
	}
	workers.Wait()
}
