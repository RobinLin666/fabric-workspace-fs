package cachepolicy

import (
	"fmt"
	"io/fs"
	"strings"
	"time"
)

// DefaultTTL is the baseline retention lifetime for every filesystem layer.
const DefaultTTL = 2 * time.Minute

// Values contains retention lifetimes, not operation, credential, or HTTP
// timeouts. A zero lifetime disables retention for that layer.
type Values struct {
	Catalog        time.Duration
	Attr           time.Duration
	Directory      time.Duration
	Definition     time.Duration
	Content        time.Duration
	KernelAttr     time.Duration
	KernelEntry    time.Duration
	KernelNegative time.Duration
}

// Selector uses immutable workspace/item UUIDs and case-sensitive Fabric type
// and surface names. An empty workspace selects the mount's root scope.
type Selector struct {
	Workspace string
	Item      string
	Type      string
	Surface   string
}

type overrides map[string]time.Duration

type typeOverrides struct {
	values   overrides
	surfaces map[string]overrides
}

type itemOverrides struct {
	kind string
	typeOverrides
}

type workspaceOverrides struct {
	values overrides
	types  map[string]typeOverrides
	items  map[string]itemOverrides
}

// Policy is immutable after construction and safe for concurrent resolution.
// Its zero value, including a nil *Policy, uses DefaultTTL for every layer.
type Policy struct {
	defaults   overrides
	types      map[string]typeOverrides
	workspaces map[string]workspaceOverrides
}

// Default returns a policy using DefaultTTL for every layer and scope.
func Default() *Policy { return &Policy{} }

// Uniform sets every supported cache layer to ttl. It does not add a content
// cache to streaming Lakehouse reads.
func Uniform(ttl time.Duration) (*Policy, error) {
	if ttl < 0 {
		return nil, fmt.Errorf("cache TTL must not be negative: %w", fs.ErrInvalid)
	}
	return &Policy{defaults: overrides{
		"catalog": ttl, "attr": ttl, "directory": ttl, "definition": ttl,
		"content": ttl, "kernelAttr": ttl, "kernelEntry": ttl, "kernelNegative": ttl,
	}}, nil
}

// Resolve overlays defaults, type, type/surface, workspace, workspace/type,
// workspace/type/surface, item, and item/surface in that order. Catalog is only
// configurable in defaults and workspace scopes, independent of item selection.
//
// Item overrides are workspace-scoped. Their required type supplies an omitted
// Selector.Type; a conflicting Selector.Type does not receive item overrides.
func (p *Policy) Resolve(s Selector) Values {
	values := Values{
		Catalog: DefaultTTL, Attr: DefaultTTL, Directory: DefaultTTL,
		Definition: DefaultTTL, Content: DefaultTTL, KernelAttr: DefaultTTL,
		KernelEntry: DefaultTTL, KernelNegative: DefaultTTL,
	}
	if p == nil {
		return values
	}
	workspace := p.workspaces[strings.ToLower(s.Workspace)]
	item, matched := workspace.items[strings.ToLower(s.Item)]
	matched = matched && (s.Type == "" || s.Type == item.kind)
	if matched && s.Type == "" {
		s.Type = item.kind
	}
	values.apply(p.defaults)
	values.apply(p.types[s.Type].values)
	values.apply(p.types[s.Type].surfaces[s.Surface])
	values.apply(workspace.values)
	values.apply(workspace.types[s.Type].values)
	values.apply(workspace.types[s.Type].surfaces[s.Surface])
	if matched {
		values.apply(item.values)
		values.apply(item.surfaces[s.Surface])
	}
	return values
}

// CatalogTTL returns the lifetime of an entire catalog generation. Root
// discovery uses an empty workspace; item/folder catalogs use their workspace
// UUID. Types, surfaces, and items cannot split a catalog generation's lifetime.
func (p *Policy) CatalogTTL(workspace string) time.Duration {
	ttl := DefaultTTL
	if p == nil {
		return ttl
	}
	if value, ok := p.defaults["catalog"]; ok {
		ttl = value
	}
	if value, ok := p.workspaces[strings.ToLower(workspace)].values["catalog"]; ok {
		ttl = value
	}
	return ttl
}

func (v *Values) apply(values overrides) {
	for key, value := range values {
		switch key {
		case "catalog":
			v.Catalog = value
		case "attr":
			v.Attr = value
		case "directory":
			v.Directory = value
		case "definition":
			v.Definition = value
		case "content":
			v.Content = value
		case "kernelAttr":
			v.KernelAttr = value
		case "kernelEntry":
			v.KernelEntry = value
		case "kernelNegative":
			v.KernelNegative = value
		}
	}
}
