package cli

import (
	"flag"
	"fmt"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
)

const cacheConfigUsage = `
Cache configuration:
  All filesystem cache layers default to 2m. --cache-config reads strict JSON
  once at mount startup, with no live reload. It cannot be combined with an
  explicitly supplied --cache-ttl, even --cache-ttl=2m.
  Top-level objects: defaults, types, workspaces (workspace UUID keys).
  Layer keys: attr, directory, definition, content, kernelAttr, kernelEntry,
  kernelNegative, catalog. Values are Go duration strings; "0s" disables
  retention, and missing/null durations inherit. Unknown/duplicate keys fail.
  types has Notebook, Lakehouse, Environment keys; each holds layers and surfaces.
  Surfaces: Notebook content/builtin; Lakehouse Files/Tables;
  Environment definition/resources. Names are case-sensitive.
  Each workspace holds layers, types, and items (item UUID keys). Each item
  requires type and can hold layers and surfaces; use IDs, not display names.
  Precedence: defaults -> type -> type/surface -> workspace -> workspace/type
  -> workspace/type/surface -> item -> item/surface.
  catalog is allowed only in defaults or directly within a workspace, so an
  entire folder/item catalog generation shares one TTL. Root uses defaults.
  content applies only to supported buffered Notebook/Environment/MWC data.
  Lakehouse uses bounded streaming/ranges, not a shared content TTL cache;
  explicit Lakehouse or Files/Tables content overrides are rejected.
  No token expiry, LRO deadline, or HTTP timeout is changed.
  Example: {"defaults":{"catalog":"2m"},"types":{"Notebook":{"surfaces":{"builtin":{"content":"30s"}}}}}
`

func mountCachePolicy(flags *flag.FlagSet, ttl time.Duration, path string) (*cachepolicy.Policy, error) {
	var ttlSet, configSet bool
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "cache-ttl":
			ttlSet = true
		case "cache-config":
			configSet = true
		}
	})
	if ttlSet && configSet {
		return nil, fmt.Errorf("--cache-ttl and --cache-config are mutually exclusive")
	}
	if configSet {
		if path == "" {
			return nil, fmt.Errorf("--cache-config requires a JSON file path")
		}
		policy, err := cachepolicy.Load(path)
		if err != nil {
			return nil, fmt.Errorf("--cache-config: %w", err)
		}
		return policy, nil
	}
	if ttlSet {
		policy, err := cachepolicy.Uniform(ttl)
		if err != nil {
			return nil, fmt.Errorf("--cache-ttl: %w", err)
		}
		return policy, nil
	}
	return cachepolicy.Default(), nil
}
