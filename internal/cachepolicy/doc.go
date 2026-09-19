// Package cachepolicy provides immutable mount-time cache retention policies.
//
// Every layer defaults to exactly two minutes. Duration strings use Go duration
// syntax; "0s" disables retention, while missing or null duration values inherit.
// Objects, keys, item types, and surface names are strict and case-sensitive.
// Unknown keys, duplicates (including case-equivalent UUIDs), negative durations,
// malformed JSON, trailing JSON, and inputs over one MiB are rejected.
//
// The JSON schema has three optional top-level objects:
//
//	{
//	  "defaults": {"catalog": "2m", "attr": "2m", "content": "2m"},
//	  "types": {
//	    "Notebook": {
//	      "definition": "1m",
//	      "surfaces": {"builtin": {"content": "30s"}}
//	    },
//	    "Lakehouse": {"surfaces": {"Files": {"attr": "0s"}}},
//	    "Environment": {"surfaces": {"definition": {"content": "5m"}}}
//	  },
//	  "workspaces": {
//	    "11111111-1111-1111-1111-111111111111": {
//	      "catalog": "3m",
//	      "types": {"Notebook": {"attr": "1m"}},
//	      "items": {
//	        "22222222-2222-2222-2222-222222222222": {
//	          "type": "Notebook",
//	          "surfaces": {"content": {"content": "0s"}}
//	        }
//	      }
//	    }
//	  }
//	}
//
// Layer keys are attr, directory, definition, content, kernelAttr, kernelEntry,
// kernelNegative, and catalog. Each workspace can set layer keys and contain
// types and items objects. Type and item objects can set layer keys and contain
// surfaces. Items must declare their type and are keyed by immutable UUIDs
// within their workspace UUID, never display names or local paths.
//
// Precedence, from lowest to highest, is defaults, type, type/surface, workspace,
// workspace/type, workspace/type/surface, item, item/surface. Missing values
// inherit independently for each layer. Workspace and item UUID matching is
// case-insensitive. A selector without an item type may infer it from a matching
// item rule; a conflicting type never receives that item's overrides.
//
// Types and their valid surfaces are Notebook (content, builtin), Lakehouse
// (Files, Tables), and Environment (definition, resources). Environment aliases
// use the target Environment UUID/type/resources surface, not the Notebook
// owner's identity. Root/workspace metadata has no item type or surface.
//
// Catalog is allowed only in defaults and directly within a workspace. It
// governs an entire catalog generation, so folder/item catalogs cannot be split
// across type/surface/item policies. Root discovery uses defaults.catalog.
//
// Content governs only supported bounded, buffered Notebook/Environment/MWC
// data. Lakehouse reads remain bounded streaming/range reads without a shared
// content TTL cache. Defaults/workspace content applies only to supported
// buffers; explicit Lakehouse or Files/Tables content keys are rejected,
// including null values. Uniform TTLs also affect only supported caches.
//
// Policies are read once at mount startup; they do not reload or alter token
// expiry, long-running-operation deadlines, or HTTP timeouts.
package cachepolicy
