package namespace

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"

	"fabric-workspace-fs/internal/fserrors"
)

type Label struct {
	ID, DisplayName string
	Suffix          string
}

const AgentRootName = ".agents"

// Catalog keeps display-name aliases bound to IDs for this mount's lifetime.
// Deleted aliases remain reserved so a later duplicate cannot inherit a path
// that previously addressed another item.
type Catalog struct {
	mu          sync.Mutex
	assignments map[string]string
	owners      map[string]string
	nextSuffix  map[string]int
}

// ReserveName protects an injected root without hiding a real remote identity.
// A conflicting display name receives the usual stable ordinal alias.
func (c *Catalog) ReserveName(scope, name string) error {
	if err := Component(name); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.assignments == nil {
		c.assignments, c.owners = make(map[string]string), make(map[string]string)
		c.nextSuffix = make(map[string]int)
	}
	key := scope + "\x00" + name
	const reserved = "\x00injected-root"
	if owner, exists := c.owners[key]; exists && owner != reserved {
		return fmt.Errorf("cannot reserve an assigned namespace name: %w", fs.ErrExist)
	}
	c.owners[key] = reserved
	return nil
}

func (c *Catalog) Names(scope string, labels []Label) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.assignments == nil {
		c.assignments, c.owners = make(map[string]string), make(map[string]string)
		c.nextSuffix = make(map[string]int)
	}
	bases, keys, names := make([]string, len(labels)), make([]string, len(labels)), make([]string, len(labels))
	order := make([]int, len(labels))
	seen := make(map[string]bool, len(labels))
	newBindings := 0
	for i, label := range labels {
		base, err := CatalogName(label.DisplayName, label.ID)
		if err != nil {
			return nil, err
		}
		if label.Suffix != "" && label.Suffix != ".Notebook" && label.Suffix != ".Lakehouse" && label.Suffix != ".Environment" {
			return nil, fmt.Errorf("invalid catalog type suffix: %w", fs.ErrInvalid)
		}
		id := strings.ToLower(label.ID)
		if seen[id] {
			return nil, fmt.Errorf("duplicate catalog identity: %w", fs.ErrInvalid)
		}
		seen[id] = true
		bases[i], keys[i], order[i] = base, scope+"\x00"+id+"\x00"+base+label.Suffix, i
		names[i] = c.assignments[keys[i]]
		if names[i] == "" {
			newBindings++
		}
	}
	if newBindings > 100000-len(c.assignments) {
		return nil, fmt.Errorf("catalog alias limit exceeded: %w", fserrors.ErrTooLarge)
	}
	sort.Slice(order, func(i, j int) bool {
		return strings.ToLower(labels[order[i]].ID) < strings.ToLower(labels[order[j]].ID)
	})
	assign := func(i int, name string) bool {
		ownerKey := scope + "\x00" + name
		if _, occupied := c.owners[ownerKey]; occupied {
			return false
		}
		names[i], c.assignments[keys[i]], c.owners[ownerKey] = name, name, keys[i]
		return true
	}
	// Claim genuine display names before allocating duplicate suffixes, so an
	// item actually called "Data (2)" keeps that name when initially discovered.
	for _, i := range order {
		if names[i] == "" {
			assign(i, bases[i]+labels[i].Suffix)
		}
	}
	for _, i := range order {
		if names[i] != "" {
			continue
		}
		counterKey := scope + "\x00" + bases[i] + labels[i].Suffix
		for suffix := max(2, c.nextSuffix[counterKey]); ; suffix++ {
			if assign(i, fmt.Sprintf("%s (%d)%s", bases[i], suffix, labels[i].Suffix)) {
				c.nextSuffix[counterKey] = suffix + 1
				break
			}
		}
	}
	return names, nil
}
