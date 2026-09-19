package namespace

import (
	"fmt"
	"io/fs"
	"net/url"
	"strings"

	"fabric-workspace-fs/internal/fserrors"
)

type Creation struct {
	DisplayName string
	Type        string
	Local       bool
}

func ParseCreation(name string) (Creation, error) {
	if err := Component(name); err != nil {
		return Creation{}, err
	}
	if len(name) > maxNameBytes {
		return Creation{}, fserrors.ErrTooLarge
	}
	if name == ".fabric.json" || name == ".platform" {
		return Creation{}, fserrors.ErrReadOnly
	}
	kind, base := "Folder", name
	for _, candidate := range []string{"Notebook", "Lakehouse", "Environment"} {
		if prefix, found := strings.CutSuffix(name, "."+candidate); found {
			kind, base = candidate, prefix
			break
		}
	}
	if base == "" {
		return Creation{}, fmt.Errorf("display name is empty: %w", fs.ErrInvalid)
	}
	raw, err := url.PathUnescape(base)
	if err != nil || raw == "" {
		return Creation{}, fmt.Errorf("invalid display name: %w", fs.ErrInvalid)
	}
	canonical, err := CatalogName(raw, "11111111-1111-1111-1111-111111111111")
	if err != nil {
		return Creation{}, err
	}
	if canonical != base {
		return Creation{}, fmt.Errorf("display name is not canonical or is too long: %w", fs.ErrInvalid)
	}
	return Creation{DisplayName: raw, Type: kind}, nil
}

func (c *Catalog) Reserved(scope, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exists := c.owners[scope+"\x00"+name]
	return exists
}
