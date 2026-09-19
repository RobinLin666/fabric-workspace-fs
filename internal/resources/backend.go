// Package resources defines item-resource storage independently of MWC/public
// endpoints and credentials. Actual implementations supply discovered roots.
package resources

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
)

type Target struct {
	WorkspaceID string
	ItemID      string
	Kind        string
}

type Path struct {
	Target   Target
	Relative string
}

// Root describes an actual resource surface or an actual Environment binding.
// An env alias always carries the target Environment identity, not its Notebook.
type Root struct {
	Name   string
	Target Target
}

type Info struct {
	Path     string
	IsDir    bool
	Size     int64
	Modified time.Time
	// ObservedAt and ValidUntil bound freshness to the original data sources.
	// A zero ValidUntil means the backend did not provide a cache deadline.
	ObservedAt time.Time
	ValidUntil time.Time
	// Version is a content fingerprint returned by content/successful mutation
	// operations. Stat/List need not populate it or fetch content to do so.
	Version string
	// MetadataVersion is an opaque listing/header validator, not a content SHA
	// and not a promise of server-side conditional write support.
	MetadataVersion string
}

type WriteGuarantee string

const (
	Conditional      WriteGuarantee = "conditional"
	CompareThenWrite WriteGuarantee = "compare-then-write"
)

type Backend interface {
	Roots(context.Context, Target) ([]Root, error)
	Stat(context.Context, Path) (Info, error)
	List(context.Context, Path) ([]Info, error)
	Read(context.Context, Path, int64, []byte, string) (int, error)
	Snapshot(context.Context, Path, string) (*Snapshot, error)
	Put(context.Context, Path, io.ReaderAt, int64, string) (Info, error)
	Mkdir(context.Context, Path) error
	Remove(context.Context, Path, bool, string) error
	Rename(context.Context, Path, Path, string, string, bool) (Info, error)
	WriteGuarantee() WriteGuarantee
}

func ValidateTarget(target Target) error {
	if fabric.ValidateID(target.WorkspaceID) != nil || fabric.ValidateID(target.ItemID) != nil {
		return fmt.Errorf("resource target must use workspace/item UUIDs: %w", fs.ErrInvalid)
	}
	if target.Kind != "Notebook" && target.Kind != "Environment" {
		return fserrors.ErrUnsupported
	}
	return nil
}

func ValidatePath(path Path, write bool) error {
	if err := ValidateTarget(path.Target); err != nil {
		return err
	}
	if write && path.Target.Kind != "Notebook" {
		return fserrors.ErrReadOnly
	}
	if path.Relative == "" {
		if write {
			return fserrors.ErrReadOnly
		}
		return nil
	}
	for _, part := range strings.Split(path.Relative, "/") {
		if err := namespace.Component(part); err != nil {
			return err
		}
	}
	return nil
}

func ValidateRoots(owner Target, roots []Root) error {
	if err := ValidateTarget(owner); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, root := range roots {
		if err := ValidateTarget(root.Target); err != nil {
			return err
		}
		if seen[root.Name] {
			return fmt.Errorf("duplicate resource root: %w", fs.ErrInvalid)
		}
		seen[root.Name] = true
		switch root.Name {
		case "builtin":
			if owner.Kind != "Notebook" || root.Target != owner {
				return fmt.Errorf("builtin resource identity mismatch: %w", fs.ErrInvalid)
			}
		case "env":
			if owner.Kind != "Notebook" || root.Target.Kind != "Environment" {
				return fmt.Errorf("env alias must resolve from a Notebook to an Environment: %w", fs.ErrInvalid)
			}
		case "resources":
			if owner.Kind != "Environment" || root.Target != owner {
				return fmt.Errorf("Environment resources identity mismatch: %w", fs.ErrInvalid)
			}
		default:
			return fmt.Errorf("unsupported resource root: %w", fs.ErrInvalid)
		}
	}
	return nil
}
