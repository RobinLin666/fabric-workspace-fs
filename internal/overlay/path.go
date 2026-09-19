package overlay

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unicode/utf8"

	"fabric-workspace-fs/internal/fserrors"
)

const maxDepth = 32

type resolvedLocation struct {
	workspace string
	scope     string
	relative  string
	path      string
}

func resolve(loc Location, allowContainer bool) (resolvedLocation, error) {
	if !validUUID(loc.Workspace) || (loc.Parent != "root" && !validUUID(loc.Parent)) {
		return resolvedLocation{}, invalid("workspace and parent must be immutable UUIDs (or parent root)")
	}
	if loc.Relative == "" && !allowContainer {
		return resolvedLocation{}, invalid("the scoped container is read-only")
	}
	if loc.Relative != "" {
		parts := strings.Split(loc.Relative, "/")
		if len(parts) > maxDepth {
			return resolvedLocation{}, invalid("overlay path is too deep")
		}
		for i, part := range parts {
			if err := validateComponent(part, i == 0); err != nil {
				return resolvedLocation{}, err
			}
		}
	}
	workspace := strings.ToLower(loc.Workspace)
	scope := workspace + "/" + strings.ToLower(loc.Parent)
	p := scope
	if loc.Relative != "" {
		p += "/" + loc.Relative
	}
	return resolvedLocation{workspace: workspace, scope: scope, relative: loc.Relative, path: p}, nil
}

func validUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func validateComponent(name string, dotRoot bool) error {
	if name == "" || name == "." || name == ".." || len(name) > 255 || !utf8.ValidString(name) {
		return invalid("invalid overlay path component")
	}
	if dotRoot && name[0] != '.' {
		return invalid("the first overlay component must be a dot-directory")
	}
	canonical := strings.ToUpper(name)
	if canonical == ".FABRIC.JSON" || canonical == ".PLATFORM" {
		return invalid("reserved metadata name")
	}
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return invalid("trailing dots and spaces are not portable")
	}
	for _, c := range name {
		if c < 32 || c == 127 || strings.ContainsRune("/\\:<>\"|?*", c) {
			return invalid("invalid character in overlay path")
		}
	}
	base, _, _ := strings.Cut(canonical, ".")
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return invalid("reserved device name")
	}
	if strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT") {
		suffix := base[3:]
		if len(suffix) == 1 && suffix[0] >= '1' && suffix[0] <= '9' ||
			suffix == "¹" || suffix == "²" || suffix == "³" {
			return invalid("reserved device name")
		}
	}
	return nil
}

func invalid(reason string) error {
	return fmt.Errorf("overlay: %s: %w", reason, fs.ErrInvalid)
}

func unsafeEntry(path, reason string) error {
	return fmt.Errorf("overlay: unsafe entry %q (%s): %w", path, reason, fs.ErrPermission)
}

func pathKey(path string) string {
	if runtime.GOOS == "windows" {
		return strings.ToUpper(path)
	}
	return path
}

func within(path, parent string) bool {
	path, parent = pathKey(path), pathKey(parent)
	return path == parent || strings.HasPrefix(path, parent+"/")
}

func diskPath(path string) string { return filepath.FromSlash(path) }

// Walking from a volume root avoids following an unchecked symlink in the
// configured storage path. Each opened directory must still be the one checked.
func openPrivateRoot(name string) (_ *os.Root, err error) {
	if !filepath.IsAbs(name) || strings.ContainsRune(name, 0) {
		return nil, invalid("storage root must be an absolute path")
	}
	volume := filepath.VolumeName(name)
	rest := strings.TrimPrefix(name, volume)
	if runtime.GOOS == "windows" {
		rest = strings.ReplaceAll(rest, "/", "\\")
	}
	for _, part := range strings.Split(rest, string(os.PathSeparator)) {
		if part == "." || part == ".." {
			return nil, invalid("storage root cannot contain traversal components")
		}
	}
	name = filepath.Clean(name)
	anchor := volume + string(os.PathSeparator)
	relative := strings.TrimPrefix(name, anchor)
	if relative == "" || relative == "." || relative == name {
		return nil, invalid("a filesystem or volume root cannot be overlay storage")
	}
	root, err := os.OpenRoot(anchor)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, root.Close())
		}
	}()
	for _, part := range strings.Split(relative, string(os.PathSeparator)) {
		before, statErr := root.Lstat(part)
		if errors.Is(statErr, fs.ErrNotExist) {
			if mkdirErr := root.Mkdir(part, 0o700); mkdirErr != nil {
				return nil, mkdirErr
			}
			before, statErr = root.Lstat(part)
		}
		if statErr != nil {
			return nil, statErr
		}
		if err := checkType(part, before); err != nil {
			return nil, err
		}
		if !before.IsDir() {
			return nil, fmt.Errorf("overlay root %q: %w", name, fserrors.ErrNotDir)
		}
		child, openErr := root.OpenRoot(part)
		if openErr != nil {
			return nil, openErr
		}
		opened, statErr := child.Stat(".")
		after, afterErr := root.Lstat(part)
		if statErr != nil || afterErr != nil {
			return nil, errors.Join(statErr, afterErr, child.Close())
		}
		if err := checkType(part, after); err != nil {
			return nil, errors.Join(err, child.Close())
		}
		if !os.SameFile(before, opened) || !os.SameFile(after, opened) {
			return nil, errors.Join(unsafeEntry(part, "directory changed while opening"), child.Close())
		}
		if closeErr := root.Close(); closeErr != nil {
			return nil, errors.Join(closeErr, child.Close())
		}
		root = child
	}
	info, err := root.Stat(".")
	if err != nil {
		return nil, err
	}
	if err := checkPrivate(name, info); err != nil {
		return nil, err
	}
	return root, nil
}

func checkType(path string, info fs.FileInfo) error {
	if isReparsePoint(info) || info.Mode()&fs.ModeSymlink != 0 {
		return unsafeEntry(path, "symlink or reparse point")
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return unsafeEntry(path, "not a regular file or directory")
	}
	return nil
}
