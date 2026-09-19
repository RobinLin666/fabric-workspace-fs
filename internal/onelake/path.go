package onelake

import (
	"fmt"
	"io/fs"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"fabric-workspace-fs/internal/fserrors"
)

// Path addresses an item by its immutable IDs. Relative is an unescaped,
// slash-separated path starting with Files or Tables.
type Path struct {
	Workspace string
	Item      string
	Relative  string
}

// ValidatePath rejects ambiguous paths rather than cleaning them. Only objects
// strictly below Files are writable; both Files and Tables are readable.
func ValidatePath(p Path, write bool) error {
	if !validID(p.Workspace) || !validID(p.Item) {
		return fmt.Errorf("OneLake requires canonical workspace and item UUIDs: %w", fs.ErrInvalid)
	}
	if p.Relative == "" {
		if write {
			return fserrors.ErrReadOnly
		}
		return fmt.Errorf("missing OneLake protected root: %w", fs.ErrInvalid)
	}
	if !safeText(p.Relative) || strings.ContainsRune(p.Relative, '\\') {
		return fmt.Errorf("invalid OneLake path characters: %w", fs.ErrInvalid)
	}
	parts := strings.Split(p.Relative, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid OneLake path component: %w", fs.ErrInvalid)
		}
	}
	if write && (parts[0] != "Files" || len(parts) == 1) {
		return fserrors.ErrReadOnly
	}
	if parts[0] != "Files" && parts[0] != "Tables" {
		return fmt.Errorf("invalid OneLake protected root: %w", fs.ErrInvalid)
	}
	return nil
}

func validID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if s[i] != '-' {
				return false
			}
		} else if !('0' <= s[i] && s[i] <= '9') &&
			!('a' <= s[i] && s[i] <= 'f') &&
			!('A' <= s[i] && s[i] <= 'F') {
			return false
		}
	}
	return true
}

func safeText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) || r == utf8.RuneError ||
			(r >= 0xfdd0 && r <= 0xfdef) || r&0xffff >= 0xfffe {
			return false
		}
	}
	return true
}

func escapedPath(p Path) string {
	parts := strings.Split(p.Workspace+"/"+p.Item+"/"+p.Relative, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return "/" + strings.Join(parts, "/")
}

func sameItem(a, b Path) bool {
	return strings.EqualFold(a.Workspace, b.Workspace) && strings.EqualFold(a.Item, b.Item)
}

// OneLake sometimes returns unquoted storage ETags. Normalize these without
// accepting a wildcard, a weak validator, or multiple conditional values.
func entityTag(s string, required bool) (string, error) {
	if s == "" {
		if required {
			return "", fmt.Errorf("a strong ETag is required: %w", fs.ErrInvalid)
		}
		return "", nil
	}
	if s == "*" || strings.HasPrefix(s, "W/") {
		return "", fmt.Errorf("a single strong ETag is required: %w", fs.ErrInvalid)
	}
	quoted := strings.HasPrefix(s, `"`)
	if quoted {
		if len(s) < 3 || !strings.HasSuffix(s, `"`) {
			return "", fmt.Errorf("malformed ETag: %w", fs.ErrInvalid)
		}
		s = s[1 : len(s)-1]
	}
	for _, b := range []byte(s) {
		if b < 0x21 || b > 0x7e || b == '"' || (!quoted && (b == ',' || b == '*')) {
			return "", fmt.Errorf("malformed ETag: %w", fs.ErrInvalid)
		}
	}
	return `"` + s + `"`, nil
}
